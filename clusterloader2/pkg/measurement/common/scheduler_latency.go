/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/master/ports"
	"k8s.io/kubernetes/pkg/util/system"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	schedulerLatencyMetricName = "SchedulingMetrics"

	singleRestCallTimeout = 5 * time.Minute

	PredicateEvaluationDuration  = "scheduling_algorithm_predicate_evaluation_seconds_bucket"
	PriorityEvaluationDuration   = "scheduling_algorithm_priority_evaluation_seconds_bucket"
	PreemptionEvaluationDuration = "scheduling_algorithm_preemption_evaluation_seconds_bucket"
	BindingDuration              = "binding_duration_seconds_bucket"
	E2eSchedulingDuration        = "e2e_scheduling_duration_seconds_bucket"
)

func init() {
	if err := measurement.Register(schedulerLatencyMetricName, createSchedulerLatencyMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", schedulerLatencyMetricName, err)
	}
}

func createSchedulerLatencyMeasurement() measurement.Measurement {
	return &schedulerLatencyMeasurement{}
}

type schedulerLatencyMeasurement struct{}

// Execute supports two actions:
// - reset - Resets latency data on api scheduler side.
// - gather - Gathers and prints current scheduler latency data.
func (s *schedulerLatencyMeasurement) Execute(config *measurement.MeasurementConfig) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}
	provider, err := util.GetStringOrDefault(config.Params, "provider", config.ClusterFramework.GetClusterConfig().Provider)
	if err != nil {
		return nil, err
	}
	masterIP, err := util.GetStringOrDefault(config.Params, "masterIP", config.ClusterFramework.GetClusterConfig().GetMasterIp())
	if err != nil {
		return nil, err
	}
	masterName, err := util.GetStringOrDefault(config.Params, "masterName", config.ClusterFramework.GetClusterConfig().MasterName)
	if err != nil {
		return nil, err
	}

	switch action {
	case "reset":
		klog.Infof("%s: resetting latency metrics in scheduler...", s)
		return nil, s.resetSchedulerMetrics(config.ClusterFramework.GetClientSets().GetClient(), masterIP, provider, masterName)
	case "gather":
		klog.Infof("%s: gathering latency metrics in scheduler...", s)
		return s.getSchedulingLatency(config.ClusterFramework.GetClientSets().GetClient(), masterIP, provider, masterName)
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

// Dispose cleans up after the measurement.
func (*schedulerLatencyMeasurement) Dispose() {}

// String returns string representation of this measurement.
func (*schedulerLatencyMeasurement) String() string {
	return schedulerLatencyMetricName
}

func (s *schedulerLatencyMeasurement) resetSchedulerMetrics(c clientset.Interface, host, provider, masterName string) error {
	_, err := s.sendRequestToScheduler(c, "DELETE", host, provider, masterName)
	if err != nil {
		return err
	}
	return nil
}

// Retrieves scheduler latency metrics.
func (s *schedulerLatencyMeasurement) getSchedulingLatency(c clientset.Interface, host, provider, masterName string) ([]measurement.Summary, error) {
	result := newSchedulingMetrics()
	data, err := s.sendRequestToScheduler(c, "GET", host, provider, masterName)
	if err != nil {
		return nil, err
	}

	samples, err := measurementutil.ExtractMetricSamples(data)
	if err != nil {
		return nil, err
	}

	for _, sample := range samples {
		var metric *measurementutil.HistogramVec
		switch sample.Metric[model.MetricNameLabel] {
		case PredicateEvaluationDuration:
			metric = &result.PredicateEvaluationDuration
		case PriorityEvaluationDuration:
			metric = &result.PriorityEvaluationDuration
		case PreemptionEvaluationDuration:
			metric = &result.PreemptionEvaluationDuration
		case BindingDuration:
			metric = &result.BindingDuration
		case E2eSchedulingDuration:
			metric = &result.E2eSchedulingDuration
		}

		if metric == nil {
			continue
		}
		measurementutil.ConvertSampleToBucket(sample, metric)
	}

	content, err := util.PrettyPrintJSON(result)
	if err != nil {
		return nil, err
	}
	summary := measurement.CreateSummary(schedulerLatencyMetricName, "json", content)
	return []measurement.Summary{summary}, nil
}

// Sends request to kube scheduler metrics
func (s *schedulerLatencyMeasurement) sendRequestToScheduler(c clientset.Interface, op, host, provider, masterName string) (string, error) {
	opUpper := strings.ToUpper(op)
	if opUpper != "GET" && opUpper != "DELETE" {
		return "", fmt.Errorf("unknown REST request")
	}

	nodes, err := c.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	var masterRegistered = false
	for _, node := range nodes.Items {
		if system.IsMasterNode(node.Name) {
			masterRegistered = true
		}
	}

	var responseText string
	if masterRegistered {
		ctx, cancel := context.WithTimeout(context.Background(), singleRestCallTimeout)
		defer cancel()

		body, err := c.CoreV1().RESTClient().Verb(opUpper).
			Context(ctx).
			Namespace(metav1.NamespaceSystem).
			Resource("pods").
			Name(fmt.Sprintf("kube-scheduler-%v:%v", masterName, ports.InsecureSchedulerPort)).
			SubResource("proxy").
			Suffix("metrics").
			Do().Raw()

		if err != nil {
			klog.Errorf("Send request to scheduler failed with err: %v", err)
			return "", err
		}
		responseText = string(body)
	} else {
		// If master is not registered fall back to old method of using SSH.
		if provider == "gke" {
			klog.Infof("%s: not grabbing scheduler metrics through master SSH: unsupported for gke", s)
			return "", nil
		}

		cmd := "curl -X " + opUpper + " http://localhost:10251/metrics"
		sshResult, err := measurementutil.SSH(cmd, host+":22", provider)
		if err != nil || sshResult.Code != 0 {
			return "", fmt.Errorf("unexpected error (code: %d) in ssh connection to master: %#v", sshResult.Code, err)
		}
		responseText = sshResult.Stdout
	}
	return responseText, nil
}

type schedulingMetrics struct {
	PredicateEvaluationDuration  measurementutil.HistogramVec `json:"predicateEvaluationDuration"`
	PriorityEvaluationDuration   measurementutil.HistogramVec `json:"priorityEvaluationDuration"`
	PreemptionEvaluationDuration measurementutil.HistogramVec `json:"preemptionEvaluationDuration"`
	BindingDuration              measurementutil.HistogramVec `json:"bindingDuration"`
	E2eSchedulingDuration        measurementutil.HistogramVec `json:"e2eSchedulingDuration"`
}

func newSchedulingMetrics() *schedulingMetrics {
	return &schedulingMetrics{
		PredicateEvaluationDuration:  make(measurementutil.HistogramVec, 0),
		PriorityEvaluationDuration:   make(measurementutil.HistogramVec, 0),
		PreemptionEvaluationDuration: make(measurementutil.HistogramVec, 0),
		BindingDuration:              make(measurementutil.HistogramVec, 0),
		E2eSchedulingDuration:        make(measurementutil.HistogramVec, 0),
	}
}
