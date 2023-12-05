/*
Copyright 2023 SAP SE
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

package query

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type MetricInfo struct {
	Name string `json:"name"`
	Help string `json:"help"`
}

type MetricDump struct {
	MetricInfo
	Labels []string `json:"labels"`
}

func MetricsWithLabels(ctx context.Context, url string, httpClient *http.Client) ([]MetricDump, error) {
	client, err := api.NewClient(api.Config{
		Address: url,
		Client:  httpClient,
	})
	if err != nil {
		return nil, err
	}
	api := v1.NewAPI(client)
	metaMap, err := api.Metadata(ctx, "", "")
	if err != nil {
		return nil, err
	}
	metrics := make([]MetricInfo, 0)
	for name, val := range metaMap {
		for _, info := range val {
			metrics = append(metrics, MetricInfo{
				Name: name,
				Help: info.Help,
			})
		}
	}
	uniqueMetrics := make(map[string]struct{})
	for _, metric := range metrics {
		uniqueMetrics[metric.Name] = struct{}{}
	}
	metricLabels := make(map[string][]string)
	now := time.Now()
	for metric := range uniqueMetrics {
		labels, warns, err := api.LabelNames(ctx, []string{metric}, time.UnixMilli(0), now)
		if err != nil {
			return nil, err
		}
		for _, warn := range warns {
			fmt.Fprintf(os.Stderr, "Prometheus API warning: %s\n", warn)
		}
		metricLabels[metric] = labels
	}
	dumps := make([]MetricDump, 0)
	for _, metric := range metrics {
		dumps = append(dumps, MetricDump{
			MetricInfo: metric,
			Labels:     metricLabels[metric.Name],
		})
	}
	return dumps, err
}
