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
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
)

type Timerange struct {
	Start time.Time
	End   time.Time
	Step  time.Duration
}

type QueryConfig struct {
	Timerange
	Query string
}

func Single(ctx context.Context, url string, query QueryConfig, httpClient *http.Client) (prommodel.Value, error) {
	cfg := api.Config{
		Address: url,
		Client:  httpClient,
	}
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	api := v1.NewAPI(client)
	result, warns, err := api.QueryRange(ctx, query.Query, v1.Range{
		Start: query.Start,
		End:   query.End,
		Step:  query.Step,
	})
	if err != nil {
		return nil, err
	}
	for _, warn := range warns {
		fmt.Fprintf(os.Stderr, "Prometheus API warning: %s\n", warn)
	}
	return result, nil
}

type MultiQueryConfig struct {
	Timerange
	Queries []string
}

func Multi(ctx context.Context, url string, query MultiQueryConfig, httpClient *http.Client) ([]prommodel.Value, error) {
	results := make([]prommodel.Value, 0)
	for _, queryStr := range query.Queries {
		cfg := QueryConfig{Query: queryStr}
		cfg.Timerange = query.Timerange
		result, err := Single(ctx, url, cfg, httpClient)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

type ProductQueryConfig struct {
	MultiQueryConfig
	URLs []string
}

type multiResult struct {
	Values []prommodel.Value
	Err    error
}

func Product(ctx context.Context, query ProductQueryConfig, httpClient *http.Client) ([]prommodel.Value, error) {
	expected := len(query.URLs)
	resultChan := make(chan multiResult)
	for i := range query.URLs {
		url := query.URLs[i]
		go func() {
			values, err := Multi(ctx, url, query.MultiQueryConfig, httpClient)
			resultChan <- multiResult{Values: values, Err: err}
		}()
	}
	errs := make([]error, 0)
	values := make([]prommodel.Value, 0)
	counter := 0
	for result := range resultChan {
		if result.Err != nil {
			errs = append(errs, result.Err)
		} else {
			values = append(values, result.Values...)
		}
		counter++
		if counter == expected {
			close(resultChan)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return values, nil
}
