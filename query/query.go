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
	prommodel "github.com/prometheus/common/model"
)

type QueryConfig struct {
	Query string
	Start time.Time
	End   time.Time
	Step  time.Duration
}

func QueryProm(ctx context.Context, url string, query QueryConfig, httpClient *http.Client) (prommodel.Value, error) {
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
