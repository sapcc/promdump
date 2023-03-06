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

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sapcc/promdump/client"
	"github.com/sapcc/promdump/compressor"
	"github.com/sapcc/promdump/model"
	"github.com/sapcc/promdump/query"
	"github.com/urfave/cli/v3"
)

func main() {
	now := time.Now()
	app := cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "backend",
				OnlyOnce: true,
				Usage:    "http backend to use",
				Aliases:  []string{"b"},
			},
			&cli.StringFlag{
				Name:     "client-cert",
				OnlyOnce: true,
				Usage:    "name of client cert to use",
			},
			&cli.StringFlag{
				Name:     "format",
				OnlyOnce: true,
				Value:    "json",
				Aliases:  []string{"f"},
			},
			&cli.StringFlag{
				Name:     "layout",
				OnlyOnce: true,
				Value:    "flat",
				Aliases:  []string{"l"},
			},
			&cli.StringFlag{
				Name:     "compress",
				OnlyOnce: true,
				Value:    "none",
				Aliases:  []string{"c"},
			},
			&cli.TimestampFlag{
				Name:     "start",
				Value:    now.Add(-5 * time.Minute),
				Aliases:  []string{"s"},
				OnlyOnce: true,
			},
			&cli.TimestampFlag{
				Name:     "end",
				Value:    now,
				Aliases:  []string{"e"},
				OnlyOnce: true,
			},
			&cli.DurationFlag{
				Name:     "step",
				Value:    1 * time.Minute,
				OnlyOnce: true,
				Aliases:  []string{"S"},
			},
		},
		Name:  "promdump",
		Usage: "Dumps data from a prometheus to stdout",
		Commands: []*cli.Command{
			{
				Name:      "dump",
				ArgsUsage: "query",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "url",
						Required: true,
						OnlyOnce: true,
						Usage:    "prometheus to query",
						Aliases:  []string{"u"},
					},
				},
				Action: func(ctx *cli.Context) error {
					if !ctx.Args().Present() {
						return fmt.Errorf("no query given")
					}
					return dump(context.Background(), dumpConfig{
						promUrl:     ctx.String("url"),
						backend:     ctx.String("backend"),
						clientCert:  ctx.String("client-cert"),
						format:      ctx.String("format"),
						layout:      ctx.String("layout"),
						compression: ctx.String("compress"),
						query: query.QueryConfig{
							Query: ctx.Args().First(),
							Start: *ctx.Timestamp("start"),
							End:   *ctx.Timestamp("end"),
							Step:  ctx.Duration("step"),
						},
					})
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}

type dumpConfig struct {
	query       query.QueryConfig
	promUrl     string
	backend     string
	format      string
	layout      string
	compression string
	clientCert  string
}

func dump(ctx context.Context, cfg dumpConfig) error {
	httpClient, cleanup := client.MakeHTTPClient(client.HTTPBackend(cfg.backend), cfg.clientCert)
	defer cleanup()
	result, err := query.QueryProm(ctx, cfg.promUrl, cfg.query, &httpClient)
	if err != nil {
		return err
	}
	marshaled, err := model.Marshal(result, model.LayoutFlat, model.Format(cfg.format))
	if err != nil {
		return err
	}
	compressed, err := compressor.Compress(marshaled, compressor.Compression(cfg.compression))
	if err != nil {
		return err
	}
	_, err = io.Copy(os.Stdout, bytes.NewBuffer(compressed))
	return err
}
