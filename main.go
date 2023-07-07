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

	"github.com/andelf/go-curl"
	"github.com/sapcc/promdump/client"
	"github.com/sapcc/promdump/compressor"
	"github.com/sapcc/promdump/model"
	"github.com/sapcc/promdump/query"
	"github.com/urfave/cli/v2"
)

var version string

func main() {
	now := time.Now()
	app := cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "backend",
				Usage:   "http backend to use",
				Aliases: []string{"b"},
			},
			&cli.StringFlag{
				Name:  "client-cert",
				Usage: "name of client cert to use",
			},
			&cli.StringFlag{
				Name:    "format",
				Value:   "json",
				Aliases: []string{"f"},
			},
			&cli.StringFlag{
				Name:    "layout",
				Value:   "flat",
				Aliases: []string{"l"},
			},
			&cli.StringFlag{
				Name:    "compress",
				Value:   "none",
				Aliases: []string{"c"},
			},
			&cli.TimestampFlag{
				Name:    "start",
				Value:   cli.NewTimestamp(now.Add(-5 * time.Minute)),
				Layout:  "2006-01-02T15:04:05",
				Aliases: []string{"s"},
				Usage:   "UTC timestamp with layout 2006-01-02T15:04:05",
			},
			&cli.TimestampFlag{
				Name:    "end",
				Value:   cli.NewTimestamp(now),
				Aliases: []string{"e"},
				Layout:  "2006-01-02T15:04:05",
				Usage:   "UTC timestamp with layout 2006-01-02T15:04:05",
			},
			&cli.DurationFlag{
				Name:    "step",
				Value:   1 * time.Minute,
				Aliases: []string{"S"},
				Usage:   "Duration according to golangs time.ParseDuration()",
			},
		},
		Name:  "promdump",
		Usage: "Dumps data from a prometheus to stdout",
		Before: func(ctx *cli.Context) error {
			if ctx.String("backend") == string(client.BackendCurl) {
				return curl.GlobalInit(curl.GLOBAL_DEFAULT)
			}
			return nil
		},
		After: func(ctx *cli.Context) error {
			if ctx.String("backend") == string(client.BackendCurl) {
				curl.GlobalCleanup()
			}
			return nil
		},
		Commands: []*cli.Command{
			{
				Name:      "dump",
				ArgsUsage: "query",
				Flags: []cli.Flag{
					&cli.StringSliceFlag{
						Name:     "url",
						Required: true,
						Usage:    "prometheis to query",
						Aliases:  []string{"u"},
					},
				},
				Action: func(ctx *cli.Context) error {
					if !ctx.Args().Present() {
						return fmt.Errorf("no query given")
					}
					return dump(context.Background(), dumpConfig{
						promURLs:    ctx.StringSlice("url"),
						backend:     ctx.String("backend"),
						clientCert:  ctx.String("client-cert"),
						format:      ctx.String("format"),
						layout:      ctx.String("layout"),
						compression: ctx.String("compress"),
						start:       *ctx.Timestamp("start"),
						end:         *ctx.Timestamp("end"),
						step:        ctx.Duration("step"),
						queries:     ctx.Args().Slice(),
					})
				},
				Usage: "Dumps data from a prometheus to stdout",
			},
			{
				Name: "version",
				Action: func(ctx *cli.Context) error {
					fmt.Fprintf(ctx.App.Writer, "%s\n", version)
					return nil
				},
				Usage: "Prints the version",
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}

type dumpConfig struct {
	queries     []string
	promURLs    []string
	backend     string
	format      string
	layout      string
	compression string
	clientCert  string
	start       time.Time
	end         time.Time
	step        time.Duration
}

func dump(ctx context.Context, cfg dumpConfig) error {
	httpClient := client.MakeHTTPClient(client.HTTPBackend(cfg.backend), cfg.clientCert)
	result, err := query.Product(ctx, query.ProductQueryConfig{
		MultiQueryConfig: query.MultiQueryConfig{
			Timerange: query.Timerange{
				Start: cfg.start,
				End:   cfg.end,
				Step:  cfg.step,
			},
			Queries: cfg.queries,
		},
		URLs: cfg.promURLs,
	}, &httpClient)
	if err != nil {
		return err
	}
	marshaled, err := model.MarshalSlice(result, model.Layout(cfg.layout), model.Format(cfg.format))
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
