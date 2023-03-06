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

package model

import (
	"encoding/json"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/writer"
)

type Layout string
type Format string

const (
	LayoutRaw     = "raw"
	LayoutNested  = "nested"
	LayoutFlat    = "flat"
	FormatJSON    = "json"
	FormatParquet = "parquet"
)

type Marshaler interface {
	AsJSON() ([]byte, error)
	AsParquet() ([]byte, error)
}

func Marshal(value model.Value, layout Layout, format Format) ([]byte, error) {
	marshaler, err := AsMarshaler(value, layout)
	if err != nil {
		return nil, err
	}
	switch format {
	case FormatJSON:
		return marshaler.AsJSON()
	case FormatParquet:
		return marshaler.AsParquet()
	}
	return nil, fmt.Errorf("unknown format: %s", layout)
}

func AsMarshaler(value model.Value, layout Layout) (Marshaler, error) {
	switch layout {
	case LayoutRaw:
		return &WrappedValue{value: value}, nil
	case LayoutNested:
		dumps, err := ValueToSampleDumps(value)
		if err != nil {
			return nil, err
		}
		return &dumps, nil
	case LayoutFlat:
		dumps, err := ValueToSampleDumps(value)
		if err != nil {
			return nil, err
		}
		flattened := FlattenDumps(dumps)
		return &flattened, nil
	}
	return nil, fmt.Errorf("unknown layout: %s", layout)
}

type WrappedValue struct {
	value model.Value
}

func (val *WrappedValue) AsJSON() ([]byte, error) {
	return json.Marshal(val.value)
}

func (val *WrappedValue) AsParquet() ([]byte, error) {
	return nil, fmt.Errorf("serializing raw prometheus values to parquet is not supported")
}

type SampleDump struct {
	Metric    string         `json:"metric" parquet:"name=metric, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Labels    model.LabelSet `json:"labels" parquet:"name=labels, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Timestamp int64          `json:"timestamp" parquet:"name=timestamp, type=INT64"`
	Value     float64        `json:"value" parquet:"name=timestamp, type=DOUBLE"`
}

type SampleDumps []SampleDump

func ValueToSampleDumps(value model.Value) (SampleDumps, error) {
	matrices, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("not a prometheus matrix")
	}
	dumps := make([]SampleDump, 0)
	for _, sampleStream := range matrices {
		name := sampleStream.Metric[model.MetricNameLabel]
		delete(sampleStream.Metric, model.MetricNameLabel)
		for _, samplePair := range sampleStream.Values {
			sampleDump := SampleDump{
				Metric:    string(name),
				Timestamp: int64(samplePair.Timestamp),
				Value:     float64(samplePair.Value),
				Labels:    model.LabelSet(sampleStream.Metric),
			}
			dumps = append(dumps, sampleDump)
		}
	}
	return dumps, nil
}

func (dumps *SampleDumps) AsJSON() ([]byte, error) {
	return json.Marshal(dumps)
}

func (dumps *SampleDumps) AsParquet() ([]byte, error) {
	buf := buffer.NewBufferFile()
	writer, err := writer.NewParquetWriter(buf, new(SampleDump), 4)
	if err != nil {
		return nil, err
	}
	for _, dump := range *dumps {
		err = writer.Write(dump)
		if err != nil {
			return nil, err
		}
	}
	err = writer.WriteStop()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type FlattenedSampleDump map[string]any

type FlattenedSampleDumps []FlattenedSampleDump

func FlattenDump(dump *SampleDump) FlattenedSampleDump {
	m := make(FlattenedSampleDump)
	m["metric"] = dump.Metric
	m["timestamp"] = dump.Timestamp
	m["value"] = dump.Value
	for key, val := range dump.Labels {
		m[string(key)] = val
	}
	return m
}

func FlattenDumps(dumps SampleDumps) FlattenedSampleDumps {
	flattened := make([]FlattenedSampleDump, 0)
	for _, dump := range dumps {
		flattened = append(flattened, FlattenDump(&dump))
	}
	return flattened
}

func (flattened *FlattenedSampleDumps) AsJSON() ([]byte, error) {
	return json.Marshal(flattened)
}

func (flattened *FlattenedSampleDumps) AsParquet() ([]byte, error) {
	buf := buffer.NewBufferFile()
	writer, err := writer.NewParquetWriter(buf, new(FlattenedSampleDump), 1)
	if err != nil {
		return nil, err
	}
	for _, dump := range *flattened {
		err = writer.Write(dump)
		if err != nil {
			return nil, err
		}
	}
	err = writer.WriteStop()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
