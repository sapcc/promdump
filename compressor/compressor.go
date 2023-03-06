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

package compressor

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

type Compression string

const (
	CompressionNone Compression = "none"
	CompressionGzip Compression = "gzip"
)

var compressorMap map[Compression]Compressor = map[Compression]Compressor{CompressionNone: NoneCompressor, CompressionGzip: GzipCompressor}

func Compress(in []byte, compression Compression) ([]byte, error) {
	compressor, ok := compressorMap[compression]
	if !ok {
		return nil, fmt.Errorf("unknown compression: %s", compression)
	}
	return compressor(in)
}

type Compressor func([]byte) ([]byte, error)

func NoneCompressor(in []byte) ([]byte, error) {
	return in, nil
}

func GzipCompressor(in []byte) ([]byte, error) {
	out := bytes.Buffer{}
	writer := gzip.NewWriter(&out)
	_, err := io.Copy(writer, bytes.NewBuffer(in))
	if err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
