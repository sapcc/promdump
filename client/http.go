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

package client

import (
	"bufio"
	"bytes"
	"net/http"
	"strings"

	"github.com/ilmari-lauhakangas/go-curl"
)

type HTTPBackend string

const (
	BackendGo   HTTPBackend = "go"
	BackendCurl HTTPBackend = "curl"
)

func MakeHTTPClient(backend HTTPBackend, clientCert string) http.Client {
	client := http.Client{}
	if backend == BackendCurl {
		crt := CurlRoundTripper{ClientCertName: clientCert}
		client.Transport = &crt
		return client
	}
	// golang backend
	return client
}

type CurlRoundTripper struct {
	ClientCertName string
}

func (crt *CurlRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// this may lead to sigsegv when golang retry logic kicks in
	defer req.Body.Close()
	easy := curl.EasyInit()
	defer easy.Cleanup()
	err := easy.Setopt(curl.OPT_URL, req.URL.String())
	if err != nil {
		return nil, err
	}
	switch req.Method {
	case http.MethodGet:
		err = easy.Setopt(curl.OPT_HTTPGET, 1)
		if err != nil {
			return nil, err
		}
	case http.MethodPost:
		err = easy.Setopt(curl.OPT_POST, 1)
		if err != nil {
			return nil, err
		}
	}
	err = easy.Setopt(curl.OPT_READFUNCTION,
		func(ptr []byte, userdata interface{}) int {
			written, _ := req.Body.Read(ptr)
			return written
		})
	if err != nil {
		return nil, err
	}
	response := bytes.Buffer{}
	err = easy.Setopt(curl.OPT_WRITEFUNCTION, func(ptr []byte, userdata interface{}) bool {
		// WARNING: never use append()
		response.Write(ptr)
		return true
	})
	if err != nil {
		return nil, err
	}
	if crt.ClientCertName != "" {
		err = easy.Setopt(curl.OPT_SSLCERT, crt.ClientCertName)
		if err != nil {
			return nil, err
		}
	}
	err = easy.Setopt(curl.OPT_HEADER, 1)
	if err != nil {
		return nil, err
	}
	err = easy.Perform()
	if err != nil {
		return nil, err
	}
	resStr := response.String()
	if strings.HasPrefix(resStr, "HTTP/2") {
		resStr = strings.Replace(resStr, "HTTP/2", "HTTP/2.0", 1)
	}
	res, err := http.ReadResponse(bufio.NewReader(strings.NewReader(resStr)), req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
