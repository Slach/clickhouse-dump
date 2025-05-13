package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

type ClickHouseClient struct {
	config *Config
	client *http.Client
}

func NewClickHouseClient(config *Config) *ClickHouseClient {
	return &ClickHouseClient{
		config: config,
		client: &http.Client{},
	}
}

func (c *ClickHouseClient) ExecuteQuery(query string) ([]byte, error) {
	body, _, err := c.ExecuteQueryStreaming(query, "")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = body.Close()
	}()

	return io.ReadAll(body)
}

func (c *ClickHouseClient) ExecuteQueryStreaming(query string, compressFormat string) (io.ReadCloser, string, error) {
	url := fmt.Sprintf("http://%s:%d/", c.config.Host, c.config.Port)
	if compressFormat != "" {
		url += "?enable_http_compression=1"
	}
	req, reqErr := http.NewRequest("POST", url, strings.NewReader(query))
	if reqErr != nil {
		return nil, "", reqErr
	}

	req.SetBasicAuth(c.config.User, c.config.Password)
	// Content-Type остается text/plain, так как это SQL по своей сути.
	// Content-Encoding укажет на сжатие.
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	// Add Accept-Encoding header if compression format is specified for the response
	if compressFormat != "" {
		url += "&enable_http_compression=1" // enable_http_compression=0 by default for POST without Accept-Encoding
		switch strings.ToLower(compressFormat) {
		case "gzip":
			req.Header.Set("Accept-Encoding", "gzip")
		case "zstd":
			req.Header.Set("Accept-Encoding", "zstd")
		}
	}

	resp, reqErr := c.client.Do(req)
	if reqErr != nil {
		return nil, "", reqErr
	}

	if resp.StatusCode != http.StatusOK {
		respText, respErr := io.ReadAll(resp.Body)
		if respErr != nil {
			respText = []byte(respErr.Error())
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		return nil, "", fmt.Errorf("HTTP request POST %s..., failed with status code: %d, response: %s", firstNChars(query, 255), resp.StatusCode, string(respText))
	}

	// Check if compression was used in the response
	contentEncoding := resp.Header.Get("Content-Encoding")

	return resp.Body, contentEncoding, nil
}

// ExecuteQueryWithBody отправляет запрос с заданным телом и Content-Encoding.
// queryForLog используется для логирования в случае ошибки.
func (c *ClickHouseClient) ExecuteQueryWithBody(body io.Reader, contentEncoding string, queryForLog string) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d/", c.config.Host, c.config.Port)

	req, reqErr := http.NewRequest("POST", url, body)
	if reqErr != nil {
		return nil, reqErr
	}

	req.SetBasicAuth(c.config.User, c.config.Password)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	// Если мы хотим указать, что клиент принимает сжатый ответ (даже для INSERT)
	// Это можно сделать на основе r.config.CompressFormat, если он доступен здесь
	// или передан как параметр. Для INSERT ответы обычно маленькие.
	// Например:
	// if c.config.CompressFormat == "gzip" { // Предполагая доступ к config или его части
	//    req.Header.Set("Accept-Encoding", "gzip")
	//    // Также может потребоваться url += "?enable_http_compression=1"
	// } else if c.config.CompressFormat == "zstd" {
	//    req.Header.Set("Accept-Encoding", "zstd")
	// }


	resp, reqErr := c.client.Do(req)
	if reqErr != nil {
		return nil, reqErr
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		respText, respErr := io.ReadAll(resp.Body)
		if respErr != nil {
			respText = []byte(respErr.Error())
		}
		return nil, fmt.Errorf("HTTP request POST %s..., failed with status code: %d, response: %s", firstNChars(queryForLog, 255), resp.StatusCode, string(respText))
	}

	return io.ReadAll(resp.Body)
}

// Helper to get first N characters of a string for logging.
func firstNChars(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Find rune boundary near n
	i := 0
	for j := range s {
		if i >= n {
			return s[:j]
		}
		i++
	}
	return s // Should not happen if n < len(s)
}
