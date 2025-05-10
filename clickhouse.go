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
	req, reqErr := http.NewRequest("POST", url, strings.NewReader(query))
	if reqErr != nil {
		return nil, "", reqErr
	}

	req.SetBasicAuth(c.config.User, c.config.Password)
	req.Header.Set("Content-Type", "text/plain")
	
	// Добавляем заголовок Accept-Encoding, если указан формат сжатия
	if compressFormat != "" {
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

	// Проверяем, использовалось ли сжатие в ответе
	contentEncoding := resp.Header.Get("Content-Encoding")
	
	return resp.Body, contentEncoding, nil
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
