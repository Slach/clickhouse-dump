package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	body, err := c.ExecuteQueryStreaming(query)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	return io.ReadAll(body)
}

func (c *ClickHouseClient) ExecuteQueryStreaming(query string) (io.ReadCloser, error) {
	encodedQuery := url.QueryEscape(query)
	url := fmt.Sprintf("http://%s:%d/?query=%s", c.config.Host, c.config.Port, encodedQuery)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.config.User, c.config.Password)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	return resp.Body, nil
}
