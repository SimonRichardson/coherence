package client

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/SimonRichardson/resilience/breaker"
	"github.com/pkg/errors"
)

const (
	defaultFailureRate    = 10
	defaultFailureTimeout = time.Minute
)

// Client represents a http client that has a one to one relationship with a url
type Client struct {
	circuit *breaker.CircuitBreaker
	client  *http.Client
}

// NewClient creates a Client with the http.Client and url
func NewClient(client *http.Client) *Client {
	return &Client{
		circuit: breaker.New(defaultFailureRate, defaultFailureTimeout),
		client:  client,
	}
}

// Post a request to the url associated.
// If the response returns anything other than a StatusOK (200), then it
// will return an error.
func (c *Client) Post(u string, p []byte) (b []byte, err error) {
	err = c.circuit.Run(func() error {

		resp, err := c.client.Post(u, "application/javascript", bytes.NewReader(p))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("invalid status code: %d", resp.StatusCode)
		}

		var requestErr error
		b, requestErr = ioutil.ReadAll(resp.Body)
		return requestErr
	})
	return
}
