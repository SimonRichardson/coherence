package client

import (
	"bytes"
	"fmt"
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
	host    string
}

// New creates a Client with the http.Client and url
func New(client *http.Client, host string) *Client {
	return &Client{
		circuit: breaker.New(defaultFailureRate, defaultFailureTimeout),
		client:  client,
		host:    host,
	}
}

// Get a request to the url associated.
// If the response returns anything other than a StatusOK (200), then it
// will return an error.
func (c *Client) Get(u string) (b []byte, err error) {
	err = c.circuit.Run(func() error {

		resp, err := c.client.Get(fmt.Sprintf("http://%s%s", c.host, u))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return NewNotFoundError(errors.Errorf("invalid status code: %d", resp.StatusCode))
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("invalid status code: %d", resp.StatusCode)
		}

		var requestErr error
		b, requestErr = ioutil.ReadAll(resp.Body)
		return requestErr
	})
	return
}

// Post a request to the url associated.
// If the response returns anything other than a StatusOK (200), then it
// will return an error.
func (c *Client) Post(u string, p []byte) (b []byte, err error) {
	err = c.circuit.Run(func() error {

		resp, err := c.client.Post(fmt.Sprintf("http://%s%s", c.host, u), "application/javascript", bytes.NewReader(p))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return NewNotFoundError(errors.Errorf("invalid status code: %d", resp.StatusCode))
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("invalid status code: %d", resp.StatusCode)
		}

		var requestErr error
		b, requestErr = ioutil.ReadAll(resp.Body)
		return requestErr
	})
	return
}

// Host returns the associated host
func (c *Client) Host() string {
	return c.host
}

type errNotFound struct {
	err error
}

// NewNotFoundError creates a new NotFoundError
func NewNotFoundError(err error) error {
	return errNotFound{err}
}

func (e errNotFound) Error() string {
	return e.err.Error()
}

// NotFoundError finds if the error passed in, is actually a partial error or not
func NotFoundError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(errNotFound)
	return ok
}
