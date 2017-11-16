package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Error is a HTTP error type that allows the sending of errors correctly.
type Error struct {
	logger log.Logger
}

// NewError creates a new Error with a logger.
func NewError(logger log.Logger) Error {
	return Error{logger}
}

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to w.
// The error message should be application/json.
func (e Error) Error(w http.ResponseWriter, err string, code int) {
	level.Error(e.logger).Log("err", err, "code", code)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)

	if err := json.NewEncoder(w).Encode(struct {
		Description string `json:"description"`
		Code        int    `json:"code"`
	}{
		Description: err,
		Code:        code,
	}); err != nil {
		panic(err)
	}
}

// NotFound replies to the request with an HTTP 404 not found error.
func (e Error) NotFound(w http.ResponseWriter, r *http.Request) {
	e.Error(w, "not found", http.StatusNotFound)
}

// BadRequest to the request with an HTTP 400 bad request error.
func (e Error) BadRequest(w http.ResponseWriter, r *http.Request, err string) {
	e.Error(w, err, http.StatusBadRequest)
}

// InternalServerError to the request with an HTTP 500 bad request error.
func (e Error) InternalServerError(w http.ResponseWriter, r *http.Request, err string) {
	e.Error(w, err, http.StatusInternalServerError)
}
