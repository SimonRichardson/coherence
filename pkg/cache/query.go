package cache

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	errs "github.com/trussle/coherence/pkg/http"
	"github.com/trussle/coherence/pkg/selectors"
)

const (
	defaultContentType = "application/json"
)

// KeyQueryParams defines all the dimensions of a query.
type KeyQueryParams struct {
	Key string
}

// DecodeFrom populates a KeyQueryParams from a URL.
func (qp *KeyQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
		return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
	}

	if rb == queryRequired {
		if qp.Key = u.Query().Get("key"); qp.Key == "" {
			return errors.Errorf("expected 'key' but got %q", qp.Key)
		}
	}

	return nil
}

// QueryResult contains statistics about the query.
type QueryResult struct {
	Errors    errs.Error
	Params    KeyQueryParams      `json:"query"`
	Duration  string              `json:"duration"`
	ChangeSet selectors.ChangeSet `json:"changeset"`
}

// EncodeTo encodes the QueryResult to the HTTP response writer.
func (qr *QueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key)

	if err := json.NewEncoder(w).Encode(qr.ChangeSet); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

const (
	httpHeaderContentType = "Content-Type"
	httpHeaderDuration    = "X-Duration"
	httpHeaderKey         = "X-Key"
)

type queryBehavior int

const (
	queryRequired queryBehavior = iota
	queryOptional
)
