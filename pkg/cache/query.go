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
	Key selectors.Key
}

// DecodeFrom populates a KeyQueryParams from a URL.
func (qp *KeyQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
		return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
	}

	if rb == queryRequired {
		key := u.Query().Get("key")
		if key == "" {
			return errors.Errorf("expected 'key' but got %q", qp.Key)
		}
		qp.Key = selectors.Key(key)
	}

	return nil
}

// ChangeSetQueryResult contains statistics about the query.
type ChangeSetQueryResult struct {
	Errors    errs.Error
	Params    KeyQueryParams      `json:"query"`
	Duration  string              `json:"duration"`
	ChangeSet selectors.ChangeSet `json:"changeset"`
}

// EncodeTo encodes the ChangeSetQueryResult to the HTTP response writer.
func (qr *ChangeSetQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key.String())

	if err := json.NewEncoder(w).Encode(struct {
		Records selectors.ChangeSet `json:"records"`
	}{
		Records: qr.ChangeSet,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// KeysQueryResult contains statistics about the query.
type KeysQueryResult struct {
	Errors   errs.Error
	Duration string          `json:"duration"`
	Keys     []selectors.Key `json:"keys"`
}

// EncodeTo encodes the KeysQueryResult to the HTTP response writer.
func (qr *KeysQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)

	if err := json.NewEncoder(w).Encode(struct {
		Records []selectors.Key `json:"records"`
	}{
		Records: qr.Keys,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// SizeQueryResult contains statistics about the query.
type SizeQueryResult struct {
	Errors   errs.Error
	Params   KeyQueryParams `json:"query"`
	Duration string         `json:"duration"`
	Size     int            `json:"size"`
}

// EncodeTo encodes the SizeQueryResult to the HTTP response writer.
func (qr *SizeQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key.String())

	if err := json.NewEncoder(w).Encode(struct {
		Records int `json:"records"`
	}{
		Records: qr.Size,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// FieldsQueryResult contains statistics about the query.
type FieldsQueryResult struct {
	Errors   errs.Error
	Params   KeyQueryParams    `json:"query"`
	Duration string            `json:"duration"`
	Fields   []selectors.Field `json:"fields"`
}

// EncodeTo encodes the FieldsQueryResult to the HTTP response writer.
func (qr *FieldsQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key.String())

	if err := json.NewEncoder(w).Encode(struct {
		Records []selectors.Field `json:"records"`
	}{
		Records: qr.Fields,
	}); err != nil {
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
