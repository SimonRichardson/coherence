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

type KeyParams interface {
	Key() selectors.Key
}

type FieldParams interface {
	Field() selectors.Field
}

// KeyQueryParams defines all the dimensions of a query.
type KeyQueryParams struct {
	key selectors.Key
}

// Key returns the key value from the parameters
func (qp *KeyQueryParams) Key() selectors.Key {
	return qp.key
}

// DecodeFrom populates a KeyQueryParams from a URL.
func (qp *KeyQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
		return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
	}

	if rb == queryRequired {
		key := u.Query().Get("key")
		if key == "" {
			return errors.Errorf("expected 'key' but got %q", key)
		}
		qp.key = selectors.Key(key)
	}

	return nil
}

// KeyFieldQueryParams defines all the dimensions of a query.
type KeyFieldQueryParams struct {
	key   selectors.Key
	field selectors.Field
}

// Key returns the key value from the parameters
func (qp *KeyFieldQueryParams) Key() selectors.Key {
	return qp.key
}

// Field returns the field value from the parameters
func (qp *KeyFieldQueryParams) Field() selectors.Field {
	return qp.field
}

// DecodeFrom populates a KeyFieldQueryParams from a URL.
func (qp *KeyFieldQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
		return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
	}

	if rb == queryRequired {
		key := u.Query().Get("key")
		if key == "" {
			return errors.Errorf("expected 'key' but got %q", key)
		}
		qp.Key = selectors.Key(key)

		field := u.Query().Get("field")
		if field == "" {
			return errors.Errorf("expected 'field' but got %q", field)
		}
		qp.Field = selectors.Field(field)
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

// Int64QueryResult contains statistics about the query.
type Int64QueryResult struct {
	Errors   errs.Error
	Params   KeyParams `json:"query"`
	Duration string    `json:"duration"`
	Integer  int64     `json:"size"`
}

// EncodeTo encodes the Int64QueryResult to the HTTP response writer.
func (qr *Int64QueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())

	if err := json.NewEncoder(w).Encode(struct {
		Records int `json:"records"`
	}{
		Records: qr.Integer,
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

// PresenceQueryResult contains statistics about the query.
type PresenceQueryResult struct {
	Errors   errs.Error
	Params   KeyFieldQueryParams `json:"query"`
	Duration string              `json:"duration"`
	Presence selectors.Presence  `json:"presence"`
}

// EncodeTo encodes the PresenceQueryResult to the HTTP response writer.
func (qr *PresenceQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key.String())

	if err := json.NewEncoder(w).Encode(struct {
		Records selectors.Presence `json:"records"`
	}{
		Records: qr.Presence,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

const (
	httpHeaderContentType = "Content-Type"
	httpHeaderDuration    = "X-Duration"
	httpHeaderKey         = "X-Key"
	httpHeaderField       = "X-Field"
)

type queryBehavior int

const (
	queryRequired queryBehavior = iota
	queryOptional
)
