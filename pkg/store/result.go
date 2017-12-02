package store

import (
	"encoding/json"
	"net/http"

	errs "github.com/trussle/coherence/pkg/http"
	"github.com/trussle/coherence/pkg/selectors"
)

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
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())

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

	keys := qr.Keys
	if keys == nil {
		keys = make([]selectors.Key, 0)
	}

	if err := json.NewEncoder(w).Encode(struct {
		Records []selectors.Key `json:"records"`
	}{
		Records: keys,
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
		Records int64 `json:"records"`
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
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())

	fields := qr.Fields
	if fields == nil {
		fields = make([]selectors.Field, 0)
	}

	if err := json.NewEncoder(w).Encode(struct {
		Records []selectors.Field `json:"records"`
	}{
		Records: fields,
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
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())
	w.Header().Set(httpHeaderField, qr.Params.Field().String())

	if err := json.NewEncoder(w).Encode(struct {
		Records selectors.Presence `json:"records"`
	}{
		Records: qr.Presence,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// FieldScoreQueryResult contains statistics about the query.
type FieldScoreQueryResult struct {
	Errors     errs.Error
	Params     KeyFieldQueryParams  `json:"query"`
	Duration   string               `json:"duration"`
	FieldScore selectors.FieldScore `json:"fieldScore"`
}

// EncodeTo encodes the FieldScoreQueryResult to the HTTP response writer.
func (qr *FieldScoreQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())
	w.Header().Set(httpHeaderField, qr.Params.Field().String())

	if err := json.NewEncoder(w).Encode(struct {
		Records selectors.FieldScore `json:"records"`
	}{
		Records: qr.FieldScore,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// FieldValueScoreQueryResult contains statistics about the query.
type FieldValueScoreQueryResult struct {
	Errors          errs.Error
	Params          KeyFieldQueryParams       `json:"query"`
	Duration        string                    `json:"duration"`
	FieldValueScore selectors.FieldValueScore `json:"fieldValueScore"`
}

// EncodeTo encodes the FieldValueScoreQueryResult to the HTTP response writer.
func (qr *FieldValueScoreQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())
	w.Header().Set(httpHeaderField, qr.Params.Field().String())

	if err := json.NewEncoder(w).Encode(struct {
		Records selectors.FieldValueScore `json:"records"`
	}{
		Records: qr.FieldValueScore,
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
