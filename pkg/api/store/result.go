package store

import (
	"encoding/json"
	"net/http"

	"github.com/SimonRichardson/coherence/pkg/api"
	errs "github.com/SimonRichardson/coherence/pkg/api/http"
	"github.com/SimonRichardson/coherence/pkg/selectors"
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
		Records api.ChangeSet `json:"records"`
	}{
		Records: api.ChangeSetOutput(qr.ChangeSet),
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
		Records []api.Key `json:"records"`
	}{
		Records: api.KeysOutput(qr.Keys),
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

	if err := json.NewEncoder(w).Encode(struct {
		Records []api.Field `json:"records"`
	}{
		Records: api.FieldsOutput(qr.Fields),
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
		Records api.Presence `json:"records"`
	}{
		Records: api.Presence{
			Inserted: qr.Presence.Inserted,
			Present:  qr.Presence.Present,
			Score:    qr.Presence.Score,
		},
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// FieldScoreQueryResult contains statistics about the query.
type FieldScoreQueryResult struct {
	Errors     errs.Error
	Params     KeyFieldQueryParams  `json:"query"`
	Duration   string               `json:"duration"`
	FieldScore selectors.FieldScore `json:"fieldscore"`
}

// EncodeTo encodes the FieldScoreQueryResult to the HTTP response writer.
func (qr *FieldScoreQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)
	w.Header().Set(httpHeaderKey, qr.Params.Key().String())
	w.Header().Set(httpHeaderField, qr.Params.Field().String())

	if err := json.NewEncoder(w).Encode(struct {
		Records api.FieldScore `json:"records"`
	}{
		Records: api.FieldScore{
			Field: api.Field(qr.FieldScore.Field.String()),
			Score: qr.FieldScore.Score,
		},
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
		Records api.FieldValueScore `json:"records"`
	}{
		Records: api.FieldValueScore{
			Field: api.Field(qr.FieldValueScore.Field.String()),
			Value: qr.FieldValueScore.Value,
			Score: qr.FieldValueScore.Score,
		},
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

const (
	httpHeaderContentType = "Content-Type"
	httpHeaderDuration    = "X-Duration"
	httpHeaderKey         = "X-Key"
	httpHeaderField       = "X-Field"
	httpHeaderQuorum      = "X-Quorum"
)
