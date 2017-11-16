package cache

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	errs "github.com/trussle/coherence/pkg/http"
	"github.com/trussle/uuid"
)

const (
	defaultContentType = "application/json"
)

// ReplicationQueryParams defines all the dimensions of a query.
type ReplicationQueryParams struct {
}

// DecodeFrom populates a ReplicationQueryParams from a URL.
func (qp *ReplicationQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
		return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
	}

	return nil
}

// ReplicationQueryResult contains statistics about the query.
type ReplicationQueryResult struct {
	Errors   errs.Error
	Params   ReplicationQueryParams `json:"query"`
	Duration string                 `json:"duration"`
	ID       uuid.UUID              `json:"id"`
}

// EncodeTo encodes the ReplicationQueryResult to the HTTP response writer.
func (qr *ReplicationQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)

	if err := json.NewEncoder(w).Encode(struct {
		ID uuid.UUID `json:"id"`
	}{
		ID: qr.ID,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// IntersectionQueryParams defines all the dimensions of a query.
type IntersectionQueryParams struct {
}

// DecodeFrom populates a IntersectionQueryParams from a URL.
func (qp *IntersectionQueryParams) DecodeFrom(u *url.URL, h http.Header, rb queryBehavior) error {
	if contentType := h.Get("Content-Type"); rb == queryRequired && strings.ToLower(contentType) != "application/json" {
		return errors.Errorf("expected 'application/json' content-type, got %q", contentType)
	}

	return nil
}

// IntersectionQueryResult contains statistics about the query.
type IntersectionQueryResult struct {
	Errors     errs.Error
	Params     IntersectionQueryParams `json:"query"`
	Duration   string                  `json:"duration"`
	Union      []string                `json:"union"`
	Difference []string                `json:"difference"`
}

// EncodeTo encodes the IntersectionQueryResult to the HTTP response writer.
func (qr *IntersectionQueryResult) EncodeTo(w http.ResponseWriter) {
	w.Header().Set(httpHeaderContentType, defaultContentType)
	w.Header().Set(httpHeaderDuration, qr.Duration)

	if err := json.NewEncoder(w).Encode(Intersections{
		Union:      qr.Union,
		Difference: qr.Difference,
	}); err != nil {
		qr.Errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type Intersections struct {
	Union      []string `json:"union"`
	Difference []string `json:"difference"`
}

const (
	httpHeaderContentType   = "Content-Type"
	httpHeaderDuration      = "X-Duration"
	httpHeaderID            = "X-ID"
	httpHeaderQueryTags     = "X-Query-Tags"
	httpHeaderQueryAuthorID = "X-Query-Author-ID"
)

type queryBehavior int

const (
	queryRequired queryBehavior = iota
	queryOptional
)
