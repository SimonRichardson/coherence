package cache

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	errs "github.com/trussle/coherence/pkg/http"
	"github.com/trussle/coherence/pkg/metrics"
	"github.com/trussle/uuid"
)

const (

	// APIPathReplication represents a way to replicate a series or records.
	APIPathReplication = "/replicate"

	// APIPathIntersection represents a way to find out what records intersect.
	APIPathIntersection = "/intersect"
)

// API serves the cache API
type API struct {
	cache    Cache
	logger   log.Logger
	clients  metrics.Gauge
	duration metrics.HistogramVec
	errors   errs.Error
}

// NewAPI creates a API with the correct dependencies.
func NewAPI(cache Cache,
	logger log.Logger,
	clients metrics.Gauge,
	duration metrics.HistogramVec,
) *API {
	return &API{
		cache:    cache,
		logger:   logger,
		clients:  clients,
		duration: duration,
		errors:   errs.NewError(logger),
	}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	level.Info(a.logger).Log("method", r.Method, "url", r.URL.String())

	iw := &interceptingWriter{http.StatusOK, w}
	w = iw

	// Metrics
	a.clients.Inc()
	defer a.clients.Dec()

	defer func(begin time.Time) {
		a.duration.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(iw.code),
		).Observe(time.Since(begin).Seconds())
	}(time.Now())

	// Routing table
	method, path := r.Method, r.URL.Path
	switch {
	case method == "POST" && path == APIPathReplication:
		a.handleReplication(w, r)
	case method == "POST" && path == APIPathIntersection:
		a.handleIntersection(w, r)
	default:
		// Nothing found
		a.errors.NotFound(w, r)
	}
}

func (a *API) handleReplication(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp ReplicationQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryRequired); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	txn, err := ingestIdentifiers(r.Body)
	if err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	if err := a.cache.Add(txn); err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Make sure we collect the document for the result.
	qr := ReplicationQueryResult{Errors: a.errors, Params: qp}
	qr.ID, err = uuid.New()
	if err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Finish
	qr.Duration = time.Since(begin).String()
	qr.EncodeTo(w)
}

func (a *API) handleIntersection(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp IntersectionQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryRequired); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	idents, err := ingestIdentifiers(r.Body)
	if err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	union, difference, err := a.cache.Intersection(idents)
	if err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Make sure we collect the document for the result.
	qr := IntersectionQueryResult{Errors: a.errors, Params: qp}
	qr.Union = union
	qr.Difference = difference

	// Finish
	qr.Duration = time.Since(begin).String()
	qr.EncodeTo(w)
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}

func ingestIdentifiers(reader io.ReadCloser) ([]string, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if len(bytes) < 1 {
		return nil, errors.New("no body content")
	}

	var input IngestInput
	if err = json.Unmarshal(bytes, &input); err != nil {
		return nil, err
	}

	return input.Identifiers, nil
}

// IngestInput defines a simple type for marshalling and unmarshalling idents
type IngestInput struct {
	Identifiers []string `json:"idents"`
}
