package status

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	errs "github.com/trussle/coherence/pkg/api/http"
	"github.com/trussle/coherence/pkg/metrics"
	"github.com/trussle/coherence/pkg/store"
)

// These are the status API URL paths.
const (
	APIPathLivenessQuery  = "/health"
	APIPathReadinessQuery = "/ready"
)

// API serves the status API
type API struct {
	store    store.Store
	logger   log.Logger
	clients  metrics.Gauge
	duration metrics.HistogramVec
	errors   errs.Error
}

// NewAPI creates a API with the correct dependencies.
func NewAPI(store store.Store,
	logger log.Logger,
	clients metrics.Gauge,
	duration metrics.HistogramVec,
) *API {
	return &API{
		store:    store,
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
	case method == "GET" && path == APIPathLivenessQuery:
		a.handleLiveness(w, r)
	case method == "GET" && path == APIPathReadinessQuery:
		a.handleReadiness(w, r)
	default:
		// Nothing found
		a.errors.NotFound(w, r)
	}
}

func (a *API) handleLiveness(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
		a.errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *API) handleReadiness(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if _, err := a.store.Keys(); err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}

	if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
		a.errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}
