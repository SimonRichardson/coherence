package store

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/SimonRichardson/coherence/pkg/api"
	errs "github.com/SimonRichardson/coherence/pkg/api/http"
	"github.com/SimonRichardson/coherence/pkg/metrics"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/SimonRichardson/coherence/pkg/store"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

const (

	// APIPathInsert represents a way to insert a series or records.
	APIPathInsert = "/insert"

	// APIPathDelete represents a way to delete a series or records.
	APIPathDelete = "/delete"

	// APIPathSelect represents a way to select a record.
	APIPathSelect = "/select"

	// APIPathKeys represents a way to find all the keys with in the cache.
	APIPathKeys = "/keys"

	// APIPathSize represents a way to find the size of a key with in the cache.
	APIPathSize = "/size"

	// APIPathMembers represents a way to find all the members for a key with in
	// the cache.
	APIPathMembers = "/members"

	// APIPathScore represents a way to find the score of a field with in a key.
	APIPathScore = "/score"
)

// API serves the cache API
type API struct {
	store    store.Store
	logger   log.Logger
	clients  metrics.Gauge
	duration metrics.HistogramVec
	errors   errs.Error
	action   chan func()
	stop     chan chan struct{}
}

// NewAPI creates a API with the correct dependencies.
func NewAPI(store store.Store,
	logger log.Logger,
	clients metrics.Gauge,
	duration metrics.HistogramVec,
) *API {
	api := &API{
		store:    store,
		logger:   logger,
		clients:  clients,
		duration: duration,
		errors:   errs.NewError(logger),
		action:   make(chan func()),
		stop:     make(chan chan struct{}),
	}

	go api.run()

	return api
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
	case method == "POST" && path == APIPathInsert:
		a.handleInsertion(w, r)
	case method == "POST" && path == APIPathDelete:
		a.handleDeletion(w, r)
	case method == "GET" && path == APIPathSelect:
		a.handleSelect(w, r)
	case method == "GET" && path == APIPathKeys:
		a.handleKeys(w, r)
	case method == "GET" && path == APIPathSize:
		a.handleSize(w, r)
	case method == "GET" && path == APIPathMembers:
		a.handleMembers(w, r)
	case method == "GET" && path == APIPathScore:
		a.handleScore(w, r)
	default:
		// Nothing found
		a.errors.NotFound(w, r)
	}
}

func (a *API) run() {
	for {
		select {
		case f := <-a.action:
			f()

		case c := <-a.stop:
			close(c)
			return
		}
	}
}

// Close out the API
func (a *API) Close() {
	c := make(chan struct{})
	a.stop <- c
	<-c
}

func (a *API) handleInsertion(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp KeyQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryRequired); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	members, err := ingestMembers(r.Body)
	if err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	var (
		internalError = make(chan error)
		result        = make(chan selectors.ChangeSet)
	)
	a.action <- func() {
		changeSet, err := a.store.Insert(qp.Key(), members)
		if err != nil {
			internalError <- err
			return
		}
		result <- changeSet
	}

	select {
	case err := <-internalError:
		a.errors.Error(w, err.Error(), http.StatusInternalServerError)
	case changeSet := <-result:
		// Make sure we collect the document for the result.
		qr := ChangeSetQueryResult{Errors: a.errors, Params: qp}
		qr.ChangeSet = changeSet

		// Finish
		qr.Duration = time.Since(begin).String()
		qr.EncodeTo(w)
	}
}

func (a *API) handleDeletion(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp KeyQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryRequired); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	members, err := ingestMembers(r.Body)
	if err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	var (
		internalError = make(chan error)
		result        = make(chan selectors.ChangeSet)
	)
	a.action <- func() {
		changeSet, err := a.store.Delete(qp.Key(), members)
		if err != nil {
			internalError <- err
			return
		}
		result <- changeSet
	}

	select {
	case err := <-internalError:
		a.errors.Error(w, err.Error(), http.StatusInternalServerError)
	case changeSet := <-result:
		// Make sure we collect the document for the result.
		qr := ChangeSetQueryResult{Errors: a.errors, Params: qp}
		qr.ChangeSet = changeSet

		// Finish
		qr.Duration = time.Since(begin).String()
		qr.EncodeTo(w)
	}
}

func (a *API) handleSelect(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp KeyFieldQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryOptional); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	member, err := a.store.Select(qp.Key(), qp.Field())
	if err != nil {
		if selectors.NotFoundError(err) {
			a.errors.NotFound(w, r)
		} else {
			a.errors.InternalServerError(w, r, err.Error())
		}
		return
	}

	// Make sure we collect the document for the result.
	qr := FieldValueScoreQueryResult{Errors: a.errors, Params: qp}
	qr.FieldValueScore = member

	// Finish
	qr.Duration = time.Since(begin).String()
	qr.EncodeTo(w)
}

func (a *API) handleKeys(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	keys, err := a.store.Keys()
	if err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Make sure we collect the document for the result.
	qr := KeysQueryResult{Errors: a.errors}
	qr.Keys = keys

	// Finish
	qr.Duration = time.Since(begin).String()
	qr.EncodeTo(w)
}

func (a *API) handleSize(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp KeyQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryOptional); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	size, err := a.store.Size(qp.Key())
	if err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Make sure we collect the document for the result.
	qr := Int64QueryResult{Errors: a.errors, Params: qp}
	qr.Integer = size

	// Finish
	qr.Duration = time.Since(begin).String()
	qr.EncodeTo(w)
}

func (a *API) handleMembers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp KeyQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryOptional); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	members, err := a.store.Members(qp.Key())
	if err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Make sure we collect the document for the result.
	qr := FieldsQueryResult{Errors: a.errors, Params: qp}
	qr.Fields = members

	// Finish
	qr.Duration = time.Since(begin).String()
	qr.EncodeTo(w)
}

func (a *API) handleScore(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// useful metrics
	begin := time.Now()

	// Validate user input.
	var qp KeyFieldQueryParams
	if err := qp.DecodeFrom(r.URL, r.Header, queryOptional); err != nil {
		a.errors.BadRequest(w, r, err.Error())
		return
	}

	presence, err := a.store.Score(qp.Key(), qp.Field())
	if err != nil {
		a.errors.InternalServerError(w, r, err.Error())
		return
	}

	// Make sure we collect the document for the result.
	qr := PresenceQueryResult{Errors: a.errors, Params: qp}
	qr.Presence = presence

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

func ingestMembers(reader io.ReadCloser) ([]selectors.FieldValueScore, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if len(bytes) < 1 {
		return nil, errors.New("no body content")
	}

	var input api.MembersInput
	if err = json.Unmarshal(bytes, &input); err != nil {
		return nil, err
	}

	res := make([]selectors.FieldValueScore, len(input.Members))
	for k, v := range input.Members {
		res[k] = selectors.FieldValueScore{
			Field: selectors.Field(v.Field),
			Value: v.Value,
			Score: v.Score,
		}
	}

	return res, nil
}
