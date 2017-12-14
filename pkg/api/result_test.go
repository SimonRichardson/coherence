package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/go-kit/kit/log"
	errs "github.com/SimonRichardson/coherence/pkg/api/http"
	"github.com/SimonRichardson/coherence/pkg/selectors"
)

func TestChangeSetQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo includes the correct headers", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ChangeSetQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.EncodeTo(recorder)

			headers := recorder.Header()
			return headers.Get(httpHeaderKey) == key.String()
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("EncodeTo with no content has correct status code", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ChangeSetQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.EncodeTo(recorder)

			return recorder.Code == 200
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("EncodeTo with no content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ChangeSetQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.EncodeTo(recorder)

			return len(recorder.Body.Bytes()) > 0
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ChangeSetQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.ChangeSet = selectors.ChangeSet{
				Success: []selectors.Field{field},
				Failure: []selectors.Field{},
			}

			res.EncodeTo(recorder)

			var cs struct {
				Records selectors.ChangeSet `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return cs.Records.Equal(res.ChangeSet)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestKeysQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, keys []selectors.Key) bool {

			recorder := httptest.NewRecorder()

			res := KeysQueryResult{Errors: errs.NewError(log.NewNopLogger())}
			res.Keys = keys

			res.EncodeTo(recorder)

			var cs struct {
				Records []selectors.Key `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(cs.Records, keys)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestInt64QueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, value int64) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := Int64QueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.Integer = value

			res.EncodeTo(recorder)

			var cs struct {
				Records int64 `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(cs.Records, value)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFieldsQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, value []selectors.Field) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := FieldsQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.Fields = value

			res.EncodeTo(recorder)

			var cs struct {
				Records []selectors.Field `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(cs.Records, value)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestPresenceQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field, value selectors.Presence) bool {
			var (
				qp KeyFieldQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s&field=%s", key.String(), field.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := PresenceQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.Presence = value

			res.EncodeTo(recorder)

			var cs struct {
				Records selectors.Presence `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(cs.Records, value)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFieldScoreQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field, value selectors.FieldScore) bool {
			var (
				qp KeyFieldQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s&field=%s", key.String(), field.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := FieldScoreQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.FieldScore = value

			res.EncodeTo(recorder)

			var cs struct {
				Records selectors.FieldScore `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(cs.Records, value)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFieldValueScoreQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo with content has correct body", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field, value selectors.FieldValueScore) bool {
			var (
				qp KeyFieldQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s&field=%s", key.String(), field.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Add("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := FieldValueScoreQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.FieldValueScore = value

			res.EncodeTo(recorder)

			var cs struct {
				Records selectors.FieldValueScore `json:"records"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &cs); err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(cs.Records, value)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
