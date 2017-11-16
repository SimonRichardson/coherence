package cache

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/quick"

	"github.com/go-kit/kit/log"
	errs "github.com/trussle/coherence/pkg/http"
	"github.com/trussle/harness/generators"
	"github.com/trussle/uuid"
)

func TestReplicationQueryParams(t *testing.T) {
	t.Parallel()

	t.Run("DecodeFrom with invalid content-type", func(t *testing.T) {
		fn := func(uid uuid.UUID, contentType generators.ASCII) bool {
			var (
				qp ReplicationQueryParams

				u, err = url.Parse(fmt.Sprintf("/?resource_id=%s", uid.String()))
				h      = make(http.Header, 0)
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", contentType.String())

			err = qp.DecodeFrom(u, h, queryRequired)

			if expected, actual := false, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("DecodeFrom with invalid content-type", func(t *testing.T) {
		fn := func(uid uuid.UUID) bool {
			var (
				qp ReplicationQueryParams

				u, err = url.Parse(fmt.Sprintf("/?resource_id=%s", uid.String()))
				h      = make(http.Header, 0)
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestReplicationQueryResult(t *testing.T) {
	t.Parallel()

	t.Run("EncodeTo includes the correct headers", func(t *testing.T) {
		fn := func(uid uuid.UUID) bool {
			var (
				qp ReplicationQueryParams

				headers = make(http.Header)
				u, err  = url.Parse("/")
			)
			if err != nil {
				t.Fatal(err)
			}

			headers.Set(httpHeaderContentType, defaultContentType)

			err = qp.DecodeFrom(u, headers, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ReplicationQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.EncodeTo(recorder)

			resHeaders := recorder.Header()
			return resHeaders.Get(httpHeaderContentType) == defaultContentType
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("EncodeTo with no id has correct status code", func(t *testing.T) {
		fn := func(uid uuid.UUID) bool {
			var (
				qp ReplicationQueryParams

				headers = make(http.Header)
				u, err  = url.Parse("/")
			)
			if err != nil {
				t.Fatal(err)
			}

			headers.Set(httpHeaderContentType, defaultContentType)

			err = qp.DecodeFrom(u, headers, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ReplicationQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.EncodeTo(recorder)

			return recorder.Code == 200
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("EncodeTo with no id has correct body", func(t *testing.T) {
		fn := func(uid uuid.UUID) bool {
			var (
				qp ReplicationQueryParams

				headers = make(http.Header)
				u, err  = url.Parse("/")
			)
			if err != nil {
				t.Fatal(err)
			}

			headers.Set(httpHeaderContentType, defaultContentType)

			err = qp.DecodeFrom(u, headers, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ReplicationQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.EncodeTo(recorder)

			type idMarshal struct {
				ID uuid.UUID `json:"id"`
			}
			bytes, err := json.Marshal(idMarshal{
				ID: uuid.Empty,
			})
			if err != nil {
				t.Fatal(err)
			}
			return string(recorder.Body.Bytes()) == string(bytes)+"\n"
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("EncodeTo with a id has correct body", func(t *testing.T) {
		fn := func(uid uuid.UUID) bool {
			var (
				qp ReplicationQueryParams

				headers = make(http.Header)
				u, err  = url.Parse("/")
			)
			if err != nil {
				t.Fatal(err)
			}

			headers.Set(httpHeaderContentType, defaultContentType)

			err = qp.DecodeFrom(u, headers, queryRequired)
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()

			res := ReplicationQueryResult{Errors: errs.NewError(log.NewNopLogger()), Params: qp}
			res.ID = uid
			res.EncodeTo(recorder)

			type idMarshal struct {
				ID uuid.UUID `json:"id"`
			}
			var obj idMarshal
			if err := json.Unmarshal(recorder.Body.Bytes(), &obj); err != nil {
				t.Fatal(err)
			}

			return obj.ID.Equals(uid)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestIntersectionQueryParams(t *testing.T) {
	t.Parallel()

	t.Run("DecodeFrom with invalid content-type", func(t *testing.T) {
		fn := func(uid uuid.UUID, contentType generators.ASCII) bool {
			var (
				qp IntersectionQueryParams

				u, err = url.Parse(fmt.Sprintf("/?resource_id=%s", uid.String()))
				h      = make(http.Header, 0)
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", contentType.String())

			err = qp.DecodeFrom(u, h, queryRequired)

			if expected, actual := false, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("DecodeFrom with invalid content-type", func(t *testing.T) {
		fn := func(uid uuid.UUID) bool {
			var (
				qp IntersectionQueryParams

				u, err = url.Parse(fmt.Sprintf("/?resource_id=%s", uid.String()))
				h      = make(http.Header, 0)
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
