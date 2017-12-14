package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/quick"

	"github.com/go-kit/kit/log"
)

func TestError(t *testing.T) {
	t.Parallel()

	t.Run("writes error", func(t *testing.T) {
		fn := func(desc string, code int) bool {
			w := httptest.NewRecorder()

			e := NewError(log.NewNopLogger())
			e.Error(w, desc, code)

			var res struct {
				Description string `json:"description"`
				Code        int    `json:"code"`
			}

			b := w.Body.Bytes()
			if err := json.Unmarshal(b, &res); err != nil {
				t.Fatal(err)
			}

			if expected, actual := w.Code, code; expected != actual {
				t.Fatalf("expected: %d, actual: %d", expected, actual)
			}

			return res.Description == desc && res.Code == code
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("writes not found", func(t *testing.T) {
		fn := func() bool {
			w := httptest.NewRecorder()

			e := NewError(log.NewNopLogger())
			e.NotFound(w, nil)

			var res struct {
				Description string `json:"description"`
				Code        int    `json:"code"`
			}

			b := w.Body.Bytes()
			if err := json.Unmarshal(b, &res); err != nil {
				t.Fatal(err)
			}

			return res.Description == "not found" && res.Code == http.StatusNotFound
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("writes bad request", func(t *testing.T) {
		fn := func(desc string) bool {
			w := httptest.NewRecorder()

			e := NewError(log.NewNopLogger())
			e.BadRequest(w, nil, desc)

			var res struct {
				Description string `json:"description"`
				Code        int    `json:"code"`
			}

			b := w.Body.Bytes()
			if err := json.Unmarshal(b, &res); err != nil {
				t.Fatal(err)
			}

			return res.Description == desc && res.Code == http.StatusBadRequest
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("writes internal server error", func(t *testing.T) {
		fn := func(desc string) bool {
			w := httptest.NewRecorder()

			e := NewError(log.NewNopLogger())
			e.InternalServerError(w, nil, desc)

			var res struct {
				Description string `json:"description"`
				Code        int    `json:"code"`
			}

			b := w.Body.Bytes()
			if err := json.Unmarshal(b, &res); err != nil {
				t.Fatal(err)
			}

			return res.Description == desc && res.Code == http.StatusInternalServerError
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
