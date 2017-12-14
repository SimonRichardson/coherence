package api

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/coherence/pkg/selectors"
)

func TestKeyQueryParams(t *testing.T) {
	t.Parallel()

	t.Run("DecodeFrom with required empty url", func(t *testing.T) {
		var (
			qp KeyQueryParams

			h      = make(http.Header)
			u, err = url.Parse("")
		)
		if err != nil {
			t.Fatal(err)
		}

		err = qp.DecodeFrom(u, h, queryRequired)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("DecodeFrom with optional empty url", func(t *testing.T) {
		var (
			qp KeyQueryParams

			h      = make(http.Header)
			u, err = url.Parse("")
		)
		if err != nil {
			t.Fatal(err)
		}

		err = qp.DecodeFrom(u, h, queryOptional)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("DecodeFrom with invalid header", func(t *testing.T) {
		var (
			qp KeyQueryParams

			h      = make(http.Header)
			u, err = url.Parse("/?key=123asd")
		)
		if err != nil {
			t.Fatal(err)
		}

		err = qp.DecodeFrom(u, h, queryRequired)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("DecodeFrom with valid key", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			var (
				qp KeyQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return key.Equal(qp.key)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestKeyFieldQueryParams(t *testing.T) {
	t.Parallel()

	t.Run("DecodeFrom with required empty url", func(t *testing.T) {
		var (
			qp KeyFieldQueryParams

			h      = make(http.Header)
			u, err = url.Parse("")
		)
		if err != nil {
			t.Fatal(err)
		}

		err = qp.DecodeFrom(u, h, queryRequired)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("DecodeFrom with optional empty url", func(t *testing.T) {
		var (
			qp KeyFieldQueryParams

			h      = make(http.Header)
			u, err = url.Parse("")
		)
		if err != nil {
			t.Fatal(err)
		}

		err = qp.DecodeFrom(u, h, queryOptional)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("DecodeFrom with invalid header", func(t *testing.T) {
		var (
			qp KeyFieldQueryParams

			h      = make(http.Header)
			u, err = url.Parse("/?key=123asd&field=abc123")
		)
		if err != nil {
			t.Fatal(err)
		}

		err = qp.DecodeFrom(u, h, queryRequired)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("DecodeFrom with valid key", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			var (
				qp KeyFieldQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s", key.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)

			return err != nil
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("DecodeFrom with valid key and field", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			var (
				qp KeyFieldQueryParams

				h      = make(http.Header)
				u, err = url.Parse(fmt.Sprintf("/?key=%s&field=%s", key.String(), field.String()))
			)
			if err != nil {
				t.Fatal(err)
			}

			h.Set("Content-Type", "application/json")

			err = qp.DecodeFrom(u, h, queryRequired)

			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return key.Equal(qp.key) && field.Equal(qp.field)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
