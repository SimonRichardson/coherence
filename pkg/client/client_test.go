package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/quick"
)

func TestClient(t *testing.T) {
	t.Parallel()

	t.Run("send", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			w.WriteHeader(http.StatusOK)
		})
		server := httptest.NewServer(mux)

		fn := func(b []byte) bool {
			client := NewClient(http.DefaultClient)
			_, err := client.Post(server.URL, b)
			return err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("send with failure", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			w.WriteHeader(http.StatusNotFound)
		})
		server := httptest.NewServer(mux)

		fn := func(b []byte) bool {
			client := NewClient(http.DefaultClient)
			_, err := client.Post(server.URL, b)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("send with url failure", func(t *testing.T) {
		client := NewClient(http.DefaultClient)
		_, err := client.Post("!!", nil)
		if expected, actual := true, err != nil; expected != actual {
			t.Errorf("expected: %t, actual: %t, err: %v", expected, actual, err)
		}
	})
}
