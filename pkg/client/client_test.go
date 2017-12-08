package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/quick"
)

func TestClientGet(t *testing.T) {
	t.Parallel()

	t.Run("get", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			w.WriteHeader(http.StatusOK)
		})
		server := httptest.NewServer(mux)

		fn := func() bool {
			client := New(http.DefaultClient, server.URL)
			_, err := client.Get("")
			return err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("get with failure", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			w.WriteHeader(http.StatusNotFound)
		})
		server := httptest.NewServer(mux)

		fn := func() bool {
			client := New(http.DefaultClient, server.URL)
			_, err := client.Get("")
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("get with url failure", func(t *testing.T) {
		client := New(http.DefaultClient, "")
		_, err := client.Get("!!")
		if expected, actual := true, err != nil; expected != actual {
			t.Errorf("expected: %t, actual: %t, err: %v", expected, actual, err)
		}
	})
}

func TestClientPost(t *testing.T) {
	t.Parallel()

	t.Run("post", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			w.WriteHeader(http.StatusOK)
		})
		server := httptest.NewServer(mux)

		fn := func(b []byte) bool {
			client := New(http.DefaultClient, server.URL)
			_, err := client.Post("", b)
			return err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("post with failure", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			w.WriteHeader(http.StatusNotFound)
		})
		server := httptest.NewServer(mux)

		fn := func(b []byte) bool {
			client := New(http.DefaultClient, server.URL)
			_, err := client.Post("", b)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("post with url failure", func(t *testing.T) {
		client := New(http.DefaultClient, "")
		_, err := client.Post("!!", nil)
		if expected, actual := true, err != nil; expected != actual {
			t.Errorf("expected: %t, actual: %t, err: %v", expected, actual, err)
		}
	})
}
