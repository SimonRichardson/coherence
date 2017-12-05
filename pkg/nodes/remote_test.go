package nodes

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/quick"

	"github.com/trussle/coherence/pkg/client"
	"github.com/trussle/coherence/pkg/selectors"
)

func TestRemoteInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/insert", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Insert(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("insert with json error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/insert", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Insert(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/insert", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				bytes, err := ioutil.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}

				var input struct {
					Members []selectors.FieldValueScore `json:"members"`
				}
				if err := json.Unmarshal(bytes, &input); err != nil {
					t.Fatal(err)
				}
				for k, v := range input.Members {
					if expected, actual := members[k], v; !expected.Equal(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
				}

				if err := json.NewEncoder(w).Encode(struct {
					Records selectors.ChangeSet `json:"records"`
				}{
					Records: selectors.ChangeSet{
						Success: extractFields(members),
						Failure: make([]selectors.Field, 0),
					},
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Insert(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)
				want := selectors.ChangeSet{
					Success: extractFields(members),
					Failure: make([]selectors.Field, 0),
				}

				if expected, actual := want, changeSet; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRemoteDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/delete", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Delete(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete with json error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/delete", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Delete(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/delete", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				bytes, err := ioutil.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}

				var input struct {
					Members []selectors.FieldValueScore `json:"members"`
				}
				if err := json.Unmarshal(bytes, &input); err != nil {
					t.Fatal(err)
				}
				for k, v := range input.Members {
					if expected, actual := members[k], v; !expected.Equal(actual) {
						t.Errorf("expected: %v, actual: %v", expected, actual)
					}
				}

				if err := json.NewEncoder(w).Encode(struct {
					Records selectors.ChangeSet `json:"records"`
				}{
					Records: selectors.ChangeSet{
						Success: extractFields(members),
						Failure: make([]selectors.Field, 0),
					},
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Delete(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)
				want := selectors.ChangeSet{
					Success: extractFields(members),
					Failure: make([]selectors.Field, 0),
				}

				if expected, actual := want, changeSet; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRemoteSelect(t *testing.T) {
	t.Parallel()

	t.Run("select with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/select", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Select(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("select with json error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/select", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Select(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("select", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field, value []byte) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/select", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				if err := json.NewEncoder(w).Encode(struct {
					Records selectors.FieldValueScore `json:"records"`
				}{
					Records: selectors.FieldValueScore{
						Field: field,
						Value: value,
						Score: 1,
					},
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Select(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				fieldValueScore := selectors.FieldValueScoreFromElement(element)
				want := selectors.FieldValueScore{
					Field: field,
					Value: value,
					Score: 1,
				}

				if expected, actual := want, fieldValueScore; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
