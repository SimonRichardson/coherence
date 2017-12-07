package nodes

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestRemoteKeys(t *testing.T) {
	t.Parallel()

	t.Run("keys with post http error", func(t *testing.T) {
		fn := func() bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/keys", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Keys()

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

	t.Run("keys with json error", func(t *testing.T) {
		fn := func() bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/keys", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Keys()

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

	t.Run("keys", func(t *testing.T) {
		fn := func(keys []selectors.Key) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/keys", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				if err := json.NewEncoder(w).Encode(struct {
					Records []selectors.Key `json:"records"`
				}{
					Records: keys,
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Keys()

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.KeysFromElement(element)

				if expected, actual := keys, got; !reflect.DeepEqual(expected, actual) {
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

func TestRemoteSize(t *testing.T) {
	t.Parallel()

	t.Run("size with post http error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/size", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Size(key)

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

	t.Run("size with json error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/size", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Size(key)

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

	t.Run("size", func(t *testing.T) {
		fn := func(key selectors.Key, size int64) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/size", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				if err := json.NewEncoder(w).Encode(struct {
					Records int64 `json:"records"`
				}{
					Records: size,
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Size(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.Int64FromElement(element)

				if expected, actual := size, got; expected != actual {
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

func TestRemoteMembers(t *testing.T) {
	t.Parallel()

	t.Run("members with post http error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/members", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Members(key)

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

	t.Run("members with json error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/members", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Members(key)

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

	t.Run("members", func(t *testing.T) {
		fn := func(key selectors.Key, fields []selectors.Field) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/members", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				if err := json.NewEncoder(w).Encode(struct {
					Records []selectors.Field `json:"records"`
				}{
					Records: fields,
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Members(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.FieldsFromElement(element)

				if expected, actual := fields, got; !reflect.DeepEqual(expected, actual) {
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

func TestRemoteScore(t *testing.T) {
	t.Parallel()

	t.Run("score with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/members", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusNotFound)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Score(key, field)

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

	t.Run("score with json error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/score", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("!!"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Score(key, field)

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

	t.Run("score", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/score", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				if err := json.NewEncoder(w).Encode(struct {
					Records selectors.Presence `json:"records"`
				}{
					Records: selectors.Presence{
						Inserted: false,
						Present:  true,
						Score:    1,
					},
				}); err != nil {
					t.Fatal(err)
				}
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := client.New(http.DefaultClient, server.URL)
			node := NewRemote(client)
			ch := node.Score(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.PresenceFromElement(element)
				want := selectors.Presence{
					Inserted: false,
					Present:  true,
					Score:    1,
				}

				if expected, actual := want, got; !expected.Equal(actual) {
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