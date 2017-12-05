package nodes

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/quick"

	"github.com/trussle/coherence/pkg/client"
	"github.com/trussle/coherence/pkg/selectors"
)

func TestRemoteInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			mux := http.NewServeMux()
			mux.HandleFunc("/store/insert", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)

				if err := json.NewEncoder(w).Encode(struct {
					Records selectors.ChangeSet `json:"records"`
				}{
					Records: selectors.ChangeSet{
						Success: extractFields(members),
						Failure: make([]selectors.Field, 0),
					},
				}); err != nil {
					panic(err)
				}
			})
			server := httptest.NewServer(mux)

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

				if expected, actual := want, changeSet; !expected.Equal(changeSet) {
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
