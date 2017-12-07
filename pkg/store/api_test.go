package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/quick"

	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/selectors"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	metricMocks "github.com/trussle/coherence/pkg/metrics/mocks"
	storeMocks "github.com/trussle/coherence/pkg/store/mocks"
	"github.com/trussle/harness/matchers"
)

func TestInsertAPI(t *testing.T) {
	t.Parallel()

	t.Run("post with no key", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fn := func(members []selectors.FieldValueScore) bool {
			var (
				clients  = metricMocks.NewMockGauge(ctrl)
				duration = metricMocks.NewMockHistogramVec(ctrl)
				observer = metricMocks.NewMockObserver(ctrl)
				store    = storeMocks.NewMockStore(ctrl)

				api    = NewAPI(store, log.NewNopLogger(), clients, duration)
				server = httptest.NewServer(api)
			)
			defer api.Close()

			clients.EXPECT().Inc().Times(1)
			clients.EXPECT().Dec().Times(1)

			duration.EXPECT().WithLabelValues("POST", "/insert", "400").Return(observer).Times(1)
			observer.EXPECT().Observe(matchers.MatchAnyFloat64()).Times(1)

			input := MembersInput{
				Members: members,
			}
			b, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := http.Post(fmt.Sprintf("%s/insert", server.URL), "application/json", bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("post with error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			if len(members) == 0 {
				return true
			}

			var (
				clients  = metricMocks.NewMockGauge(ctrl)
				duration = metricMocks.NewMockHistogramVec(ctrl)
				observer = metricMocks.NewMockObserver(ctrl)
				store    = storeMocks.NewMockStore(ctrl)

				api    = NewAPI(store, log.NewNopLogger(), clients, duration)
				server = httptest.NewServer(api)
			)
			defer api.Close()

			clients.EXPECT().Inc().Times(1)
			clients.EXPECT().Dec().Times(1)

			duration.EXPECT().WithLabelValues("POST", "/insert", "500").Return(observer).Times(1)
			observer.EXPECT().Observe(matchers.MatchAnyFloat64()).Times(1)

			for _, v := range members {
				store.EXPECT().Insert(key, v).Return(selectors.ChangeSet{}, errors.New("bad"))
				break
			}

			input := MembersInput{
				Members: members,
			}
			b, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := http.Post(fmt.Sprintf("%s/insert?key=%s", server.URL, key.String()), "application/json", bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			return resp.StatusCode == http.StatusInternalServerError
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("post", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			var (
				clients  = metricMocks.NewMockGauge(ctrl)
				duration = metricMocks.NewMockHistogramVec(ctrl)
				observer = metricMocks.NewMockObserver(ctrl)
				store    = storeMocks.NewMockStore(ctrl)

				api    = NewAPI(store, log.NewNopLogger(), clients, duration)
				server = httptest.NewServer(api)
			)
			defer api.Close()

			clients.EXPECT().Inc().Times(1)
			clients.EXPECT().Dec().Times(1)

			duration.EXPECT().WithLabelValues("POST", "/insert", "200").Return(observer).Times(1)
			observer.EXPECT().Observe(matchers.MatchAnyFloat64()).Times(1)

			for _, v := range members {
				store.EXPECT().Insert(key, v).Return(selectors.ChangeSet{
					Success: extractFields(members),
					Failure: make([]selectors.Field, 0),
				}, nil)
			}

			input := MembersInput{
				Members: members,
			}
			b, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := http.Post(fmt.Sprintf("%s/insert?key=%s", server.URL, key.String()), "application/json", bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			rb, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}

			var cs struct {
				Records selectors.ChangeSet `json:"records"`
			}
			if err := json.Unmarshal(rb, &cs); err != nil {
				t.Fatal(err)
			}

			/*
				cs.Records.Equal(selectors.ChangeSet{
					Success: extractFields(members),
					Failure: make([]selectors.Field, 0),
				})
			*/

			return true
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func extractFields(members []selectors.FieldValueScore) []selectors.Field {
	res := make([]selectors.Field, len(members))
	for k, v := range members {
		res[k] = v.Field
	}
	return res
}
