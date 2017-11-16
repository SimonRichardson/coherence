package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	metricMocks "github.com/trussle/coherence/pkg/metrics/mocks"
)

func TestAPIIntersect(t *testing.T) {
	t.Parallel()

	t.Run("intersect with bad body", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("POST", "/intersect", "400").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		response, err := http.Post(fmt.Sprintf("%s/intersect", server.URL), defaultContentType, bytes.NewBufferString("{!}"))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()

		if expected, actual := http.StatusBadRequest, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("intersect with no replicate", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("POST", "/intersect", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		idents := []string{"a", "b", "c"}
		b, err := json.Marshal(IngestInput{
			Identifiers: idents,
		})
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.Post(fmt.Sprintf("%s/intersect", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()

		b, err = ioutil.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}

		var intersections Intersections
		if err := json.Unmarshal(b, &intersections); err != nil {
			t.Fatal(err)
		}

		if expected, actual := []string{}, intersections.Union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{"a", "b", "c"}, intersections.Difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("intersect with no body", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(2)
		clients.EXPECT().Dec().Times(2)

		duration.EXPECT().WithLabelValues("POST", "/replicate", "200").Return(observer).Times(1)
		duration.EXPECT().WithLabelValues("POST", "/intersect", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(2)

		idents := []string{}
		b, err := json.Marshal(IngestInput{
			Identifiers: idents,
		})
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.Post(fmt.Sprintf("%s/replicate", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		response, err = http.Post(fmt.Sprintf("%s/intersect", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()

		b, err = ioutil.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}

		var intersections Intersections
		if err := json.Unmarshal(b, &intersections); err != nil {
			t.Fatal(err)
		}

		if expected, actual := []string{}, intersections.Union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{}, intersections.Difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("intersect", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(2)
		clients.EXPECT().Dec().Times(2)

		duration.EXPECT().WithLabelValues("POST", "/replicate", "200").Return(observer).Times(1)
		duration.EXPECT().WithLabelValues("POST", "/intersect", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(2)

		idents := []string{"a", "b", "c"}
		b, err := json.Marshal(IngestInput{
			Identifiers: idents,
		})
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.Post(fmt.Sprintf("%s/replicate", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		response, err = http.Post(fmt.Sprintf("%s/intersect", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		b, err = ioutil.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}

		var intersections Intersections
		if err := json.Unmarshal(b, &intersections); err != nil {
			t.Fatal(err)
		}

		if expected, actual := []string{"a", "b", "c"}, intersections.Union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{}, intersections.Difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestAPIReplicate(t *testing.T) {
	t.Parallel()

	t.Run("replicate with bad body", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("POST", "/replicate", "400").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		response, err := http.Post(fmt.Sprintf("%s/replicate", server.URL), defaultContentType, bytes.NewBufferString("{!}"))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusBadRequest, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("replicate with no body", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("POST", "/replicate", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		idents := []string{}
		b, err := json.Marshal(IngestInput{
			Identifiers: idents,
		})
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.Post(fmt.Sprintf("%s/replicate", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		union, difference, err := cache.Intersection(idents)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := []string{}, union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{}, difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("replicate", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			cache    = newVirtualCache(10)
			api      = NewAPI(cache, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("POST", "/replicate", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		idents := []string{"a", "b", "c"}
		b, err := json.Marshal(IngestInput{
			Identifiers: idents,
		})
		if err != nil {
			t.Fatal(err)
		}

		response, err := http.Post(fmt.Sprintf("%s/replicate", server.URL), defaultContentType, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		union, difference, err := cache.Intersection(idents)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := []string{"a", "b", "c"}, union; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{}, difference; !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestAPINotFound(t *testing.T) {
	t.Parallel()

	t.Run("not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			api      = NewAPI(newNopCache(), log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("GET", "/bad", "404").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		response, err := http.Get(fmt.Sprintf("%s/bad", server.URL))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusNotFound, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}

type float64Matcher struct{}

func (float64Matcher) Matches(x interface{}) bool {
	_, ok := x.(float64)
	return ok
}

func (float64Matcher) String() string {
	return "is float64"
}

func Float64() gomock.Matcher { return float64Matcher{} }
