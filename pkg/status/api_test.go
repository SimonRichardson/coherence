package status

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/farm/mocks"
	metricMocks "github.com/trussle/coherence/pkg/metrics/mocks"
	"github.com/trussle/harness/matchers"
)

func TestAPI(t *testing.T) {
	t.Parallel()

	t.Run("liveness", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			farm     = mocks.NewMockFarm(ctrl)
			api      = NewAPI(farm, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("GET", "/health", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(matchers.MatchAnyFloat64()).Times(1)

		response, err := http.Get(fmt.Sprintf("%s/health", server.URL))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("readiness", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			farm     = mocks.NewMockFarm(ctrl)
			api      = NewAPI(farm, log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("GET", "/ready", "200").Return(observer).Times(1)
		observer.EXPECT().Observe(matchers.MatchAnyFloat64()).Times(1)

		farm.EXPECT().Keys().Times(1)

		response, err := http.Get(fmt.Sprintf("%s/ready", server.URL))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}
