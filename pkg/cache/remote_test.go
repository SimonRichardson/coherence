package cache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/cluster"
	"github.com/trussle/coherence/pkg/cluster/mocks"
)

func TestRemoteAdd(t *testing.T) {
	t.Parallel()

	t.Run("add without zero instances", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock   = mocks.NewMockPeer(ctrl)
			config = &RemoteConfig{
				ReplicationFactor: 10,
				Peer:              mock,
			}
			instances = []string{}
			cache     = newRemoteCache(100, config, log.NewNopLogger())
		)

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := cache.Add([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add without meeting replication factor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock   = mocks.NewMockPeer(ctrl)
			config = &RemoteConfig{
				ReplicationFactor: 10,
				Peer:              mock,
			}
			instances = []string{
				"http://a.com",
				"http://b.com",
			}
			cache = newRemoteCache(100, config, log.NewNopLogger())
		)

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := cache.Add([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add with post failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: len(instances),
				Peer:              mock,
			}
			cache = newRemoteCache(2, config, log.NewNopLogger())
		)

		handle := func(k int) func(http.ResponseWriter, *http.Request) {
			return func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				if k == 0 {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
			}
		}

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", handle(k))

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := cache.Add([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: len(instances),
				Peer:              mock,
			}
			cache = newRemoteCache(2, config, log.NewNopLogger())
		)

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
			})

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := cache.Add([]string{"a", "b"})
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func TestRemoteIntersection(t *testing.T) {
	t.Parallel()

	t.Run("intersection without meeting replication factor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: 100,
				Peer:              mock,
			}
			cache = newRemoteCache(2, config, log.NewNopLogger())
		)

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
			})

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		_, _, err := cache.Intersection([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("intersection with post failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: len(instances),
				Peer:              mock,
			}
			cache = newRemoteCache(2, config, log.NewNopLogger())
		)

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusInternalServerError)
			})

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		_, _, err := cache.Intersection([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("intersection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: len(instances),
				Peer:              mock,
			}
			cache = newRemoteCache(2, config, log.NewNopLogger())
		)

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(Intersections{
					Union:      []string{"a"},
					Difference: []string{"b"},
				})
			})

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		union, difference, err := cache.Intersection([]string{"a", "b"})
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		if expected, actual := []string{"a"}, union; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []string{"b"}, difference; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
