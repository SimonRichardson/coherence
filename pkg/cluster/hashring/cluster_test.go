package hashring

import (
	"reflect"
	"sort"
	"testing"

	"github.com/spaolacci/murmur3"

	apiMocks "github.com/SimonRichardson/coherence/pkg/api/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
)

func TestClusterRead(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		strategy := apiMocks.NewMockTransportStrategy(ctrl)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		nodes := cluster.Read(selectors.Key("a"), selectors.Strong)

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateRemoteActors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8080")))
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8081")))

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := cluster.Read(selectors.Key("a"), selectors.Strong)
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("0.0.0.0:8080")),
			murmur3.Sum32([]byte("0.0.0.0:8081")),
		}, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateRemoteActors twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8080")))
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8081")))

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := cluster.Read(selectors.Key("a"), selectors.Strong)
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("0.0.0.0:8080")),
			murmur3.Sum32([]byte("0.0.0.0:8081")),
		}, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestClusterWrite(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		strategy := apiMocks.NewMockTransportStrategy(ctrl)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		nodes, _ := cluster.Write(selectors.Key("a"), selectors.Strong)

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateRemoteActors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		a, b := murmur3.Sum32([]byte("0.0.0.0:8080")), murmur3.Sum32([]byte("0.0.0.0:8081"))

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(a)
		transport.EXPECT().Hash().Return(b)

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		want := []uint32{
			a,
			b,
		}

		nodes, finish := cluster.Write(selectors.Key("a"), selectors.Strong)
		if expected, actual := want, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := true, finish(want) == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("updateRemoteActors twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		a, b := murmur3.Sum32([]byte("0.0.0.0:8080")), murmur3.Sum32([]byte("0.0.0.0:8081"))

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(a)
		transport.EXPECT().Hash().Return(b)

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		cluster := NewCluster(peer, strategy, 3, "0.0.0.0:9090", log.NewNopLogger())
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		cluster.updateRemoteActors([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		want := []uint32{
			a,
			b,
		}

		nodes, finish := cluster.Write(selectors.Key("a"), selectors.Strong)
		if expected, actual := want, extractAddresses(nodes); !match(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := true, finish(want) == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func extractAddresses(nodes []nodes.Node) []uint32 {
	res := make([]uint32, 0)
	for _, v := range nodes {
		res = append(res, v.Hash())
	}
	return res
}

func match(a, b []uint32) bool {
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i] < b[j]
	})
	return reflect.DeepEqual(a, b)
}
