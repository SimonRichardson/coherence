package hashring

import (
	"reflect"
	"testing"

	"github.com/spaolacci/murmur3"

	apiMocks "github.com/SimonRichardson/coherence/pkg/api/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
)

func TestNodeSet(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		strategy := apiMocks.NewMockTransportStrategy(ctrl)

		nodeSet := NewNodeSet(peer, strategy, 3, log.NewNopLogger())
		nodes := nodeSet.Snapshot(selectors.Key("a"), selectors.Strong)

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateNodes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8080")))
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8081")))

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		nodeSet := NewNodeSet(peer, strategy, 3, log.NewNopLogger())
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := nodeSet.Snapshot(selectors.Key("a"), selectors.Strong)
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("0.0.0.0:8080")),
			murmur3.Sum32([]byte("0.0.0.0:8081")),
		}, extractAddresses(nodes); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateNodes twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		transport := apiMocks.NewMockTransport(ctrl)
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8080")))
		transport.EXPECT().Hash().Return(murmur3.Sum32([]byte("0.0.0.0:8081")))

		strategy := apiMocks.NewMockTransportStrategy(ctrl)
		strategy.EXPECT().Apply("0.0.0.0:8080").Return(transport)
		strategy.EXPECT().Apply("0.0.0.0:8081").Return(transport)

		nodeSet := NewNodeSet(peer, strategy, 3, log.NewNopLogger())
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := nodeSet.Snapshot(selectors.Key("a"), selectors.Strong)
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("0.0.0.0:8080")),
			murmur3.Sum32([]byte("0.0.0.0:8081")),
		}, extractAddresses(nodes); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
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
