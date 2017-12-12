package hashring

import (
	"reflect"
	"testing"

	"github.com/spaolacci/murmur3"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/cluster/mocks"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/selectors"
)

func TestNodeSet(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		nodeSet := NewNodeSet(peer, defaultReplicationFactor, log.NewNopLogger())
		nodes := nodeSet.Snapshot(selectors.Key("a"))

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateNodes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		nodeSet := NewNodeSet(peer, defaultReplicationFactor, log.NewNopLogger())
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := nodeSet.Snapshot(selectors.Key("a"))
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("http://0.0.0.0:8080")),
			murmur3.Sum32([]byte("http://0.0.0.0:8081")),
		}, extractAddresses(nodes); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateNodes twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		nodeSet := NewNodeSet(peer, defaultReplicationFactor, log.NewNopLogger())
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := nodeSet.Snapshot(selectors.Key("a"))
		if expected, actual := []uint32{
			murmur3.Sum32([]byte("http://0.0.0.0:8080")),
			murmur3.Sum32([]byte("http://0.0.0.0:8081")),
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
