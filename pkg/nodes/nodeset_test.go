package nodes

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/cluster/mocks"
)

func TestNodeSet(t *testing.T) {
	t.Parallel()

	t.Run("snapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)

		nodeSet := NewNodeSet(peer)
		nodes := nodeSet.Snapshot()

		if expected, actual := 0, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateNodes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		peer.EXPECT().Address().Return("0.0.0.0:8080")

		nodeSet := NewNodeSet(peer)
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := nodeSet.Snapshot()
		if expected, actual := []string{
			"http://0.0.0.0:8081",
		}, extractAddresses(nodes); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("updateNodes twice, has no duplicates", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		peer := mocks.NewMockPeer(ctrl)
		peer.EXPECT().Address().Return("0.0.0.0:8080").Times(2)

		nodeSet := NewNodeSet(peer)
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})
		nodeSet.updateNodes([]string{
			"0.0.0.0:8080",
			"0.0.0.0:8081",
		})

		nodes := nodeSet.Snapshot()
		if expected, actual := []string{
			"http://0.0.0.0:8081",
		}, extractAddresses(nodes); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func extractAddresses(nodes []Node) []string {
	res := make([]string, 0)
	for _, v := range nodes {
		if r, ok := v.(remoteNode); ok {
			res = append(res, r.Host())
		}
	}
	return res
}
