package hashring

import (
	"bytes"
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/coherence/pkg/cluster/bloom"

	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes/mocks"
	"github.com/golang/mock/gomock"
	"github.com/trussle/harness/generators"
	"github.com/trussle/uuid"
)

func TestActor(t *testing.T) {
	t.Parallel()

	t.Run("contains", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)

		fn := func(data generators.ASCII) bool {
			actor := NewActor(func() nodes.Node {
				return node
			})
			return !actor.Contains(data.String())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("hash", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)

		fn := func(data uint32) bool {
			node.EXPECT().Hash().Return(data)

			actor := NewActor(func() nodes.Node {
				return node
			})
			return actor.Hash() == data
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("time", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)

		actor := NewActor(func() nodes.Node {
			return node
		})
		if expected, actual := uint64(0), actor.Time().Value(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("add", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)

		fn := func(data generators.ASCII) bool {
			actor := NewActor(func() nodes.Node {
				return node
			})
			if err := actor.Add(data.String()); err != nil {
				t.Error(err)
			}
			if expected, actual := uint64(1), actor.Time().Value(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("update", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)

		fn := func(data uuid.UUID) bool {
			actor := NewActor(func() nodes.Node {
				return node
			})

			buf := new(bytes.Buffer)
			enc := bloom.New(defaultBloomCapacity, 4)
			if _, err := enc.Write(buf); err != nil {
				t.Error(err)
			}

			if err := actor.Update(buf.Bytes()); err != nil {
				t.Error(err)
			}
			if expected, actual := uint64(1), actor.Time().Value(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
