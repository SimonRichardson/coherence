package hashring

import (
	"bytes"
	"reflect"
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

	t.Run("host", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)

		fn := func(data string) bool {
			node.EXPECT().Host().Return(data)

			actor := NewActor(func() nodes.Node {
				return node
			})
			return actor.Host() == data
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

func TestActors(t *testing.T) {
	t.Parallel()

	t.Run("get", func(t *testing.T) {
		fn := func(addr string) bool {
			actors := NewActors()
			_, ok := actors.Get(hash(addr))
			return !ok
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("set and get", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := mocks.NewMockNode(ctrl)

			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			actors := NewActors()
			actors.Set(actor)
			got, ok := actors.Get(hash)

			if expected, actual := actor, got; expected != actual {
				t.Errorf("expected: %T, actual: %T", expected, actual)
			}

			return ok
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("remove", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := mocks.NewMockNode(ctrl)

			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			actors := NewActors()
			actors.Set(actor)
			actors.Remove(hash)
			_, ok := actors.Get(hash)

			return !ok
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("hashes - empty", func(t *testing.T) {
		actors := NewActors()
		got := len(actors.Hashes())
		if expected, actual := 0, got; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("hashes - non empty", func(t *testing.T) {
		fn := func(hash uint32) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := mocks.NewMockNode(ctrl)

			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			actors := NewActors()
			actors.Set(actor)

			return reflect.DeepEqual([]uint32{hash}, actors.Hashes())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("update - match", func(t *testing.T) {
		fn := func(hash uint32, data uuid.UUID) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Hash().Return(hash)

			actor := NewActor(func() nodes.Node {
				return node
			})

			buf := new(bytes.Buffer)
			enc := bloom.New(defaultBloomCapacity, 4)
			enc.Add(data.String())
			if _, err := enc.Write(buf); err != nil {
				t.Error(err)
			}

			actors := NewActors()
			actors.Set(actor)
			return actors.Update(hash, buf.Bytes()) == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("update - non match", func(t *testing.T) {
		fn := func(hash uint32) bool {
			actors := NewActors()
			return actors.Update(hash, []byte{}) == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("string", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		node := mocks.NewMockNode(ctrl)
		node.EXPECT().Host().Return("")
		node.EXPECT().Hash().Return(uint32(0)).Times(2)

		actor := NewActor(func() nodes.Node {
			return node
		})

		actors := NewActors()
		actors.Set(actor)
		if expected, actual := "", actors.String(); expected == actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})
}
