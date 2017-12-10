package rbtree

import "testing"

type TestKey int

func (t TestKey) Compare(b Key) int {
	return int(t) - int(b.(TestKey))
}

func TestEmptyTree(t *testing.T) {
	t.Parallel()

	t.Run("root", func(t *testing.T) {
		tree := NewRBTree()

		if expected, actual := true, tree.Root() == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("size", func(t *testing.T) {
		tree := NewRBTree()

		if expected, actual := 0, tree.Size(); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
