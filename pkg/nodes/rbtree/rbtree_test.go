package rbtree

import (
	"fmt"
	"testing"
	"testing/quick"

	"github.com/pkg/errors"
)

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

func makeTreeWithAmount(amount int) *RBTree {
	tree := NewRBTree()

	for i := 1; i <= amount; i++ {
		tree.Insert(TestKey(i), fmt.Sprintf("%d", i))
	}

	return tree
}

func makeTree() *RBTree {
	return makeTreeWithAmount(3)
}

func verifyRBTree(n *RBNode) error {
	if n == nil {
		return nil
	}

	if isRed(n) && (isRed(n.left) || isRed(n.right)) {
		return errors.Errorf("red violation with key %v", n.key)
	}

	if err := verifyRBTree(n.left); err != nil {
		return err
	}
	if err := verifyRBTree(n.right); err != nil {
		return err
	}

	if n.left != nil && n.left.key.Compare(n.key) >= 0 ||
		n.right != nil && n.right.key.Compare(n.key) <= 0 {
		return errors.Errorf("binary tree violation with key %v", n.key)
	}
	return nil
}

func blackHeight(node *RBNode) int {
	if node == nil {
		return 1
	}

	leftHeight, rightHeight := blackHeight(node.left), blackHeight(node.right)
	if leftHeight == 0 || rightHeight == 0 {
		return 0
	}

	if leftHeight != rightHeight {
		return 0
	}
	if isRed(node) {
		return leftHeight
	}
	return leftHeight + 1
}

func TestInsert(t *testing.T) {

	t.Run("insert", func(t *testing.T) {
		fn := func(a uint) bool {
			var (
				amount = int(a%360) + 1
				tree   = makeTreeWithAmount(amount)
			)

			err := verifyRBTree(tree.Root())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
			}
			if expected, actual := 1, blackHeight(tree.Root()); actual <= expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := amount, tree.Size(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("manual", func(t *testing.T) {
		//               4,B
		//             /     \
		//         2,R         6,R
		//       /     \     /     \
		//     1,B    3,B   5,B    7,B
		//                             \
		//                              8,R

		tree := makeTreeWithAmount(8)

		err := verifyRBTree(tree.Root())
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
		if expected, actual := 3, blackHeight(tree.Root()); actual != expected {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 8, tree.Size(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if err := verifyNode(tree.root, TestKey(4), Black, Both); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.left, TestKey(2), Red, Both); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.left.left, TestKey(1), Black, None); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.left.right, TestKey(3), Black, None); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right, TestKey(6), Red, Both); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right.left, TestKey(5), Black, None); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right.right, TestKey(7), Black, Right); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right.right.right, TestKey(8), Red, None); err != nil {
			t.Error(err)
		}
	})
}

type Presence int

const (
	Left Presence = iota
	Right
	Both
	None
)

func (p Presence) Left() bool {
	return p == Left || p == Both
}

func (p Presence) Right() bool {
	return p == Right || p == Both
}

func verifyNode(node *RBNode, key TestKey, nodeType NodeType, presence Presence) error {
	if expected, actual := key, node.key; expected != actual {
		return errors.Errorf("node key - expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := fmt.Sprintf("%d", int(key)), node.str; expected != actual {
		return errors.Errorf("node value - expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := nodeType, node.nodeType; expected != actual {
		return errors.Errorf("node type - expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := presence.Left(), node.left != nil; expected != actual {
		return errors.Errorf("node left - expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := presence.Right(), node.right != nil; expected != actual {
		return errors.Errorf("node right - expected: %v, actual: %v", expected, actual)
	}
	return nil
}
