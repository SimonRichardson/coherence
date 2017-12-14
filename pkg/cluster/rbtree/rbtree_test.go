package rbtree

import (
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/pkg/errors"
)

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

func TestInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert", func(t *testing.T) {
		fn := func(a uint) bool {
			var (
				amount = int(a%360) + 1
				tree   = makeTreeWithAmount(amount)
			)

			err := verifyTreeStructure(tree.Root())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
			}
			if expected, actual := 1, blackHeight(tree.Root()); actual <= expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("duplication", func(t *testing.T) {
		fn := func(a uint) bool {
			var (
				amount = int(a%100) + 1
				tree   = makeTreeWithAmount(amount)
			)

			err := verifyTreeStructure(tree.Root())
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
			}
			if expected, actual := amount, tree.Size(); actual != expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			for i := 1; i <= 10; i++ {
				if ok := tree.Insert(amount+i, fmt.Sprintf("%d", amount+1)); !ok {
					t.Errorf("expected: %t, actual: %t", true, ok)
				}

				if ok := tree.Insert(1, fmt.Sprintf("%d", 1)); ok {
					t.Errorf("expected: %t, actual: %t", false, ok)
				}

				err := verifyTreeStructure(tree.Root())
				if expected, actual := true, err == nil; expected != actual {
					t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
				}
				if expected, actual := amount+i, tree.Size(); actual != expected {
					t.Errorf("expected: %d, actual: %d", expected, actual)
				}
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("manual", func(t *testing.T) {
		tree := makeTree()

		err := verifyTreeStructure(tree.Root())
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
		if expected, actual := 3, blackHeight(tree.Root()); actual != expected {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 8, tree.Size(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if err := verifyNode(tree.root, 4, Black, Both); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.left, 2, Red, Both); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.left.left, 1, Black, None); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.left.right, 3, Black, None); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right, 6, Red, Both); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right.left, 5, Black, None); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right.right, 7, Black, Right); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root.right.right.right, 8, Red, None); err != nil {
			t.Error(err)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()

	t.Run("remove", func(t *testing.T) {
		tree := makeTree()

		if err := verifyTree(tree, 3, 8); err != nil {
			t.Error(err)
		}

		// Remove 2
		{
			if expected, actual := true, tree.Delete(2); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 7); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 4, Black, Both); err != nil {
				t.Error(err)
			}
		}

		// Remove 4
		{
			if expected, actual := true, tree.Delete(4); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 6); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}
		}

		// Insert 2
		{
			if expected, actual := true, tree.Insert(2, fmt.Sprintf("%d", 2)); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 7); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}
		}

		// Insert 4
		{
			if expected, actual := true, tree.Insert(4, fmt.Sprintf("%d", 2)); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 8); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}
		}
	})

	t.Run("manual", func(t *testing.T) {
		tree := makeTree()

		if err := verifyTree(tree, 3, 8); err != nil {
			t.Error(err)
		}
		if err := verifyNode(tree.root, 4, Black, Both); err != nil {
			t.Error(err)
		}

		// Remove first
		{
			//               4,B
			//             /     \
			//         2,B         6,R
			//             \     /     \
			//            3,R   5,B    7,B
			//                             \
			//                              8,R

			if expected, actual := true, tree.Delete(1); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 7); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 4, Black, Both); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 4, Black, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left, 2, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left.right, 3, Red, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right, 6, Red, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right.left, 5, Black, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right.right, 7, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right.right.right, 8, Red, None); err != nil {
				t.Error(err)
			}
		}

		// Remove second
		{
			//                        6,B
			//                      /     \
			//                  4,R        7,B
			//                 /   \          \
			//               3,B   5,B        8,R

			if expected, actual := true, tree.Delete(2); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 6); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left, 4, Red, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left.left, 3, Black, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left.right, 5, Black, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right, 7, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right.right, 8, Red, None); err != nil {
				t.Error(err)
			}
		}

		// Remove third
		{
			//                        6,B
			//                      /     \
			//                  4,B        7,B
			//                     \          \
			//                     5,R        8,R

			if expected, actual := true, tree.Delete(3); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 5); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left, 4, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left.right, 5, Red, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right, 7, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right.right, 8, Red, None); err != nil {
				t.Error(err)
			}
		}

		// Remove fourth
		{
			//                        6,B
			//                      /     \
			//                  5,B        7,B
			//                                \
			//                                8,R

			if expected, actual := true, tree.Delete(4); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 4); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 6, Black, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left, 5, Black, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right, 7, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right.right, 8, Red, None); err != nil {
				t.Error(err)
			}
		}

		// Remove fifth
		{
			//                        7,B
			//                      /     \
			//                  6,B        8,B

			if expected, actual := true, tree.Delete(5); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 3, 3); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 7, Black, Both); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 7, Black, Both); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.left, 6, Black, None); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right, 8, Black, None); err != nil {
				t.Error(err)
			}
		}

		// Remove sixth
		{
			//                        7,B
			//                            \
			//                             8,R

			if expected, actual := true, tree.Delete(6); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 2, 2); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 7, Black, Right); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 7, Black, Right); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root.right, 8, Red, None); err != nil {
				t.Error(err)
			}
		}

		// Remove seventh
		{
			//                        8,B

			if expected, actual := true, tree.Delete(7); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 2, 1); err != nil {
				t.Error(err)
			}
			if err := verifyNode(tree.root, 8, Black, None); err != nil {
				t.Error(err)
			}

			if err := verifyNode(tree.root, 8, Black, None); err != nil {
				t.Error(err)
			}
		}

		// Remove eighth
		{

			if expected, actual := true, tree.Delete(8); expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if err := verifyTree(tree, 1, 0); err != nil {
				t.Error(err)
			}

			if expected, actual := true, tree.root == nil; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		}
	})
}

func TestLookupNUniqueAt(t *testing.T) {
	t.Parallel()

	t.Run("lookup", func(t *testing.T) {
		tree := NewRBTree()

		if ok := tree.Insert(1, "1"); !ok {
			t.Errorf("expected: %t, actual: %t", true, ok)
		}
		if ok := tree.Insert(2, "2"); !ok {
			t.Errorf("expected: %t, actual: %t", true, ok)
		}
		if ok := tree.Insert(3, "3"); !ok {
			t.Errorf("expected: %t, actual: %t", true, ok)
		}
		err := verifyTreeStructure(tree.Root())
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}

		if expected, actual := []string{"1", "2", "3"}, tree.LookupNUniqueAt(3, 1); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := []string{"2", "3", "1"}, tree.LookupNUniqueAt(3, 2); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := []string{"2"}, tree.LookupNUniqueAt(1, 2); !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := 3, len(tree.LookupNUniqueAt(10, 4)); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		if expected, actual := 0, len(tree.LookupNUniqueAt(0, 1)); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestSearch(t *testing.T) {
	t.Parallel()

	t.Run("manual", func(t *testing.T) {
		tree := makeTree()

		err := verifyTreeStructure(tree.Root())
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}

		value, ok := tree.Search(5)
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
		if expected, actual := "5", value; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}

		value, ok = tree.Search(3)
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
		if expected, actual := "3", value; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}

		value, ok = tree.Search(8)
		if expected, actual := true, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
		if expected, actual := "8", value; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}

		value, ok = tree.Search(10)
		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
		if expected, actual := "", value; expected != actual {
			t.Errorf("expected: %v, actual: %v, err: %v", expected, actual, err)
		}
	})
}

func makeTreeWithAmount(amount int) *RBTree {
	tree := NewRBTree()

	for i := 1; i <= amount; i++ {
		tree.Insert(i, fmt.Sprintf("%d", i))
	}

	return tree
}

func makeTree() *RBTree {
	//               4,B
	//             /     \
	//         2,R         6,R
	//       /     \     /     \
	//     1,B    3,B   5,B    7,B
	//                             \
	//                              8,R
	return makeTreeWithAmount(8)
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

func verifyTree(tree *RBTree, height, size int) error {
	err := verifyTreeStructure(tree.Root())
	if expected, actual := true, err == nil; expected != actual {
		return errors.Errorf("tree structure - expected: %v, actual: %v, err: %v", expected, actual, err)
	}
	if expected, actual := height, blackHeight(tree.Root()); actual != expected {
		return errors.Errorf("tree height - expected: %d, actual: %d", expected, actual)
	}
	if expected, actual := size, tree.Size(); expected != actual {
		return errors.Errorf("tree size - expected: %d, actual: %d", expected, actual)
	}
	return nil
}

func verifyNode(node *RBNode, key int, nodeType NodeType, presence Presence) error {
	if expected, actual := key, node.key; expected != actual {
		return errors.Errorf("node key - expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := fmt.Sprintf("%d", int(key)), node.value; expected != actual {
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

func verifyTreeStructure(n *RBNode) error {
	if n == nil {
		return nil
	}

	if isRed(n) && (isRed(n.left) || isRed(n.right)) {
		return errors.Errorf("red violation with key %v", n.key)
	}

	if err := verifyTreeStructure(n.left); err != nil {
		return err
	}
	if err := verifyTreeStructure(n.right); err != nil {
		return err
	}

	if n.left != nil && n.left.key-n.key >= 0 ||
		n.right != nil && n.right.key-n.key <= 0 {
		return errors.Errorf("binary tree violation with key %v", n.key)
	}
	return nil
}
