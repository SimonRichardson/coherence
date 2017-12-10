package rbtree

type Key interface {
	Compare(Key) int
}

// NodeType describes which type the node represents
type NodeType int

// IsRed returns if the NodeType is Red
func (n NodeType) IsRed() bool {
	return n == Red
}

// IsBlack returns if the NodeType is Black
func (n NodeType) IsBlack() bool {
	return n == Black
}

const (
	// Red represents a red type of the RBTree
	Red NodeType = iota

	// Black represents a black type of the RBTree
	Black
)

// RBTree implements a non-thread safe fixed size Red, Black tree
type RBTree struct {
	root *RBNode
	size int
}

// NewRBTree creates a new RBTree
func NewRBTree() *RBTree {
	return &RBTree{}
}

// Root returns the root node within the tree
func (t *RBTree) Root() *RBNode {
	return t.root
}

// Size returns the number of nodes
func (t *RBTree) Size() int {
	return t.size
}

// Insert inserts a value and string into the tree
// Returns true on insertion and false if a duplicate exists
func (t *RBTree) Insert(key Key, str string) bool {
	if t.root == nil {
		t.root = &RBNode{
			key:      key,
			str:      str,
			nodeType: Black,
		}
		t.size = 1
		return true
	}

	var (
		insertion       bool
		direction, last bool

		head = &RBNode{}

		parent, grandParent *RBNode
		root                = head
		node                = t.root
	)

	root.right = t.root

	for {
		if node == nil {
			node = &RBNode{
				key:      key,
				str:      str,
				nodeType: Red,
			}
			parent.setChild(direction, node)
			insertion = true
		} else if isRed(node.left) && isRed(node.right) {
			node.nodeType = Red
			node.left.nodeType, node.right.nodeType = Black, Black
		}

		if isRed(node) && isRed(parent) {
			dir := root.right == grandParent
			if node == parent.child(last) {
				root.setChild(dir, singleRotate(grandParent, !last))
			} else {
				root.setChild(dir, doubleRotate(grandParent, !last))
			}
		}

		comparator := node.key.Compare(key)

		if comparator == 0 {
			break
		}

		last = direction
		direction = comparator < 0

		if grandParent != nil {
			root = grandParent
		}
		grandParent = parent
		parent = node

		node = node.child(direction)
	}

	t.root = head.right
	t.root.nodeType = Black

	if insertion {
		t.size++
	}

	return insertion
}

// RBNode is a RBTree node
type RBNode struct {
	key         Key
	str         string
	left, right *RBNode
	nodeType    NodeType
}

func (n *RBNode) child(right bool) *RBNode {
	if right {
		return n.right
	}
	return n.left
}

func (n *RBNode) setChild(right bool, node *RBNode) {
	if right {
		n.right = node
	} else {
		n.left = node
	}
}

func isRed(n *RBNode) bool {
	return n != nil && n.nodeType.IsRed()
}

func singleRotate(node *RBNode, dir bool) *RBNode {
	root := node.child(!dir)

	node.setChild(!dir, root.child(dir))
	root.setChild(dir, node)

	node.nodeType = Red
	root.nodeType = Black

	return root
}

func doubleRotate(node *RBNode, dir bool) *RBNode {
	node.setChild(!dir, singleRotate(node.child(!dir), !dir))
	return singleRotate(node, dir)
}
