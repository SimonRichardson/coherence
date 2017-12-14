package rbtree

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
func (t *RBTree) Insert(key int, value string) bool {
	if t.root == nil {
		t.root = &RBNode{
			key:      key,
			value:    value,
			nodeType: Black,
		}
		t.size = 1
		return true
	}

	var (
		insertion           bool
		parent, grandParent *RBNode

		head            = &RBNode{}
		root            = head
		node            = t.root
		direction, last = true, true
	)

	root.right = t.root

	for {
		if node == nil {
			node = &RBNode{
				key:      key,
				value:    value,
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

		comparator := node.key - key

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

// Delete removes the entry for key from the redBlackTree. Returns true on
// successful deletion, false if the key is not in tree
func (t *RBTree) Delete(key int) bool {
	if t.root == nil {
		return false
	}

	var (
		head = &RBNode{
			nodeType: Red,
		}

		parent, grandParent *RBNode
		found               *RBNode

		node      = head
		direction = true
	)

	node.right = t.root

	for {
		child := node.child(direction)
		if child == nil {
			break
		}

		last := direction

		grandParent = parent
		parent = node
		node = child

		comparator := node.key - key
		if comparator == 0 {
			found = node
		}

		direction = comparator < 0
		if !isRed(node) && !isRed(node.child(direction)) {
			if isRed(node.child(!direction)) {
				n := singleRotate(node, direction)
				parent.setChild(last, n)
				parent = n
			} else {
				if sibling := parent.child(!last); sibling != nil {
					if !isRed(sibling.child(!last)) && !isRed(sibling.child(last)) {
						parent.nodeType = Black
						sibling.nodeType, node.nodeType = Red, Red
					} else {
						dir := grandParent.right == parent
						if isRed(sibling.child(last)) {
							grandParent.setChild(dir, doubleRotate(parent, last))
						} else if isRed(sibling.child(!last)) {
							grandParent.setChild(dir, singleRotate(parent, last))
						}

						c := grandParent.child(dir)
						c.nodeType, node.nodeType = Red, Red
						c.left.nodeType, c.right.nodeType = Black, Black
					}
				}
			}
		}
	}

	if found != nil {
		found.key = node.key
		found.value = node.value
		parent.setChild(parent.right == node, node.child(node.left == nil))
		t.size--
	}

	t.root = head.right
	if t.root != nil {
		t.root.nodeType = Black
	}

	return found != nil
}

// LookupNUniqueAt iterates through the tree from the last node that is smaller
// than key or equal, and returns the next n unique values.
func (t *RBTree) LookupNUniqueAt(n int, key int) []string {
	var (
		res    = make([]string, 0, n)
		unique = make(map[string]struct{})
	)
	find(t.root, n, key, unique, &res)
	if len(res) < n {
		find(t.root, n, 0, unique, &res)
	}
	return res
}

func find(node *RBNode, n int, key int, m map[string]struct{}, s *[]string) {
	if len(m) >= n || node == nil {
		return
	}

	comparator := node.key - key
	if comparator >= 0 {
		find(node.left, n, key, m, s)
	}

	if len(m) >= n {
		return
	}

	if comparator >= 0 {
		if _, ok := m[node.value]; !ok {
			*s = append(*s, node.value)
		}
		m[node.value] = struct{}{}
	}

	find(node.right, n, key, m, s)
}

// Search searches for a value in the redBlackTree, returns the string and true
// if found or the empty string and false if val is not in the tree.
func (t *RBTree) Search(key int) (string, bool) {
	if t.root == nil {
		return "", false
	}
	return t.root.search(key)
}

// RBNode is a RBTree node
type RBNode struct {
	key         int
	value       string
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

func (n *RBNode) search(key int) (string, bool) {
	if n.key == key {
		return n.value, true
	} else if key < n.key {
		if n.left != nil {
			return n.left.search(key)
		}
	} else {
		if n.right != nil {
			return n.right.search(key)
		}
	}
	return "", false
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
