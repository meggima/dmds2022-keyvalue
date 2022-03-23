package keyvaluestore

import "errors"

const (
	MAX_DEGREE = 4 // TODO calculate degree based on chosen page size and size of a kvEntry
)

type bTree struct {
	root       *node
	nextNodeId uint64
}

type node struct {
	nodeId   uint64
	n        int // Number of keys
	keys     []uint64
	values   [][10]byte
	children []*node
	isLeaf   bool
	next     *node
}

func NewTree() *bTree {
	var tree = &bTree{
		nextNodeId: 0,
	}

	tree.Init()

	return tree
}

func (t *bTree) Init() {
	t.root = t.NewLeaf()
}

func (t *bTree) NewNode() *node {
	var node *node = &node{
		nodeId:   t.nextNodeId,
		n:        0,
		keys:     make([]uint64, MAX_DEGREE),
		values:   make([][10]byte, MAX_DEGREE),
		children: make([]*node, MAX_DEGREE+1), // Note the +1 as we have one child pointer more than keys
		isLeaf:   false,
		next:     nil,
	}

	t.nextNodeId += 1

	return node
}

func (t *bTree) NewLeaf() *node {
	var node *node = &node{
		nodeId:   t.nextNodeId,
		n:        0,
		keys:     make([]uint64, MAX_DEGREE),
		values:   make([][10]byte, MAX_DEGREE),
		children: make([]*node, MAX_DEGREE),
		isLeaf:   true,
		next:     nil,
	}

	t.nextNodeId += 1

	return node
}

func (t *bTree) Find(key uint64, errorIfExists bool) (*node, int, error) {
	return t.root.Find(key, errorIfExists)
}

func (n *node) Find(key uint64, errorIfExists bool) (*node, int, error) {
	if n.isLeaf {
		return n.FindInLeaf(key, errorIfExists)
	} else {
		// Falls into the leftmost child
		if key < n.keys[0] {
			// The leftmost child always exists at this point
			return n.children[0].Find(key, errorIfExists)
		}

		// Falls into the rightmost child
		if key >= n.keys[n.n-1] {
			return n.children[n.n].Find(key, errorIfExists)
		}

		// Falls into one of the intermediate children
		var i int = 1

		for i < n.n {
			if key > n.keys[i] {
				i++
			} else {
				break
			}
		}

		return n.children[i].Find(key, errorIfExists)
	}
}

func (n *node) FindInLeaf(key uint64, errorIfExists bool) (*node, int, error) {
	var i int = 0

	for i < n.n {
		if key > n.keys[i] {
			i++
		} else {
			break
		}
	}

	if n.keys[i] == key {
		if errorIfExists {
			return n, i, errors.New("key already exists")
		} else {
			return n, i, nil
		}
	} else {
		return n, i, nil
	}
}

func (t *bTree) Put(key uint64, value *[10]byte) error {
	return nil
}

func (t *bTree) Get(key uint64) (*[10]byte, error) {
	n, i, _ := t.Find(key, false)

	if n.isLeaf && n.keys[i] == key {
		return &(n.values[i]), nil
	}

	return nil, errors.New("key does not exist")
}
