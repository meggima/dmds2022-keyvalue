package keyvaluestore

import (
	"errors"
	"fmt"
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"strings"
)

const (
	MAX_DEGREE  = 6    // TODO calculate degree based on chosen page size and size of a kvEntry
	BUFFER_SIZE = 1000 // TODO gather from kv init
)

type bTree struct {
	root       *node
	nextNodeId uint64
	buffer     BufferManager
}

func NewTree() *bTree {
	var buffer BufferManager = NewBufferManager(BUFFER_SIZE, &NullNodeReader{}, &NullNodeWriter{}) // TODO use real reader/writer

	var tree = &bTree{
		nextNodeId: 1,
		buffer:     buffer,
	}

	tree.Init()

	return tree
}

func (t *bTree) Init() {
	t.root = t.NewNode()
	t.root.isLeaf = true
}

func (t *bTree) getNodeById(nodeId uint64) *node {
	if nodeId == 0 {
		return nil
	}

	node, _ := t.buffer.Get(nodeId)

	return node
}

func (t *bTree) NewNode() *node {
	var node *node = &node{
		nodeId:   t.nextNodeId,
		n:        0,
		keys:     make([]uint64, MAX_DEGREE),    // The arrays are one element larger than they need
		values:   make([]*[10]byte, MAX_DEGREE), // to be to allow overfilling them while inserting new keys.
		children: make([]uint64, MAX_DEGREE+1),  // Note the +1 as we have one child pointer more than keys.
		isLeaf:   false,
		next:     0,
		parent:   0,
		tree:     t,
	}

	t.buffer.Put(node)

	t.nextNodeId += 1

	return node
}

func (t *bTree) Find(key uint64, errorIfExists bool) (*node, int, error) {
	return t.root.find(key, errorIfExists)
}

func (t *bTree) Put(key uint64, value *[10]byte) error {
	n, i, _ := t.Find(key, false)

	if !n.isLeaf {
		return errors.New("no leaf found for inserting")
	}

	err := n.insertValueToLeaf(key, value, i)
	t.Print()
	return err
}

func (t *bTree) createNewRootWithChildren(leftChild *node, rightChild *node) {
	// create new root
	root := t.NewNode()
	t.root = root
	root.setChildAt(0, leftChild)
	root.setChildAt(1, rightChild)
	root.keys[0] = rightChild.getLowestKeyInSubtree()
	root.n = 1
	leftChild.setParent(root)
	rightChild.setParent(root)
}

func (t *bTree) Get(key uint64) ([10]byte, error) {
	n, i, _ := t.Find(key, false)

	if n.isLeaf && n.keys[i] == key {
		return (*n.values[i]), nil
	}

	return [10]byte{}, keyvaluestore.ErrNotFound
}

// returns a string representation of the keys in the leaves
func (t *bTree) Print() {
	// get left-most leaf
	firstNodeInLevel := t.root
	fmt.Println("====== Tree Start ======")
	for firstNodeInLevel != nil {
		n := firstNodeInLevel
		var sb strings.Builder

		sb.WriteString("[")
		for n != nil {
			sb.WriteString("[ ")
			for i := 0; i < n.n; i++ {
				sb.WriteString(fmt.Sprint(n.keys[i]))
				sb.WriteString(", ")
			}
			sb.WriteString(" ],")

			n = n.getNext()
		}

		sb.WriteString("]")
		fmt.Println(sb.String())

		firstNodeInLevel = firstNodeInLevel.getChildAt(0)
	}
	fmt.Println("====== Tree End ======")
}
