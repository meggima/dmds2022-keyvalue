package keyvaluestore

import (
	"errors"
	"fmt"
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"math"
	"strings"
)

const (
	MAX_DEGREE = 6 // TODO calculate degree based on chosen page size and size of a kvEntry
)

type bTree struct {
	root       *node
	nextNodeId uint64
}

type node struct {
	nodeId   uint64
	n        int // Number of keys
	keys     []uint64
	values   []*[10]byte
	children []*node
	isLeaf   bool
	next     *node
	parent   *node
	tree     *bTree
}

func NewTree() *bTree {
	var tree = &bTree{
		nextNodeId: 0,
	}

	tree.Init()

	return tree
}

func (t *bTree) Init() {
	t.root = t.NewNode()
	t.root.isLeaf = true
}

func (t *bTree) NewNode() *node {
	var node *node = &node{
		nodeId:   t.nextNodeId,
		n:        0,
		keys:     make([]uint64, MAX_DEGREE),    // The arrays are one element larger than they need
		values:   make([]*[10]byte, MAX_DEGREE), // to be to allow overfilling them while inserting new keys.
		children: make([]*node, MAX_DEGREE+1),   // Note the +1 as we have one child pointer more than keys.
		isLeaf:   false,
		next:     nil,
		parent:   nil,
		tree:     t,
	}

	t.nextNodeId += 1

	return node
}

func (t *bTree) Find(key uint64, errorIfExists bool) (*node, int, error) {
	return t.root.find(key, errorIfExists)
}

func (n *node) find(key uint64, errorIfExists bool) (*node, int, error) {
	if n.isLeaf {
		return n.findInLeaf(key, errorIfExists)
	} else {
		// Falls into the leftmost child
		if key < n.keys[0] {
			// The leftmost child always exists at this point
			return n.children[0].find(key, errorIfExists)
		}

		// Falls into the rightmost child
		if key >= n.keys[n.n-1] {
			return n.children[n.n].find(key, errorIfExists)
		}

		// Falls into one of the intermediate children
		var i int = 1

		for i < n.n {
			if key >= n.keys[i] {
				i++
			} else {
				break
			}
		}

		return n.children[i].find(key, errorIfExists)
	}
}

func (n *node) findInLeaf(key uint64, errorIfExists bool) (*node, int, error) {
	i := n.findIndexForKey(key)

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

func (n *node) findIndexForKey(key uint64) int {
	var i int = 0

	for i < n.n {
		if key > n.keys[i] {
			i++
		} else {
			break
		}
	}

	return i
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

func (n *node) insertValueToLeaf(key uint64, value *[10]byte, index int) error {
	if n.keys[index] == key {
		// overwrite existing key
		n.values[index] = value
		return nil
	} else if n.n < MAX_DEGREE {
		// insert value into leaf

		n.shiftElementsRightAndInsertKey(index, key, value, nil)

		// node is over-full after insertion. try to shift the right-most key/value pair to the next node or split it
		if n.n == MAX_DEGREE {
			if n.next != nil && n.next.n < MAX_DEGREE-1 {
				return n.shiftRightmostElementToNext()
			}

			return n.splitNode()
		}
	} else {
		return errors.New("cannot insert value to leaf. node is already over-full")
	}
	return nil
}

func (n *node) shiftElementsRightAndInsertKey(index int, key uint64, value *[10]byte, child *node) {
	// shift keys/children to the right of the index by one
	for j := n.n; j > index; j-- {
		n.keys[j] = n.keys[j-1]
		n.values[j] = n.values[j-1]
		n.children[j+1] = n.children[j]
	}
	n.keys[index] = key
	n.values[index] = value
	n.children[index+1] = child
	n.n++
}

func (n *node) shiftRightmostElementToNext() error {
	if !n.isLeaf {
		return errors.New("cannot shift on non-leaf nodes")
	}
	next := n.next

	for i := next.n; i >= 0; i-- {
		next.keys[i+1] = next.keys[i]
		next.values[i+1] = next.values[i]
	}
	next.keys[0] = n.keys[n.n-1]
	next.values[0] = n.values[n.n-1]
	n.keys[n.n-1] = 0
	n.values[n.n-1] = &[10]byte{}

	n.n--
	next.n++

	return next.parent.recalculateKeys()
}

func (n *node) recalculateKeys() error {
	if n.isLeaf {
		return errors.New("cannot recalculate keys on leaf nodes")
	}
	for i := 0; i < n.n; i++ {
		n.keys[i] = n.children[i+1].getLowestKeyInSubtree()
	}

	if n.parent != nil {
		return n.parent.recalculateKeys()
	}
	return nil
}

func (n *node) splitNode() error {
	newNode := n.tree.NewNode()
	newNode.isLeaf = n.isLeaf

	if n.isLeaf {
		leftSize, rightSize := n.transplantHalfElementsTo(newNode)
		n.n = leftSize
		newNode.n = rightSize
	} else {
		leftSize, rightSize := n.transplantHalfElementsTo(newNode)

		// remove middle key, it isn't needed anymore
		n.keys[leftSize] = 0

		// move the last child manually
		newNode.children[rightSize] = n.children[n.n]
		newNode.children[rightSize].parent = newNode
		n.children[n.n] = nil

		n.n = leftSize
		newNode.n = rightSize
	}
	newNode.parent = n.parent
	newNode.next = n.next
	n.next = newNode

	if n.parent != nil {
		// add new node to parent
		return n.parent.appendChildNode(newNode)
	} else {
		n.tree.createNewRootWithChildren(n, newNode)
	}
	return nil
}

func (t *bTree) createNewRootWithChildren(leftChild *node, rightChild *node) {
	// create new root
	root := t.NewNode()
	t.root = root
	root.children[0] = leftChild
	root.children[1] = rightChild
	root.keys[0] = rightChild.getLowestKeyInSubtree()
	root.n = 1
	leftChild.parent = root
	rightChild.parent = root
}

func (n *node) transplantHalfElementsTo(newNode *node) (sizeoldNodeN int, sizeNewNode int) {
	sizeNewNode = 0
	sizeoldNodeN = int(math.Ceil(float64(n.n) / 2))
	for j := sizeoldNodeN; j < n.n; j++ {
		newNode.keys[sizeNewNode] = n.keys[j]
		newNode.values[sizeNewNode] = n.values[j]
		newNode.children[sizeNewNode] = n.children[j]

		if newNode.children[sizeNewNode] != nil {
			newNode.children[sizeNewNode].parent = newNode
		}

		n.keys[j] = 0
		n.values[j] = &[10]byte{}
		n.children[j] = nil
		sizeNewNode++
	}

	return sizeoldNodeN, sizeNewNode
}

func (n *node) getLowestKeyInSubtree() uint64 {
	if n.isLeaf {
		return n.keys[0]
	} else {
		return n.children[0].getLowestKeyInSubtree()
	}
}

func (n *node) appendChildNode(child *node) error {
	if n.isLeaf {
		return errors.New("cannot append child to leaf node")
	}

	if child.n == 0 {
		return errors.New("cannot append empty child")
	}

	if n.n < MAX_DEGREE {
		key := child.getLowestKeyInSubtree()

		// insert rightmost key/child
		if key >= n.keys[n.n-1] {
			n.keys[n.n] = key
			n.children[n.n+1] = child
			n.n++
		} else {
			// find index to insert key/child
			i := n.findIndexForKey(key)

			n.shiftElementsRightAndInsertKey(i, key, nil, child)

		}

		// node is over-full after insertion. split it
		if n.n == MAX_DEGREE {
			return n.splitNode()
		}

	} else {
		return errors.New("cannot append another child node. node is already over-full")
	}

	return nil
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

			n = n.next
		}

		sb.WriteString("]")
		fmt.Println(sb.String())

		firstNodeInLevel = firstNodeInLevel.children[0]
	}
	fmt.Println("====== Tree End ======")
}
