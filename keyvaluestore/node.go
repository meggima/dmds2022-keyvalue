package keyvaluestore

import (
	"errors"
	"math"
)

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

func (n *node) getChildAt(index int) *node {
	return n.children[index]
}

func (n *node) setChildAt(index int, node *node) {
	n.children[index] = node
}

func (n *node) getParent() *node {
	return n.parent
}

func (n *node) setParent(parent *node) {
	n.parent = parent
}

func (n *node) getNext() *node {
	return n.next
}

func (n *node) setNext(next *node) {
	n.next = next
}

func (n *node) find(key uint64, errorIfExists bool) (*node, int, error) {
	if n.isLeaf {
		return n.findInLeaf(key, errorIfExists)
	} else {
		// Falls into the leftmost child
		if key < n.keys[0] {
			// The leftmost child always exists at this point
			return n.getChildAt(0).find(key, errorIfExists)
		}

		// Falls into the rightmost child
		if key >= n.keys[n.n-1] {
			return n.getChildAt(n.n).find(key, errorIfExists)
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

		return n.getChildAt(i).find(key, errorIfExists)
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
			if n.getNext() != nil && n.getNext().n < MAX_DEGREE-1 {
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
		n.setChildAt(j+1, n.getChildAt(j))
	}
	n.keys[index] = key
	n.values[index] = value
	n.setChildAt(index+1, child)
	n.n++
}

func (n *node) shiftRightmostElementToNext() error {
	if !n.isLeaf {
		return errors.New("cannot shift on non-leaf nodes")
	}
	next := n.getNext()

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

	return next.getParent().recalculateKeys()
}

func (n *node) recalculateKeys() error {
	if n.isLeaf {
		return errors.New("cannot recalculate keys on leaf nodes")
	}
	for i := 0; i < n.n; i++ {
		n.keys[i] = n.getChildAt(i + 1).getLowestKeyInSubtree()
	}

	if n.getParent() != nil {
		return n.getParent().recalculateKeys()
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
		newNode.setChildAt(rightSize, n.getChildAt(n.n))
		newNode.getChildAt(rightSize).setParent(newNode)
		n.setChildAt(n.n, nil)

		n.n = leftSize
		newNode.n = rightSize
	}

	newNode.setParent(n.getParent())
	newNode.setNext(n.getNext())
	n.setNext(newNode)

	if n.getParent() != nil {
		// add new node to parent
		return n.getParent().appendChildNode(newNode)
	} else {
		n.tree.createNewRootWithChildren(n, newNode)
	}
	return nil
}

func (n *node) transplantHalfElementsTo(newNode *node) (sizeoldNodeN int, sizeNewNode int) {
	sizeNewNode = 0
	sizeoldNodeN = int(math.Ceil(float64(n.n) / 2))
	for j := sizeoldNodeN; j < n.n; j++ {
		newNode.keys[sizeNewNode] = n.keys[j]
		newNode.values[sizeNewNode] = n.values[j]
		newNode.setChildAt(sizeNewNode, n.getChildAt(j))

		if newNode.getChildAt(sizeNewNode) != nil {
			newNode.getChildAt(sizeNewNode).setParent(newNode)
		}

		n.keys[j] = 0
		n.values[j] = &[10]byte{}
		n.setChildAt(j, nil)
		sizeNewNode++
	}

	return sizeoldNodeN, sizeNewNode
}

func (n *node) getLowestKeyInSubtree() uint64 {
	if n.isLeaf {
		return n.keys[0]
	} else {
		return n.getChildAt(0).getLowestKeyInSubtree()
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
			n.setChildAt(n.n+1, child)
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