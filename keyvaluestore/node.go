package keyvaluestore

import (
	"errors"
	"math"
)

type node struct {
	nodeId   uint64
	n        uint32 // Number of keys
	keys     []uint64
	values   []*[10]byte
	children []uint64
	isLeaf   bool
	next     uint64
	parent   uint64
	tree     *bTree
	isDirty  bool
}

func (n *node) getChildAt(index uint32) *node {
	return n.tree.getNodeById(n.children[index])
}

func (n *node) setChildAt(index uint32, node *node) {
	if node == nil {
		n.children[index] = 0
	} else {
		n.children[index] = node.nodeId
	}
}

func (n *node) getParent() *node {
	return n.tree.getNodeById(n.parent)
}

func (n *node) setParent(parent *node) {
	if parent == nil {
		n.parent = 0
	} else {
		n.parent = parent.nodeId
	}
}

func (n *node) getNext() *node {
	return n.tree.getNodeById(n.next)
}

func (n *node) setNext(next *node) {
	if next == nil {
		n.next = 0
	} else {
		n.next = next.nodeId
	}
}

func (n *node) find(key uint64, errorIfExists bool) (*node, uint32, error) {
	if n.isLeaf {
		return n.findInLeaf(key, errorIfExists)
	} else {

		i := n.findIndexForKey(key)

		return n.getChildAt(i).find(key, errorIfExists)
	}
}

func (n *node) findInLeaf(key uint64, errorIfExists bool) (*node, uint32, error) {
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

func (n *node) findIndexForKey(key uint64) uint32 {
	comparer := nonLeafKeyComparer

	if n.isLeaf {
		comparer = leafKeyComparer
	}

	return BinarySearch(key, &n.keys, n.n, comparer)
}

func (n *node) insertValueToLeaf(key uint64, value *[10]byte, index uint32) error {
	if n.keys[index] == key {
		// overwrite existing key
		n.values[index] = value
		n.isDirty = true
		return nil
	} else if n.n < n.tree.max_degree {
		// insert value into leaf

		n.shiftElementsRightAndInsertKey(index, key, value, nil)

		// node is over-full after insertion. try to shift the right-most key/value pair to the next node or split it
		if n.n == n.tree.max_degree {
			if n.getNext() != nil && n.getNext().n < n.tree.max_degree-1 {
				return n.shiftRightmostElementToNext()
			}

			return n.splitNode()
		}
	} else {
		return errors.New("cannot insert value to leaf. node is already over-full")
	}
	return nil
}

func (n *node) shiftElementsRightAndInsertKey(index uint32, key uint64, value *[10]byte, child *node) {
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
	n.isDirty = true
}

func (n *node) shiftRightmostElementToNext() error {
	if !n.isLeaf {
		return errors.New("cannot shift on non-leaf nodes")
	}
	next := n.getNext()

	for i := int(next.n); i >= 0; i-- {
		next.keys[i+1] = next.keys[i]
		next.values[i+1] = next.values[i]
	}
	next.keys[0] = n.keys[n.n-1]
	next.values[0] = n.values[n.n-1]
	n.keys[n.n-1] = 0
	n.values[n.n-1] = &[10]byte{}

	n.n--
	next.n++
	next.isDirty = true

	return next.getParent().recalculateKeys()
}

func (n *node) recalculateKeys() error {
	if n.isLeaf {
		return errors.New("cannot recalculate keys on leaf nodes")
	}
	var i uint32 = 0
	for ; i < n.n; i++ {
		n.keys[i] = n.getChildAt(i + 1).getLowestKeyInSubtree()
	}
	n.isDirty = true

	if n.getParent() != nil {
		return n.getParent().recalculateKeys()
	}
	return nil
}

func (n *node) splitNode() error {
	newNode := n.tree.NewNode()
	newNode.isLeaf = n.isLeaf
	newNode.isDirty = true

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

func (n *node) transplantHalfElementsTo(newNode *node) (sizeoldNodeN uint32, sizeNewNode uint32) {
	sizeNewNode = 0
	sizeoldNodeN = uint32(math.Ceil(float64(n.n) / 2))
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

	if n.n < n.tree.max_degree {
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
		n.isDirty = true

		// node is over-full after insertion. split it
		if n.n == n.tree.max_degree {
			return n.splitNode()
		}

	} else {
		return errors.New("cannot append another child node. node is already over-full")
	}

	return nil
}
