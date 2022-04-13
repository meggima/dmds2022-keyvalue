package keyvaluestore

import (
	"errors"
	"fmt"
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"math"
	"os"
	"strings"
)

const (
	DEFAULT_MAX_DEGREE  = 225
	DEFAULT_MEMORY_SIZE = 10_485_760 // 10 MB

	PAGE_SIZE_OVERHEAD_BYTES = 29
	PAGE_SIZE_VARIABLE_BYTES = 18
)

type bTree struct {
	root       *node
	nextNodeId uint64
	buffer     BufferManager
	max_degree uint32
}

func NewTree(file *os.File, pageSize int, memorySize uint64) (*bTree, error) {
	var tree = &bTree{
		nextNodeId: 1,
		buffer:     nil,
		max_degree: calculateTreeDegree(pageSize),
	}

	err := tree.Init(file, pageSize, memorySize)
	if err != nil {
		return nil, err
	}

	return tree, nil
}

func (t *bTree) Init(file *os.File, pageSize int, memorySize uint64) error {
	if file == nil {
		t.initEmpty(memorySize)
		return nil
	}

	return t.initFromFile(file, pageSize, memorySize)
}

func (t *bTree) initEmpty(memorySize uint64) {
	reader := NewMemoryNodeReaderWriter()
	writer := reader
	t.buffer = NewBufferManager(calculateBufferSize(memorySize), reader, writer)
	t.root = t.NewNode()
	t.root.isLeaf = true
}

func (t *bTree) initFromFile(file *os.File, pageSize int, memorySize uint64) error {
	rootId, nextNodeId, _ := ReadFileHeader(file)
	reader := NewNodeReader(file, t.max_degree, pageSize)
	writer := NewNodeWriter(file, pageSize)
	t.buffer = NewBufferManager(calculateBufferSize(memorySize), reader, writer)

	if nextNodeId == 1 {
		// new tree
		t.root = t.NewNode()
		t.root.isLeaf = true
	} else {
		t.nextNodeId = nextNodeId
		var err error
		t.root, err = reader.ReadNode(rootId)
		t.root.tree = t
		if err != nil {
			return err
		}
	}

	return nil
}

func calculateBufferSize(memorySize uint64) uint64 {
	return uint64(math.Floor(float64(memorySize) / float64(MEMORY_PER_ENTRY)))
}

func calculateTreeDegree(pageSize int) uint32 {
	return uint32(math.Floor(float64(pageSize-PAGE_SIZE_OVERHEAD_BYTES) / PAGE_SIZE_VARIABLE_BYTES))
}

func (t *bTree) getNodeById(nodeId uint64) *node {
	if nodeId == 0 {
		return nil
	}

	node, _ := t.buffer.Get(nodeId)
	node.tree = t

	return node
}

func (t *bTree) NewNode() *node {
	var node *node = &node{
		nodeId:   t.nextNodeId,
		n:        0,
		keys:     make([]uint64, t.max_degree),    // The arrays are one element larger than they need
		values:   make([]*[10]byte, t.max_degree), // to be to allow overfilling them while inserting new keys.
		children: make([]uint64, t.max_degree+1),  // Note the +1 as we have one child pointer more than keys.
		isLeaf:   false,
		next:     0,
		parent:   0,
		tree:     t,
		isDirty:  false,
	}

	t.buffer.Put(node)

	t.nextNodeId += 1

	return node
}

func (t *bTree) Find(key uint64, errorIfExists bool) (*node, uint32, error) {
	return t.root.find(key, errorIfExists)
}

func (t *bTree) Put(key uint64, value *[10]byte) error {
	n, i, _ := t.Find(key, false)

	if !n.isLeaf {
		return errors.New("no leaf found for inserting")
	}

	err := n.insertValueToLeaf(key, value, i)
	//t.Print()
	return err
}

func (t *bTree) createNewRootWithChildren(leftChild *node, rightChild *node) {
	// create new root
	root := t.NewNode()
	t.root = root
	root.setChildAt(0, leftChild)
	root.setChildAt(1, rightChild)
	root.keys[0] = rightChild.keys[0]
	root.n = 1
	root.isDirty = true
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
			sb.WriteString("(")
			sb.WriteString(fmt.Sprint(n.nodeId))
			sb.WriteString(":")
			sb.WriteString(fmt.Sprint(n.parent))
			sb.WriteString(") ")
			var i uint32 = 0
			for ; i < n.n; i++ {
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
