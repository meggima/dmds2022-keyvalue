package keyvaluestore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWriteSingleNode(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	node := &node{
		nodeId: 1,
		n:      2,
		isLeaf: true,
		keys:   []uint64{1, 2},
		values: []*[10]byte{
			{0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1},
			{0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2},
		},
		children: []uint64{0, 0, 0},
		next:     2,
		parent:   3,
	}

	file, _ := os.CreateTemp("", "kv.tmp")

	writer := NewNodeWriter(file, 128)
	reader := NewNodeReader(file, 2, 128)

	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	// Act
	writer.WriteNode(node)

	rn, _ := reader.ReadNode(1)

	// Assert
	assert.NotNil(rn)
	assert.Equal(node.nodeId, rn.nodeId)
	assert.Equal(node.n, rn.n)
	assert.Equal(node.parent, rn.parent)
	assert.Equal(node.next, rn.next)
	assert.Equal(node.isLeaf, rn.isLeaf)
	assert.Equal(node.keys, rn.keys)
	assert.Equal(node.values, rn.values)
	assert.Equal(node.children, rn.children)
}

func TestReadWriteMultipleNodes(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	node1 := &node{
		nodeId: 1,
		n:      2,
		isLeaf: true,
		keys:   []uint64{1, 2},
		values: []*[10]byte{
			{0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1},
			{0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2},
		},
		children: []uint64{0, 0, 0},
		next:     2,
		parent:   3,
	}

	node2 := &node{
		nodeId:   2,
		n:        2,
		isLeaf:   false,
		keys:     []uint64{4, 5},
		values:   []*[10]byte{nil, nil},
		children: []uint64{1, 2, 3},
		next:     2,
		parent:   3,
	}

	file, _ := os.CreateTemp("", "kv.tmp")

	writer := NewNodeWriter(file, 128)
	reader := NewNodeReader(file, 2, 128)

	t.Cleanup(func() {
		file.Close()
		os.Remove(file.Name())
	})

	// Act
	writer.WriteNode(node1)
	writer.WriteNode(node2)

	rn1, _ := reader.ReadNode(1)
	rn2, _ := reader.ReadNode(2)

	// Assert
	assert.NotNil(rn1)
	assert.Equal(node1.nodeId, rn1.nodeId)
	assert.Equal(node1.n, rn1.n)
	assert.Equal(node1.parent, rn1.parent)
	assert.Equal(node1.next, rn1.next)
	assert.Equal(node1.isLeaf, rn1.isLeaf)
	assert.Equal(node1.keys, rn1.keys)
	assert.Equal(node1.values, rn1.values)
	assert.Equal(node1.children, rn1.children)

	assert.NotNil(rn2)
	assert.Equal(node2.nodeId, rn2.nodeId)
	assert.Equal(node2.n, rn2.n)
	assert.Equal(node2.parent, rn2.parent)
	assert.Equal(node2.next, rn2.next)
	assert.Equal(node2.isLeaf, rn2.isLeaf)
	assert.Equal(node2.keys, rn2.keys)
	assert.Equal(node2.values, rn2.values)
	assert.Equal(node2.children, rn2.children)
}
