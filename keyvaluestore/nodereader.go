package keyvaluestore

import (
	"errors"
	"os"
)

type NodeReader interface {
	ReadNode(nodeId uint64) (*node, error)
}

type NodeReaderImpl struct {
	tree *bTree
	file *os.File
}

func (reader *NodeReaderImpl) ReadNode(nodeId uint64) (*node, error) {
	if reader.file == nil {
		return nil, nil
	}

	pageSize := os.Getpagesize()
	bytes := make([]byte, os.Getpagesize())
	_, err := reader.file.ReadAt(bytes, int64(nodeId)*int64(pageSize))
	if err != nil {
		return nil, err
	}

	var node *node = &node{
		nodeId:   nodeId,
		n:        0,
		keys:     make([]uint64, reader.tree.max_degree),
		values:   make([]*[10]byte, reader.tree.max_degree),
		children: make([]uint64, reader.tree.max_degree+1),
		isLeaf:   false,
		next:     0,
		parent:   0,
		tree:     reader.tree,
		isDirty:  false,
	}

	node.isLeaf = ToBool(bytes[0])

	if nodeId != ToUInt64(bytes[1:9]) {
		return nil, errors.New("node id does not match")
	}

	node.n = ToUInt32(bytes[9:13])
	node.next = ToUInt64(bytes[13:21])
	node.parent = ToUInt64(bytes[21:29])

	offset := 29
	var i uint32 = 0
	for ; i < node.n; i++ {
		node.keys[i] = ToUInt64(bytes[offset : offset+8])
		offset += 8
	}
	if node.isLeaf {
		for i = 0; i < node.n; i++ {
			var val [10]byte
			copy(val[:], bytes[offset:offset+10])
			node.values[i] = &val
			offset += 10
		}
	} else {
		for i = 0; i < node.n+1; i++ {
			node.children[i] = ToUInt64(bytes[offset : offset+8])
			offset += 8
		}
	}

	return node, nil
}
