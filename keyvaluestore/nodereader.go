package keyvaluestore

import (
	"os"
)

type NodeReader interface {
	ReadNode(nodeId uint64) (*node, error)
}

type NodeReaderImpl struct {
	file *os.File
}

func (reader *NodeReaderImpl) ReadNode(nodeId uint64) (*node, error) {
	if reader.file == nil {
		return nil, nil
	}

	return nil, nil
}
