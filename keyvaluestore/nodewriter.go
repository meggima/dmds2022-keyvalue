package keyvaluestore

import (
	"errors"
	"os"
)

type NodeWriter interface {
	WriteNode(node *node) error
}

type NodeWriterImpl struct {
	file     *os.File
	pageSize int
}

func NewNodeWriter(file *os.File, pageSize int) NodeWriter {
	return &NodeWriterImpl{
		file:     file,
		pageSize: pageSize,
	}
}

func (writer *NodeWriterImpl) WriteNode(node *node) error {
	if writer.file == nil {
		return nil
	}

	id := node.nodeId

	bytes := ConvertBool(node.isLeaf)
	bytes = append(bytes, ConvertUInt64(id)...)
	bytes = append(bytes, ConvertUInt32(node.n)...)
	bytes = append(bytes, ConvertUInt64(node.next)...)
	bytes = append(bytes, ConvertUInt64(node.parent)...)

	var i uint32 = 0
	for ; i < node.n; i++ {
		bytes = append(bytes, ConvertUInt64(node.keys[i])...)
	}

	if node.isLeaf {
		i = 0
		for ; i < node.n; i++ {
			bytes = append(bytes, node.values[i][:]...)
		}
	} else {
		i = 0
		for ; i < node.n+1; i++ {
			bytes = append(bytes, ConvertUInt64(node.children[i])...)
		}
	}

	// fill rest of page with 0s

	free := writer.pageSize - len(bytes)
	bytes = append(bytes, make([]byte, free)...)

	_, err := writer.file.WriteAt(bytes, int64(id)*int64(writer.pageSize))

	return err
}

type NullNodeWriter struct{}

func (writer *NullNodeWriter) WriteNode(node *node) error {
	return errors.New("not implemented")
}
