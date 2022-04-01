package keyvaluestore

import (
	"os"
)

type NodeWriter interface {
	WriteNode(node *node) error
}

type NodeWriterImpl struct {
	file *os.File
}

func (writer *NodeWriterImpl) WriteNode(node *node) error {
	if writer.file == nil {
		return nil
	}

	return nil
}
