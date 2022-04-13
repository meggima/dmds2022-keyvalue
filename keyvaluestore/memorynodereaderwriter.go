package keyvaluestore

type MemoryNodeReaderWriter struct {
	buffer map[uint64]*node
}

func NewMemoryNodeReaderWriter() *MemoryNodeReaderWriter {
	return &MemoryNodeReaderWriter{
		buffer: make(map[uint64]*node, 1000),
	}
}

func (rw *MemoryNodeReaderWriter) ReadNode(nodeId uint64) (*node, error) {
	return rw.buffer[nodeId], nil
}

func (rw *MemoryNodeReaderWriter) WriteNode(node *node) error {
	rw.buffer[node.nodeId] = node
	return nil
}
