package keyvaluestore

type NodeReader interface {
	ReadNode(nodeId uint64) (*node, error)
}

type NullNodeReader struct {
}

func (reader *NullNodeReader) ReadNode(nodeId uint64) (*node, error) {
	return nil, nil
}
