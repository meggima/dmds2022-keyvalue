package keyvaluestore

type NodeWriter interface {
	WriteNode(node *node) error
}

type NullNodeWriter struct {
}

func (writer *NullNodeWriter) WriteNode(node *node) error {
	return nil
}
