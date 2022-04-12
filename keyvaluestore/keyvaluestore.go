package keyvaluestore

import "os"

type KeyValueStore struct {
	tree *bTree
	file *os.File
}

func New(file *os.File, pageSize int, memorySize uint64) (*KeyValueStore, error) {
	tree, err := NewTree(file, pageSize, memorySize)
	if err != nil {
		return nil, err
	}
	return &KeyValueStore{
		tree: tree,
		file: file,
	}, nil
}

func (kv *KeyValueStore) Put(key uint64, value *[10]byte) error {
	return kv.tree.Put(key, value)
}

func (kv *KeyValueStore) Get(key uint64) ([10]byte, error) {
	val, err := kv.tree.Get(key)
	if err != nil {
		return [10]byte{}, err
	}

	return val, nil
}

func (kv *KeyValueStore) Flush() error {
	rootId, nextNodeId, err := ReadFileHeader(kv.file)
	if err != nil {
		return err
	}
	if rootId != kv.tree.root.nodeId || nextNodeId != kv.tree.nextNodeId {
		err = WriteFileHeader(kv.file, kv.tree.root.nodeId, kv.tree.nextNodeId)
		if err != nil {
			return err
		}
	}

	return kv.tree.buffer.Flush()
}

func (kv *KeyValueStore) CloseFile() error {
	return kv.file.Close()
}
