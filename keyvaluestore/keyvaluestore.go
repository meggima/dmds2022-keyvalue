package keyvaluestore

import "os"

type KeyValueStore struct {
	tree *bTree
}

func New(file *os.File) (*KeyValueStore, error) {
	tree, err := NewTree(file)
	if err != nil {
		return nil, err
	}
	return &KeyValueStore{
		tree: tree,
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
