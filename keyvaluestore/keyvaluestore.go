package keyvaluestore

type KeyValueStore struct {
	tree *bTree
}

func New() *KeyValueStore {
	return &KeyValueStore{
		tree: NewTree(),
	}
}

func (kv *KeyValueStore) Put(key uint64, value [10]byte) error {
	return kv.tree.Put(key, value)
}

func (kv *KeyValueStore) Get(key uint64) ([10]byte, error) {
	val, err := kv.tree.Get(key)
	if err != nil {
		return [10]byte{}, err
	}

	return val, nil
}
