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
	item := &kvEntry{
		key:   key,
		value: value,
	}
	return kv.tree.Put(item)
}

func (kv *KeyValueStore) Get(key uint64) ([10]byte, error) {
	item := &kvKey{
		key: key,
	}
	val, err := kv.tree.Get(item)
	if err != nil {
		return [10]byte{}, err
	}
	return val.(kvEntry).value, nil
}
