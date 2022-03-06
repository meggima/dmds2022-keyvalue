package keyvaluestore

type KeyValueStore struct {
}

func (kv *KeyValueStore) Put(key uint64, value [10]byte) error {
	return nil
}

func (kv *KeyValueStore) Get(key uint64) ([10]byte, error) {
	return [10]byte{}, nil
}
