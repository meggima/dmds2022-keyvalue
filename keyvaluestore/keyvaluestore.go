package keyvaluestore

import keyvaluestore "keyvaluestore/keyvaluestore/errors"

type KeyValueStore struct {
}

func (kv *KeyValueStore) Put(key uint64, value [10]byte) error {
	return keyvaluestore.ErrNotImplemented
}

func (kv *KeyValueStore) Get(key uint64) ([10]byte, error) {
	return [10]byte{}, keyvaluestore.ErrNotImplemented
}
