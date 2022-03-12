package keyvaluestore

import keyvaluestore "keyvaluestore/keyvaluestore/errors"

type KeyValueStoreManager struct {
}

func (kv *KeyValueStoreManager) Create(directoryName string, memorySize uint64) error {
	return keyvaluestore.ErrNotImplemented
}

func (kv *KeyValueStoreManager) Open(directoryName string) (KeyValueStoreAccessor, error) {
	return nil, keyvaluestore.ErrNotImplemented
}

func (kv *KeyValueStoreManager) Close(accessor KeyValueStoreAccessor) error {
	return keyvaluestore.ErrNotImplemented
}

func (kv *KeyValueStoreManager) Delete(directoryName string) error {
	return keyvaluestore.ErrNotImplemented
}
