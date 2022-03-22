package keyvaluestore

import keyvaluestore "keyvaluestore/keyvaluestore/errors"

type KeyValueStoreManager struct {
	dummyStore *KeyValueStore // TODO change this once we write stores to memory
}

func (kv *KeyValueStoreManager) Create(directoryName string, memorySize uint64) error {
	kv.dummyStore = New()
	return nil
}

func (kv *KeyValueStoreManager) Open(directoryName string) (KeyValueStoreAccessor, error) {
	if kv.dummyStore != nil {
		return kv.dummyStore, nil
	}
	return nil, keyvaluestore.ErrNotFound
}

func (kv *KeyValueStoreManager) Close(keyValueStore KeyValueStoreAccessor) error {
	if kv.dummyStore != nil {
		return keyvaluestore.ErrNotFound
	}
	return nil // do nothing
}

func (kv *KeyValueStoreManager) Delete(directoryName string) error {
	if kv.dummyStore != nil {
		kv.dummyStore = nil
	}
	return keyvaluestore.ErrNotFound
}
