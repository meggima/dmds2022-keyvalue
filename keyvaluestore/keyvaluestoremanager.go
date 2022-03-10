package keyvaluestore

type KeyValueStoreManager struct {
}

func (kv *KeyValueStoreManager) Create(directoryName string, memorySize uint64) error {
	return nil
}

func (kv *KeyValueStoreManager) Open(directoryName string) (KeyValueStoreAccessor, error) {
	return nil, nil
}

func (kv *KeyValueStoreManager) Delete(directoryName string) error {
	return nil
}
