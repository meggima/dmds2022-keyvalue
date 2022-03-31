package keyvaluestore

import (
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"os"
)

const (
	FILENAME = "kv.store"
)

type KeyValueStoreManager struct {
}

func (kv *KeyValueStoreManager) Create(directoryName string, memorySize uint64) error {
	if _, err := os.Stat(directoryName); os.IsNotExist(err) {
		return keyvaluestore.ErrDirectoryExists
	}
	filepath := getStoreFileName(directoryName)
	f, err := os.Create(filepath)
	if os.IsExist(err) {
		return keyvaluestore.ErrStoreExists
	} else if err != nil {
		return err
	}
	defer f.Close()
	
	rootId := uint64(1)
	nextNodeId := uint64(1)

	_, err = f.Write(ConvertUInt64(rootId))
	if err != nil {
		return err
	}
	_, err = f.Write(ConvertUInt64(nextNodeId))
	if err != nil {
		return err
	}
	_, err = f.Write(ConvertUInt64(memorySize))
	if err != nil {
		return err
	}

	return nil
}

func (kv *KeyValueStoreManager) Open(directoryName string) (KeyValueStoreAccessor, error) {
	filepath := getStoreFileName(directoryName)
	f, err := os.OpenFile(filepath, os.O_RDWR, 0)
	if os.IsNotExist(err) {
		return nil, keyvaluestore.ErrStoreNotExists
	}

	kvStore, err := New(f)
	if err != nil {
		return nil, err
	}

	return kvStore, nil
}

func (kv *KeyValueStoreManager) Close(keyValueStore KeyValueStoreAccessor) error {
	// TODO implement this

	return nil // do nothing
}

func (kv *KeyValueStoreManager) Delete(directoryName string) error {
	// TODO implement this
	// filepath := getStoreFileName(directoryName)

	return keyvaluestore.ErrNotFound
}

func getStoreFileName(directoryName string) string {
	filepath := directoryName
	if string(filepath[len(filepath)-1]) != "/" {
		filepath += filepath
	}
	filepath += FILENAME
	return filepath
}