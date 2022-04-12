package keyvaluestore

import (
	"fmt"
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"os"
)

const (
	FILENAME = "kv.store"
)

type KeyValueStoreManager struct {
}

func (kv *KeyValueStoreManager) Create(directoryName string) error {
	if directoryName == "" {
		// create in current directory
		directoryName = "."
	}
	if _, err := os.Stat(directoryName); os.IsNotExist(err) {
		return keyvaluestore.ErrDirectoryNotExists
	}
	filepath := getStoreFileName(directoryName)

	if _, err := os.Stat(filepath); !os.IsNotExist(err) {
		// file already exists. throw error
		return keyvaluestore.ErrStoreExists
	}

	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	rootId := uint64(1)
	nextNodeId := uint64(1)

	return WriteFileHeader(f, rootId, nextNodeId)
}

func (kv *KeyValueStoreManager) Open(directoryName string, memorySize uint64) (KeyValueStoreAccessor, error) {
	filepath := getStoreFileName(directoryName)
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		// file doesn't exists. throw error
		return nil, keyvaluestore.ErrStoreNotExists
	}
	f, err := os.OpenFile(filepath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if memorySize == 0 {
		memorySize = DEFAULT_MEMORY_SIZE
	}
	if memorySize < DEFAULT_MEMORY_SIZE {
		return nil, fmt.Errorf("memory size must be at least %d bytes", DEFAULT_MEMORY_SIZE)
	}

	kvStore, err := New(f, os.Getpagesize(), memorySize)
	if err != nil {
		return nil, err
	}

	return kvStore, nil
}

func (kv *KeyValueStoreManager) Close(keyValueStore KeyValueStoreAccessor) error {
	store, ok := keyValueStore.(*KeyValueStore)
	if !ok {
		return keyvaluestore.ErrStoreNotExists
	}
	err := store.Flush()
	if err != nil {
		return err
	}

	store.CloseFile()
	return nil
}

func (kv *KeyValueStoreManager) Delete(directoryName string) error {
	filepath := getStoreFileName(directoryName)
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		// file doesn't exists. throw error
		return keyvaluestore.ErrStoreNotExists
	}

	return os.Remove(filepath)
}

func getStoreFileName(directoryName string) string {
	filepath := directoryName
	if filepath == "" {
		filepath = "."
	}
	if string(filepath[len(filepath)-1]) != "/" {
		filepath += "/"
	}
	filepath += FILENAME
	return filepath
}
