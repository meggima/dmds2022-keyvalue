package keyvaluestore

import "errors"

var (
	ErrDirectoryExists = errors.New("directory does not exist")
	ErrStoreExists     = errors.New("key value store already exists")
	ErrStoreNotExists  = errors.New("key value store does not exist")
	ErrNotImplemented  = errors.New("method has not been implemented yet")
	ErrNotFound        = errors.New("key not found in store")
)
