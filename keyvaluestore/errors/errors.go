package keyvaluestore

import "errors"

var (
	ErrDirectoryExists = errors.New("directory does not exist")
	ErrStoreExists     = errors.New("key value store already exists")
	ErrStoreNotExists  = errors.New("key value store does not exist")
)
