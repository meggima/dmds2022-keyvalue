package keyvaluestore

// An interface for a key value store
type KeyValueStoreAccessor interface {
	// Put the given value into the store using the given key.
	// Returns an error in case something went wrong.
	Put(key uint64, value *[10]byte) error

	// Get the value for the given key from the store.
	// Returns the value or an error if the given key does not exist in the store
	// or something went wrong.
	Get(key uint64) ([10]byte, error)
}
