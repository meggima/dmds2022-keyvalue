package keyvaluestore

// An interface to manage key value stores
type KeyValueStoreController interface {

	// Create a new key value store in the specified directory.
	// If no directory name is specified (= empty string) the current directory is used.
	// The function returns an error if something goes wrong.
	Create(directoryName string) error

	// Open the key value store in the specified directory using the given memory size (in Bytes).
	// If memory size is unspecified (=0) we use a default value of 1 MByte.
	// Returns a key value store object.
	// If no key value store exists in the current directory the function returns an error.
	Open(directoryName string, memorySize uint64) (KeyValueStoreAccessor, error)

	// Close the key value store.
	// Returns an error if something goes wrong.
	Close(keyValueStore KeyValueStoreAccessor) error

	// Deletes the key value store in the specified directory.
	// If no key value store exists in the current directory the function returns an error.
	Delete(directoryName string) error
}
