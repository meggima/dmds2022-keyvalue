package keyvaluestore

type KeyValueStoreController interface {

	// Create a new key value store in the specified directory using the given memory size (in MB).
	// If no directory name is specified (= empty string) the current directory is used.
	// If memory size is unspecified (=0) we use a default value of 100 MBytes.
	// The function returns an error if something goes wrong.
	Create(directoryName string, memorySize uint64) error

	// Open the key value store in the specified directory.
	// Returns a key value store object.
	// If no key value store exists in the current directory the function returns an error.
	Open(directoryName string) (KeyValueStoreAccessor, error)

	// Deletes the key value store in the specified directory.
	// If no key value store exists in the current directory the function returns an error.
	Delete(directoryName string) error
}
