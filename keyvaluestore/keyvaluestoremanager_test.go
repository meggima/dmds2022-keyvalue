package keyvaluestore

import (
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateShouldReturnErrorWhenPathNotExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}

	// Act
	err := keyValueManager.Create("/some/not/existing/directory", 0)

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrDirectoryExists)
}

func TestCreateShouldCreateKeyValueStore(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	t.Cleanup(func() { os.RemoveAll("/tmp/store.keyvaluestore") })
	keyValueManager := KeyValueStoreManager{}

	// Act
	err := keyValueManager.Create("/tmp", 0)

	// Assert
	assert.NoError(err)
	assert.FileExists("/tmp/store.keyvaluestore")
}

func TestCreateShouldCreateKeyValueStoreInCurrentDirectoryWhenPathEmpty(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	t.Cleanup(func() { os.RemoveAll("./store.keyvaluestore") })
	keyValueManager := KeyValueStoreManager{}

	// Act
	err := keyValueManager.Create("", 0)

	// Assert
	assert.NoError(err)
	assert.FileExists("./store.keyvaluestore")
}

func TestCreateShouldReturnErrorWhenKeyValueStoreAlreadyExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	t.Cleanup(func() { os.RemoveAll("/tmp/store.keyvaluestore") })
	keyValueManager := KeyValueStoreManager{}
	os.Create("/tmp/store.keyvaluestore")

	// Act
	err := keyValueManager.Create("/tmp", 0)

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrStoreExists)
}

func TestOpenShouldReturnErrorWhenKeyValueStoreNotExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}

	// Act
	kv, err := keyValueManager.Open("/tmp")

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrStoreNotExists)
	assert.Nil(kv)
}

func TestOpenShouldReturnStoreWhenStoreExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}
	_ = keyValueManager.Create("/tmp", 0)

	// Act
	kv, err := keyValueManager.Open("/tmp")

	// Assert
	assert.NoError(err)
	assert.NotNil(kv)
}

func TestCloseShouldNotReturnError(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}
	_ = keyValueManager.Create("/tmp", 0)
	kv, _ := keyValueManager.Open("/tmp")

	// Act
	err := keyValueManager.Close(kv)

	// Assert
	assert.NoError(err)
}

func TestDeleteShouldReturnErrorWhenKeyValueStoreNotExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}

	// Act
	err := keyValueManager.Delete("/tmp")

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrStoreNotExists)
}

func TestDeleteShouldDeleteStoreWhenStoreExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}
	_ = keyValueManager.Create("/tmp", 0)

	// Act
	err := keyValueManager.Delete("/tmp")

	// Assert
	assert.NoError(err)
	assert.NoFileExists("/tmp/store.keyvaluestore")
}
