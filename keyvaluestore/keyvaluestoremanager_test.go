package keyvaluestore

import (
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	err = os.MkdirAll(home+"/kvstoremanagertest/", os.ModePerm) // create directory if it doesn't exists
	if err != nil {
		return err
	}
	err = os.RemoveAll(home + "/kvstoremanagertest/" + FILENAME) // cleanup store file at start of test
	if err != nil {
		return err
	}

	t.Cleanup(func() { os.RemoveAll(home + "/kvstoremanagertest/" + FILENAME) })
	return nil
}

func TestCreateShouldReturnErrorWhenPathNotExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	keyValueManager := KeyValueStoreManager{}

	// Act
	err := keyValueManager.Create("/some/not/existing/directory")

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrDirectoryNotExists)
}

func TestCreateShouldCreateKeyValueStore(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	home, err := os.UserHomeDir()
	assert.Nil(err)

	keyValueManager := KeyValueStoreManager{}

	// Act
	err = keyValueManager.Create(home + "/kvstoremanagertest")

	// Assert
	assert.NoError(err)
	assert.FileExists(home + "/kvstoremanagertest/" + FILENAME)
}

func TestCreateShouldCreateKeyValueStoreInCurrentDirectoryWhenPathEmpty(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	t.Cleanup(func() { os.RemoveAll("./" + FILENAME) })
	keyValueManager := KeyValueStoreManager{}

	// Act
	err := keyValueManager.Create("")

	// Assert
	assert.NoError(err)
	assert.FileExists("./" + FILENAME)
}

func TestCreateShouldReturnErrorWhenKeyValueStoreAlreadyExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	home, err := os.UserHomeDir()
	assert.NoError(err)
	keyValueManager := KeyValueStoreManager{}
	file, err := os.Create(home + "/kvstoremanagertest/" + FILENAME)
	assert.NoError(err)
	assert.NoError(file.Close())
	// Act
	err = keyValueManager.Create(home + "/kvstoremanagertest")

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrStoreExists)
}

func TestOpenShouldReturnErrorWhenKeyValueStoreNotExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	keyValueManager := KeyValueStoreManager{}

	home, err := os.UserHomeDir()
	assert.Nil(err)

	// Act
	kv, err := keyValueManager.Open(home+"/kvstoremanagertest", DEFAULT_MEMORY_SIZE)

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrStoreNotExists)
	assert.Nil(kv)
}

func TestOpenShouldReturnStoreWhenStoreExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	home, err := os.UserHomeDir()
	assert.Nil(err)
	keyValueManager := KeyValueStoreManager{}
	_ = keyValueManager.Create(home + "/kvstoremanagertest")

	// Act
	kv, err := keyValueManager.Open(home+"/kvstoremanagertest", DEFAULT_MEMORY_SIZE)

	// Assert
	assert.NoError(err)
	assert.NotNil(kv)

	kv.(*KeyValueStore).CloseFile()
}

func TestCloseShouldNotReturnError(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	home, err := os.UserHomeDir()
	assert.Nil(err)
	keyValueManager := KeyValueStoreManager{}
	_ = keyValueManager.Create(home + "/kvstoremanagertest")
	kv, _ := keyValueManager.Open(home+"/kvstoremanagertest", DEFAULT_MEMORY_SIZE)

	t.Cleanup(func() { kv.(*KeyValueStore).CloseFile() }) // close file, in case Close method fails

	// Act
	err = keyValueManager.Close(kv)

	// Assert
	assert.NoError(err)
}

func TestDeleteShouldReturnErrorWhenKeyValueStoreNotExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	home, err := os.UserHomeDir()
	assert.Nil(err)
	keyValueManager := KeyValueStoreManager{}

	// Act
	err = keyValueManager.Delete(home + "/kvstoremanagertest")

	// Assert
	assert.ErrorIs(err, keyvaluestore.ErrStoreNotExists)
}

func TestDeleteShouldDeleteStoreWhenStoreExists(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	assert.NoError(setup(t))
	home, err := os.UserHomeDir()
	assert.Nil(err)
	keyValueManager := KeyValueStoreManager{}
	_ = keyValueManager.Create(home + "/kvstoremanagertest")

	// Act
	err = keyValueManager.Delete(home + "/kvstoremanagertest")

	// Assert
	assert.NoError(err)
	assert.NoFileExists(home + "/kvstoremanagertest/" + FILENAME)
}
