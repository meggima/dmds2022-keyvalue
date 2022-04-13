package keyvaluestore

import (
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	NUMBER_OF_ENTRIES = 1000000 // 32768 // twice as much as there are memory bytes for the tree
)

func setupStore(t *testing.T) (KeyValueStoreAccessor, error) {
	log.Println("Initalizing kv store")
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(home+"/kvstoretest/", os.ModePerm) // create directory if it doesn't exists
	if err != nil {
		return nil, err
	}
	err = os.RemoveAll(home + "/kvstoretest/" + FILENAME) // cleanup store file at start of test
	if err != nil {
		return nil, err
	}

	keyValueManager := KeyValueStoreManager{}
	keyValueManager.Create(home + "/kvstoretest/")
	kv, err := keyValueManager.Open(home+"/kvstoretest/", DEFAULT_MEMORY_SIZE)
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		keyValueManager.Close(kv)
		keyValueManager.Delete(home + "/kvstoretest/")
	})
	return kv, nil
}

func TestSimpleInsertAndGet(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	var val [10]byte
	copy(val[:], "Test")
	assert.NoError(kv.Put(1, &val))
	ret, err := kv.Get(1)
	assert.NoError(err)
	assert.NotNil(ret)
	assert.Equal(val, ret)
}

func TestInsertAndReadInOrder(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	var i uint64
	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		ret, err := kv.Get(i)
		assert.NoError(err)
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}

func TestReadMissingValue(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	_, err = kv.Get(9999)

	assert.ErrorIs(err, keyvaluestore.ErrNotFound)
}

func TestInsertAndReadInReverseOrder(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	var i uint64
	for i = NUMBER_OF_ENTRIES; i > 0; i-- {
		var val [10]byte
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		ret, err := kv.Get(i)
		assert.NoError(err)
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}

func TestInsertAndReadInCollapsingOrder(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	var i uint64
	reverse := false
	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		j := uint64(math.Ceil(float64(i) / 2))
		if reverse {
			j = NUMBER_OF_ENTRIES + 1 - j
		}
		copy(val[:], "Test"+strconv.FormatUint(j, 10))
		err := kv.Put(j, &val)
		if !assert.NoError(err) {
			return
		}
		reverse = !reverse
	}

	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		ret, err := kv.Get(i)
		assert.NoError(err)
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}

func TestInsertAndReadInRandomOrder(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	var i uint64
	for _, j := range rand.Perm(NUMBER_OF_ENTRIES + 1) {
		var val [10]byte
		i = uint64(j)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		ret, err := kv.Get(i)
		if !assert.NoError(err) {
			return
		}
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}

func TestInsertAndReadInRandomOrderWithReopen(t *testing.T) {
	assert := assert.New(t)
	kv, err := setupStore(t)
	assert.NoError(err)

	var i uint64
	for _, j := range rand.Perm(NUMBER_OF_ENTRIES + 1) {
		var val [10]byte
		i = uint64(j)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	// Write store to file and reopen it
	keyValueManager := KeyValueStoreManager{}
	keyValueManager.Close(kv)
	home, err := os.UserHomeDir()
	assert.NoError(err)
	kv, err = keyValueManager.Open(home+"/kvstoretest/", DEFAULT_MEMORY_SIZE)
	assert.NoError(err)

	for i = 1; i <= NUMBER_OF_ENTRIES; i++ {
		var val [10]byte
		ret, err := kv.Get(i)
		if !assert.NoError(err) {
			return
		}
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}

func TestPageSize(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(4096, os.Getpagesize())
}
