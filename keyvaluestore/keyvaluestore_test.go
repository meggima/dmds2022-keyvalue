package keyvaluestore

import (
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"log"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var kv KeyValueStoreAccessor

func TestMain(m *testing.M) {
	log.Println("Initalizing kv store")
	keyValueManager := KeyValueStoreManager{}
	keyValueManager.Delete("/tmp")
	keyValueManager.Create("/tmp", 100)
	//kv, _ = keyValueManager.Open("/tmp")
	kv = &KeyValueStore{}
	m.Run()
}

func TestSimpleInsertAndGet(t *testing.T) {
	assert := assert.New(t)
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	var val [10]byte
	copy(val[:], "Test")
	assert.NoError(kv.Put(1, val))
	ret, err := kv.Get(1)
	assert.NoError(err)
	assert.NotNil(ret)
	assert.Equal(val, ret)
}

func TestInsertAndReadMoreThanMemory(t *testing.T) {
	assert := assert.New(t)
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	var val [10]byte
	var i uint64
	for i = 1; i <= 100; i++ {
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= 100; i++ {
		ret, err := kv.Get(i)
		assert.NoError(err)
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}

func TestReadMissingValue(t *testing.T) {
	assert := assert.New(t)
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	_, err := kv.Get(9999)

	assert.ErrorIs(err, keyvaluestore.ErrNotFound)
}
