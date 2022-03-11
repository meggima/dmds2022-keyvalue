package keyvaluestore

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleInsertAndGet(t *testing.T) {
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}
	keyValueManager.Delete("/tmp")
	keyValueManager.Create("/tmp", 100)
	kv, _ := keyValueManager.Open("/tmp")
	assert.NotNil(kv)

	var val [10]byte
	copy(val[:], "Test")
	assert.NoError(kv.Put(1, val))
	ret, err := kv.Get(1)
	assert.NoError(err)
	assert.Equal("Test", string(ret[:]))
}

func TestInsertAndReadMoreThanMemory(t *testing.T) {
	assert := assert.New(t)
	keyValueManager := KeyValueStoreManager{}
	keyValueManager.Delete("/tmp")
	keyValueManager.Create("/tmp", 100)
	kv, _ := keyValueManager.Open("/tmp")
	assert.NotNil(kv)

	var val [10]byte
	var i uint64
	for i = 1; i <= 100; i++ {
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, val)
		assert.NoError(err)
	}

	for i = 1; i <= 100; i++ {
		ret, err := kv.Get(i)
		assert.NoError(err)
		assert.Equal("Test"+strconv.FormatUint(i, 10), string(ret[:]))
	}
}
