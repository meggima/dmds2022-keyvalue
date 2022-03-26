package keyvaluestore

import (
	keyvaluestore "keyvaluestore/keyvaluestore/errors"
	"log"
	"math"
	"math/rand"
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
	kv, _ = keyValueManager.Open("/tmp")
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
	assert.NoError(kv.Put(1, &val))
	ret, err := kv.Get(1)
	assert.NoError(err)
	assert.NotNil(ret)
	assert.Equal(val, ret)
}

func TestInsertAndReadInOrder(t *testing.T) {
	assert := assert.New(t)
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	var i uint64
	for i = 1; i <= 100; i++ {
		var val [10]byte
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= 100; i++ {
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
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	_, err := kv.Get(9999)

	assert.ErrorIs(err, keyvaluestore.ErrNotFound)
}

func TestInsertAndReadInReverseOrder(t *testing.T) {
	assert := assert.New(t)
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	var i uint64
	for i = 100; i > 0; i-- {
		var val [10]byte
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= 100; i++ {
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
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	var i uint64
	reverse := false
	for i = 1; i <= 100; i++ {
		var val [10]byte
		j := uint64(math.Ceil(float64(i) / 2))
		if reverse {
			j = 101 - j
		}
		copy(val[:], "Test"+strconv.FormatUint(j, 10))
		err := kv.Put(j, &val)
		if !assert.NoError(err) {
			return
		}
		reverse = !reverse
	}

	for i = 1; i <= 100; i++ {
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
	if kv == nil {
		assert.Fail("Unable to initialize KeyValueStore")
		return
	}

	var i uint64
	for _, j := range rand.Perm(100) {
		var val [10]byte
		i = uint64(j)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		err := kv.Put(i, &val)
		if !assert.NoError(err) {
			return
		}
	}

	for i = 1; i <= 99; i++ {
		var val [10]byte
		ret, err := kv.Get(i)
		assert.NoError(err)
		assert.NotNil(ret)
		copy(val[:], "Test"+strconv.FormatUint(i, 10))
		assert.Equal(val, ret)
	}
}
