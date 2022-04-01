package keyvaluestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferManager(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	// Act
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	// Assert
	assert.Equal(uint64(10), bm.capacity)
	assert.Equal(uint64(0), bm.size)
	assert.Nil(bm.leastRecentlyAccessedEntry)
	assert.Nil(bm.mostRecentlyAccessedEntry)
	assert.Empty(bm.buffer)
}

func TestGetItemBufferEmpty(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	// Act
	node, _ := bm.Get(1)

	// Assert
	assert.Nil(node)
}

func TestPutSingleItem(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node *node = &node{
		nodeId: 1,
	}

	// Act
	bm.Put(node)

	// Assert
	var entry *BufferEntry = bm.buffer[1]

	assert.NotNil(entry)
	assert.Nil(entry.nextAccessedEntry)
	assert.Nil(entry.previousAccessedEntry)
	assert.Equal(uint64(1), bm.size)
	assert.Equal(entry, bm.leastRecentlyAccessedEntry)
	assert.Equal(entry, bm.mostRecentlyAccessedEntry)
}

func TestPutAndGetSingleItem(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	bm.Put(node1)

	// Act
	node, _ := bm.Get(1)

	// Assert
	var entry *BufferEntry = bm.buffer[1]

	assert.NotNil(entry)
	assert.Equal(node1, node)
	assert.Nil(entry.nextAccessedEntry)
	assert.Nil(entry.previousAccessedEntry)
	assert.Equal(uint64(1), bm.size)
	assert.Equal(entry, bm.leastRecentlyAccessedEntry)
	assert.Equal(entry, bm.mostRecentlyAccessedEntry)
}

func TestPutTwoItems(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	var node2 *node = &node{
		nodeId: 2,
	}

	// Act
	bm.Put(node1)
	bm.Put(node2)

	// Assert
	var entry1 *BufferEntry = bm.buffer[1]
	var entry2 *BufferEntry = bm.buffer[2]

	assert.NotNil(entry1)
	assert.Equal(entry2, entry1.nextAccessedEntry)
	assert.Nil(entry1.previousAccessedEntry)

	assert.NotNil(entry2)
	assert.Equal(entry1, entry2.previousAccessedEntry)
	assert.Nil(entry2.nextAccessedEntry)

	assert.Equal(uint64(2), bm.size)
	assert.Equal(entry1, bm.leastRecentlyAccessedEntry)
	assert.Equal(entry2, bm.mostRecentlyAccessedEntry)
}

func TestPutTwoAndGetOneItem(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	var node2 *node = &node{
		nodeId: 2,
	}

	bm.Put(node1)
	bm.Put(node2)

	// Act
	node, _ := bm.Get(1)

	// Assert
	assert.Equal(node1, node)
}

func TestPutAndGetOneItemCorrectRecentlyUsedValues(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	bm.Put(node1)

	// Act
	node, _ := bm.Get(1)

	// Assert
	assert.NotNil(node)
	var entry1 *BufferEntry = bm.buffer[1]
	assert.Equal(entry1, bm.mostRecentlyAccessedEntry)
	assert.Equal(entry1, bm.leastRecentlyAccessedEntry)
	assert.Nil(entry1.nextAccessedEntry)
	assert.Nil(entry1.previousAccessedEntry)
}

func TestPutTwoAndGetOneItemCorrectRecentlyUsedValues(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	var node2 *node = &node{
		nodeId: 2,
	}

	bm.Put(node1)
	bm.Put(node2)

	// Act
	node, _ := bm.Get(1)

	// Assert
	assert.NotNil(node)
	var entry1 *BufferEntry = bm.buffer[1]
	var entry2 *BufferEntry = bm.buffer[2]
	assert.Equal(entry1, bm.mostRecentlyAccessedEntry)
	assert.Equal(entry2, bm.leastRecentlyAccessedEntry)
}

func TestPutThreeAndGetMiddleItemCorrectRecentlyUsedValues(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	var node2 *node = &node{
		nodeId: 2,
	}

	var node3 *node = &node{
		nodeId: 3,
	}

	bm.Put(node1)
	bm.Put(node2)
	bm.Put(node3)

	// Act
	node, _ := bm.Get(2)

	// Assert
	assert.NotNil(node)
	var entry1 *BufferEntry = bm.buffer[1]
	var entry2 *BufferEntry = bm.buffer[2]
	var entry3 *BufferEntry = bm.buffer[3]

	assert.Equal(entry2, bm.mostRecentlyAccessedEntry)
	assert.Equal(entry1, bm.leastRecentlyAccessedEntry)

	assert.Equal(entry3, entry2.previousAccessedEntry)
	assert.Equal(entry1, entry3.previousAccessedEntry)
	assert.Nil(entry1.previousAccessedEntry)

	assert.Equal(entry3, entry1.nextAccessedEntry)
	assert.Equal(entry2, entry3.nextAccessedEntry)
	assert.Nil(entry2.nextAccessedEntry)
}

func TestPutThreeAndGetMostRecentItemCorrectRecentlyUsedValues(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	var node2 *node = &node{
		nodeId: 2,
	}

	var node3 *node = &node{
		nodeId: 3,
	}

	bm.Put(node1)
	bm.Put(node2)
	bm.Put(node3)

	// Act
	node, _ := bm.Get(3)

	// Assert
	assert.NotNil(node)
	var entry1 *BufferEntry = bm.buffer[1]
	var entry2 *BufferEntry = bm.buffer[2]
	var entry3 *BufferEntry = bm.buffer[3]

	assert.Equal(entry3, bm.mostRecentlyAccessedEntry)
	assert.Equal(entry1, bm.leastRecentlyAccessedEntry)

	assert.Equal(entry2, entry3.previousAccessedEntry)
	assert.Equal(entry1, entry2.previousAccessedEntry)
	assert.Nil(entry1.previousAccessedEntry)

	assert.Equal(entry2, entry1.nextAccessedEntry)
	assert.Equal(entry3, entry2.nextAccessedEntry)
	assert.Nil(entry3.nextAccessedEntry)
}

func TestPutThreeAndGetLeastRecentItemCorrectRecentlyUsedValues(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var bm *BufferManagerImpl = NewBufferManager(10, &NodeReaderImpl{}, &NodeWriterImpl{})

	var node1 *node = &node{
		nodeId: 1,
	}

	var node2 *node = &node{
		nodeId: 2,
	}

	var node3 *node = &node{
		nodeId: 3,
	}

	bm.Put(node1)
	bm.Put(node2)
	bm.Put(node3)

	// Act
	node, _ := bm.Get(1)

	// Assert
	assert.NotNil(node)
	var entry1 *BufferEntry = bm.buffer[1]
	var entry2 *BufferEntry = bm.buffer[2]
	var entry3 *BufferEntry = bm.buffer[3]

	assert.Equal(entry1, bm.mostRecentlyAccessedEntry)
	assert.Equal(entry2, bm.leastRecentlyAccessedEntry)

	assert.Equal(entry3, entry1.previousAccessedEntry)
	assert.Equal(entry2, entry3.previousAccessedEntry)
	assert.Nil(entry2.previousAccessedEntry)

	assert.Equal(entry3, entry2.nextAccessedEntry)
	assert.Equal(entry1, entry3.nextAccessedEntry)
	assert.Nil(entry1.nextAccessedEntry)
}

func TestSingleItemInMemoryReplace(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	node2 := &node{nodeId: 2}
	node1 := &node{nodeId: 1}

	nodeWriter := &RecordingNodeWriter{}

	var bm *BufferManagerImpl = NewBufferManager(1, &MockNodeReader{
		toReturn: node2,
	}, nodeWriter)

	bm.Put(node1)

	// Act
	n, _ := bm.Get(2)

	// Assert
	assert.NotNil(n)
	assert.Equal(node2, n)
	assert.Len(nodeWriter.writtenNodes, 1)
}

func TestTwoItemsInMemoryReplace(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	node1 := &node{nodeId: 1}
	node2 := &node{nodeId: 2}
	node3 := &node{nodeId: 3}

	nodeWriter := &RecordingNodeWriter{}

	var bm *BufferManagerImpl = NewBufferManager(2, &MockNodeReader{
		toReturn: node1,
	}, nodeWriter)

	bm.Put(node1)
	bm.Put(node2)
	bm.Put(node3)

	// Act
	n, _ := bm.Get(1)

	// Assert
	assert.NotNil(n)
	assert.Equal(node1, n)
	assert.Len(nodeWriter.writtenNodes, 2)
}

type MockNodeReader struct {
	toReturn *node
}

func (mnr *MockNodeReader) ReadNode(nodeId uint64) (*node, error) {
	return mnr.toReturn, nil
}

type RecordingNodeWriter struct {
	writtenNodes []*node
}

func (rnw *RecordingNodeWriter) WriteNode(node *node) error {
	rnw.writtenNodes = append(rnw.writtenNodes, node)

	return nil
}
