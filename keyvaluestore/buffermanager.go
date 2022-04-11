package keyvaluestore

const (
	MEMORY_OVERHEAD  = uint64(16) // TODO: these are not correct yet
	MEMORY_PER_ENTRY = uint64(16) // TODO: these are not correct yet
)

type BufferManager interface {
	Get(nodeId uint64) (*node, error)
	Put(node *node) error
	Flush() error
}

type BufferEntry struct {
	node                  *node
	previousAccessedEntry *BufferEntry
	nextAccessedEntry     *BufferEntry
}

type BufferManagerImpl struct {
	capacity                   uint64
	size                       uint64
	buffer                     map[uint64]*BufferEntry
	mostRecentlyAccessedEntry  *BufferEntry
	leastRecentlyAccessedEntry *BufferEntry
	nodeReader                 NodeReader
	nodeWriter                 NodeWriter
}

func NewBufferManager(bufferSize uint64, reader NodeReader, writer NodeWriter) *BufferManagerImpl {
	var bm *BufferManagerImpl = &BufferManagerImpl{
		size:                       0,
		capacity:                   bufferSize,
		buffer:                     make(map[uint64]*BufferEntry, bufferSize),
		mostRecentlyAccessedEntry:  nil,
		leastRecentlyAccessedEntry: nil,
		nodeReader:                 reader,
		nodeWriter:                 writer,
	}

	return bm
}

func (bm *BufferManagerImpl) Get(nodeId uint64) (*node, error) {
	var bufferEntry *BufferEntry = bm.buffer[nodeId]

	if bufferEntry != nil {
		bm.setAsMostRecentlyUsed(bufferEntry)
		return bufferEntry.node, nil
	} else {
		return bm.getFromDisk(nodeId)
	}
}

func (bm *BufferManagerImpl) getFromDisk(nodeId uint64) (*node, error) {
	bufferEntry, err := bm.readFromDisk(nodeId)

	// error happened during loading from disk
	if err != nil {
		return nil, err
	}

	// no node with the given ID exists on disk
	if bufferEntry == nil {
		return nil, nil
	}

	if bm.size == bm.capacity {
		bm.removeLeastAccessedEntry()
	}

	bm.setAsMostRecentlyUsed(bufferEntry)
	bm.buffer[nodeId] = bufferEntry

	return bufferEntry.node, nil
}

func (bm *BufferManagerImpl) Put(node *node) error {
	var bufferEntry *BufferEntry = &BufferEntry{
		node:                  node,
		previousAccessedEntry: bm.mostRecentlyAccessedEntry,
		nextAccessedEntry:     nil,
	}

	if bm.size == bm.capacity {
		err := bm.removeLeastAccessedEntry()

		if err != nil {
			return err
		}
	}

	bm.buffer[node.nodeId] = bufferEntry
	bm.setAsMostRecentlyUsed(bufferEntry)

	bm.size += 1

	return nil
}

func (bm *BufferManagerImpl) Flush() error {
	for bm.leastRecentlyAccessedEntry != nil {
		if bm.leastRecentlyAccessedEntry.node.isDirty {
			err := bm.nodeWriter.WriteNode(bm.leastRecentlyAccessedEntry.node)
			if err != nil {
				return err
			}
		}
		bm.leastRecentlyAccessedEntry = bm.leastRecentlyAccessedEntry.nextAccessedEntry
	}
	bm.mostRecentlyAccessedEntry = nil
	return nil
}

func (bm *BufferManagerImpl) setAsMostRecentlyUsed(bufferEntry *BufferEntry) {
	if bm.mostRecentlyAccessedEntry == bufferEntry {
		return
	}

	if bm.leastRecentlyAccessedEntry == bufferEntry {
		// A nextAccessedEntry always exists because there is more than
		// one element in the buffer and bufferEntry is not the most recently
		// accessed item
		bm.leastRecentlyAccessedEntry = bufferEntry.nextAccessedEntry
	}

	if bufferEntry.previousAccessedEntry != nil {
		bufferEntry.previousAccessedEntry.nextAccessedEntry = bufferEntry.nextAccessedEntry
	}

	if bufferEntry.nextAccessedEntry != nil {
		bufferEntry.nextAccessedEntry.previousAccessedEntry = bufferEntry.previousAccessedEntry
	}

	bufferEntry.previousAccessedEntry = bm.mostRecentlyAccessedEntry

	if bm.mostRecentlyAccessedEntry != nil {
		bm.mostRecentlyAccessedEntry.nextAccessedEntry = bufferEntry
	}

	bufferEntry.nextAccessedEntry = nil

	bm.mostRecentlyAccessedEntry = bufferEntry

	if bm.leastRecentlyAccessedEntry == nil {
		bm.leastRecentlyAccessedEntry = bufferEntry
	}
}

func (bm *BufferManagerImpl) removeLeastAccessedEntry() error {
	var leastRecentlyAccessedEntry *BufferEntry = bm.leastRecentlyAccessedEntry
	err := bm.writeToDisk(leastRecentlyAccessedEntry)

	if err != nil {
		return err
	}

	delete(bm.buffer, leastRecentlyAccessedEntry.node.nodeId)

	bm.size -= 1

	bm.leastRecentlyAccessedEntry = leastRecentlyAccessedEntry.nextAccessedEntry

	if leastRecentlyAccessedEntry.nextAccessedEntry != nil {
		leastRecentlyAccessedEntry.nextAccessedEntry.previousAccessedEntry = nil
	}

	if bm.mostRecentlyAccessedEntry == leastRecentlyAccessedEntry {
		bm.mostRecentlyAccessedEntry = nil
	}

	return nil
}

func (bm *BufferManagerImpl) writeToDisk(bufferEntry *BufferEntry) error {
	return bm.nodeWriter.WriteNode(bufferEntry.node)
}

func (bm *BufferManagerImpl) readFromDisk(nodeId uint64) (*BufferEntry, error) {
	node, err := bm.nodeReader.ReadNode(nodeId)

	if err != nil {
		return nil, err
	}

	if node == nil {
		return nil, nil
	}

	var bufferEntry *BufferEntry = &BufferEntry{
		node:                  node,
		previousAccessedEntry: nil, // Correct value will be set in Get function
		nextAccessedEntry:     nil, // Correct value will be set in Get function
	}

	return bufferEntry, nil
}
