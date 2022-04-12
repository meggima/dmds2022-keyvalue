package keyvaluestore

const (
	// Full leafs are < 10 KB and > 9KB, full inner nodes ~7KB
	// We use a uniform size for entries of 10 KB. This wastes some memory
	// but because we have no control over garbage collection in Go and because Go
	// performs some magic during allocation of maps and slices we
	// stay on the safe side.
	// See the memstats_test.go file for allocation results.
	MEMORY_PER_ENTRY = uint64(10_240)
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

	err = bm.putIntoBuffer(bufferEntry)

	if err != nil {
		return nil, err
	}

	return bufferEntry.node, nil
}

func (bm *BufferManagerImpl) Put(node *node) error {
	var bufferEntry *BufferEntry = &BufferEntry{
		node:                  node,
		previousAccessedEntry: bm.mostRecentlyAccessedEntry,
		nextAccessedEntry:     nil,
	}

	err := bm.putIntoBuffer(bufferEntry)

	if err != nil {
		return err
	}

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

func (bm *BufferManagerImpl) putIntoBuffer(bufferEntry *BufferEntry) error {
	var err error

	if bm.size == bm.capacity {
		err = bm.removeLeastAccessedEntry()
	}

	if err != nil {
		return err
	}

	bm.buffer[bufferEntry.node.nodeId] = bufferEntry
	bm.setAsMostRecentlyUsed(bufferEntry)

	bm.size += 1

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
