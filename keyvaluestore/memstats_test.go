package keyvaluestore

import (
	"runtime"
	"testing"
)

func memUsage(m1, m2 *runtime.MemStats, t *testing.T) {
	t.Log("Alloc:", m2.Alloc-m1.Alloc,
		"TotalAlloc:", m2.TotalAlloc-m1.TotalAlloc,
		"HeapAlloc:", m2.HeapAlloc-m1.HeapAlloc)
}

func TestMemoryInnerNode(t *testing.T) {
	var m1, m2 runtime.MemStats

	var n uint32 = 226
	runtime.ReadMemStats(&m1)

	node := createNode(n)

	runtime.ReadMemStats(&m2)

	// ~ 7 KB
	memUsage(&m1, &m2, t)

	t.Log(node.nodeId)
}

func TestMemoryLeafNode(t *testing.T) {
	var m1, m2 runtime.MemStats

	var n uint32 = 226
	runtime.ReadMemStats(&m1)

	node := createNode(n)
	fillNode(node)

	runtime.ReadMemStats(&m2)

	// ~ 10 KB
	memUsage(&m1, &m2, t)

	t.Log(node.nodeId)
}

func createNode(n uint32) *node {
	var node *node = &node{
		nodeId:   0,
		n:        n,
		keys:     make([]uint64, n),    // The arrays are one element larger than they need
		values:   make([]*[10]byte, n), // to be to allow overfilling them while inserting new keys.
		children: make([]uint64, n+1),  // Note the +1 as we have one child pointer more than keys.
		isLeaf:   false,
		next:     0,
		parent:   0,
		tree:     nil,
		isDirty:  false,
	}

	return node
}

func fillNode(node *node) {
	for i := 0; i < int(node.n); i++ {
		(*node).values[i] = &[10]byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}
	}
}
