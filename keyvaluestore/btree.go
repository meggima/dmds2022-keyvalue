package keyvaluestore

const (
	MAX_DEGREE = 4 // TODO calculate degree based on chosen page size and size of a kvEntry
)

type bTree struct {
	root *node
}

type node struct {
	items    []item
	children []*node
}

type item interface {
	Less(than item) bool
}

// implementations of Item
type kvEntry struct {
	key   uint64
	value [10]byte
}

func (a kvEntry) Less(b item) bool {
	e, ok := b.(kvEntry)
	if ok {
		return a.key < e.key
	}
	k, ok := b.(kvKey)
	if ok {
		return a.key < k.key
	}
	return false
}

type kvKey struct {
	key uint64
}

func (a kvKey) Less(b item) bool {
	e, ok := b.(kvEntry)
	if ok {
		return a.key < e.key
	}
	k, ok := b.(kvKey)
	if ok {
		return a.key < k.key
	}
	return false
}

func NewTree() *bTree {
	return &bTree{
		root: &node{
			items:    []item{},
			children: []*node{},
		},
	}
}

func (t *bTree) Put(item item) error {
	n := t.root

	// traverse tree to matching leaf
	for len(n.children) > 0 {
		n = n.children[0]
		for i := 0; i < len(n.items); i++ {
			if n.items[i].Less(item) {
				n = n.children[i+1]
			} else {
				break
			}
		}
	}

	if len(n.items) < MAX_DEGREE-1 {
		// leaf has space, insert item
		index := 0
		for ; index < len(n.items); index++ {
			if !n.items[index].Less(item) {
				break
			}
		}

		if len(n.items) == index {
			n.items = append(n.items, item)
		} else {
			n.items = append(n.items[:index+1], n.items[index:]...)
			n.items[index] = item
		}
	}
	if len(n.items) == MAX_DEGREE-1 {
		// leaf is full, split node

		// TODO
	}

	return nil
}

func (t *bTree) Get(key item) (item, error) {
	return nil, nil
}
