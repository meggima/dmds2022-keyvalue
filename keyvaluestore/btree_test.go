package keyvaluestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTreeShouldCreateTree(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	// Act
	var tree *bTree = NewTree()

	// Assert
	assert.NotNil(tree.root)
	assert.Equal(0, tree.root.n)
}

func TestFindNonExistingKeyEmptyTree(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var tree *bTree = NewTree()

	// Act
	n, i, err := tree.Find(uint64(20), false)

	// Assert
	assert.Nil(err)
	assert.Equal(tree.root, n)
	assert.Equal(0, i)
}

func TestFindNonExistingKeyOnlyRoot(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	var tree *bTree = NewTree()

	tree.root.n = 1
	tree.root.keys[0] = 10

	// Act
	n, i, err := tree.Find(uint64(20), false)

	// Assert
	assert.Nil(err)
	assert.Equal(tree.root, n)
	assert.Equal(1, i)
}

func TestFindNonExistingLargerKeyTwoLeaves(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	var tree *bTree = NewTree()

	var leaf1 *node = tree.NewLeaf()
	leaf1.n = 2
	leaf1.keys[0] = 5
	leaf1.keys[1] = 10

	var leaf2 *node = tree.NewLeaf()
	leaf2.n = 2
	leaf2.keys[0] = 11
	leaf2.keys[0] = 20

	leaf1.next = leaf2

	tree.root.n = 1
	tree.root.isLeaf = false
	tree.root.keys[0] = 11
	tree.root.children = make([]*node, MAX_DEGREE+1)
	tree.root.children[0] = leaf1
	tree.root.children[1] = leaf2

	// Act
	n, i, err := tree.Find(uint64(30), false)

	// Assert
	assert.Nil(err)
	assert.Equal(leaf2, n)
	assert.Equal(2, i)
}

func TestFindNonExistingSmallerKeyTwoLeaves(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	var tree *bTree = NewTree()

	var leaf1 *node = tree.NewLeaf()
	leaf1.n = 2
	leaf1.keys[0] = 6
	leaf1.keys[1] = 10

	var leaf2 *node = tree.NewLeaf()
	leaf2.n = 2
	leaf2.keys[0] = 11
	leaf2.keys[1] = 20

	leaf1.next = leaf2

	tree.root.n = 1
	tree.root.keys[0] = 11
	tree.root.isLeaf = false
	tree.root.children = make([]*node, MAX_DEGREE+1)
	tree.root.children[0] = leaf1
	tree.root.children[1] = leaf2

	// Act
	n, i, err := tree.Find(uint64(5), false)

	// Assert
	assert.Nil(err)
	assert.Equal(leaf1, n)
	assert.Equal(0, i)
}

func TestFindNonExistingBetweenKeyTwoLeaves(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	var tree *bTree = NewTree()

	var leaf1 *node = tree.NewLeaf()
	leaf1.n = 3
	leaf1.keys[0] = 1
	leaf1.keys[1] = 5
	leaf1.keys[2] = 10

	var leaf2 *node = tree.NewLeaf()
	leaf2.n = 2
	leaf2.keys[0] = 11
	leaf2.keys[1] = 20

	leaf1.next = leaf2

	tree.root.n = 1
	tree.root.keys[0] = 11
	tree.root.isLeaf = false
	tree.root.children = make([]*node, MAX_DEGREE+1)
	tree.root.children[0] = leaf1
	tree.root.children[1] = leaf2

	// Act
	n, i, err := tree.Find(uint64(4), false)

	// Assert
	assert.Nil(err)
	assert.Equal(leaf1, n)
	assert.Equal(1, i)
}

func TestFindNonExistingBetweenKeyThreeLeaves(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	var tree *bTree = NewTree()

	var leaf1 *node = tree.NewLeaf()
	leaf1.n = 2
	leaf1.keys[0] = 5
	leaf1.keys[1] = 10

	var leaf2 *node = tree.NewLeaf()
	leaf2.n = 2
	leaf2.keys[0] = 11
	leaf2.keys[1] = 20

	var leaf3 *node = tree.NewLeaf()
	leaf3.n = 2
	leaf3.keys[0] = 25
	leaf3.keys[1] = 30

	leaf1.next = leaf2
	leaf2.next = leaf3

	tree.root.n = 2
	tree.root.keys[0] = 11
	tree.root.keys[1] = 25
	tree.root.isLeaf = false
	tree.root.children = make([]*node, MAX_DEGREE+1)
	tree.root.children[0] = leaf1
	tree.root.children[1] = leaf2
	tree.root.children[2] = leaf3

	// Act
	n, i, err := tree.Find(uint64(15), false)

	// Assert
	assert.Nil(err)
	assert.Equal(leaf2, n)
	assert.Equal(1, i)
}

func TestFindNonExistingLargerKeyThreeLeaves(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	var tree *bTree = NewTree()

	var leaf1 *node = tree.NewLeaf()
	leaf1.n = 2
	leaf1.keys[0] = 1
	leaf1.keys[1] = 10

	var leaf2 *node = tree.NewLeaf()
	leaf2.n = 2
	leaf2.keys[0] = 11
	leaf2.keys[1] = 20

	var leaf3 *node = tree.NewLeaf()
	leaf3.n = 2
	leaf3.keys[0] = 25
	leaf3.keys[1] = 30

	tree.root.n = 2
	tree.root.keys[0] = 11
	tree.root.keys[1] = 25
	tree.root.children = make([]*node, MAX_DEGREE+1)
	tree.root.isLeaf = false
	tree.root.children[0] = leaf1
	tree.root.children[1] = leaf2
	tree.root.children[2] = leaf3

	// Act
	n, i, err := tree.Find(uint64(40), false)

	// Assert
	assert.Nil(err)
	assert.Equal(leaf3, n)
	assert.Equal(2, i)
}

func TestFindKeysMultipleInnerNodes(t *testing.T) {
	// Arrange
	assert := assert.New(t)

	//                     17
	//                  /         \
	//          10 15--------------->17 30
	//       /    |    \          /    |    \
	// (7  8)->(10 11)->(15 16) ---->17 20--->30 40

	var tree *bTree = NewTree()

	var leaf1 *node = tree.NewLeaf()
	leaf1.n = 2
	leaf1.keys[0] = 7
	leaf1.keys[1] = 8

	var leaf2 *node = tree.NewLeaf()
	leaf2.n = 2
	leaf2.keys[0] = 10
	leaf2.keys[1] = 11

	var leaf3 *node = tree.NewLeaf()
	leaf3.n = 2
	leaf3.keys[0] = 15
	leaf3.keys[1] = 16

	var inner1 *node = tree.NewNode()
	inner1.n = 2
	inner1.keys[0] = 10
	inner1.keys[1] = 15
	inner1.children[0] = leaf1
	inner1.children[1] = leaf2
	inner1.children[2] = leaf3

	var leaf4 *node = tree.NewLeaf()
	leaf4.n = 2
	leaf4.keys[0] = 17
	leaf4.keys[1] = 20

	var leaf5 *node = tree.NewLeaf()
	leaf5.n = 2
	leaf5.keys[0] = 30
	leaf5.keys[1] = 40

	var inner2 *node = tree.NewNode()
	inner2.n = 2
	inner2.keys[0] = 17
	inner2.keys[1] = 30
	inner2.children[1] = leaf4
	inner2.children[2] = leaf5

	leaf1.next = leaf2
	leaf2.next = leaf3
	leaf3.next = leaf4
	leaf4.next = leaf5

	inner1.next = inner2

	tree.root.n = 1
	tree.root.keys[0] = 17
	tree.root.children = make([]*node, MAX_DEGREE+1)
	tree.root.isLeaf = false
	tree.root.children[0] = inner1
	tree.root.children[1] = inner2

	// Act
	n1, i1, err1 := tree.Find(uint64(41), false) // non existing key
	n2, i2, err2 := tree.Find(uint64(16), true)  // existing key
	n3, i3, err3 := tree.Find(uint64(17), true)  // existing key

	// Assert
	assert.Nil(err1)
	assert.Equal(leaf5, n1)
	assert.Equal(2, i1)

	assert.NotNil(err2)
	assert.Equal(leaf3, n2)
	assert.Equal(1, i2)

	assert.NotNil(err3)
	assert.Equal(leaf4, n3)
	assert.Equal(0, i3)
}
