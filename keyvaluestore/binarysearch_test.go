package keyvaluestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var bTestsExisting = []struct {
	searchKey     uint64
	expectedIndex uint32
}{
	{1, 0},
	{2, 1},
	{3, 2},
	{4, 3},
	{5, 4},
	{6, 5},
	{7, 6},
	{8, 7},
}

func comparer(a uint64, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func TestBinarySearchForExistingKey(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var arr []uint64 = []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	for _, testData := range bTestsExisting {
		// Act
		res := BinarySearch(testData.searchKey, &arr, 8, comparer)

		// Assert
		assert.Equal(testData.expectedIndex, res)
	}
}

var bTestsNotExisting = []struct {
	searchKey     uint64
	expectedIndex uint32
}{
	{1, 0},
	{11, 1},
	{21, 2},
	{31, 3},
	{41, 4},
	{51, 5},
	{61, 6},
	{71, 7},
	{81, 8},
}

func TestBinarySearchForNotExistingKey(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var arr []uint64 = []uint64{10, 20, 30, 40, 50, 60, 70, 80}

	for _, testData := range bTestsNotExisting {
		// Act
		res := BinarySearch(testData.searchKey, &arr, 8, comparer)

		// Assert
		assert.Equal(testData.expectedIndex, res)
	}
}

func TestBinarySearchEmptyArray(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var arr []uint64 = []uint64{}

	// Act
	res := BinarySearch(0, &arr, 0, comparer)

	// Assert
	assert.Equal(uint32(0), res)
}

var bLeafTests = []struct {
	searchKey     uint64
	expectedIndex uint32
}{
	{1, 0},
	{2, 0},
	{10, 0},
	{11, 1},
	{20, 1},
	{80, 7},
	{81, 8},
}

func TestBinarySearchLeaf(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var arr []uint64 = []uint64{10, 20, 30, 40, 50, 60, 70, 80}

	for _, testData := range bLeafTests {
		// Act
		res := BinarySearch(testData.searchKey, &arr, 8, leafKeyComparer)

		// Assert
		assert.Equal(testData.expectedIndex, res)
	}
}

var bNonLeafTests = []struct {
	searchKey     uint64
	expectedIndex uint32
}{
	{1, 0},
	{2, 0},
	{10, 1},
	{11, 1},
	{20, 2},
	{80, 8},
	{81, 8},
}

func TestBinarySearchNonLeaf(t *testing.T) {
	// Arrange
	assert := assert.New(t)
	var arr []uint64 = []uint64{10, 20, 30, 40, 50, 60, 70, 80}

	for _, testData := range bNonLeafTests {
		// Act
		res := BinarySearch(testData.searchKey, &arr, 8, nonLeafKeyComparer)

		// Assert
		assert.Equal(testData.expectedIndex, res)
	}
}
