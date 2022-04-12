package keyvaluestore

func BinarySearch(key uint64, array *[]uint64, n uint32, comparer comparator) uint32 {
	if n == 0 {
		return 0
	}

	var low uint32 = 0
	var high uint32 = n - 1

	var median uint32

	for low <= high {
		median = low + (high-low)/2

		res := comparer((*array)[median], key)

		if res < 0 {
			low = median + 1
		} else if res > 0 {
			if median == 0 {
				return median
			}

			high = median - 1
		} else {
			return median
		}
	}

	return low
}

type comparator func(uint64, uint64) int
