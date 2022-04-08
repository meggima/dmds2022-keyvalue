package keyvaluestore

func leafKeyComparer(a uint64, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func nonLeafKeyComparer(a uint64, b uint64) int {
	switch {
	case a <= b:
		return -1
	default:
		return 1
	}
}
