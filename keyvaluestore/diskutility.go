package keyvaluestore

import "encoding/binary"

func ConvertUInt64(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(val))
	return b
}