package keyvaluestore

import (
	"encoding/binary"
	"os"
)

func ConvertUInt64(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func ConvertBool(val bool) []byte {
	if val {
		return []byte{byte(1)}
	} else {
		return []byte{byte(0)}
	}
}

func ConvertUInt32(val uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, val)
	return b
}

func ToBool(val byte) bool {
	if val == 0 {
		return false
	} else {
		return true
	}
}

func ToUInt32(val []byte) uint32 {
	return binary.LittleEndian.Uint32(val)
}

func ToUInt64(val []byte) uint64 {
	return binary.LittleEndian.Uint64(val)
}

func WriteFileHeader(f *os.File, rootId uint64, nextNodeId uint64) error {
	_, err := f.Write(ConvertUInt64(rootId))
	if err != nil {
		return err
	}
	_, err = f.Write(ConvertUInt64(nextNodeId))
	if err != nil {
		return err
	}

	return nil
}

func ReadFileHeader(f *os.File) (rootId uint64, nextNodeId uint64, err error) {
	rootId, err = ReadUInt64(f, 0)
	if err != nil {
		return 0, 0, err
	}
	nextNodeId, err = ReadUInt64(f, 8)
	if err != nil {
		return 0, 0, err
	}

	return rootId, nextNodeId, nil
}

func ReadUInt64(f *os.File, offsetByte int64) (uint64, error) {
	b := make([]byte, 8)
	_, err := f.ReadAt(b, offsetByte)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(b), nil
}
