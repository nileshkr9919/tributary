package store

import (
	"encoding/binary"
	"fmt"
)

// Key builds an order-preserving key. The first part is a one-byte family tag.
// Strings are length-prefixed (4 bytes, big-endian) and uint64 values are 8 bytes
// big-endian, so numeric parts sort numerically and string parts never collide.
func Key(tag byte, parts ...any) []byte {
	k := []byte{tag}
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			k = binary.BigEndian.AppendUint32(k, uint32(len(v)))
			k = append(k, v...)
		case uint64:
			k = binary.BigEndian.AppendUint64(k, v)
		case int:
			k = binary.BigEndian.AppendUint64(k, uint64(v))
		default:
			panic(fmt.Sprintf("store.Key: unsupported part %T", p))
		}
	}
	return k
}

// Prefix is Key used as a scan prefix.
func Prefix(tag byte, parts ...any) []byte { return Key(tag, parts...) }

// Uint64At reads a uint64 key part at byte offset off.
func Uint64At(k []byte, off int) uint64 { return binary.BigEndian.Uint64(k[off : off+8]) }

// Key family tags.
const (
	TagTopic      = 'T'
	TagOffset     = 'O'
	TagMessage    = 'M'
	TagIdem       = 'I'
	TagSub        = 'S'
	TagState      = 'X'
	TagFloor      = 'F'
	TagPool       = 'P'
	TagDeadLetter = 'D'
)
