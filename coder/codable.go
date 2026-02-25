// Package coder provides a binary encoding/decoding library for network communication.
// It supports various data types including basic types, variable-length integers,
// strings, byte arrays, and complex types like maps and slices.
package coder

// Encodable defines an interface for types that can be encoded to a binary buffer.
// Implementations should write their state to the provided Encoder.
type Encodable interface {
	// WriteTo encodes the object to the provided Encoder.
	WriteTo(Encoder) error
}

// Decodable defines an interface for types that can be decoded from a binary buffer.
// Implementations should read their state from the provided Decoder.
type Decodable interface {
	// ReadFrom decodes the object from the provided Decoder.
	ReadFrom(Decoder) error
}

// Codable defines an interface that combines both Encodable and Decodable.
// Types implementing this interface can be both serialized and deserialized.
type Codable interface {
	Encodable
	Decodable
}

// Error defines error codes for the coder package.
type Error uint8

const (
	// ErrVarintOverflow is returned when decoding a varint that exceeds 64 bits.
	ErrVarintOverflow Error = 1
	// ErrBufferTooShort is returned when there are not enough bytes in the buffer to complete decoding.
	ErrBufferTooShort Error = 2
)

// Error returns the string representation of the error code.
func (e Error) Error() string {
	switch e {
	case ErrVarintOverflow:
		return "varint overflow"
	case ErrBufferTooShort:
		return "buffer too short"
	default:
		return "unknown error"
	}
}

// Marshal encodes an Encodable object into a binary byte slice.
// Returns the encoded bytes and any error encountered during encoding.
func Marshal(ec Encodable) ([]byte, error) {
	coder := NewEncoder()
	err := ec.WriteTo(coder)
	return coder.Pick(), err
}

// Unmarshal decodes a binary byte slice into a Decodable object.
// Returns any error encountered during decoding.
func Unmarshal(b []byte, dc Decodable) error {
	coder := NewDecoder(b)
	return dc.ReadFrom(coder)
}

type Sizeable interface {
	Size() int
}

func BoolSize(v bool) int {
	return 1
}
func VarintSize(v uint64) int {
	if v < 1<<7 {
		return 1
	}
	if v < 1<<14 {
		return 2
	}
	if v < 1<<21 {
		return 3
	}
	if v < 1<<28 {
		return 4
	}
	if v < 1<<35 {
		return 5
	}
	if v < 1<<42 {
		return 6
	}
	if v < 1<<49 {
		return 7
	}
	if v < 1<<56 {
		return 8
	}
	if v < 1<<63 {
		return 9
	}
	return 10
}
func Uint8Size(v uint8) int {
	return 1
}
func Uint16Size(v uint16) int {
	return 2
}
func Uint32Size(v uint32) int {
	return 4
}
func Uint64Size(v uint64) int {
	return 8
}
func Int8Size(v int8) int {
	return 1
}
func Int16Size(v int16) int {
	return 2
}
func Int32Size(v int32) int {
	return 4
}
func Int64Size(v int64) int {
	return 8
}
func DataSize(v []byte) int {
	l := len(v)
	return VarintSize(uint64(l)) + l
}

func ArraySize[S Sizeable](a []S) int {
	var size int
	for _, v := range a {
		size += v.Size()
	}
	return VarintSize(uint64(len(a))) + size
}
func VarintsSize(a []uint64) int {
	var size int
	for _, v := range a {
		size += VarintSize(v)
	}
	return VarintSize(uint64(len(a))) + size
}
