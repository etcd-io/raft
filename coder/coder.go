// Package coder provides a binary encoding/decoding library for network communication.
// It supports various data types including basic types, variable-length integers,
// strings, byte arrays, and complex types like maps and slices.
package coder

import (
	"encoding/binary"
	"slices"
)

// Encoder defines methods for encoding various data types into a binary buffer.
// The encoded data can be retrieved using the Bytes() method.
// All encoding methods use big-endian byte order for multi-byte values.
type Encoder interface {
	// Pick returns the encoded binary data.
	// Waring: After calling Pick(), the Encoder is no longer usable.
	Pick() []byte
	// WriteBytes writes a slice of bytes directly to the buffer.
	WriteBytes(p []byte)
	// WriteUInt8 writes an 8-bit unsigned integer to the buffer.
	WriteUInt8(i uint8)
	// WriteUInt16 writes a 16-bit unsigned integer to the buffer in big-endian order.
	WriteUInt16(i uint16)
	// WriteUInt32 writes a 32-bit unsigned integer to the buffer in big-endian order.
	WriteUInt32(i uint32)
	// WriteUInt64 writes a 64-bit unsigned integer to the buffer in big-endian order.
	WriteUInt64(i uint64)
	// WriteBool writes a boolean value to the buffer (1 for true, 0 for false).
	WriteBool(b bool)
	// WriteInt8 writes an 8-bit signed integer to the buffer.
	WriteInt8(i int8)
	// WriteInt16 writes a 16-bit signed integer to the buffer in big-endian order.
	WriteInt16(i int16)
	// WriteInt32 writes a 32-bit signed integer to the buffer in big-endian order.
	WriteInt32(i int32)
	// WriteInt64 writes a 64-bit signed integer to the buffer in big-endian order.
	WriteInt64(i int64)
	// WriteVarint writes a variable-length integer to the buffer.
	// Uses Protocol Buffers varint encoding for efficient storage of small values.
	WriteVarint(i uint64)
	// WriteData writes a byte slice to the buffer with a varint length prefix.
	WriteData(data []byte)
	// WriteVarints writes a slice of variable-length integers to the buffer.
	// Format: [varint length] [varint1] [varint2] ...
	WriteVarints(vs []uint64)
}

// Decoder defines methods for decoding binary data into various Go types.
// Implements io.Reader interface for compatibility with standard library functions.
// All decoding methods use big-endian byte order for multi-byte values.
type Decoder interface {
	// ReadBytes reads exactly l bytes from the buffer.
	// Returns an error if there are not enough bytes remaining.
	ReadBytes(l uint64) ([]byte, error)
	// ReadUInt8 reads an 8-bit unsigned integer from the buffer.
	ReadUInt8() (uint8, error)
	// ReadUInt16 reads a 16-bit unsigned integer from the buffer in big-endian order.
	ReadUInt16() (uint16, error)
	// ReadUInt32 reads a 32-bit unsigned integer from the buffer in big-endian order.
	ReadUInt32() (uint32, error)
	// ReadUInt64 reads a 64-bit unsigned integer from the buffer in big-endian order.
	ReadUInt64() (uint64, error)
	// ReadBool reads a boolean value from the buffer (1 = true, 0 = false).
	ReadBool() (bool, error)
	// ReadInt8 reads an 8-bit signed integer from the buffer.
	ReadInt8() (int8, error)
	// ReadInt16 reads a 16-bit signed integer from the buffer in big-endian order.
	ReadInt16() (int16, error)
	// ReadInt32 reads a 32-bit signed integer from the buffer in big-endian order.
	ReadInt32() (int32, error)
	// ReadInt64 reads a 64-bit signed integer from the buffer in big-endian order.
	ReadInt64() (int64, error)
	// ReadVarint reads a variable-length integer from the buffer.
	// Uses Protocol Buffers varint encoding.
	ReadVarint() (uint64, error)
	// ReadData reads a byte slice from the buffer with a varint length prefix.
	ReadData() ([]byte, error)
	// ReadVarints reads a slice of variable-length integers from the buffer.
	// Expects format: [varint length] [varint1] [varint2] ...
	ReadVarints() ([]uint64, error)
}

// NewEncoder creates a new Encoder with an optional initial buffer capacity.
// If no capacity is provided, defaults to 256 bytes.
// The buffer will automatically grow as needed.
func NewEncoder() Encoder {
	return _pool.get()
}

// NewDecoder creates a new Decoder that reads from the provided byte slice.
// The decoder maintains an internal position pointer that advances as data is read.
func NewDecoder(bytes []byte) Decoder {
	return &decoder{pos: 0, buf: bytes}
}

// encoder implements both Encoder and Decoder interfaces.
// Maintains a buffer for encoding/decoding and a position pointer for decoding.
type encoder struct {
	buf  []byte
	pool encPool
}

func (e *encoder) free() {
	e.buf = e.buf[:0]
	e.pool.put(e)
}
func (e *encoder) Pick() []byte {
	defer e.free()
	return slices.Clone(e.buf)
}

// Write bytes directly
func (e *encoder) WriteBytes(p []byte) {
	e.buf = append(e.buf, p...)
}

// Write UInt 8/16/32/64
func (e *encoder) WriteUInt8(i uint8) {
	e.buf = append(e.buf, i)
}
func (e *encoder) WriteUInt16(i uint16) {
	e.buf = binary.BigEndian.AppendUint16(e.buf, i)
}
func (e *encoder) WriteUInt32(i uint32) {
	e.buf = binary.BigEndian.AppendUint32(e.buf, i)
}
func (e *encoder) WriteUInt64(i uint64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, i)
}

func (e *encoder) WriteBool(bo bool) {
	if bo {
		e.WriteUInt8(1)
	} else {
		e.WriteUInt8(0)
	}
}

// Write Int 8/16/32/64
func (e *encoder) WriteInt8(i int8) {
	e.WriteUInt8(uint8(i))
}
func (e *encoder) WriteInt16(i int16) {
	e.WriteUInt16(uint16(i))
}
func (e *encoder) WriteInt32(i int32) {
	e.WriteUInt32(uint32(i))
}
func (e *encoder) WriteInt64(i int64) {
	e.WriteUInt64(uint64(i))
}

// Write Varint
func (e *encoder) WriteVarint(i uint64) {
	e.buf = binary.AppendUvarint(e.buf, i)
}

// Write Binary Data with Varint Length Prefix
func (e *encoder) WriteData(data []byte) {
	l := len(data)
	e.WriteVarint(uint64(l))
	if l > 0 {
		e.WriteBytes(data)
	}
}

func (e *encoder) WriteVarints(vs []uint64) {
	e.WriteVarint(uint64(len(vs)))
	for _, v := range vs {
		e.WriteVarint(v)
	}
}

type decoder struct {
	pos uint64
	buf []byte
}

// Read bytes directly
func (d *decoder) ReadBytes(l uint64) ([]byte, error) {
	if l == 0 {
		return nil, nil
	}
	if d.pos+l > uint64(len(d.buf)) {
		return nil, ErrBufferTooShort
	}
	p := d.buf[d.pos : d.pos+l]
	d.pos += l
	return p, nil
}

// Read UInt 8/16/32/64
func (d *decoder) ReadUInt8() (uint8, error) {
	if d.pos+1 > uint64(len(d.buf)) {
		return 0, ErrBufferTooShort
	}
	i := d.buf[d.pos]
	d.pos++
	return i, nil
}
func (d *decoder) ReadUInt16() (uint16, error) {
	bytes, err := d.ReadBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(bytes), nil
}

func (d *decoder) ReadUInt32() (uint32, error) {
	bytes, err := d.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(bytes), nil
}

func (d *decoder) ReadUInt64() (uint64, error) {
	bytes, err := d.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bytes), nil
}

func (d *decoder) ReadBool() (bool, error) {
	i, err := d.ReadUInt8()
	if err != nil {
		return false, err
	}
	return i == 1, nil
}
func (d *decoder) ReadInt8() (int8, error) {
	u, err := d.ReadUInt8()
	if err != nil {
		return 0, err
	}
	return int8(u), nil
}
func (d *decoder) ReadInt16() (int16, error) {
	u, err := d.ReadUInt16()
	if err != nil {
		return 0, err
	}
	return int16(u), nil
}
func (d *decoder) ReadInt32() (int32, error) {
	u, err := d.ReadUInt32()
	if err != nil {
		return 0, err
	}
	return int32(u), nil
}
func (d *decoder) ReadInt64() (int64, error) {
	u, err := d.ReadUInt64()
	if err != nil {
		return 0, err
	}
	return int64(u), nil
}

func (d *decoder) ReadVarint() (uint64, error) {
	varint, len := binary.Uvarint(d.buf[d.pos:])
	if len < 0 {
		return 0, ErrVarintOverflow
	}
	if len == 0 {
		return 0, ErrBufferTooShort
	}
	d.pos += uint64(len)
	return varint, nil
}
func (d *decoder) ReadString() (string, error) {
	if bytes, err := d.ReadData(); err != nil {
		return "", err
	} else {
		return string(bytes), nil
	}
}
func (d *decoder) ReadData() ([]byte, error) {
	l, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	return d.ReadBytes(l)
}
func (d *decoder) ReadVarints() ([]uint64, error) {
	l, err := d.ReadVarint()
	if err != nil {
		return nil, err
	}
	vs := make([]uint64, l)
	for i := 0; i < int(l); i++ {
		v, err := d.ReadVarint()
		if err != nil {
			return nil, err
		}
		vs[i] = v
	}
	return vs, nil
}
