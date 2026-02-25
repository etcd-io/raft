package coder

import (
	"testing"
)

// TestBasicTypes tests encoding and decoding of basic types
func TestBasicTypes(t *testing.T) {
	// Test UInt8
	t.Run("UInt8", func(t *testing.T) {
		encoder := NewEncoder()
		original := uint8(42)
		encoder.WriteUInt8(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadUInt8()
		if err != nil {
			t.Fatalf("ReadUInt8 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("UInt8 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test UInt16
	t.Run("UInt16", func(t *testing.T) {
		encoder := NewEncoder()
		original := uint16(12345)
		encoder.WriteUInt16(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadUInt16()
		if err != nil {
			t.Fatalf("ReadUInt16 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("UInt16 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test UInt32
	t.Run("UInt32", func(t *testing.T) {
		encoder := NewEncoder()
		original := uint32(123456789)
		encoder.WriteUInt32(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadUInt32()
		if err != nil {
			t.Fatalf("ReadUInt32 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("UInt32 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test UInt64
	t.Run("UInt64", func(t *testing.T) {
		encoder := NewEncoder()
		original := uint64(123456789012345)
		encoder.WriteUInt64(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadUInt64()
		if err != nil {
			t.Fatalf("ReadUInt64 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("UInt64 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test Int8
	t.Run("Int8", func(t *testing.T) {
		encoder := NewEncoder()
		original := int8(-42)
		encoder.WriteInt8(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadInt8()
		if err != nil {
			t.Fatalf("ReadInt8 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("Int8 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test Int16
	t.Run("Int16", func(t *testing.T) {
		encoder := NewEncoder()
		original := int16(-12345)
		encoder.WriteInt16(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadInt16()
		if err != nil {
			t.Fatalf("ReadInt16 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("Int16 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test Int32
	t.Run("Int32", func(t *testing.T) {
		encoder := NewEncoder()
		original := int32(-123456789)
		encoder.WriteInt32(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadInt32()
		if err != nil {
			t.Fatalf("ReadInt32 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("Int32 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test Int64
	t.Run("Int64", func(t *testing.T) {
		encoder := NewEncoder()
		original := int64(-123456789012345)
		encoder.WriteInt64(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadInt64()
		if err != nil {
			t.Fatalf("ReadInt64 failed: %v", err)
		}
		if decoded != original {
			t.Errorf("Int64 mismatch: expected %v, got %v", original, decoded)
		}
	})

	// Test Bool
	t.Run("Bool", func(t *testing.T) {
		// Test true
		encoder := NewEncoder()
		encoder.WriteBool(true)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadBool()
		if err != nil {
			t.Fatalf("ReadBool failed: %v", err)
		}
		if decoded != true {
			t.Errorf("Bool mismatch: expected true, got %v", decoded)
		}

		// Test false
		encoder = NewEncoder()
		encoder.WriteBool(false)

		decoder = NewDecoder(encoder.Pick())
		decoded, err = decoder.ReadBool()
		if err != nil {
			t.Fatalf("ReadBool failed: %v", err)
		}
		if decoded != false {
			t.Errorf("Bool mismatch: expected false, got %v", decoded)
		}
	})

	// Test Varint
	t.Run("Varint", func(t *testing.T) {
		testCases := []uint64{0, 1, 100, 1000, 1000000, 18446744073709551615}
		for _, original := range testCases {
			encoder := NewEncoder()
			encoder.WriteVarint(original)

			decoder := NewDecoder(encoder.Pick())
			decoded, err := decoder.ReadVarint()
			if err != nil {
				t.Fatalf("ReadVarint failed for %v: %v", original, err)
			}
			if decoded != original {
				t.Errorf("Varint mismatch: expected %v, got %v", original, decoded)
			}
		}
	})
}

// TestComplexTypes tests encoding and decoding of complex types
func TestComplexTypes(t *testing.T) {
	// Test Data ([]byte)
	t.Run("Data", func(t *testing.T) {
		testCases := [][]byte{nil, {}, {1, 2, 3}, {100, 200, 255}}
		for _, original := range testCases {
			encoder := NewEncoder()
			encoder.WriteData(original)

			decoder := NewDecoder(encoder.Pick())
			decoded, err := decoder.ReadData()
			if err != nil {
				t.Fatalf("ReadData failed for %v: %v", original, err)
			}
			// Compare byte slices
			if len(decoded) != len(original) {
				t.Errorf("Data length mismatch: expected %d, got %d", len(original), len(decoded))
				continue
			}
			for i := range original {
				if decoded[i] != original[i] {
					t.Errorf("Data mismatch at index %d: expected %v, got %v", i, original[i], decoded[i])
					break
				}
			}
		}
	})
}

// TestBytes tests WriteBytes and ReadBytes
func TestBytes(t *testing.T) {
	testCases := [][]byte{nil, {}, {1, 2, 3}, {100, 200, 255}}
	for _, original := range testCases {
		encoder := NewEncoder()
		encoder.WriteBytes(original)

		decoder := NewDecoder(encoder.Pick())
		decoded, err := decoder.ReadBytes(uint64(len(original)))
		if err != nil {
			t.Fatalf("ReadBytes failed for %v: %v", original, err)
		}
		// Compare byte slices
		if len(decoded) != len(original) {
			t.Errorf("Bytes length mismatch: expected %d, got %d", len(original), len(decoded))
			continue
		}
		for i := range original {
			if decoded[i] != original[i] {
				t.Errorf("Bytes mismatch at index %d: expected %v, got %v", i, original[i], decoded[i])
				break
			}
		}
	}
}
