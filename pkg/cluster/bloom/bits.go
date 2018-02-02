package bloom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"strconv"
)

// the wordSize of a bit set
const wordSize = uint(64)

// log2WordSize is lg(wordSize)
const log2WordSize = uint(6)

type Bits struct {
	len uint
	b   []uint64
}

// NewBits creates a new Bits of a required length
func NewBits(len uint) *Bits {
	return &Bits{
		len: len,
		b:   make([]uint64, requiredLen(len)),
	}
}

// Set inserts a value into the Bits
func (b *Bits) Set(i uint) {
	b.grow(i)
	b.b[i>>log2WordSize] |= 1 << (i & (wordSize - 1))
}

// Clear removes a value from the Bits
func (b *Bits) Clear(i uint) {
	b.b[i>>log2WordSize] &^= 1 << (i & (wordSize - 1))
}

// Contains checks if the value is found with in the bits
func (b *Bits) Contains(i uint) bool {
	if i >= b.len {
		return false
	}
	return b.b[i>>log2WordSize]&(1<<(i&(wordSize-1))) != 0
}

// Union performs an inplace merge of two Bits
func (b *Bits) Union(other *Bits) {
	size := int(other.wordCount())
	if s := int(b.wordCount()); size > s {
		size = s
	}
	if other.len > 0 {
		b.grow(other.len - 1)
	}
	for i := 0; i < size; i++ {
		b.b[i] |= other.b[i]
	}
	if len(other.b) > size {
		for i := size; i < len(other.b); i++ {
			b.b[i] = other.b[i]
		}
	}
}

// Len returns the required length of the Bits
func (b *Bits) Len() uint {
	return b.len
}

func (b *Bits) Read(r io.Reader) (int, error) {
	var len uint64
	if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
		return 0, err
	}

	set := NewBits(uint(len))
	if uint64(set.len) != len {
		return 0, fmt.Errorf("invalid binary parsing error")
	}

	if err := binary.Read(r, binary.LittleEndian, set.b); err != nil {
		return 0, err
	}

	*b = *set
	return binary.Size(uint64(0)) + binary.Size(b.b), nil
}

func (b *Bits) Write(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, uint64(b.len)); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, b.b); err != nil {
		return 0, err
	}
	return binary.Size(uint64(0)) + binary.Size(b.b), nil
}

func (b *Bits) String() string {
	var (
		buf     bytes.Buffer
		idx, ok = b.index(0)
	)
	if ok {
		for c := 0; c < 0x40000; c++ {
			buf.WriteString(strconv.FormatInt(int64(idx), 10))
			idx, ok = b.index(idx + 1)
			if !ok {
				break
			}
			buf.WriteString(",")
		}
	}
	return fmt.Sprintf("{%s}", buf.String())
}

func (b *Bits) index(i uint) (uint, bool) {
	x := int(i >> log2WordSize)
	if x >= len(b.b) {
		return 0, false
	}

	w := b.b[x] >> (i & (wordSize - 1))
	if w != 0 {
		return i + uint(bits.TrailingZeros64(w)), true
	}

	for y := x + 1; y < len(b.b); y++ {
		if b.b[y] != 0 {
			return uint(y)*wordSize + uint(bits.TrailingZeros64(b.b[y])), true
		}
	}
	return 0, false
}

func (b *Bits) wordCount() int {
	return len(b.b)
}

func (b *Bits) grow(i uint) {
	if i >= b.len {
		size := requiredLen(i + 1)
		if cap(b.b) >= size {
			b.b = b.b[:size]
		} else if len(b.b) < size {
			v := make([]uint64, size, 2*size)
			copy(v, b.b)
			b.b = v
		}
		b.len = i + 1
	}
}

func requiredLen(len uint) int {
	return int((len + (wordSize - 1)) >> log2WordSize)
}
