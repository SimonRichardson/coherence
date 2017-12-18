package bloom

// the wordSize of a bit set
const wordSize = uint(64)

// log2WordSize is lg(wordSize)
const log2WordSize = uint(6)

type Bits struct {
	len uint
	b   []uint64
}

func NewBits(len uint) *Bits {
	return &Bits{
		len: len,
		b:   make([]uint64, requiredLen(len)),
	}
}

func (b *Bits) Set(i uint) {
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

	b.b[i>>log2WordSize] |= 1 << (i & (wordSize - 1))
}

func (b *Bits) Contains(i uint) bool {
	if i >= b.len {
		return false
	}
	return b.b[i>>log2WordSize]&(1<<(i&(wordSize-1))) != 0
}

func (b *Bits) Len() uint {
	return b.len
}

func requiredLen(len uint) int {
	return int((len + (wordSize - 1)) >> log2WordSize)
}
