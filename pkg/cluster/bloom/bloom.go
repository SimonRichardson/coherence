package bloom

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/spaolacci/murmur3"
)

type Bloom struct {
	cap, recursions uint
	b               *Bits
}

func New(cap, recursions uint) *Bloom {
	return &Bloom{
		cap:        cap,
		recursions: recursions,
		b:          NewBits(cap),
	}
}

// Add data to the Bloom, returns an error if hashing fails.
func (b *Bloom) Add(data string) error {
	h, err := hash(data)
	if err != nil {
		return err
	}
	for i := uint(0); i < b.recursions; i++ {
		b.b.Set(location(h, b.cap, i))
	}
	return nil
}

// Clear data from the Bloom, returns an error if hashing fails.
func (b *Bloom) Clear(data string) error {
	h, err := hash(data)
	if err != nil {
		return err
	}
	for i := uint(0); i < b.recursions; i++ {
		b.b.Clear(location(h, b.cap, i))
	}
	return nil
}

// Union two blooms in place together
// Return error if the blooms can not be safely merged
func (b *Bloom) Union(other *Bloom) error {
	if b.cap != other.cap || b.recursions != other.recursions {
		return fmt.Errorf("bloom properties don't match")
	}
	b.b.Union(other.b)
	return nil
}

// Contains returns true if the data is in the Bloom or not.
// If true the result might be a false positive, if the result is false, the
// data is not in the set.
func (b *Bloom) Contains(data string) (bool, error) {
	h, err := hash(data)
	if err != nil {
		return false, err
	}

	for i := uint(0); i < b.recursions; i++ {
		if !b.b.Contains(location(h, b.cap, i)) {
			return false, nil
		}
	}
	return true, nil
}

// Cap returns the capacity of the Bloom
func (b *Bloom) Cap() uint {
	return b.cap
}

// Write a series of underlying bytes to the bloom. This wipes out any previous
// additions and starts from scratch.
func (b *Bloom) Write(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, uint64(b.cap)); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(b.recursions)); err != nil {
		return 0, err
	}
	n, err := b.b.Write(w)
	return n + (2 * binary.Size(uint64(0))), err
}

// Read from the underlying bytes of the bloom.
func (b *Bloom) Read(r io.Reader) (int, error) {
	var cap, rec uint64
	if err := binary.Read(r, binary.LittleEndian, &cap); err != nil {
		return 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &rec); err != nil {
		return 0, err
	}

	var (
		set    = new(Bits)
		n, err = set.Read(r)
	)
	if err != nil {
		return 0, err
	}

	b.cap = uint(cap)
	b.recursions = uint(rec)
	b.b = set

	return n + (2 * binary.Size(uint64(0))), nil
}

func (b *Bloom) String() string {
	return b.b.String()
}

func hash(data string) ([4]uint64, error) {
	h := murmur3.New128()
	if _, err := h.Write([]byte(data)); err != nil {
		return [4]uint64{}, err
	}

	a, b := h.Sum128()

	if _, err := h.Write([]byte{1}); err != nil {
		return [4]uint64{}, err
	}

	c, d := h.Sum128()

	return [4]uint64{
		a,
		b,
		c,
		d,
	}, nil

}

func location(h [4]uint64, cap, offset uint) uint {
	return uint(loc(h, uint64(offset)) % uint64(cap))
}

func loc(h [4]uint64, offset uint64) uint64 {
	rem := offset % 2
	return h[rem] + offset*h[2+(((offset+rem)%4)/2)]
}
