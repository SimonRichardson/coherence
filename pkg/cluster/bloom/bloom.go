package bloom

import "github.com/spaolacci/murmur3"

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
