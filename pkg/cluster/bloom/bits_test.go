package bloom

import (
	"math"
	"testing"
	"testing/quick"

	"github.com/spaolacci/murmur3"
	"github.com/trussle/uuid"
)

func TestBits(t *testing.T) {
	t.Parallel()

	t.Run("len", func(t *testing.T) {
		fn := func(a uint) bool {
			b := (a % 1024) * 10
			return NewBits(b).Len() == b
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("set", func(t *testing.T) {
		fn := func(a uuid.UUID) bool {
			h := uint(murmur3.Sum32(a.Bytes()))
			bits := NewBits(uuid.EncodedSize * 1024)
			bits.Set(h)
			return bits.Contains(h)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func benchmarkBits(t *testing.B, amount int) {
	b := NewBits(math.MaxUint32)

	res := true
	for i := 0; i < t.N; i++ {
		for j := 0; j < amount; j++ {
			a := uuid.MustNew()
			h := uint(murmur3.Sum32(a.Bytes()))
			b.Set(h)

			res = res && b.Contains(h)
		}
	}
}

func BenchmarkBits1(b *testing.B) { benchmarkBits(b, 1) }
func BenchmarkBits2(b *testing.B) { benchmarkBits(b, 2) }
func BenchmarkBits3(b *testing.B) { benchmarkBits(b, 3) }
func BenchmarkBits4(b *testing.B) { benchmarkBits(b, 4) }
func BenchmarkBits5(b *testing.B) { benchmarkBits(b, 5) }
