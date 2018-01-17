package bloom

import (
	"math/rand"
	"testing"
	"testing/quick"
)

func TestBits(t *testing.T) {
	t.Parallel()

	t.Run("len", func(t *testing.T) {
		fn := func(a uint) bool {
			b := (a % 512) * 10
			return NewBits(b).Len() == b
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("contains", func(t *testing.T) {
		fn := func(a uint) bool {
			h := a % 512
			bits := NewBits(512)
			return !bits.Contains(h)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("contains negative", func(t *testing.T) {
		fn := func(a uint) bool {
			var (
				h = a % 512
				j = h + 1
			)
			bits := NewBits(h)
			return !bits.Contains(j)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("set", func(t *testing.T) {
		fn := func(a uint) bool {
			h := a % 512
			bits := NewBits(512)
			bits.Set(h)
			return bits.Contains(h)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("set regrowth", func(t *testing.T) {
		fn := func(a uint) bool {
			var (
				h = a % 512
				j = h + 1
			)
			bits := NewBits(h)
			bits.Set(j)
			return bits.Contains(j)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

}

func benchmarkBits(t *testing.B, amount int) {
	max := uint(256 * 2)
	b := NewBits(max)

	t.ResetTimer()

	res := true
	for i := 0; i < t.N; i++ {
		for j := 0; j < amount; j++ {
			a := uint(rand.Uint32()) % max
			b.Set(a)

			res = res && b.Contains(a)
		}
	}
}

func BenchmarkBits1(b *testing.B) { benchmarkBits(b, 1) }
func BenchmarkBits2(b *testing.B) { benchmarkBits(b, 2) }
func BenchmarkBits3(b *testing.B) { benchmarkBits(b, 3) }
func BenchmarkBits4(b *testing.B) { benchmarkBits(b, 4) }
func BenchmarkBits5(b *testing.B) { benchmarkBits(b, 5) }
