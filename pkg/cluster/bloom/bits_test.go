package bloom

import (
	"bytes"
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

	t.Run("clear", func(t *testing.T) {
		fn := func(a uint) bool {
			h := a % 512
			bits := NewBits(512)
			bits.Set(h)
			bits.Clear(h)
			return !bits.Contains(h)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("string", func(t *testing.T) {
		bits := NewBits(100)
		if expected, actual := "{}", bits.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		bits.Set(10)

		if expected, actual := "{10}", bits.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		bits.Set(12)

		if expected, actual := "{10,12}", bits.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		bits.Set(54)

		if expected, actual := "{10,12,54}", bits.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		bits.Clear(12)

		if expected, actual := "{10,54}", bits.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("union", func(t *testing.T) {
		a := NewBits(100)
		b := NewBits(100)

		a.Set(10)
		b.Set(10)

		a.Union(b)

		if expected, actual := "{10}", a.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		b.Set(12)

		a.Union(b)

		if expected, actual := "{10,12}", a.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		b.Set(54)

		a.Union(b)

		if expected, actual := "{10,12,54}", a.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}

		b.Clear(54)

		a.Union(b)

		if expected, actual := "{10,12,54}", a.String(); expected != actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})

	t.Run("read and write", func(t *testing.T) {
		fn := func(a uint) bool {
			h := a % 512
			bits := NewBits(512)
			bits.Set(h)

			buf := new(bytes.Buffer)
			if _, err := bits.Write(buf); err != nil {
				t.Error(err)
			}

			other := new(Bits)
			if _, err := other.Read(buf); err != nil {
				t.Error(err)
			}

			return bits.String() == other.String()
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
