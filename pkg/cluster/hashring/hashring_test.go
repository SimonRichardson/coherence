package hashring

import (
	"reflect"
	"testing"
	"testing/quick"

	"github.com/trussle/harness/generators"
)

func TestHashRingAddRemove(t *testing.T) {
	t.Parallel()

	t.Run("add", func(t *testing.T) {
		fn := func(a generators.ASCII) bool {
			ring := NewHashRing(2)
			return ring.Add(a.String())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("add duplicate", func(t *testing.T) {
		fn := func(a generators.ASCII) bool {
			ring := NewHashRing(2)
			ring.Add(a.String())
			return !ring.Add(a.String())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("remove", func(t *testing.T) {
		fn := func(a generators.ASCII) bool {
			ring := NewHashRing(2)
			return !ring.Remove(a.String())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("add then remove", func(t *testing.T) {
		fn := func(a generators.ASCII) bool {
			ring := NewHashRing(2)
			ring.Add(a.String())
			return ring.Remove(a.String())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestHashRingLookup(t *testing.T) {
	t.Parallel()

	t.Run("lookup", func(t *testing.T) {
		fn := func(a generators.ASCII) bool {
			ring := NewHashRing(10)
			if expected, actual := true, ring.Add(a.String()); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			want := []string{
				a.String(),
			}
			got := ring.LookupN(a.String(), 2)

			return reflect.DeepEqual(want, got)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
