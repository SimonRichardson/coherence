package cache

import (
	"reflect"
	"testing"
	"testing/quick"
)

func TestNop(t *testing.T) {
	t.Parallel()

	t.Run("add", func(t *testing.T) {
		fn := func(a []string) bool {
			cache := newNopCache()
			return cache.Add(a) == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("intersection", func(t *testing.T) {
		fn := func(a []string) bool {
			cache := newNopCache()
			union, difference, err := cache.Intersection(a)
			if expected, actual := 0, len(union); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			return reflect.DeepEqual(a, difference)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
