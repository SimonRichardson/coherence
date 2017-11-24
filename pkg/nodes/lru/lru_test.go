package lru_test

import (
	"errors"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/trussle/coherence/pkg/nodes/lru"
	"github.com/trussle/coherence/pkg/selectors"
)

func TestLRU_Add(t *testing.T) {
	t.Parallel()

	t.Run("adding with eviction", func(t *testing.T) {
		fn := func(id0, id1 selectors.Field, rec0, rec1 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := lru.NewLRU(1, onEviction)

			if expected, actual := false, l.Add(id0, rec0); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := true, l.Add(id1, rec1); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := 1, l.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []selectors.FieldScore{
				selectors.FieldScore{id1, rec1},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("adding sorts keys", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2, rec3 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Add(id0, rec3)

			values := []selectors.FieldScore{
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
				selectors.FieldScore{id0, rec3},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Get(t *testing.T) {
	t.Parallel()

	t.Run("get", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			value, ok := l.Get(id0)

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := rec0, value; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("get sorts keys", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Get(id0)

			values := []selectors.FieldScore{
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
				selectors.FieldScore{id0, rec0},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Peek(t *testing.T) {
	t.Parallel()

	t.Run("peek", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			value, ok := l.Peek(id0)

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := rec0, value; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("peek does not sorts keys", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Peek(id0)

			values := []selectors.FieldScore{
				selectors.FieldScore{id0, rec0},
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Contains(t *testing.T) {
	t.Parallel()

	t.Run("contains", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			ok := l.Contains(id1)

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("does not contains", func(t *testing.T) {
		fn := func(id0, id1, id2, id3 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			ok := l.Contains(id3)

			if expected, actual := false, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Remove(t *testing.T) {
	t.Parallel()

	t.Run("removes key value pair", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Remove(id0)

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []selectors.FieldScore{
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Pop(t *testing.T) {
	t.Parallel()

	t.Run("pop on empty", func(t *testing.T) {
		onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
			t.Fatal("failed if called")
		}

		l := lru.NewLRU(3, onEviction)

		_, _, ok := l.Pop()

		if expected, actual := false, ok; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("pop", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			key, value, ok := l.Pop()

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			if expected, actual := true, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
			if expected, actual := id0, key; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if expected, actual := rec0, value; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("pop results", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				if expected, actual := id0, k; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				evictted += 1
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Pop()

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values := []selectors.FieldScore{
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Purge(t *testing.T) {
	t.Parallel()

	t.Run("purge", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				evictted += 1
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			values := []selectors.FieldScore{
				selectors.FieldScore{id0, rec0},
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			l.Purge()

			if expected, actual := 3, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			values = []selectors.FieldScore{}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Keys(t *testing.T) {
	t.Parallel()

	t.Run("keys", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			got := l.Keys()

			values := []selectors.Field{
				id0,
				id1,
				id2,
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("keys after get", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				t.Fatal("failed if called")
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			l.Get(id0)

			got := l.Keys()

			values := []selectors.Field{
				id1,
				id2,
				id0,
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestLRU_Dequeue(t *testing.T) {
	t.Parallel()

	t.Run("dequeue", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				evictted += 1
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			values := []selectors.FieldScore{
				selectors.FieldScore{id0, rec0},
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			got, err := l.Dequeue(func(key selectors.Field, value float64) error {
				return nil
			})
			if expected, actual := true, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 3, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			values = []selectors.FieldScore{}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue with error", func(t *testing.T) {
		fn := func(id0, id1, id2 selectors.Field, rec0, rec1, rec2 float64) bool {
			evictted := 0
			onEviction := func(reason lru.EvictionReason, k selectors.Field, v float64) {
				evictted += 1
			}

			l := lru.NewLRU(3, onEviction)

			l.Add(id0, rec0)
			l.Add(id1, rec1)
			l.Add(id2, rec2)

			values := []selectors.FieldScore{
				selectors.FieldScore{id0, rec0},
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			got, err := l.Dequeue(func(key selectors.Field, value float64) error {
				if key.Equal(id1) {
					return errors.New("bad")
				}
				return nil
			})
			if expected, actual := false, err == nil; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}

			if expected, actual := 1, evictted; expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			values = []selectors.FieldScore{
				selectors.FieldScore{id0, rec0},
			}
			if expected, actual := values, got; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			values = []selectors.FieldScore{
				selectors.FieldScore{id1, rec1},
				selectors.FieldScore{id2, rec2},
			}
			if expected, actual := values, l.Slice(); !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
