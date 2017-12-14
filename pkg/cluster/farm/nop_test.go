package farm

import (
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/coherence/pkg/selectors"
)

func TestNopInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			farm := NewNop()
			changeSet, err := farm.Insert(key, members)
			if err != nil {
				t.Error(err)
			}

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			if expected, actual := want, changeSet; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			farm := NewNop()
			changeSet, err := farm.Delete(key, members)
			if err != nil {
				t.Error(err)
			}

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			if expected, actual := want, changeSet; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopSelect(t *testing.T) {
	t.Parallel()

	t.Run("select", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			farm := NewNop()
			_, err := farm.Select(key, field)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopKeys(t *testing.T) {
	t.Parallel()

	t.Run("keys", func(t *testing.T) {
		fn := func() bool {
			farm := NewNop()
			keys, err := farm.Keys()
			return len(keys) == 0 && err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopSize(t *testing.T) {
	t.Parallel()

	t.Run("size", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			farm := NewNop()
			size, err := farm.Size(key)
			return size == -1 && err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopMembers(t *testing.T) {
	t.Parallel()

	t.Run("members", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			farm := NewNop()
			members, err := farm.Members(key)
			return len(members) == 0 && err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopScore(t *testing.T) {
	t.Parallel()

	t.Run("presence", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			farm := NewNop()
			presence, err := farm.Score(key, field)
			return selectors.Presence{}.Equal(presence) && err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestNopRepair(t *testing.T) {
	t.Parallel()

	t.Run("repair", func(t *testing.T) {
		fn := func(members []selectors.KeyFieldValue) bool {
			farm := NewNop()
			err := farm.Repair(members)
			return err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
