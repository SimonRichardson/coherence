package store

import (
	"reflect"
	"testing"
	"testing/quick"

	"github.com/trussle/coherence/pkg/selectors"
)

func TestMemoryInsertion(t *testing.T) {
	t.Parallel()

	t.Run("inserting key and value", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			changeSet, err := store.Insert(key, member)
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					member.Field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting key value with older score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			if _, err := store.Insert(key, member); err != nil {
				t.Fatal(err)
			}

			changeSet, err := store.Insert(key, selectors.FieldValueScore{
				Field: member.Field,
				Value: member.Value,
				Score: member.Score - 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					member.Field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting then select", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Insert(key, member)
			if err != nil {
				t.Fatal(err)
			}

			fieldValueScore, err := store.Select(key, member.Field)
			if err != nil {
				t.Fatal(err)
			}

			return fieldValueScore.Equal(member)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting keys", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Insert(key, member)
			if err != nil {
				t.Fatal(err)
			}

			keys, err := store.Keys()
			if err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(keys, []selectors.Key{
				key,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting members", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Insert(key, member)
			if err != nil {
				t.Fatal(err)
			}

			members, err := store.Members(key)
			if err != nil {
				t.Fatal(err)
			}

			return reflect.DeepEqual(members, []selectors.Field{
				member.Field,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Insert(key, member)
			if err != nil {
				t.Fatal(err)
			}

			presence, err := store.Score(key, member.Field)
			if err != nil {
				t.Fatal(err)
			}

			return presence.Equal(selectors.Presence{
				Inserted: true,
				Present:  true,
				Score:    member.Score,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestMemoryDeletion(t *testing.T) {
	t.Parallel()

	t.Run("deleting key and value", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			changeSet, err := store.Delete(key, member)
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					member.Field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting key value with older score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			if _, err := store.Delete(key, member); err != nil {
				t.Fatal(err)
			}

			changeSet, err := store.Delete(key, selectors.FieldValueScore{
				Field: member.Field,
				Value: member.Value,
				Score: member.Score - 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					member.Field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting then select", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Delete(key, member)
			if err != nil {
				t.Fatal(err)
			}

			_, err = store.Select(key, member.Field)
			return NotFoundError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting keys", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)

			_, err := store.Insert(key, member)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Delete(key, selectors.FieldValueScore{
				Field: member.Field,
				Value: member.Value,
				Score: member.Score + 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			keys, err := store.Keys()
			if err != nil {
				t.Fatal(err)
			}
			return len(keys) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting members", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Delete(key, member)
			if err != nil {
				t.Fatal(err)
			}

			members, err := store.Members(key)
			if err != nil {
				t.Fatal(err)
			}

			return len(members) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			store := New(1, 1)
			_, err := store.Delete(key, member)
			if err != nil {
				t.Fatal(err)
			}

			presence, err := store.Score(key, member.Field)
			if err != nil {
				t.Fatal(err)
			}

			return presence.Equal(selectors.Presence{
				Inserted: false,
				Present:  true,
				Score:    member.Score,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
