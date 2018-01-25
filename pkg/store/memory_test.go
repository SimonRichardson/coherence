package store

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/trussle/fsys"
)

func TestMemoryInsertion(t *testing.T) {
	t.Parallel()

	t.Run("inserting key and value", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			changeSet, err := store.Insert(key, members)
			if err != nil {
				t.Fatal(err)
			}

			sort.Slice(changeSet.Success, func(i, j int) bool {
				return changeSet.Success[i] < changeSet.Success[j]
			})

			return changeSet.Equal(selectors.ChangeSet{
				Success: extractFields(members),
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting key value with older score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			members0 := []selectors.FieldValueScore{
				member,
			}
			members1 := []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: member.Field,
					Value: member.Value,
					Score: member.Score - 1,
				},
			}
			store, err := New(fsys.NewNopFilesystem(), 1, 1, log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Insert(key, members0); err != nil {
				t.Fatal(err)
			}

			changeSet, err := store.Insert(key, members1)
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: extractFields(members0),
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting then select", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			members := []selectors.FieldValueScore{
				member,
			}

			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Insert(key, members)
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
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			if len(members) == 0 {
				return true
			}

			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Insert(key, members)
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
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Insert(key, members)
			if err != nil {
				t.Fatal(err)
			}

			fields, err := store.Members(key)
			if err != nil {
				t.Fatal(err)
			}

			sort.Slice(fields, func(i, j int) bool {
				return fields[i] < fields[j]
			})

			want := extractFields(members)

			if len(fields) == 0 && len(want) == 0 {
				return true
			}

			return reflect.DeepEqual(fields, want)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			members := []selectors.FieldValueScore{
				member,
			}

			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Insert(key, members)
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
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			changeSet, err := store.Delete(key, members)
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: extractFields(members),
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting key value with older score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			members0 := []selectors.FieldValueScore{
				member,
			}
			members1 := []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: member.Field,
					Value: member.Value,
					Score: member.Score - 1,
				},
			}

			store, err := New(fsys.NewNopFilesystem(), 1, 1, log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Delete(key, members0); err != nil {
				t.Fatal(err)
			}

			changeSet, err := store.Delete(key, members1)
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
			members := []selectors.FieldValueScore{
				member,
			}

			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Delete(key, members)
			if err != nil {
				t.Fatal(err)
			}

			_, err = store.Select(key, member.Field)
			return selectors.NotFoundError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting keys", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}

			_, err = store.Insert(key, members)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Delete(key, incScore(members))
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
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			store, err := New(fsys.NewNopFilesystem(), 1, uint(len(members)*2), log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Delete(key, members)
			if err != nil {
				t.Fatal(err)
			}

			fields, err := store.Members(key)
			if err != nil {
				t.Fatal(err)
			}

			return len(fields) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting score", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			members := []selectors.FieldValueScore{
				member,
			}

			store, err := New(fsys.NewNopFilesystem(), 1, 1, log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Delete(key, members)
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

func TestMemoryString(t *testing.T) {
	t.Parallel()

	t.Run("string", func(t *testing.T) {
		store, err := New(fsys.NewNopFilesystem(), 1, 2, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := "", store.String(); expected == actual {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	})
}

func extractFields(members []selectors.FieldValueScore) []selectors.Field {
	res := make([]selectors.Field, len(members))
	for k, v := range members {
		res[k] = v.Field
	}

	res = unique(res)

	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})

	return res
}

func unique(a []selectors.Field) []selectors.Field {
	x := make(map[selectors.Field]struct{})
	for _, v := range a {
		x[v] = struct{}{}
	}

	var (
		index int
		res   = make([]selectors.Field, len(x))
	)
	for k := range x {
		res[index] = k
		index++
	}

	return res
}

func incScore(members []selectors.FieldValueScore) []selectors.FieldValueScore {
	res := make([]selectors.FieldValueScore, len(members))
	for k, v := range members {
		res[k] = selectors.FieldValueScore{
			Field: v.Field,
			Value: v.Value,
			Score: v.Score + 1,
		}
	}
	return res
}
