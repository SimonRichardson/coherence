package nodes

import "testing"
import "github.com/trussle/coherence/pkg/selectors"
import "testing/quick"
import "reflect"

func TestNopInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			node := NewNop()
			ch := node.Insert(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)
				want := selectors.ChangeSet{
					Success: make([]selectors.Field, 0),
					Failure: extractFields(members),
				}

				if expected, actual := want, changeSet; !expected.Equal(changeSet) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
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
			node := NewNop()
			ch := node.Delete(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)
				want := selectors.ChangeSet{
					Success: make([]selectors.Field, 0),
					Failure: extractFields(members),
				}

				if expected, actual := want, changeSet; !expected.Equal(changeSet) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
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
			node := NewNop()
			ch := node.Select(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				result := selectors.FieldValueScoreFromElement(element)
				want := selectors.FieldValueScore{
					Field: field,
					Value: nil,
					Score: -1,
				}

				if expected, actual := want, result; !expected.Equal(result) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
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
			node := NewNop()
			ch := node.Keys()

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				result := selectors.KeysFromElement(element)
				want := make([]selectors.Key, 0)

				if expected, actual := want, result; !reflect.DeepEqual(expected, actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
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
			node := NewNop()
			ch := node.Size(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				result := selectors.Int64FromElement(element)

				if expected, actual := int64(0), result; expected != actual {
					t.Errorf("expected: %d, actual: %d", expected, actual)
				}

				found = true
			}

			return found
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
			node := NewNop()
			ch := node.Members(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				result := selectors.FieldsFromElement(element)
				want := make([]selectors.Field, 0)

				if expected, actual := want, result; !reflect.DeepEqual(expected, actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				found = true
			}

			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
