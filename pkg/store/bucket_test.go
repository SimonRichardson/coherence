package store

import "testing"
import "github.com/trussle/coherence/pkg/selectors"
import "testing/quick"

func TestBucketInsertion(t *testing.T) {
	t.Parallel()

	t.Run("inserting field and value pair", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			changeSet, err := bucket.Insert(field, value)
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting same field with a older score should be idempotent", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			_, err := bucket.Insert(field, value)
			if err != nil {
				t.Fatal(err)
			}
			changeSet, err := bucket.Insert(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score - 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting same field that was a delete with a older score should be idempotent", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			_, err := bucket.Delete(field, value)
			if err != nil {
				t.Fatal(err)
			}
			changeSet, err := bucket.Insert(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score - 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting then select should return field value and score", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			_, err := bucket.Insert(field, value)
			if err != nil {
				t.Fatal(err)
			}

			fieldValueScore, err := bucket.Select(field)
			if err != nil {
				t.Fatal(err)
			}

			return fieldValueScore.Equal(selectors.FieldValueScore{
				Field: field,
				Value: value.Value,
				Score: value.Score,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting expectations", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)

			if _, err := bucket.Delete(field, value); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Insert(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 1,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Insert(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 2,
			}); err != nil {
				t.Fatal(err)
			}

			if expected, actual := 1, bucket.insert.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := 0, bucket.delete.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			amount, err := bucket.Len()
			if err != nil {
				t.Fatal(err)
			}
			return amount == 1
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting scores", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)

			if _, err := bucket.Delete(field, value); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Insert(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 1,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Insert(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 2,
			}); err != nil {
				t.Fatal(err)
			}

			presence, err := bucket.Score(field)
			if err != nil {
				t.Fatal(err)
			}
			return presence.Equal(selectors.Presence{
				Inserted: true,
				Present:  true,
				Score:    value.Score + 2,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("inserting after bucket size", func(t *testing.T) {
		fn := func(field0, field1 selectors.Field, value0, value1 selectors.ValueScore) bool {
			bucket := NewBucket(1)

			if _, err := bucket.Insert(field0, value0); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Insert(field1, value1); err != nil {
				t.Fatal(err)
			}

			presence0, err := bucket.Score(field0)
			if err != nil {
				t.Fatal(err)
			}

			presence1, err := bucket.Score(field1)
			if err != nil {
				t.Fatal(err)
			}
			return presence0.Equal(selectors.Presence{
				Inserted: false,
				Present:  false,
				Score:    -1,
			}) && presence1.Equal(selectors.Presence{
				Inserted: true,
				Present:  true,
				Score:    value1.Score,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestBucketDeletion(t *testing.T) {
	t.Parallel()

	t.Run("deleting field and value pair", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			changeSet, err := bucket.Delete(field, value)
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting same field with a older score should be idempotent", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			_, err := bucket.Delete(field, value)
			if err != nil {
				t.Fatal(err)
			}
			changeSet, err := bucket.Delete(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score - 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting same field that was a delete with a older score should be idempotent", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			_, err := bucket.Insert(field, value)
			if err != nil {
				t.Fatal(err)
			}
			changeSet, err := bucket.Delete(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score - 1,
			})
			if err != nil {
				t.Fatal(err)
			}

			return changeSet.Equal(selectors.ChangeSet{
				Success: []selectors.Field{
					field,
				},
				Failure: make([]selectors.Field, 0),
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting then select should return not found error", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)
			_, err := bucket.Delete(field, value)
			if err != nil {
				t.Fatal(err)
			}

			_, err = bucket.Select(field)
			return NotFoundError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting expectations", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)

			if _, err := bucket.Insert(field, value); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Delete(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 1,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Delete(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 2,
			}); err != nil {
				t.Fatal(err)
			}

			if expected, actual := 0, bucket.insert.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			if expected, actual := 1, bucket.delete.Len(); expected != actual {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}

			amount, err := bucket.Len()
			if err != nil {
				t.Fatal(err)
			}
			return amount == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting scores", func(t *testing.T) {
		fn := func(field selectors.Field, value selectors.ValueScore) bool {
			bucket := NewBucket(1)

			if _, err := bucket.Insert(field, value); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Delete(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 1,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Delete(field, selectors.ValueScore{
				Value: value.Value,
				Score: value.Score + 2,
			}); err != nil {
				t.Fatal(err)
			}

			presence, err := bucket.Score(field)
			if err != nil {
				t.Fatal(err)
			}
			return presence.Equal(selectors.Presence{
				Inserted: false,
				Present:  true,
				Score:    value.Score + 2,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("deleting after bucket size", func(t *testing.T) {
		fn := func(field0, field1 selectors.Field, value0, value1 selectors.ValueScore) bool {
			bucket := NewBucket(1)

			if _, err := bucket.Delete(field0, value0); err != nil {
				t.Fatal(err)
			}
			if _, err := bucket.Delete(field1, value1); err != nil {
				t.Fatal(err)
			}

			presence0, err := bucket.Score(field0)
			if err != nil {
				t.Fatal(err)
			}

			presence1, err := bucket.Score(field1)
			if err != nil {
				t.Fatal(err)
			}
			return presence0.Equal(selectors.Presence{
				Inserted: false,
				Present:  false,
				Score:    -1,
			}) && presence1.Equal(selectors.Presence{
				Inserted: false,
				Present:  true,
				Score:    value1.Score,
			})
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
