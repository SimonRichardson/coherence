package farm

import (
	"reflect"
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/coherence/pkg/selectors"
)

func TestChangeSetRecords(t *testing.T) {
	t.Parallel()

	t.Run("no variance", func(t *testing.T) {
		fn := func(values selectors.ChangeSet) bool {
			records := changeSetRecords{}
			records.Add(values)
			records.Add(values)
			return records.Err() == nil && values.Equal(records.ChangeSet())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("variance", func(t *testing.T) {
		fn := func(value0, value1 selectors.ChangeSet) bool {
			records := changeSetRecords{}
			records.Add(value0)
			records.Add(value1)
			return records.Err() != nil && value0.Equal(records.ChangeSet())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestKeysRecords(t *testing.T) {
	t.Parallel()

	t.Run("no variance", func(t *testing.T) {
		fn := func(values []selectors.Key) bool {
			records := keysRecords{}
			records.Add(values)
			records.Add(values)
			return records.Err() == nil && reflect.DeepEqual(values, records.Keys())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("variance", func(t *testing.T) {
		fn := func(value0, value1 []selectors.Key) bool {
			records := keysRecords{}
			records.Add(value0)
			records.Add(value1)
			return records.Err() != nil && reflect.DeepEqual(value0, records.Keys())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestFieldsRecords(t *testing.T) {
	t.Parallel()

	t.Run("no variance", func(t *testing.T) {
		fn := func(values []selectors.Field) bool {
			records := fieldsRecords{}
			records.Add(values)
			records.Add(values)
			return records.Err() == nil && reflect.DeepEqual(values, records.Fields())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("variance", func(t *testing.T) {
		fn := func(value0, value1 []selectors.Field) bool {
			records := fieldsRecords{}
			records.Add(value0)
			records.Add(value1)
			return records.Err() != nil && reflect.DeepEqual(value0, records.Fields())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestInt64Records(t *testing.T) {
	t.Parallel()

	t.Run("no variance", func(t *testing.T) {
		fn := func(values int64) bool {
			records := int64Records{}
			records.Add(values)
			records.Add(values)
			return records.Err() == nil && reflect.DeepEqual(values, records.Int64())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("variance", func(t *testing.T) {
		fn := func(value0, value1 int64) bool {
			records := int64Records{}
			records.Add(value0)
			records.Add(value1)
			return records.Err() != nil && reflect.DeepEqual(value0, records.Int64())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestPresenceRecords(t *testing.T) {
	t.Parallel()

	t.Run("no variance", func(t *testing.T) {
		fn := func(values selectors.Presence) bool {
			records := presenceRecords{}
			records.Add(values)
			records.Add(values)
			return records.Err() == nil && values.Equal(records.Presence())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("variance", func(t *testing.T) {
		fn := func(value0, value1 selectors.Presence) bool {
			records := presenceRecords{}
			records.Add(value0)
			records.Add(value1)
			return records.Err() != nil && value0.Equal(records.Presence())
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
