package nodes

import (
	"errors"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/selectors"
	"github.com/trussle/coherence/pkg/store/mocks"
)

func TestVirtualInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert with error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			for _, v := range members {
				store.EXPECT().Insert(key, v).Return(selectors.ChangeSet{}, errors.New("bad"))
			}

			node := NewVirtual(store)

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

				if expected, actual := want, changeSet; !expected.Equal(actual) {
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

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			for _, v := range members {
				store.EXPECT().Insert(key, v).Return(selectors.ChangeSet{}, nil)
			}

			node := NewVirtual(store)

			ch := node.Insert(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)
				want := selectors.ChangeSet{
					Success: extractFields(members),
					Failure: make([]selectors.Field, 0),
				}

				if expected, actual := want, changeSet; !expected.Equal(actual) {
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

func TestVirtualDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete with error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			for _, v := range members {
				store.EXPECT().Delete(key, v).Return(selectors.ChangeSet{}, errors.New("bad"))
			}

			node := NewVirtual(store)

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

				if expected, actual := want, changeSet; !expected.Equal(actual) {
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

	t.Run("delete", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			for _, v := range members {
				store.EXPECT().Delete(key, v).Return(selectors.ChangeSet{}, nil)
			}

			node := NewVirtual(store)

			ch := node.Delete(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)
				want := selectors.ChangeSet{
					Success: extractFields(members),
					Failure: make([]selectors.Field, 0),
				}

				if expected, actual := want, changeSet; !expected.Equal(actual) {
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

func TestVirtualSelect(t *testing.T) {
	t.Parallel()

	t.Run("select with error", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Select(key, member.Field).Return(member, errors.New("bad"))

			node := NewVirtual(store)

			ch := node.Select(key, member.Field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}
			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("select", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Select(key, member.Field).Return(member, nil)

			node := NewVirtual(store)

			ch := node.Select(key, member.Field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.FieldValueScoreFromElement(element)
				want := member

				if expected, actual := want, got; !expected.Equal(actual) {
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

func TestVirtualKeys(t *testing.T) {
	t.Parallel()

	t.Run("keys with error", func(t *testing.T) {
		fn := func() bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Keys().Return(nil, errors.New("bad"))

			node := NewVirtual(store)

			ch := node.Keys()

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}
			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("keys", func(t *testing.T) {
		fn := func(keys []selectors.Key) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Keys().Return(keys, nil)

			node := NewVirtual(store)

			ch := node.Keys()

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.KeysFromElement(element)
				want := keys

				if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
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

func TestVirtualSize(t *testing.T) {
	t.Parallel()

	t.Run("size with error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Size(key).Return(int64(0), errors.New("bad"))

			node := NewVirtual(store)

			ch := node.Size(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}
			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("size", func(t *testing.T) {
		fn := func(key selectors.Key, size int64) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Size(key).Return(size, nil)

			node := NewVirtual(store)

			ch := node.Size(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.Int64FromElement(element)
				want := size

				if expected, actual := want, got; expected != actual {
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

func TestVirtualMembers(t *testing.T) {
	t.Parallel()

	t.Run("members with error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Members(key).Return(nil, errors.New("bad"))

			node := NewVirtual(store)

			ch := node.Members(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}
			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("members", func(t *testing.T) {
		fn := func(key selectors.Key, fields []selectors.Field) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Members(key).Return(fields, nil)

			node := NewVirtual(store)

			ch := node.Members(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.FieldsFromElement(element)
				want := fields

				if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
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

func TestVirtualScore(t *testing.T) {
	t.Parallel()

	t.Run("score with error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Score(key, field).Return(selectors.Presence{}, errors.New("bad"))

			node := NewVirtual(store)

			ch := node.Score(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					found = true
					continue
				}
			}
			return found
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("score", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mocks.NewMockStore(ctrl)
			store.EXPECT().Score(key, field).Return(selectors.Presence{
				Inserted: true,
				Present:  true,
				Score:    1,
			}, nil)

			node := NewVirtual(store)

			ch := node.Score(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.PresenceFromElement(element)
				want := selectors.Presence{
					Inserted: true,
					Present:  true,
					Score:    1,
				}

				if expected, actual := want, got; !expected.Equal(actual) {
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
