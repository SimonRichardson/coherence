package nodes

import (
	"errors"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	apiMocks "github.com/SimonRichardson/coherence/pkg/api/mocks"
	"github.com/SimonRichardson/coherence/pkg/selectors"
)

func TestRemoteInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Insert(key, members).Return(selectors.ChangeSet{}, errors.New("bad"))

			node := NewRemote(transport)
			ch := node.Insert(key, members)

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

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			want := selectors.ChangeSet{
				Success: extractFields(members),
				Failure: make([]selectors.Field, 0),
			}

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Insert(key, members).Return(want, nil)

			node := NewRemote(transport)
			ch := node.Insert(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)

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

func TestRemoteDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Delete(key, members).Return(selectors.ChangeSet{}, errors.New("bad"))

			node := NewRemote(transport)
			ch := node.Delete(key, members)

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

	t.Run("delete", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			want := selectors.ChangeSet{
				Success: extractFields(members),
				Failure: make([]selectors.Field, 0),
			}

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Delete(key, members).Return(want, nil)

			node := NewRemote(transport)
			ch := node.Delete(key, members)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				changeSet := selectors.ChangeSetFromElement(element)

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

func TestRemoteSelect(t *testing.T) {
	t.Parallel()

	t.Run("select with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Select(key, field).Return(selectors.FieldValueScore{}, errors.New("bad"))

			node := NewRemote(transport)
			ch := node.Select(key, field)

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
		fn := func(key selectors.Key, field selectors.Field, value []byte) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			want := selectors.FieldValueScore{
				Field: field,
				Value: value,
				Score: 1,
			}

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Select(key, field).Return(want, nil)

			node := NewRemote(transport)
			ch := node.Select(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				fieldValueScore := selectors.FieldValueScoreFromElement(element)

				if expected, actual := want, fieldValueScore; !expected.Equal(actual) {
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

func TestRemoteKeys(t *testing.T) {
	t.Parallel()

	t.Run("keys with post http error", func(t *testing.T) {
		fn := func() bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Keys().Return(nil, errors.New("bad"))

			node := NewRemote(transport)
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

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Keys().Return(keys, nil)

			node := NewRemote(transport)
			ch := node.Keys()

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.KeysFromElement(element)

				if expected, actual := keys, got; !reflect.DeepEqual(expected, actual) {
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

func TestRemoteSize(t *testing.T) {
	t.Parallel()

	t.Run("size with post http error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Size(key).Return(int64(0), errors.New("bad"))

			node := NewRemote(transport)
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

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Size(key).Return(size, nil)

			node := NewRemote(transport)
			ch := node.Size(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.Int64FromElement(element)

				if expected, actual := size, got; expected != actual {
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

func TestRemoteMembers(t *testing.T) {
	t.Parallel()

	t.Run("members with post http error", func(t *testing.T) {
		fn := func(key selectors.Key) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Members(key).Return(nil, errors.New("bad"))

			node := NewRemote(transport)
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

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Members(key).Return(fields, nil)

			node := NewRemote(transport)
			ch := node.Members(key)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.FieldsFromElement(element)

				if expected, actual := fields, got; !reflect.DeepEqual(expected, actual) {
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

func TestRemoteScore(t *testing.T) {
	t.Parallel()

	t.Run("score with post http error", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Score(key, field).Return(selectors.Presence{}, errors.New("bad"))

			node := NewRemote(transport)
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

			want := selectors.Presence{
				Inserted: false,
				Present:  true,
				Score:    1,
			}

			transport := apiMocks.NewMockTransport(ctrl)
			transport.EXPECT().Hash().Return(uint32(1))
			transport.EXPECT().Score(key, field).Return(want, nil)

			node := NewRemote(transport)
			ch := node.Score(key, field)

			var found bool
			for element := range ch {
				if err := selectors.ErrorFromElement(element); err != nil {
					t.Error(err)
				}
				got := selectors.PresenceFromElement(element)

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
