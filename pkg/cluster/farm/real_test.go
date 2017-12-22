package farm

import (
	"reflect"
	"testing"
	"testing/quick"

	hashringMocks "github.com/SimonRichardson/coherence/pkg/cluster/hashring/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes/mocks"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

func TestRealInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert with partial errors", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
				ch <- selectors.NewChangeSetElement(hash, want)
				ch <- selectors.NewChangeSetElement(hash, want)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Insert(key, members).Return(ch).Times(2)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Insert(key, members, selectors.Strong)
			return PartialError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("insert with partial errors", func(t *testing.T) {
		fn := func(key selectors.Key, members0, members1 []selectors.FieldValueScore) bool {
			if len(members0) == 0 || len(members1) == 0 {
				return true
			}

			hash := key.Hash()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			want0 := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members0),
			}
			want1 := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members1),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewChangeSetElement(hash, want0)
				ch <- selectors.NewChangeSetElement(hash, want1)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Insert(key, members0).Return(ch)
			node.EXPECT().Insert(key, members0).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Insert(key, members0, selectors.Strong)
			return PartialError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("insert with errors", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Insert(key, members).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Insert(key, members, selectors.Strong)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewChangeSetElement(hash, want)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Insert(key, members).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			changeSet, err := farm.Insert(key, members, selectors.Strong)
			if err != nil {
				t.Error(err)
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

func TestRealDelete(t *testing.T) {
	t.Parallel()

	t.Run("delete with partial errors", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
				ch <- selectors.NewChangeSetElement(hash, want)
				ch <- selectors.NewChangeSetElement(hash, want)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Delete(key, members).Return(ch).Times(2)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Delete(key, members, selectors.Strong)
			return PartialError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete with partial errors", func(t *testing.T) {
		fn := func(key selectors.Key, members0, members1 []selectors.FieldValueScore) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			want0 := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members0),
			}
			want1 := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members1),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewChangeSetElement(hash, want0)
				ch <- selectors.NewChangeSetElement(hash, want1)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Delete(key, members0).Return(ch)
			node.EXPECT().Delete(key, members0).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Delete(key, members0, selectors.Strong)
			return PartialError(err)
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete with errors", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Delete(key, members).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Delete(key, members, selectors.Strong)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewChangeSetElement(hash, want)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Delete(key, members).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			changeSet, err := farm.Delete(key, members, selectors.Strong)
			if err != nil {
				t.Error(err)
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

func TestRealSelect(t *testing.T) {
	t.Parallel()

	t.Run("select with errors", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Select(key, member.Field).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Select(key, member.Field, selectors.Strong)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("select", func(t *testing.T) {
		fn := func(key selectors.Key, member selectors.FieldValueScore) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewFieldValueScoreElement(hash, member)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Select(key, member.Field).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			value, err := farm.Select(key, member.Field, selectors.Strong)
			if err != nil {
				t.Error(err)
			}

			if expected, actual := member, value; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRealKeys(t *testing.T) {
	t.Parallel()

	t.Run("keys with errors", func(t *testing.T) {
		fn := func() bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := defaultAllKey.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Keys().Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(defaultAllKey, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Keys()
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("keys", func(t *testing.T) {
		fn := func(keys []selectors.Key) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := defaultAllKey.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewKeysElement(hash, keys)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Keys().Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(defaultAllKey, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			value, err := farm.Keys()
			if err != nil {
				t.Error(err)
			}

			if expected, actual := keys, value; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRealSize(t *testing.T) {
	t.Parallel()

	t.Run("size with errors", func(t *testing.T) {
		fn := func(key selectors.Key) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Size(key).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Size(key)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("size", func(t *testing.T) {
		fn := func(key selectors.Key, member int64) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewInt64Element(hash, member)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Size(key).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			value, err := farm.Size(key)
			if err != nil {
				t.Error(err)
			}

			if expected, actual := member, value; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRealMembers(t *testing.T) {
	t.Parallel()

	t.Run("members with errors", func(t *testing.T) {
		fn := func(key selectors.Key) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Members(key).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Members(key)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("members", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.Field) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewFieldsElement(hash, members)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Members(key).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			value, err := farm.Members(key)
			if err != nil {
				t.Error(err)
			}

			if expected, actual := members, value; !reflect.DeepEqual(expected, actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestRealScore(t *testing.T) {
	t.Parallel()

	t.Run("score with errors", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewErrorElement(hash, errors.New("bad"))
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Score(key, field).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			_, err := farm.Score(key, field)
			return err != nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("score", func(t *testing.T) {
		fn := func(key selectors.Key, field selectors.Field) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			hash := key.Hash()

			want := selectors.Presence{
				Inserted: true,
				Present:  true,
				Score:    2,
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewPresenceElement(hash, want)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Score(key, field).Return(ch)

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Read(key, selectors.Strong).Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			value, err := farm.Score(key, field)
			if err != nil {
				t.Error(err)
			}

			if expected, actual := want, value; !expected.Equal(actual) {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
