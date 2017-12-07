package farm

import (
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/nodes/mocks"
	"github.com/trussle/coherence/pkg/selectors"
)

func TestRealInsert(t *testing.T) {
	t.Parallel()

	t.Run("insert", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			want := selectors.ChangeSet{
				Success: make([]selectors.Field, 0),
				Failure: extractFields(members),
			}

			ch := make(chan selectors.Element)
			go func() {
				defer close(ch)
				ch <- selectors.NewChangeSetElement(want)
			}()

			node := mocks.NewMockNode(ctrl)
			node.EXPECT().Insert(key, members).Return(ch)

			nodeSet := mocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Snapshot().Return([]nodes.Node{
				node,
			})

			farm := NewReal(nodeSet)
			changeSet, err := farm.Insert(key, members)
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
