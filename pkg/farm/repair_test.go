package farm

import (
	"testing"
	"testing/quick"

	"github.com/golang/mock/gomock"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/nodes/mocks"
	"github.com/trussle/coherence/pkg/selectors"
)

func TestRepair(t *testing.T) {
	t.Parallel()

	t.Run("repair with insertion", func(t *testing.T) {
		fn := func(members []selectors.KeyFieldValue) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			m := make(map[selectors.Key][]selectors.FieldValueScore)

			node := mocks.NewMockNode(ctrl)
			for _, v := range members {
				ch := make(chan selectors.Element)
				go func() {
					defer close(ch)
					ch <- selectors.NewPresenceElement(selectors.Presence{
						Inserted: true,
						Present:  true,
						Score:    2,
					})
				}()

				node.EXPECT().Score(v.Key, v.Field).Return(ch)
				m[v.Key] = append(m[v.Key], selectors.FieldValueScore{
					Field: v.Field,
					Value: v.Value,
					Score: 3,
				})
			}

			for k, v := range m {
				ch := make(chan selectors.Element)
				go func() {
					defer close(ch)
					ch <- selectors.NewChangeSetElement(selectors.ChangeSet{
						Success: extractFields(v),
						Failure: make([]selectors.Field, 0),
					})
				}()

				node.EXPECT().Insert(k, v).Return(ch)
			}

			nodeSet := mocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Snapshot().Return([]nodes.Node{
				node,
			}).AnyTimes()

			strategy := repairStrategy{nodeSet}
			err := strategy.Repair(members)
			return err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("repair with deletion", func(t *testing.T) {
		fn := func(members []selectors.KeyFieldValue) bool {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			m := make(map[selectors.Key][]selectors.FieldValueScore)

			node := mocks.NewMockNode(ctrl)
			for _, v := range members {
				ch := make(chan selectors.Element)
				go func() {
					defer close(ch)
					ch <- selectors.NewPresenceElement(selectors.Presence{
						Inserted: false,
						Present:  true,
						Score:    2,
					})
				}()

				node.EXPECT().Score(v.Key, v.Field).Return(ch)
				m[v.Key] = append(m[v.Key], selectors.FieldValueScore{
					Field: v.Field,
					Value: v.Value,
					Score: 3,
				})
			}

			for k, v := range m {
				ch := make(chan selectors.Element)
				go func() {
					defer close(ch)
					ch <- selectors.NewChangeSetElement(selectors.ChangeSet{
						Success: extractFields(v),
						Failure: make([]selectors.Field, 0),
					})
				}()

				node.EXPECT().Delete(k, v).Return(ch)
			}

			nodeSet := mocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Snapshot().Return([]nodes.Node{
				node,
			}).AnyTimes()

			strategy := repairStrategy{nodeSet}
			err := strategy.Repair(members)
			return err == nil
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}