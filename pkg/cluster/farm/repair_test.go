package farm

import (
	"testing"
	"testing/quick"

	hashringMocks "github.com/SimonRichardson/coherence/pkg/cluster/hashring/mocks"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes/mocks"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/golang/mock/gomock"
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
				hash := v.Key.Hash()

				ch := make(chan selectors.Element)
				go func(hash uint32) {
					defer close(ch)
					ch <- selectors.NewPresenceElement(hash, selectors.Presence{
						Inserted: true,
						Present:  true,
						Score:    2,
					})
				}(hash)

				node.EXPECT().Score(v.Key, v.Field).Return(ch)
				m[v.Key] = append(m[v.Key], selectors.FieldValueScore{
					Field: v.Field,
					Value: v.Value,
					Score: 3,
				})
			}

			for k, v := range m {
				hash := k.Hash()

				ch := make(chan selectors.Element)
				go func(hash uint32) {
					defer close(ch)
					ch <- selectors.NewChangeSetElement(hash, selectors.ChangeSet{
						Success: extractFields(v),
						Failure: make([]selectors.Field, 0),
					})
				}(hash)

				node.EXPECT().Insert(k, v).Return(ch)
			}

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(gomock.Any(), selectors.Strong).Return([]nodes.Node{
				node,
			}, func([]uint32) error { return nil }).AnyTimes()
			nodeSet.EXPECT().Read(gomock.Any(), selectors.Strong).Return([]nodes.Node{
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
				hash := v.Key.Hash()

				ch := make(chan selectors.Element)
				go func(hash uint32) {
					defer close(ch)
					ch <- selectors.NewPresenceElement(hash, selectors.Presence{
						Inserted: false,
						Present:  true,
						Score:    2,
					})
				}(hash)

				node.EXPECT().Score(v.Key, v.Field).Return(ch)
				m[v.Key] = append(m[v.Key], selectors.FieldValueScore{
					Field: v.Field,
					Value: v.Value,
					Score: 3,
				})
			}

			for k, v := range m {
				hash := k.Hash()

				ch := make(chan selectors.Element)
				go func(hash uint32) {
					defer close(ch)
					ch <- selectors.NewChangeSetElement(hash, selectors.ChangeSet{
						Success: extractFields(v),
						Failure: make([]selectors.Field, 0),
					})
				}(hash)

				node.EXPECT().Delete(k, v).Return(ch)
			}

			nodeSet := hashringMocks.NewMockSnapshot(ctrl)
			nodeSet.EXPECT().Write(gomock.Any(), selectors.Strong).Return([]nodes.Node{
				node,
			}, func([]uint32) error { return nil }).AnyTimes()
			nodeSet.EXPECT().Read(gomock.Any(), selectors.Strong).Return([]nodes.Node{
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
