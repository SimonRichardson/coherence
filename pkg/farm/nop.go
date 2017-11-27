package farm

import "github.com/trussle/coherence/pkg/selectors"

type nop struct{}

func (nop) Insert(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	return selectors.ChangeSet{
		Success: make([]selectors.Field, 0),
		Failure: extractFields(members),
	}, nil
}
func (nop) Delete(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	return selectors.ChangeSet{
		Success: make([]selectors.Field, 0),
		Failure: extractFields(members),
	}, nil
}
func (nop) Keys() ([]selectors.Key, error)                   { return nil, nil }
func (nop) Size(selectors.Key) (int64, error)                { return -1, nil }
func (nop) Members(selectors.Key) ([]selectors.Field, error) { return nil, nil }
func (nop) Score(selectors.Key, selectors.Field) (selectors.Presence, error) {
	return selectors.Presence{}, nil
}
func (nop) Repair([]selectors.KeyField) error { return nil }

func extractFields(members []selectors.FieldScore) []selectors.Field {
	res := make([]selectors.Field, len(members))
	for k, v := range members {
		res[k] = v.Field
	}
	return res
}
