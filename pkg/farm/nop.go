package farm

import "github.com/trussle/coherence/pkg/selectors"

type nop struct{}

func (nop) Insert(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	return selectors.ChangeSet{Success: 0, Failure: len(members)}, nil
}
func (nop) Delete(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	return selectors.ChangeSet{Success: 0, Failure: len(members)}, nil
}
func (nop) Keys() ([]selectors.Key, error)                   { return nil, nil }
func (nop) Size(selectors.Key) (int, error)                  { return -1, nil }
func (nop) Members(selectors.Key) ([]selectors.Field, error) { return nil, nil }
func (nop) Repair([]selectors.KeyField) error                { return nil }
