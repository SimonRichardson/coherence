package farm

import "github.com/trussle/coherence/pkg/selectors"

type nop struct{}

func (nop) Insert(selectors.Key, []selectors.FieldScore) error { return nil }
func (nop) Delete(selectors.Key, []selectors.FieldScore) error { return nil }
func (nop) Keys() ([]selectors.Key, error)                     { return nil, nil }
func (nop) Size(selectors.Key) (int, error)                    { return -1, nil }
func (nop) Members(selectors.Key) ([]selectors.Field, error)   { return nil, nil }
func (nop) Repair([]selectors.KeyField) error                  { return nil }
