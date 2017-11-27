package farm

import (
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/selectors"
)

type real struct {
	nodes          *nodes.Nodes
	repairStrategy *repairStrategy
}

// NewRealFarm creates a farm that talks to various nodes
func NewRealFarm(nodes *nodes.Nodes) Farm {
	return &real{
		nodes:          nodes,
		repairStrategy: &repairStrategy{nodes},
	}
}

func (r *real) Insert(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	changeSet, err := r.write(func(n nodes.Node) <-chan selectors.Element {
		return n.Insert(key, members)
	})
	if PartialError(err) {
		go r.Repair(mergeKeyFields(key, changeSet.Failure))
	}
	return changeSet, err
}

func (r *real) Delete(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	changeSet, err := r.write(func(n nodes.Node) <-chan selectors.Element {
		return n.Delete(key, members)
	})
	if PartialError(err) {
		go r.Repair(mergeKeyFields(key, changeSet.Failure))
	}
	return changeSet, err
}

func (r *real) Keys() ([]selectors.Key, error) {
	return r.readKeys(func(n nodes.Node) <-chan selectors.Element {
		return n.Keys()
	})
}

func (r *real) Size(key selectors.Key) (int64, error) {
	return r.readSize(func(n nodes.Node) <-chan selectors.Element {
		return n.Size(key)
	})
}

func (r *real) Members(key selectors.Key) ([]selectors.Field, error) {
	return r.readMembers(func(n nodes.Node) <-chan selectors.Element {
		return n.Members(key)
	})
}

func (r *real) Score(key selectors.Key, field selectors.Field) (selectors.Presence, error) {
	return r.readScore(func(n nodes.Node) <-chan selectors.Element {
		return n.Score(key, field)
	})
}

func (r *real) Repair(members []selectors.KeyField) error {
	return r.repairStrategy.Repair(members)
}

func (r *real) write(fn func(nodes.Node) <-chan selectors.Element) (selectors.ChangeSet, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs    []error
		records = &changeSetRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return selectors.ChangeSet{}, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		changeSet := selectors.ChangeSetFromElement(element)
		records.Add(changeSet)

		// Bail out, if there is an error
		if err := records.Err(); err != nil {
			return selectors.ChangeSet{}, errPartial{err}
		}
	}

	if len(errs) > 0 {
		return selectors.ChangeSet{}, errors.Wrapf(joinErrors(errs), "partial error")
	}
	return records.ChangeSet(), nil
}

func (r *real) readKeys(fn func(nodes.Node) <-chan selectors.Element) ([]selectors.Key, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs    []error
		records = &keysRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return nil, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		keys := selectors.KeysFromElement(element)
		records.Add(keys)

		// Bail out, if there is an error
		if err := records.Err(); err != nil {
			return nil, errPartial{err}
		}
	}

	if len(errs) > 0 {
		return nil, errors.Wrapf(joinErrors(errs), "partial error")
	}
	return records.Keys(), nil
}

func (r *real) readSize(fn func(nodes.Node) <-chan selectors.Element) (int64, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs    []error
		records = &int64Records{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return -1, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		size := selectors.Int64FromElement(element)
		records.Add(size)

		// Bail out, if there is an error
		if err := records.Err(); err != nil {
			return -1, errPartial{err}
		}
	}

	if len(errs) > 0 {
		return -1, errors.Wrapf(joinErrors(errs), "partial error")
	}
	return records.Int64(), nil
}

func (r *real) readMembers(fn func(nodes.Node) <-chan selectors.Element) ([]selectors.Field, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs    []error
		records = &fieldsRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return nil, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		keys := selectors.FieldsFromElement(element)
		records.Add(keys)

		// Bail out, if there is an error
		if err := records.Err(); err != nil {
			return nil, errPartial{err}
		}
	}

	if len(errs) > 0 {
		return nil, errors.Wrapf(joinErrors(errs), "partial error")
	}
	return records.Fields(), nil
}

func (r *real) readScore(fn func(nodes.Node) <-chan selectors.Element) (selectors.Presence, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs    []error
		records = &presenceRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return selectors.Presence{}, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		presence := selectors.PresenceFromElement(element)
		records.Add(presence)

		// Bail out, if there is an error
		if err := records.Err(); err != nil {
			return selectors.Presence{}, errPartial{err}
		}
	}

	if len(errs) > 0 {
		return selectors.Presence{}, errors.Wrapf(joinErrors(errs), "partial error")
	}
	return records.Presence(), nil
}

func scatterRequests(n []nodes.Node,
	fn func(nodes.Node) <-chan selectors.Element,
	wg *sync.WaitGroup,
	dst chan selectors.Element,
) error {
	return tactic(n, func(k int, n nodes.Node) {
		for e := range fn(n) {
			dst <- e
		}
	})
}

func tactic(n []nodes.Node, fn func(k int, n nodes.Node)) error {
	for k, v := range n {
		go func(k int, v nodes.Node) {
			fn(k, v)
		}(k, v)
	}
	return nil
}

func joinErrors(e []error) error {
	var buf []string
	for _, v := range e {
		buf = append(buf, v.Error())
	}
	return errors.New(strings.Join(buf, "; "))
}

type errPartial struct {
	err error
}

func (e errPartial) Error() string {
	return e.err.Error()
}

// PartialError finds if the error passed in, is actually a partial error or not
func PartialError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(errPartial)
	return ok
}

func mergeKeyFields(key selectors.Key, fields []selectors.Field) []selectors.KeyField {
	res := make([]selectors.KeyField, len(fields))
	for k, v := range fields {
		res[k] = selectors.KeyField{
			Key:   key,
			Field: v,
		}
	}
	return res
}
