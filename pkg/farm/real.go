package farm

import (
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/selectors"
)

type real struct {
	nodes []nodes.Node
}

func NewRealFarm(nodes []nodes.Node) Farm {
	return &real{
		nodes: nodes,
	}
}

func (r *real) Insert(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	return r.write(func(n nodes.Node) <-chan selectors.Element {
		return n.Insert(key, members)
	})
}

func (r *real) Delete(key selectors.Key, members []selectors.FieldScore) (selectors.ChangeSet, error) {
	return r.write(func(n nodes.Node) <-chan selectors.Element {
		return n.Delete(key, members)
	})
}

func (r *real) Keys() ([]selectors.Key, error) {
	return r.readKeys(func(n nodes.Node) <-chan selectors.Element {
		return n.Keys()
	})
}

func (r *real) Size(key selectors.Key) (int, error) {
	return r.readSize(func(n nodes.Node) <-chan selectors.Element {
		return n.Size(key)
	})
}

func (r *real) Members(key selectors.Key) ([]selectors.Field, error) {
	return r.readMembers(func(n nodes.Node) <-chan selectors.Element {
		return n.Members(key)
	})
}

func (r *real) Repair([]selectors.KeyField) error {
	return nil
}

func (r *real) write(fn func(nodes.Node) <-chan selectors.Element) (selectors.ChangeSet, error) {
	var (
		retrieved = 0
		returned  = 0

		elements = make(chan selectors.Element, len(r.nodes))

		errs    []error
		records = &changeSetRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(r.nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(r.nodes, fn, wg, elements); err != nil {
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

		elements = make(chan selectors.Element, len(r.nodes))

		errs    []error
		records = &keysRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(r.nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(r.nodes, fn, wg, elements); err != nil {
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

func (r *real) readSize(fn func(nodes.Node) <-chan selectors.Element) (int, error) {
	var (
		retrieved = 0
		returned  = 0

		elements = make(chan selectors.Element, len(r.nodes))

		errs    []error
		records = &intRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(r.nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(r.nodes, fn, wg, elements); err != nil {
		return -1, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		keys := selectors.IntFromElement(element)
		records.Add(keys)

		// Bail out, if there is an error
		if err := records.Err(); err != nil {
			return -1, errPartial{err}
		}
	}

	if len(errs) > 0 {
		return -1, errors.Wrapf(joinErrors(errs), "partial error")
	}
	return records.Int(), nil
}

func (r *real) readMembers(fn func(nodes.Node) <-chan selectors.Element) ([]selectors.Field, error) {
	var (
		retrieved = 0
		returned  = 0

		elements = make(chan selectors.Element, len(r.nodes))

		errs    []error
		records = &fieldsRecords{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(r.nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(r.nodes, fn, wg, elements); err != nil {
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
