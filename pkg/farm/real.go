package farm

import (
	"strings"
	"sync"
	"time"

	"github.com/SimonRichardson/resilience/breaker"
	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/client"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/selectors"
)

const (
	defaultFailureRate    = 3
	defaultFailureTimeout = time.Second
)

type real struct {
	nodes          *nodes.NodeSet
	repairStrategy *repairStrategy
	circuit        *breaker.CircuitBreaker
}

// NewRealFarm creates a farm that talks to various nodes
func NewRealFarm(nodes *nodes.NodeSet) Farm {
	return &real{
		nodes:          nodes,
		repairStrategy: &repairStrategy{nodes},
		circuit:        breaker.New(defaultFailureRate, defaultFailureTimeout),
	}
}

func (r *real) Insert(key selectors.Key, members []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	var changeSet selectors.ChangeSet
	err := r.circuit.Run(func() error {
		var err error
		changeSet, err = r.write(func(n nodes.Node) <-chan selectors.Element {
			return n.Insert(key, members)
		})
		return err
	})
	if PartialError(err) {
		go r.Repair(mergeKeyFieldMembers(key, changeSet.Failure, members))
	}
	return changeSet, err
}

func (r *real) Delete(key selectors.Key, members []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	var changeSet selectors.ChangeSet
	err := r.circuit.Run(func() error {
		var err error
		changeSet, err = r.write(func(n nodes.Node) <-chan selectors.Element {
			return n.Delete(key, members)
		})
		return err
	})
	if PartialError(err) {
		go r.Repair(mergeKeyFieldMembers(key, changeSet.Failure, members))
	}
	return changeSet, err
}

func (r *real) Select(key selectors.Key, field selectors.Field) (selectors.FieldValueScore, error) {
	return r.read(key, func(n nodes.Node) <-chan selectors.Element {
		return n.Select(key, field)
	})
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

func (r *real) Repair(members []selectors.KeyFieldValue) error {
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
	}

	// Handle how we meet consensus, if there is an error and we've still met
	// consensus, then send back a partial error so it can handle read repairs.
	if consensus(len(nodes), returned) {
		if len(errs) > 0 {
			return selectors.ChangeSet{}, errPartial{errors.Wrap(joinErrors(errs), "partial")}
		} else if err := records.Err(); err != nil {
			return selectors.ChangeSet{}, errPartial{errors.Wrap(err, "partial")}
		}
		return records.ChangeSet(), nil
	}

	// No consensus met, return a total failure
	if len(errs) > 0 {
		return selectors.ChangeSet{}, mapErrors(errs)
	} else if err := records.Err(); err != nil {
		return selectors.ChangeSet{}, errors.Wrap(err, "total")
	}
	return selectors.ChangeSet{}, errors.New("total: invalid state")
}

func (r *real) read(key selectors.Key,
	fn func(nodes.Node) <-chan selectors.Element,
) (selectors.FieldValueScore, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs    []error
		results []TupleSet
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return selectors.FieldValueScore{}, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		result := selectors.FieldValueScoreFromElement(element)
		results = append(results, MakeTupleSet([]selectors.FieldValueScore{
			result,
		}))
	}
	union, difference := UnionDifference(results)

	go r.Repair(FieldValueScoresToKeyField(key, difference))

	if len(errs) > 0 {
		return selectors.FieldValueScore{}, mapErrors(errs)
	} else if len(union) == 1 {
		return union[0], nil
	}
	return selectors.FieldValueScore{}, errors.New("invalid results")
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
		return nil, mapErrors(errs)
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
		return -1, mapErrors(errs)
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
		return nil, mapErrors(errs)
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
		return selectors.Presence{}, mapErrors(errs)
	}
	return records.Presence(), nil
}

func scatterRequests(n []nodes.Node,
	fn func(nodes.Node) <-chan selectors.Element,
	wg *sync.WaitGroup,
	dst chan selectors.Element,
) error {
	return tactic(n, func(k int, n nodes.Node) {
		defer wg.Done()

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

func mapErrors(errs []error) error {
	notFound := true
	for _, v := range errs {
		notFound = notFound && client.NotFoundError(v)
	}
	if notFound {
		return client.NewNotFoundError(errors.New("not found"))
	}
	return errors.Wrapf(joinErrors(errs), "partial error")
}

func joinErrors(e []error) error {
	var buf []string
	for _, v := range e {
		buf = append(buf, v.Error())
	}
	return errors.New(strings.Join(buf, "; "))
}

func mergeKeyFieldMembers(key selectors.Key, fields []selectors.Field, members []selectors.FieldValueScore) []selectors.KeyFieldValue {
	lookup := make(map[selectors.Field]selectors.FieldValueScore)
	for _, v := range members {
		lookup[v.Field] = v
	}

	res := make([]selectors.KeyFieldValue, len(fields))
	for k, v := range fields {
		res[k] = selectors.KeyFieldValue{
			Key:   key,
			Field: v,
			Value: lookup[v].Value,
		}
	}
	return res
}

func consensus(total, returned int) bool {
	return float64(returned)/float64(total) >= .51
}
