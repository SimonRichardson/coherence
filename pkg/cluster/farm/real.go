package farm

import (
	"strings"
	"sync"
	"time"

	"github.com/SimonRichardson/coherence/pkg/cluster/hashring"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/SimonRichardson/resilience/breaker"
	"github.com/pkg/errors"
)

const (
	defaultAllKey         = selectors.Key("all")
	defaultFailureRate    = 3
	defaultFailureTimeout = time.Second
)

type real struct {
	nodes          hashring.Snapshot
	repairStrategy *repairStrategy
	circuit        *breaker.CircuitBreaker
}

// NewReal creates a farm that talks to various nodes
func NewReal(nodes hashring.Snapshot) Farm {
	return &real{
		nodes:          nodes,
		repairStrategy: &repairStrategy{nodes},
		circuit:        breaker.New(defaultFailureRate, defaultFailureTimeout),
	}
}

func (r *real) Insert(key selectors.Key,
	members []selectors.FieldValueScore,
	quorum selectors.Quorum,
) (selectors.ChangeSet, error) {
	var changeSet selectors.ChangeSet
	err := r.circuit.Run(func() error {
		var err error
		changeSet, err = r.write(key, quorum, func(n nodes.Node) <-chan selectors.Element {
			return n.Insert(key, members)
		})
		return err
	})
	if PartialError(err) {
		go r.Repair(mergeKeyFieldMembers(key, changeSet.Failure, members))
	}
	return changeSet, err
}

func (r *real) Delete(key selectors.Key,
	members []selectors.FieldValueScore,
	quorum selectors.Quorum,
) (selectors.ChangeSet, error) {
	var changeSet selectors.ChangeSet
	err := r.circuit.Run(func() error {
		var err error
		changeSet, err = r.write(key, quorum, func(n nodes.Node) <-chan selectors.Element {
			return n.Delete(key, members)
		})
		return err
	})
	if PartialError(err) {
		go r.Repair(mergeKeyFieldMembers(key, changeSet.Failure, members))
	}
	return changeSet, err
}

func (r *real) Select(key selectors.Key,
	field selectors.Field,
	quorum selectors.Quorum,
) (selectors.FieldValueScore, error) {
	return r.read(key, quorum, func(n nodes.Node) <-chan selectors.Element {
		return n.Select(key, field)
	})
}

func (r *real) Keys() ([]selectors.Key, error) {
	return r.readKeys(defaultAllKey, func(n nodes.Node) <-chan selectors.Element {
		return n.Keys()
	})
}

func (r *real) Size(key selectors.Key) (int64, error) {
	return r.readSize(key, func(n nodes.Node) <-chan selectors.Element {
		return n.Size(key)
	})
}

func (r *real) Members(key selectors.Key) ([]selectors.Field, error) {
	return r.readMembers(key, func(n nodes.Node) <-chan selectors.Element {
		return n.Members(key)
	})
}

func (r *real) Score(key selectors.Key, field selectors.Field) (selectors.Presence, error) {
	return r.readScore(key, func(n nodes.Node) <-chan selectors.Element {
		return n.Score(key, field)
	})
}

func (r *real) Repair(members []selectors.KeyFieldValue) error {
	return r.repairStrategy.Repair(members)
}

func (r *real) write(key selectors.Key,
	quorum selectors.Quorum,
	fn func(nodes.Node) <-chan selectors.Element,
) (selectors.ChangeSet, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes, finish = r.nodes.WriteSnapshot(key, quorum)
		elements      = make(chan selectors.Element, len(nodes))

		errs    []error
		hosts   []string
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

		hosts = append(hosts, element.Host())
	}

	// Finish and close the snapshot back to the node set
	go finish(hosts)

	// Handle how we meet consensus, if there is an error and we've still met
	// consensus, then send back a partial error so it can handle read repairs.
	if consensus(quorum, len(nodes), returned) {
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
	quorum selectors.Quorum,
	fn func(nodes.Node) <-chan selectors.Element,
) (selectors.FieldValueScore, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.ReadSnapshot(key, quorum)
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
	union, difference := UnionDifference(results, quorum)

	go r.Repair(FieldValueScoresToKeyField(key, difference))

	if len(errs) > 0 {
		return selectors.FieldValueScore{}, mapErrors(errs)
	} else if len(union) == 1 {
		return union[0], nil
	}
	return selectors.FieldValueScore{}, errors.New("invalid results")
}

func (r *real) readKeys(key selectors.Key, fn func(nodes.Node) <-chan selectors.Element) ([]selectors.Key, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.ReadSnapshot(key, selectors.Strong)
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

func (r *real) readSize(key selectors.Key, fn func(nodes.Node) <-chan selectors.Element) (int64, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.ReadSnapshot(key, selectors.Strong)
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

func (r *real) readMembers(key selectors.Key, fn func(nodes.Node) <-chan selectors.Element) ([]selectors.Field, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.ReadSnapshot(key, selectors.Strong)
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

func (r *real) readScore(key selectors.Key, fn func(nodes.Node) <-chan selectors.Element) (selectors.Presence, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.ReadSnapshot(key, selectors.Strong)
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
		notFound = notFound && selectors.NotFoundError(v)
	}
	if notFound {
		return selectors.NewNotFoundError(errors.New("not found"))
	}
	return errors.Wrapf(joinErrors(errs), "partial error")
}

func joinErrors(e []error) error {
	if len(e) == 0 {
		return nil
	}

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

func consensus(quorum selectors.Quorum, total, returned int) bool {
	switch quorum {
	case selectors.One:
		return returned > 0
	case selectors.Strong:
		return returned == total
	case selectors.Consensus:
		return float64(returned)/float64(total) >= .51
	}
	return false
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
