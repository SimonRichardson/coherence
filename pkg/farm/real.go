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

func (r *real) Insert(key selectors.Key, members []selectors.FieldScore) error {
	_, err := r.write(func(n nodes.Node) <-chan selectors.Element {
		return n.Insert(key, members)
	})
	return err
}

func (r *real) Delete(key selectors.Key, members []selectors.FieldScore) error {
	_, err := r.write(func(n nodes.Node) <-chan selectors.Element {
		return n.Delete(key, members)
	})
	return err
}

func (r *real) Keys() ([]selectors.Key, error) {
	return nil, nil
}

func (r *real) Size(selectors.Key) (int, error) {
	return -1, nil
}

func (r *real) Members(selectors.Key) ([]selectors.Field, error) {
	return nil, nil
}

func (r *real) Repair([]selectors.KeyField) error {
	return nil
}

func (r *real) write(fn func(nodes.Node) <-chan selectors.Element) (int, error) {
	var (
		retrieved = 0
		returned  = 0

		elements = make(chan selectors.Element, len(r.nodes))

		errs    []error
		records = &record{}
		wg      = &sync.WaitGroup{}
	)

	wg.Add(len(r.nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterWrites(r.nodes, fn, wg, elements); err != nil {
		return -1, err
	}

	for element := range elements {
		amount := selectors.IntFromElement(element)
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		records.Add(amount)
	}

	if len(errs) > 0 {
		return -1, errors.Wrapf(joinErrors(errs), "partial error")
	} else if err := records.Err(); err != nil {
		return -1, errPartial{err}
	}
	return records.Value(), nil
}

func scatterWrites(n []nodes.Node,
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

type record struct {
	value int
	set   bool
	err   error
}

func (r *record) Add(v int) {
	if r.set && r.value != v {
		r.err = errors.New("variance detected from replication")
		return
	}
	r.value = v
	r.set = true
}

func (r *record) Err() error {
	return r.err
}

func (r *record) Value() int {
	return r.value
}

type errPartial struct {
	err error
}

func (e errPartial) Error() string {
	return e.err.Error()
}
