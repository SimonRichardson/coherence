package store

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/trussle/fsys"
)

// TODO: We should run some sort of internal cleaning process to remove keys
// that have no value.

type memory struct {
	size    uint
	fsys    fsys.Filesystem
	buckets []*Bucket
	keys    map[selectors.Key]struct{}
	logger  log.Logger
}

// New creates a new in-memory Store according to the size required by
// the value requested.
func New(fsys fsys.Filesystem, amountBuckets, amountPerBucket uint, logger log.Logger) (Store, error) {
	buckets := make([]*Bucket, amountBuckets)
	for k := range buckets {
		file, err := fsys.Create(fmt.Sprintf("bucket-%d", k))
		if err != nil {
			return nil, err
		}
		buckets[k] = NewBucket(file, int(amountPerBucket), log.With(logger, "component", "bucket"))
	}

	return &memory{
		size:    amountBuckets,
		buckets: buckets,
		keys:    make(map[selectors.Key]struct{}),
		logger:  logger,
	}, nil
}

func (m *memory) Insert(key selectors.Key, members []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	if _, ok := m.keys[key]; !ok {
		m.keys[key] = struct{}{}
	}

	var (
		errors    []error
		changeSet selectors.ChangeSet

		index = uint(key.Hash()) % m.size
	)
	for _, member := range members {
		res, err := m.buckets[index].Insert(member.Field, member.ValueScore())
		if err != nil {
			errors = append(errors, err)
			continue
		}

		changeSet = changeSet.Append(res)
	}

	return changeSet, joinErrors(errors)
}

func (m *memory) Delete(key selectors.Key, members []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	var (
		errors    []error
		changeSet selectors.ChangeSet

		index = uint(key.Hash()) % m.size
	)

	for _, member := range members {
		res, err := m.buckets[index].Delete(member.Field, member.ValueScore())
		if err != nil {
			errors = append(errors, err)
			continue
		}

		changeSet = changeSet.Append(res)
	}

	if amount, err := m.buckets[index].Len(); err != nil {
		return changeSet, joinErrors(append(errors, err))
	} else if amount == 0 {
		delete(m.keys, key)
	}

	return changeSet, joinErrors(errors)
}

func (m *memory) Select(key selectors.Key, field selectors.Field) (selectors.FieldValueScore, error) {
	idx := index(key, m.size)
	level.Info(m.logger).Log("key", key, "index", idx, "field", field)
	return m.buckets[idx].Select(field)
}

func (m *memory) Keys() ([]selectors.Key, error) {
	var res []selectors.Key
	for k := range m.keys {
		s, err := m.Size(k)
		if err != nil {
			return nil, err
		}
		if s > 0 {
			res = append(res, k)
		}
	}
	return res, nil
}

func (m *memory) Size(key selectors.Key) (int64, error) {
	idx := index(key, m.size)
	return m.buckets[idx].Len()
}

func (m *memory) Members(key selectors.Key) ([]selectors.Field, error) {
	idx := index(key, m.size)
	return m.buckets[idx].Members()
}

func (m *memory) Score(key selectors.Key, field selectors.Field) (selectors.Presence, error) {
	idx := index(key, m.size)
	return m.buckets[idx].Score(field)
}

func (m *memory) Repair([]selectors.KeyFieldValue) error {
	return nil
}

func (m *memory) String() string {
	buf := new(bytes.Buffer)
	writer := tabwriter.NewWriter(buf, 0, 0, 1, ' ', tabwriter.Debug)

	fmt.Fprintln(writer, "bucket key\t field\t score\t value\t")
	for k := range m.keys {
		idx := index(k, m.size)
		m.buckets[idx].insert.Walk(func(field selectors.Field, value selectors.ValueScore) error {
			fmt.Fprintf(writer, "%s\t %s\t %d\t %s\t\n", k, field, value.Score, hex.EncodeToString(value.Value))
			return nil
		})
	}
	writer.Flush()

	return fmt.Sprintf("\n%s", buf.String())
}

func index(key selectors.Key, size uint) uint {
	return uint(key.Hash()) % size
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
