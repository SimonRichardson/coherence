package farm

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/selectors"
)

type repairStrategy struct {
	nodes *nodes.Nodes
}

func (r *repairStrategy) Repair(members []selectors.KeyField) error {
	clues := make([]selectors.Clue, 0)
	for _, v := range members {
		// This can be optimised to send all of them at once, but could flood the
		// hosts
		clue, err := r.readScoreRepair(func(n nodes.Node) <-chan selectors.Element {
			return n.Score(v.Key, v.Field)
		})
		if err != nil {
			continue
		}
		clues = append(clues, clue.SetKeyField(v.Key, v.Field))
	}

	var (
		inserts = make(map[selectors.Key][]selectors.FieldScore)
		deletes = make(map[selectors.Key][]selectors.FieldScore)
	)
	for _, v := range clues {
		if v.Ignore {
			continue
		}

		if v.Insert {
			inserts[v.Key] = append(inserts[v.Key], selectors.FieldScore{
				Field: v.Field,
				Score: v.Score + 1,
			})
		} else {
			inserts[v.Key] = append(inserts[v.Key], selectors.FieldScore{
				Field: v.Field,
				Score: v.Score + 1,
			})
		}
	}

	var errs []error
	for key, members := range inserts {
		if err := r.write(func(n nodes.Node) <-chan selectors.Element {
			return n.Insert(key, members)
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for key, members := range deletes {
		if err := r.write(func(n nodes.Node) <-chan selectors.Element {
			return n.Delete(key, members)
		}); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Wrapf(joinErrors(errs), "repair error")
	}
	return nil
}

func (r *repairStrategy) readScoreRepair(fn func(nodes.Node) <-chan selectors.Element) (selectors.Clue, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot()
		elements = make(chan selectors.Element, len(nodes))

		errs      []error
		presences = make([]selectors.Presence, len(nodes))
		wg        = &sync.WaitGroup{}
	)

	wg.Add(len(nodes))
	go func() { wg.Wait(); close(elements) }()

	if err := scatterRequests(nodes, fn, wg, elements); err != nil {
		return selectors.Clue{}, err
	}

	for element := range elements {
		retrieved++

		if err := selectors.ErrorFromElement(element); err != nil {
			errs = append(errs, err)
			continue
		}

		returned++
		presence := selectors.PresenceFromElement(element)
		presences = append(presences, presence)
	}

	// We should just send everything again, as we have no idea what the condition
	// of the clusters are in.
	if returned != retrieved {
		return selectors.Clue{}, errors.Errorf("unable to perform repair")
	}

	var (
		found        = false
		wasInserted  = false
		highestScore = int64(0)
	)
	for _, presence := range presences {
		if presence.Present && presence.Score > highestScore {
			found = true
			highestScore = presence.Score
			wasInserted = wasInserted || presence.Inserted
		}
	}

	return selectors.Clue{
		Ignore: !found,
		Insert: wasInserted,
		Score:  highestScore,
	}, nil
}

func (r *repairStrategy) write(fn func(nodes.Node) <-chan selectors.Element) error {
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
		return err
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
			return errPartial{err}
		}
	}

	if len(errs) > 0 {
		return errors.Wrapf(joinErrors(errs), "partial error")
	}
	return nil
}