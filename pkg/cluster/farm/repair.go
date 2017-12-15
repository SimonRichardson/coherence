package farm

import (
	"sync"

	"github.com/SimonRichardson/coherence/pkg/cluster/hashring"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/pkg/errors"
)

type repairStrategy struct {
	nodes hashring.Snapshot
}

func (r *repairStrategy) Repair(members []selectors.KeyFieldValue) error {
	clues := make([]selectors.Clue, 0)
	for _, v := range members {
		// This can be optimised to send all of them at once, but could flood the
		// hosts
		clue, err := r.readScoreRepair(v.Key, func(n nodes.Node) <-chan selectors.Element {
			return n.Score(v.Key, v.Field)
		})
		if err != nil {
			continue
		}
		// Ignore the clue, we don't want to perform any read repairs.
		if clue.Ignore {
			continue
		}
		clues = append(clues, clue.SetKeyFieldValue(v.Key, v.Field, v.Value))
	}

	var (
		inserts = make(map[selectors.Key][]selectors.FieldValueScore)
		deletes = make(map[selectors.Key][]selectors.FieldValueScore)
	)
	for _, v := range clues {
		// If we've not met quorum then we shouldn't be doing anything.
		if v.Ignore || !v.Quorum {
			continue
		}

		if v.Insert {
			inserts[v.Key] = append(inserts[v.Key], selectors.FieldValueScore{
				Field: v.Field,
				Value: v.Value,
				Score: v.Score + 1,
			})
		} else {
			deletes[v.Key] = append(deletes[v.Key], selectors.FieldValueScore{
				Field: v.Field,
				Value: v.Value,
				Score: v.Score + 1,
			})
		}
	}

	var errs []error
	for key, members := range inserts {
		if err := r.write(key, func(n nodes.Node) <-chan selectors.Element {
			return n.Insert(key, members)
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for key, members := range deletes {
		if err := r.write(key, func(n nodes.Node) <-chan selectors.Element {
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

func (r *repairStrategy) readScoreRepair(key selectors.Key, fn func(nodes.Node) <-chan selectors.Element) (selectors.Clue, error) {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot(key, selectors.Strong)
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
	if !consensus(len(nodes), returned) {
		return selectors.Clue{}, errors.Errorf("unable to perform repair")
	}

	var (
		present      = 0
		found        = false
		wasInserted  = false
		highestScore = int64(0)
	)
	for _, presence := range presences {
		if presence.Present {
			present++

			if presence.Score > highestScore {
				found = true
				highestScore = presence.Score
				wasInserted = wasInserted || presence.Inserted
			}
		}
	}

	return selectors.Clue{
		Ignore: !found,
		Insert: wasInserted,
		Score:  highestScore,
		Quorum: consensus(len(nodes), present),
	}, nil
}

func (r *repairStrategy) write(key selectors.Key, fn func(nodes.Node) <-chan selectors.Element) error {
	var (
		retrieved = 0
		returned  = 0

		nodes    = r.nodes.Snapshot(key, selectors.Strong)
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
