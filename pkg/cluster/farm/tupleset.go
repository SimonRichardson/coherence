package farm

import (
	"github.com/SimonRichardson/coherence/pkg/selectors"
)

// TupleSet defines unique map of KeyField values
type TupleSet map[selectors.FieldScore]selectors.ValueScore

// MakeTupleSet creates a new TupleSet with the results
func MakeTupleSet(members []selectors.FieldValueScore) TupleSet {
	m := make(TupleSet)
	for _, v := range members {
		m[v.FieldScore()] = v.ValueScore()
	}
	return m
}

// UnionDifference returns the union and difference from a slice of TupleSets
func UnionDifference(sets []TupleSet, quorum selectors.Quorum) ([]selectors.FieldValueScore, []selectors.FieldValueScore) {
	var (
		expectedCount = len(sets)
		scores        = make(map[selectors.Field]selectors.ValueScore)
		counts        = make(map[selectors.Field]int)
	)

	// Aggregate all the tuple sets together.
	for _, set := range sets {
		for tuple, value := range set {
			// Check the score is greater than zero, if not, we should skip it.
			if tuple.Score < 0 {
				continue
			}

			// union
			member := tuple.Field
			if vs, ok := scores[member]; !ok || tuple.Score > vs.Score {
				scores[member] = selectors.ValueScore{
					Value: value.Value,
					Score: tuple.Score,
				}
			}

			// difference
			counts[member]++
		}
	}

	var (
		union      = make([]selectors.FieldValueScore, 0)
		difference = make([]selectors.FieldValueScore, 0)
	)

	for member, value := range scores {
		if count, ok := counts[member]; ok && consensus(quorum, expectedCount, count) {
			union = append(union, selectors.FieldValueScore{
				Field: member,
				Value: value.Value,
				Score: value.Score,
			})
		}
	}

	for member, count := range counts {
		// Drop anything that has only ever been replicated to one node
		if count < expectedCount && consensus(quorum, expectedCount, count) {
			vs := scores[member]

			difference = append(difference, selectors.FieldValueScore{
				Field: member,
				Value: vs.Value,
				Score: vs.Score,
			})
		}
	}

	return union, difference
}

// FieldValueScoresToKeyField converts a slice of members to a slice of KeyField
func FieldValueScoresToKeyField(key selectors.Key, members []selectors.FieldValueScore) []selectors.KeyFieldValue {
	res := make([]selectors.KeyFieldValue, len(members))
	for k, v := range members {
		res[k] = selectors.KeyFieldValue{
			Key:   key,
			Field: v.Field,
			Value: v.Value,
		}
	}
	return res
}
