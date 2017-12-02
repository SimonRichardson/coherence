package farm

import "github.com/trussle/coherence/pkg/selectors"

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
func UnionDifference(sets []TupleSet) ([]selectors.FieldValueScore, []selectors.FieldValueScore) {
	var (
		scores = make(map[selectors.FieldScore]selectors.ValueScore)
		counts = make(map[selectors.FieldScore]int)
	)

	// Aggregate all the tuple sets together.
	for _, set := range sets {
		for tuple, value := range set {
			// Check the score is greater than zero, if not, we should skip it.
			if tuple.Score < 0 {
				continue
			}

			// union
			member := selectors.FieldScore{
				Field: tuple.Field,
				Score: tuple.Score,
			}

			if vs, ok := scores[member]; !ok || member.Score > vs.Score {
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
		index      int
		union      = make([]selectors.FieldValueScore, len(scores))
		difference = make([]selectors.FieldValueScore, len(counts))
	)

	for member, value := range scores {
		union[index] = selectors.FieldValueScore{
			Field: member.Field,
			Value: value.Value,
			Score: value.Score,
		}
		index++
	}

	index = 0
	for member, count := range counts {
		if count < len(sets) {
			vs := scores[member]
			difference[index] = selectors.FieldValueScore{
				Field: member.Field,
				Value: vs.Value,
				Score: vs.Score,
			}
			index++
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
