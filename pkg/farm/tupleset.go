package farm

import "github.com/trussle/coherence/pkg/selectors"

// TupleSet defines unique map of KeyField values
type TupleSet map[selectors.FieldScore]struct{}

// MakeTupleSet creates a new TupleSet with the results
func MakeTupleSet(members []selectors.FieldScore) TupleSet {
	m := make(TupleSet)
	for _, v := range members {
		m[v] = struct{}{}
	}
	return m
}

// UnionDifference returns the union and difference from a slice of TupleSets
func UnionDifference(sets []TupleSet) ([]selectors.FieldScore, []selectors.FieldScore) {
	var (
		scores = make(map[selectors.FieldScore]int64)
		counts = make(map[selectors.FieldScore]int)
	)

	// Aggregate all the tuple sets together.
	for _, set := range sets {
		for tuple := range set {
			// Check the score is greater than zero, if not, we should skip it.
			if tuple.Score < 0 {
				continue
			}

			// union
			member := selectors.FieldScore{
				Field: tuple.Field,
				Score: tuple.Score,
			}

			if score, ok := scores[member]; !ok || member.Score > score {
				scores[member] = tuple.Score
			}

			// difference
			counts[member]++
		}
	}

	var (
		index      int
		union      = make([]selectors.FieldScore, len(scores))
		difference = make([]selectors.FieldScore, len(counts))
	)

	for member, score := range scores {
		union[index] = selectors.FieldScore{
			Field: member.Field,
			Score: score,
		}
		index++
	}

	index = 0
	for member, count := range counts {
		if count < len(sets) {
			score := member.Score
			if s, ok := scores[member]; ok {
				score = s
			}

			difference[index] = selectors.FieldScore{
				Field: member.Field,
				Score: score,
			}
			index++
		}
	}

	return union, difference
}

// FieldScoresToKeyField converts a slice of members to a slice of KeyField
func FieldScoresToKeyField(key selectors.Key, members []selectors.FieldScore) []selectors.KeyField {
	res := make([]selectors.KeyField, len(members))
	for k, v := range members {
		res[k] = selectors.KeyField{
			Key:   key,
			Field: v.Field,
		}
	}
	return res
}
