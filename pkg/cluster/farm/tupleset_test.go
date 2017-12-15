package farm

import (
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/SimonRichardson/coherence/pkg/selectors"
)

func TestTupleSet(t *testing.T) {
	t.Parallel()

	t.Run("make", func(t *testing.T) {
		fn := func(members []selectors.FieldValueScore) bool {
			members = unique(members)
			tupleSet := MakeTupleSet(members)

			for _, v := range members {
				if vs, ok := tupleSet[v.FieldScore()]; ok && reflect.DeepEqual(vs.Value, v.Value) {
					continue
				}
				return false
			}
			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestUnionDifference(t *testing.T) {
	t.Parallel()

	t.Run("single", func(t *testing.T) {
		fn := func(members []selectors.FieldValueScore) bool {
			members = unique(members)
			tupleSet := MakeTupleSet(members)

			union, difference := UnionDifference([]TupleSet{
				tupleSet,
			}, selectors.Consensus)

			return fieldValueScoreEqual(members, union) && len(difference) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("dual", func(t *testing.T) {
		fn := func(members []selectors.FieldValueScore) bool {
			members = unique(members)
			tupleSet := MakeTupleSet(members)

			union, difference := UnionDifference([]TupleSet{
				tupleSet,
				tupleSet,
			}, selectors.Consensus)

			return fieldValueScoreEqual(members, union) && len(difference) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("triple", func(t *testing.T) {
		fn := func(members []selectors.FieldValueScore) bool {
			members = unique(members)
			tupleSet := MakeTupleSet(members)

			union, difference := UnionDifference([]TupleSet{
				tupleSet,
				tupleSet,
			}, selectors.Consensus)

			return fieldValueScoreEqual(members, union) && len(difference) == 0
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("manual", func(t *testing.T) {
		var (
			m0 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 1,
				},
				selectors.FieldValueScore{
					Field: "b",
					Value: []byte{2},
					Score: 2,
				},
			}
			m1 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 1,
				},
				selectors.FieldValueScore{
					Field: "c",
					Value: []byte{3},
					Score: 3,
				},
			}

			ts0 = MakeTupleSet(m0)
			ts1 = MakeTupleSet(m1)
		)

		// perform the union and difference
		union, difference := UnionDifference([]TupleSet{
			ts0,
			ts1,
			ts1,
		}, selectors.Consensus)

		// expectations
		want := []selectors.FieldValueScore{
			selectors.FieldValueScore{
				Field: "a",
				Value: []byte{1},
				Score: 1,
			},
			selectors.FieldValueScore{
				Field: "c",
				Value: []byte{3},
				Score: 3,
			},
		}
		if expected, actual := want, union; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		want = []selectors.FieldValueScore{
			selectors.FieldValueScore{
				Field: "c",
				Value: []byte{3},
				Score: 3,
			},
		}
		if expected, actual := want, difference; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("manual with different scores", func(t *testing.T) {
		var (
			m0 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 1,
				},
				selectors.FieldValueScore{
					Field: "b",
					Value: []byte{2},
					Score: 2,
				},
			}
			m1 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 4,
				},
				selectors.FieldValueScore{
					Field: "c",
					Value: []byte{3},
					Score: 3,
				},
			}

			ts0 = MakeTupleSet(m0)
			ts1 = MakeTupleSet(m1)
		)

		// perform the union and difference
		union, difference := UnionDifference([]TupleSet{
			ts0,
			ts1,
			ts1,
		}, selectors.Consensus)

		// expectations
		want := []selectors.FieldValueScore{
			selectors.FieldValueScore{
				Field: "a",
				Value: []byte{1},
				Score: 4,
			},
			selectors.FieldValueScore{
				Field: "c",
				Value: []byte{3},
				Score: 3,
			},
		}
		if expected, actual := want, union; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		want = []selectors.FieldValueScore{
			selectors.FieldValueScore{
				Field: "c",
				Value: []byte{3},
				Score: 3,
			},
		}
		if expected, actual := want, difference; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("manual with one node", func(t *testing.T) {
		var (
			m0 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 1,
				},
				selectors.FieldValueScore{
					Field: "b",
					Value: []byte{2},
					Score: 2,
				},
			}

			ts0 = MakeTupleSet(m0)
		)

		// perform the union and difference
		union, difference := UnionDifference([]TupleSet{
			ts0,
		}, selectors.Consensus)

		// expectations
		want := []selectors.FieldValueScore{
			selectors.FieldValueScore{
				Field: "a",
				Value: []byte{1},
				Score: 1,
			},
			selectors.FieldValueScore{
				Field: "b",
				Value: []byte{2},
				Score: 2,
			},
		}
		if expected, actual := want, union; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		want = []selectors.FieldValueScore{}
		if expected, actual := want, difference; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})

	t.Run("manual with two nodes", func(t *testing.T) {
		var (
			m0 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 1,
				},
				selectors.FieldValueScore{
					Field: "b",
					Value: []byte{2},
					Score: 2,
				},
			}
			m1 = []selectors.FieldValueScore{
				selectors.FieldValueScore{
					Field: "a",
					Value: []byte{1},
					Score: 4,
				},
				selectors.FieldValueScore{
					Field: "c",
					Value: []byte{3},
					Score: 3,
				},
			}

			ts0 = MakeTupleSet(m0)
			ts1 = MakeTupleSet(m1)
		)

		// perform the union and difference
		union, difference := UnionDifference([]TupleSet{
			ts0,
			ts1,
		}, selectors.Consensus)

		// expectations
		want := []selectors.FieldValueScore{
			selectors.FieldValueScore{
				Field: "a",
				Value: []byte{1},
				Score: 4,
			},
		}
		if expected, actual := want, union; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		want = []selectors.FieldValueScore{}
		if expected, actual := want, difference; !fieldValueScoreEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestFieldValueScoresToKeyField(t *testing.T) {
	t.Parallel()

	t.Run("make", func(t *testing.T) {
		fn := func(key selectors.Key, members []selectors.FieldValueScore) bool {
			res := FieldValueScoresToKeyField(key, members)

			for k, v := range res {
				if expected, actual := key, v.Key; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}

				if expected, actual := members[k].Field, v.Field; !expected.Equal(actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
				if expected, actual := members[k].Value, v.Value; !reflect.DeepEqual(expected, actual) {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
			}

			return true
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}

func unique(members []selectors.FieldValueScore) []selectors.FieldValueScore {
	m := make(map[selectors.FieldScore]selectors.FieldValueScore)
	for _, v := range members {
		m[v.FieldScore()] = v
	}

	var (
		index int
		r     = make([]selectors.FieldValueScore, len(m))
	)
	for _, v := range m {
		r[index] = v
		index++
	}
	return r
}

func fieldValueScoreEqual(a, b []selectors.FieldValueScore) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool {
		return a[i].Field < a[j].Field
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i].Field < b[j].Field
	})

	for k, v := range a {
		if !v.Equal(b[k]) {
			return false
		}
	}

	return true
}
