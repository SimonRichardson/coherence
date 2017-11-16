package cache

type nopCache struct{}

func newNopCache() Cache {
	return nopCache{}
}

func (nopCache) Add([]string) error { return nil }
func (nopCache) Intersection(m []string) (union, difference []string, err error) {
	difference = m
	return
}
