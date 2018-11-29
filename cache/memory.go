package cache

type memoryCache struct{

}
type memoryStore struct {
	data map[string][]byte
	region string
}

func MemoryCache() Cache {
	cache := &memoryCache{}
	return cache
}
func (c *memoryCache) Store(region string) Store {
	return &memoryStore{
		make(map[string][]byte),
		region,
	}
}

func (s *memoryStore) Get(id string) ([]byte, bool) {
	b, ok:=s.data[id]
	return b, ok
}

func (s *memoryStore) Set(id string, b []byte) error {
	s.data[id] = b
	return nil
}