package cache

type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, b []byte) error
}

type Cache interface {
	Store() Store
}
