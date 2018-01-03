package cache

import "github.com/garyburd/redigo/redis"

type redisCache struct {
	redis *redis.Pool
}
type redisStore struct {
	cache *redisCache
}

func RedisCache(redis *redis.Pool) Cache {
	cache := &redisCache{
		redis,
	}
	return cache
}

func (c *redisCache) Store() Store {
	return &redisStore{
		c,
	}
}

func (s *redisStore) Get(id string) ([]byte, bool) {
	r := s.cache.redis.Get()
	defer r.Close()
	re, err := r.Do("get", id)
	if err != nil {
		return nil, false
	}
	return re.([]byte), true
}

func (s *redisStore) Set(id string, b []byte) error {
	r := s.cache.redis.Get()
	defer r.Close()
	return r.Send("set", id, b)
}
