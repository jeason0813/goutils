package cache

import "github.com/garyburd/redigo/redis"

type redisCache struct {
	redis    *redis.Pool
	protocol string
}
type redisStore struct {
	cache  *redisCache
	region string
}

func RedisCache(redis *redis.Pool, protocol string) Cache {
	cache := &redisCache{
		redis,
		protocol,
	}
	return cache
}

func (c *redisCache) Store(region string) Store {
	return &redisStore{
		c,
		region,
	}
}

func (s *redisStore) Get(id string) ([]byte, bool) {
	r := s.cache.redis.Get()
	defer r.Close()
	re, err := r.Do("get", s.cache.protocol+"://"+s.region+"/"+id)
	if err != nil {
		return nil, false
	}
	if re == nil {
		return nil, true
	}
	return re.([]byte), true
}

func (s *redisStore) Set(id string, b []byte) error {
	r := s.cache.redis.Get()
	defer r.Close()
	return r.Send("set", s.cache.protocol+"://"+s.region+"/"+id, b)
}
