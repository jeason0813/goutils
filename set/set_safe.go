package set

import (
	"sync"
)

type SetSafe struct {
	m map[interface{}]bool
	sync.RWMutex
}

func NewSafe() *SetSafe {
	return &SetSafe{
		m: map[interface{}]bool{},
	}
}

func (s *SetSafe) Add(item interface{}) {
	s.Lock()
	defer s.Unlock()
	s.m[item] = true
}

func (s *SetSafe) Remove(item interface{}) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, item)
}

func (s *SetSafe) Has(item interface{}) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[item]
	return ok
}

func (s *SetSafe) Len() int {
	return len(s.List())
}

func (s *SetSafe) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = map[interface{}]bool{}
}

func (s *SetSafe) IsEmpty() bool {
	if s.Len() == 0 {
		return true
	}
	return false
}

func (s *SetSafe) List() []interface{} {
	s.RLock()
	defer s.RUnlock()
	list := []interface{}{}
	for item := range s.m {
		list = append(list, item)
	}
	return list
}
