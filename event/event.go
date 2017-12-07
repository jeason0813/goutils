package event

import (
	"errors"
	"reflect"
	"sync"
)

func New() Event {
	return &event{
		functionMap: make(map[string]interface{}),
	}
}

type Event interface {
	On(event string, task interface{}) error
	Fire(event string, params ...interface{}) ([]reflect.Value, error)
	FireBackground(event string, params ...interface{}) (chan []reflect.Value, error)
	Clear(event string) error
	ClearEvents()
	HasEvent(event string) bool
	Events() []string
	EventCount() int
}

type event struct {
	functionMap map[string]interface{}

	mu sync.Mutex
}

func (t *event) On(event string, task interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.functionMap[event]; ok {
		return errors.New("event already defined")
	}
	if reflect.ValueOf(task).Type().Kind() != reflect.Func {
		return errors.New("task is not a function")
	}
	t.functionMap[event] = task
	return nil
}

func (t *event) Fire(event string, params ...interface{}) ([]reflect.Value, error) {
	f, in, err := t.read(event, params...)
	if err != nil {
		return nil, err
	}
	result := f.Call(in)
	return result, nil
}

func (t *event) FireBackground(event string, params ...interface{}) (chan []reflect.Value, error) {
	f, in, err := t.read(event, params...)
	if err != nil {
		return nil, err
	}
	results := make(chan []reflect.Value)
	go func() {
		results <- f.Call(in)
	}()
	return results, nil
}

func (t *event) Clear(event string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.functionMap[event]; !ok {
		return errors.New("event not defined")
	}
	delete(t.functionMap, event)
	return nil
}

func (t *event) ClearEvents() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.functionMap = make(map[string]interface{})
}

func (t *event) HasEvent(event string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.functionMap[event]
	return ok
}

func (t *event) Events() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	events := make([]string, 0)
	for k := range t.functionMap {
		events = append(events, k)
	}
	return events
}

func (t *event) EventCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.functionMap)
}

func (t *event) read(event string, params ...interface{}) (reflect.Value, []reflect.Value, error) {
	t.mu.Lock()
	task, ok := t.functionMap[event]
	t.mu.Unlock()
	if !ok {
		return reflect.Value{}, nil, errors.New("no task found for event")
	}
	f := reflect.ValueOf(task)
	if len(params) != f.Type().NumIn() {
		return reflect.Value{}, nil, errors.New("parameter mismatched")
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	return f, in, nil
}
