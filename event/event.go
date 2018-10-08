package event

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

func New() Event {
	return &event{
		functionMap: make(map[interface{}]interface{}),
		mu: sync.RWMutex{},
	}
}

type Event interface {
	On(event interface{}, task interface{}) error
	Fire(event interface{}, params ...interface{}) ([]reflect.Value, error)
	FireBackground(event interface{}, params ...interface{}) (chan []reflect.Value, error)
	Clear(event interface{}) error
	ClearEvents()
	HasEvent(event interface{}) bool
	Events() []interface{}
	EventCount() int
}

type event struct {
	functionMap map[interface{}]interface{}

	mu sync.RWMutex
}

func (t *event) On(event interface{}, task interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.functionMap[event]; ok {
		return errors.New("event already defined")
	}
	if reflect.ValueOf(task).Type().Kind() != reflect.Func {
		return errors.New("task is not a function")
	}
	t.functionMap[event] = task
	//fmt.Println(t.functionMap)
	return nil
}

func (t *event) Fire(event interface{}, params ...interface{}) ([]reflect.Value, error) {
	f, in, err := t.read(event, params...)
	if err != nil {
		return nil, err
	}
	result := f.Call(in)
	return result, nil
}

func (t *event) FireBackground(event interface{}, params ...interface{}) (chan []reflect.Value, error) {
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

func (t *event) Clear(event interface{}) error {
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
	t.functionMap = make(map[interface{}]interface{})
}

func (t *event) HasEvent(event interface{}) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.functionMap[event]
	return ok
}

func (t *event) Events() []interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()
	events := make([]interface{}, 0)
	for k := range t.functionMap {
		events = append(events, k)
	}
	return events
}

func (t *event) EventCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.functionMap)
}

func (t *event) read(event interface{}, params ...interface{}) (reflect.Value, []reflect.Value, error) {
	t.mu.RLock()
	task, ok := t.functionMap[event]
	t.mu.RUnlock()
	if !ok {
		return reflect.Value{}, nil, errors.New("no task found for event")
	}
	f := reflect.ValueOf(task)
	ft := f.Type()
	if !ft.IsVariadic() && len(params) != ft.NumIn() {
		fmt.Println("Event Debug=======>")
		fmt.Println(event)
		fmt.Println(len(params), params)
		fmt.Println(ft.NumIn())
		fmt.Println(ft.IsVariadic())
		fmt.Println("<========Event Debug End")
		return reflect.Value{}, nil, errors.New("parameter mismatched")
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		field := ft.In(k)
		fv := reflect.ValueOf(param)
		if field != fv.Type() && (!ft.IsVariadic() || k-1 < ft.NumIn()) {
			fv = fv.Convert(field)
		}
		in[k] = fv
	}
	return f, in, nil
}
