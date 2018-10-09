package event

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type task struct {
	f interface{}
	v reflect.Value
	t reflect.Type
	p []reflect.Type
	n int
}

func New() Event {
	return &event{
		taskMap: make(map[interface{}]*task),
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
	taskMap map[interface{}]*task

	mu sync.RWMutex
}

func (e *event) On(event interface{}, f interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.taskMap[event]; ok {
		return errors.New("event already defined")
	}
	v := reflect.ValueOf(f)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return errors.New("task is not a function")
	}
	n := t.NumIn()
	p := make([]reflect.Type, n)
	for k, _:=range p{
		p[k] = t.In(k)
	}
	e.taskMap[event] = &task{
		f,
		v,
		t,
		p,
		n,
	}
	//fmt.Println(e.taskMap)
	return nil
}

func (e *event) Fire(event interface{}, params ...interface{}) ([]reflect.Value, error) {
	f, in, err := e.read(event, params...)
	if err != nil {
		return nil, err
	}
	result := f.Call(in)
	return result, nil
}

func (e *event) FireBackground(event interface{}, params ...interface{}) (chan []reflect.Value, error) {
	f, in, err := e.read(event, params...)
	if err != nil {
		return nil, err
	}
	results := make(chan []reflect.Value)
	go func() {
		results <- f.Call(in)
	}()
	return results, nil
}

func (e *event) Clear(event interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.taskMap[event]; !ok {
		return errors.New("event not defined")
	}
	delete(e.taskMap, event)
	return nil
}

func (e *event) ClearEvents() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.taskMap = make(map[interface{}]*task)
}

func (e *event) HasEvent(event interface{}) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.taskMap[event]
	return ok
}

func (e *event) Events() []interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	events := make([]interface{}, len(e.taskMap))
	i := 0
	for _, task := range e.taskMap {
		events[i] = task.f
		i++
	}
	return events
}

func (e *event) EventCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.taskMap)
}

func (e *event) read(event interface{}, params ...interface{}) (reflect.Value, []reflect.Value, error) {
	e.mu.RLock()
	task, ok := e.taskMap[event]
	e.mu.RUnlock()
	if !ok {
		return reflect.Value{}, nil, errors.New("no task found for event")
	}
	variadic := task.t.IsVariadic()
	if !variadic && len(params) != task.n {
		fmt.Println("Event Debug=======>")
		fmt.Println(event)
		fmt.Println(len(params), params)
		fmt.Println(task.n)
		fmt.Println(variadic)
		fmt.Println("<========Event Debug End")
		return reflect.Value{}, nil, errors.New("parameter mismatched")
	}
	in := make([]reflect.Value, len(params))
	for k, _ := range params {
		field := task.p[k]
		fv := reflect.ValueOf(params[k])
		if field != fv.Type() && (!variadic || k - 1 < task.n) {
			fv = fv.Convert(field)
		}
		in[k] = fv
	}
	return task.v, in, nil
}
