package orm

import (
	"errors"
	"reflect"

	"github.com/ueffort/goutils/event"
)

type Model struct {
	name     string
	schema   *Schema
	executor *executor

	event event.Event
}

func NewModel(name string, record interface{}, executor *executor) (*Model, error) {
	schema, err := NewSchema(record)
	if err != nil {
		return nil, err
	}
	return NewModelWithSchema(name, schema, executor), nil
}

func NewModelWithSchema(name string, schema *Schema, executor *executor) *Model {
	if executor == nil {
		executor = defaultExecutor
	}

	model := &Model{
		name:     name,
		schema:   schema,
		executor: executor,
	}

	return model
}

func (m *Model) Query() ModelQuery {
	return m.bind(m.executor.getQuery())
}

func (m *Model) bind(query *query) *query {
	query.Table(m.name)
	query.setModel(m)

	auto := m.schema.AutoIncrement
	if auto != "" {
		query.Id(m.schema.fields[auto])
	}
	return query
}

func (m *Model) Bind(query Query) Query {
	return query.Table(m.name)
}

func (m *Model) PK(key ...interface{}) ModelQuery {
	query := m.executor.getQuery()
	if m.schema.Primary == nil {
		return query.error(errors.New("Model do not have primary"))
	}
	for k, v := range m.schema.Primary {
		query.Where(m.schema.fields[v], "=", key[k])
	}
	return m.bind(query)
}

func (m *Model) Find(query Query) ([]interface{}, error) {
	if m.schema.softDelete {
		query.Where(m.schema.deleteTime, "=", 0)
	}
	return m.FindByQuery(query)
}

func (m *Model) One(query Query) (interface{}, error) {
	query.Limit(0, 1)
	if m.schema.softDelete {
		query.Where(m.schema.deleteTime, "=", 0)
	}
	result, err := m.FindByQuery(query)
	if err != nil {
		return nil, err
	}
	return result[0], nil
}

func (m *Model) FindByQuery(query Query) ([]interface{}, error) {
	value := reflect.MakeSlice(m.schema.schemaPtrType, 0, 0).Interface()
	err := m.Bind(query).Find(value)
	if err != nil {
		return nil, err
	}
	return value.([]interface{}), nil
}

func (m *Model) Rows(query Query) (Rows, error) {
	rows, err := m.Bind(query).Rows()
	if err != nil {
		return nil, err
	}
	rows.With(m.schema)
	return rows, nil
}

func (m *Model) Insert(record interface{})(int64, error){
	return m.Query().Save(record)
}

func (m *Model) Update(query Query, record interface{}) (int64, error) {
	if m.schema.softDelete {
		query.Where(m.schema.deleteTime, "=", 0)
	}
	return m.UpdateByQuery(query, record)
}

func (m *Model) UpdateByQuery(query Query, record interface{}) (int64, error) {
	data, err := interface2map(m.schema, record)
	if err != nil {
		return 0, err
	}
	return m.Bind(query).Update(data)
}

func (m *Model) Delete(query Query) (int64, error) {
	if m.schema.softDelete {
		query.Where(m.schema.deleteTime, "=", 0)
	}
	return m.DeleteByQuery(query)
}

func (m *Model) DeleteByQuery(query Query) (int64, error) {
	return m.Bind(query).Delete()
}

func (m *Model) Count(query Query) (int64, error) {
	if m.schema.softDelete {
		query.Where(m.schema.deleteTime, "=", 0)
	}
	return m.Bind(query).Count()
}

func (m *Model) CountByQuery(query Query) (int64, error) {
	return m.Bind(query).Count()
}
