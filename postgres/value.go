package postgres

import (
	"database/sql"
	"database/sql/driver"
)

type Value interface {
	driver.Valuer
	sql.Scanner
	IsNull() bool
	String() string
	bytes() ([]byte, error)
	Val() interface{}
}

type IteratorValue interface {
	Value
	Values() []Value
	ValueAt(int) Value
	Append(interface{}) error
}

type MapValue interface {
	Value
	Map() map[string]Value
	ValueBy(name string) Value
	Get(name string) interface{}
	Set(name string, src interface{}) error
}

type RecordValue interface {
	IteratorValue
	Map() map[string]Value
	ValueBy(name string) Value
	Get(name string) interface{}
	Set(name string, src interface{}) error
	Relation() *Relation
	SetRelation(*Relation)
}

type ToValue func(data interface{}) (Value, error)
