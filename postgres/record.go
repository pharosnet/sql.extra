package postgres

import (
	"database/sql/driver"
	"fmt"
)

func Record(cols ...*col) ToValue {
	return func(data interface{}) (v Value, err error) {
		k := new(pgRecord)
		k.cs = cols
		k.vs = make([]Value, len(cols))
		for i, c := range cols {
			k.vs[i], err = c.k(nil)
			if err != nil {
				return nil, err
			}
		}
		return k, k.Scan(data)
	}
}

type pgRecord struct {
	vs    []Value
	cs    []*col
	valid bool
	rel   *Relation
}

func (k *pgRecord) Relation() *Relation {
	return k.rel
}

func (k *pgRecord) SetRelation(rel *Relation) {
	k.rel = rel
}

func (k *pgRecord) IsNull() bool {
	return !k.valid
}

func (k *pgRecord) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	return rowScanner(src, k.vs)
}

func (k *pgRecord) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.bytes()
}

func (k *pgRecord) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

func (k *pgRecord) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make([]interface{}, len(k.vs))
	for i, v := range k.vs {
		vals[i] = v.Val()
	}
	return vals
}

func (k *pgRecord) Map() map[string]Value {
	m := make(map[string]Value)
	for i, v := range k.vs {
		m[k.cs[i].name] = v
	}
	return m
}

func (k *pgRecord) Values() []Value {
	return k.vs
}

func (k *pgRecord) ValueAt(idx int) Value {
	return k.vs[idx]
}

func (k *pgRecord) ValueBy(name string) Value {
	for i, c := range k.cs {
		if c.name == name {
			return k.vs[i]
		}
	}
	return nil
}

func (k *pgRecord) Get(name string) interface{} {
	v := k.ValueBy(name)
	if v == nil {
		panic(fmt.Sprintf("No column %s", name))
	}
	return v.Val()
}

func (k *pgRecord) Set(name string, src interface{}) error {
	v := k.ValueBy(name)
	if v == nil {
		return fmt.Errorf("No column %s", name)
	}
	return v.Scan(src)
}

func (k *pgRecord) Append(src interface{}) error {
	return fmt.Errorf("Cannot append more than %d Values to record", len(k.vs))
}

func (k *pgRecord) bytes() ([]byte, error) {
	return rowBytes(k.valid, k.vs)
}
