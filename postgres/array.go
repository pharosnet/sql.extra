package postgres

import (
	"bytes"
	"database/sql/driver"
)

func Array(el ToValue) ToValue {
	return func(data interface{}) (v Value, err error) {
		k := new(pgArray)
		k.el = el
		return k, k.Scan(data)
	}
}

type pgArray struct {
	vs    []Value
	el    ToValue
	valid bool
}

func (k *pgArray) Scan(src interface{}) (err error) {
	// reset
	k.vs = make([]Value, 0)
	// check null
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	// check supported scan types
	switch x := src.(type) {
	case []interface{}:
		for _, d := range x {
			err = k.Append(d)
			if err != nil {
				return err
			}
		}
		k.valid = true
	default:
		k.valid = true
		// src -> string
		b, err := srcToBytes(src)
		if err != nil {
			return err
		}
		// split on ','
		parts, err := split(b)
		if err != nil {
			return err
		}
		// add vals
		for _, part := range parts {
			err = k.Append(part)
			if err != nil {
				return err
			}
		}
	}
	return
}

func (k *pgArray) IsNull() bool {
	return !k.valid
}

func (k *pgArray) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.bytes()
}

func (k *pgArray) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	b := bytes.NewBufferString("")
	b.WriteString("{")
	last := len(k.vs) - 1
	for i, child := range k.vs {
		cb, err := child.bytes()
		if err != nil {
			return nil, err
		}
		switch child.(type) {
		case *pgNumeric, *pgInteger, *pgFloat, *pgBool, *pgArray, *pgTimestamp:
			b.Write(cb)
		default:
			b.WriteString(`"`)
			b.Write(escape(cb, 1))
			b.WriteString(`"`)
		}
		if i != last {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.Bytes(), nil
}

func (k *pgArray) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

func (k *pgArray) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make([]interface{}, len(k.vs))
	for i, v := range k.vs {
		vals[i] = v.Val()
	}
	return vals
}

func (k *pgArray) Values() []Value {
	return k.vs
}

func (k *pgArray) ValueAt(idx int) Value {
	return k.vs[idx]
}

func (k *pgArray) Append(src interface{}) error {
	switch v := src.(type) {
	case Value:
		// TODO: check v is type t
		k.vs = append(k.vs, v)
	default:
		vx, err := k.el(nil)
		if err != nil {
			return err
		}
		err = vx.Scan(src)
		if err != nil {
			return err
		}
		k.vs = append(k.vs, vx)
	}
	k.valid = true // ensure not null
	return nil
}
