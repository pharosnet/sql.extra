package postgres

import (
	"bytes"
	"database/sql/driver"
	"fmt"
)

func Row(ks ...ToValue) ToValue {
	return func(data interface{}) (v Value, err error) {
		k := new(pgRow)
		k.vs = make([]Value, len(ks))
		for i, vk := range ks {
			k.vs[i], err = vk(nil)
			if err != nil {
				return nil, err
			}
		}
		return k, k.Scan(data)
	}
}

type pgRow struct {
	vs    []Value
	valid bool
}

func (k *pgRow) IsNull() bool {
	return !k.valid
}

func rowScanner(src interface{}, dests []Value) error {
	switch srcType := src.(type) {
	case nil:
		panic("Should not be possible - check nil before calling rowScanner")
	case []interface{}:
		if len(dests) != len(srcType) {
			return fmt.Errorf("Number of input values does not match number of Row columns. Need %d Got: %d", len(dests), len(srcType))
		}
		for i, vx := range dests {
			err := vx.Scan(srcType[i])
			if err != nil {
				return err
			}
		}
	default:
		// src -> bytes
		b, err := srcToBytes(src)
		if err != nil {
			return err
		}
		// split into parts
		parts, err := split(b)
		if err != nil {
			return err
		}
		// check col lengths match
		if len(parts) != len(dests) {
			return fmt.Errorf("Number of input columns does not match number of Row columns. Need: %d Got %d parts: %v",
				len(dests), len(parts), string(bytes.Join(parts, []byte(","))))
		}
		// parse each part
		for i, vx := range dests {
			// parse
			err = vx.Scan(parts[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (k *pgRow) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	return rowScanner(src, k.vs)
}
func (k *pgRow) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.bytes()
}

func (k *pgRow) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

func (k *pgRow) Val() interface{} {
	if !k.valid {
		return nil
	}
	values := make([]interface{}, len(k.vs))
	for i, v := range k.vs {
		values[i] = v.Val()
	}
	return values
}

func (k *pgRow) Values() []Value {
	return k.vs
}

func (k *pgRow) ValueAt(idx int) Value {
	return k.vs[idx]
}

func (k *pgRow) Append(src interface{}) error {
	return fmt.Errorf("Cannot append to Row as it already has %d (max) cols", len(k.vs))
}

func (k *pgRow) bytes() ([]byte, error) {
	return rowBytes(k.valid, k.vs)
}

func rowBytes(valid bool, vs []Value) ([]byte, error) {
	if !valid {
		return nullBytes, nil
	}
	b := bytes.NewBufferString("")
	b.WriteString("(")
	last := len(vs) - 1
	for i, child := range vs {
		cb, err := child.bytes()
		if err != nil {
			return nil, err
		}
		switch child.(type) {
		case *pgNumeric, *pgInteger, *pgFloat, *pgBool:
			b.Write(cb)
		default:
			b.WriteString(`"`)
			b.Write(escape(cb, 2))
			b.WriteString(`"`)
		}
		if i != last {
			b.WriteString(",")
		}
	}
	b.WriteString(")")
	return b.Bytes(), nil
}
