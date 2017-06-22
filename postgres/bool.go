package postgres

import (
	"database/sql/driver"
	"fmt"
)

func Bool(data interface{}) (Value, error) {
	k := &pgBool{false, false}
	return k, k.Scan(data)
}

type pgBool struct {
	b     bool
	valid bool
}

func (k *pgBool) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	k.b = false
	switch x := src.(type) {
	case string:
		if x[0] == 't' || x[0] == '1' {
			k.b = true
		}
	case int:
		if x == 1 {
			k.b = true
		}
	case []byte:
		if x[0] == 't' || x[0] == '1' {
			k.b = true
		}
	case bool:
		k.b = x
	default:
		return fmt.Errorf("cannot set Boolean Value with %T -> %v", src, src)
	}
	return nil
}

func (k *pgBool) IsNull() bool {
	return !k.valid
}

func (k *pgBool) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.b, nil
}

func (k *pgBool) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(k.String()), nil
}

func (k *pgBool) String() string {
	if !k.valid {
		return ""
	}
	if k.b {
		return "t"
	}
	return "f"
}

func (k *pgBool) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.b
}
