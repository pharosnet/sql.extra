package postgres

import (
	"database/sql/driver"
	"fmt"
)

func Bytes(data interface{}) (Value, error) {
	k := new(pgBytea)
	return k, k.Scan(data)
}

type pgBytea struct {
	b     []byte
	valid bool
}

func (k *pgBytea) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch s := src.(type) {
	case []byte:
		k.b = s
	default:
		return fmt.Errorf("cannot set BYTEA value with %T -> %v", src, src)
	}
	return nil
}

func (k *pgBytea) IsNull() bool {
	return !k.valid
}

func (k *pgBytea) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.b, nil
}

func (k *pgBytea) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(fmt.Sprintf("\\x%x", k.b)), nil
}

func (k *pgBytea) String() string {
	if !k.valid {
		return ""
	}
	return string(k.b)
}

func (k *pgBytea) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.b
}
