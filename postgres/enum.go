package postgres

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

func Enum(labels ...string) ToValue {
	if len(labels) == 0 {
		panic("Cannot create Enum Value with no labels")
	}
	return func(data interface{}) (Value, error) {
		k := &pgEnum{"", labels, false}
		return k, k.Scan(data)
	}
}

type pgEnum struct {
	s     string
	ls    []string
	valid bool
}

func (k *pgEnum) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	var s string
	switch x := src.(type) {
	case string:
		k.s = x
	case []byte:
		k.s = string(x)
	default:
		return fmt.Errorf("cannot set Enum Value with %T -> %v", src, src)
	}
	// check it's valid
	var ok bool
	for _, s2 := range k.ls {
		if k.s == s2 {
			ok = true
		}
	}
	if !ok {
		return fmt.Errorf("Value should be one of %s got %s", strings.Join(k.ls, ","), s)
	}
	return nil
}
func (k *pgEnum) IsNull() bool {
	return !k.valid
}

func (k *pgEnum) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.s, nil
}

func (k *pgEnum) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(k.s), nil
}

func (k *pgEnum) String() string {
	if !k.valid {
		return ""
	}
	return k.s
}

func (k *pgEnum) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.s
}
