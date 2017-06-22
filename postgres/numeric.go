package postgres

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

// stored as string currently
// TODO: use some Value of any precision for this
func Numeric(prec int, scale int) ToValue {
	return func(data interface{}) (Value, error) {
		k := &pgNumeric{"", prec, scale, false}
		return k, k.Scan(data)
	}
}

type pgNumeric struct {
	s     string
	prec  int
	scale int
	valid bool
}

func (k *pgNumeric) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case float32:
		k.s = strconv.FormatFloat(float64(x), 'f', k.scale, 64)
	case float64:
		k.s = strconv.FormatFloat(x, 'f', k.scale, 64)
	case string:
		k.s = x
	case []byte:
		k.s = string(x)
	default:
		return fmt.Errorf("cannot set Numeric(%d,%d) Value with %T -> %v", k.prec, k.scale, src, src)
	}
	return nil
}
func (k *pgNumeric) IsNull() bool {
	return !k.valid
}

func (k *pgNumeric) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.s, nil
}

func (k *pgNumeric) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(k.s), nil
}

func (k *pgNumeric) String() string {
	if !k.valid {
		return ""
	}
	return k.s
}

func (k *pgNumeric) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.s
}
