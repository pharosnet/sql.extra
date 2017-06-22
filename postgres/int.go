package postgres

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

// Value aliases
var (
	Decimal   = Numeric
	Int       = Integer
	Int2      = SmallInt
	Int4      = Integer
	Int8      = BigInt
	Serial    = Integer
	BigSerial = BigInt
)

func SmallInt(data interface{}) (Value, error) {
	return newInt(16, data)
}

func Integer(data interface{}) (Value, error) {
	return newInt(32, data)
}

func BigInt(data interface{}) (Value, error) {
	return newInt(64, data)
}

func newInt(bs int, data interface{}) (Value, error) {
	k := &pgInteger{0, bs, false}
	return k, k.Scan(data)
}

type pgInteger struct {
	n     int64
	bs    int
	valid bool
}

func (k *pgInteger) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		k.n, err = fitInt(src, k.bs)
		if err != nil {
			return err
		}
	case []byte:
		k.n, err = strconv.ParseInt(string(x), 10, k.bs)
		if err != nil {
			return err
		}
	case string:
		k.n, err = strconv.ParseInt(x, 10, k.bs)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot set %dbit Integer Value with %T -> %v", k.bs, src, src)
	}
	return nil
}

func (k *pgInteger) IsNull() bool {
	return !k.valid
}

func (k *pgInteger) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.n, nil
}

func (k *pgInteger) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(fmt.Sprintf("%d", k.n)), nil
}

func (k *pgInteger) String() string {
	if !k.valid {
		return ""
	}
	return fmt.Sprintf("%d", k.n)
}

func (k *pgInteger) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.n
}
