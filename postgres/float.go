package postgres

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
)

func newFloat(bs int, data interface{}) (Value, error) {
	k := &pgFloat{0, bs, false}
	return k, k.Scan(data)
}

func Real(data interface{}) (Value, error) {
	return newFloat(32, data)
}

func Double(data interface{}) (Value, error) {
	return newFloat(64, data)
}

type pgFloat struct {
	n     float64
	bs    int
	valid bool
}

func (k *pgFloat) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case float64:
		if k.bs == 32 && x > math.MaxFloat32 {
			return fmt.Errorf("cannot fit float64 %f into REAL Value", x)
		}
		k.n = x
	case float32:
		k.n = float64(x)
	case string:
		k.n, err = strconv.ParseFloat(x, k.bs)
		if err != nil {
			return err
		}
	case []byte:
		k.n, err = strconv.ParseFloat(string(x), k.bs)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot set %dbit Float Value with %T -> %v", k.bs, src, src)
	}
	return nil
}

func (k *pgFloat) IsNull() bool {
	return !k.valid
}

func (k *pgFloat) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.n, nil
}

func (k *pgFloat) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(k.String()), nil
}

func (k *pgFloat) String() string {
	if !k.valid {
		return ""
	}
	return strconv.FormatFloat(k.n, 'f', -1, 32)
}

func (k *pgFloat) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.n
}
