package postgres

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

func Text(data interface{}) (Value, error) {
	k := &pgText{"", 0, false, false}
	return k, k.Scan(data)
}

func VarChar(n int) ToValue {
	return func(data interface{}) (Value, error) {
		k := &pgText{"", n, false, false}
		err := k.Scan(data)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
}

func Char(n int) ToValue {
	return func(data interface{}) (Value, error) {
		k := &pgText{"", n, true, false}
		err := k.Scan(data)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
}

type pgText struct {
	s     string // data
	n     int    // limit to n chars
	p     bool   // padding
	valid bool
}

func (k *pgText) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case string:
		k.s = x
	case []byte:
		k.s = string(x)
	default:
		return fmt.Errorf("cannot set string Value with %T -> %v", src, src)
	}
	if k.p {
		// trim - weird bit of SQL standard where space is ignored
		k.s = strings.TrimRight(k.s, " ")
		// error if does not fit
		if len(k.s) > k.n {
			return fmt.Errorf("cannot fit %s into Char(%d) Value", k.s, k.n)
		}
		// space-pad string to fit n
		if len(k.s) < k.n {
			k.s = fmt.Sprintf("%-"+strconv.Itoa(k.n)+"s", k.s)
		}
	} else if k.n > 0 && len(k.s) > k.n {
		// silently truncate value as per SQL standard
		k.s = k.s[0:k.n]
	}
	return nil
}

func (k *pgText) IsNull() bool {
	return !k.valid
}

func (k *pgText) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.s, nil
}

func (k *pgText) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(k.s), nil
}

func (k *pgText) String() string {
	if !k.valid {
		return ""
	}
	return k.s
}

func (k *pgText) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.s
}
