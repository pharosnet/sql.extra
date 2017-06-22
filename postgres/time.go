package postgres

import (
	"database/sql/driver"
	"fmt"
	"time"
)

func Timestamp(data interface{}) (Value, error) {
	k := new(pgTimestamp)
	return k, k.Scan(data)
}

type pgTimestamp struct {
	t     time.Time
	tz    string
	valid bool
}

var timeFormats = []string{
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"15:04:05-07",
	"15:04:05",
	"2006-01-02",
}

func parseTime(s string, t *time.Time) (err error) {
	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if s[len(s)-2] == '.' {
		s += "0"
	}
	// check timestampz for a 30-minute-offset timezone
	// s[len(s)-3] == ':' {
	// f += ":00"

	// try to parse each format til will find one
	for _, f := range timeFormats {
		*t, err = time.Parse(f, s)
		if err == nil {
			break
		} else {
			err = fmt.Errorf("could not parse time string %s", s)
		}
	}
	return err
}

func (k *pgTimestamp) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case time.Time:
		k.t = x
	case string:
		return parseTime(x, &k.t)
	case []byte:
		return parseTime(string(x), &k.t)
	default:
		return fmt.Errorf("cannot set TIMESTAMP value with %T -> %v", src, src)
	}
	return nil
}

func (k *pgTimestamp) IsNull() bool {
	return !k.valid
}

func (k *pgTimestamp) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.t, nil
}

func (k *pgTimestamp) bytes() ([]byte, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return []byte(k.t.Format(time.RFC3339Nano)), nil
}

func (k *pgTimestamp) String() string {
	if !k.valid {
		return ""
	}
	return k.t.Format(time.RFC3339Nano)
}

func (k *pgTimestamp) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.t
}
