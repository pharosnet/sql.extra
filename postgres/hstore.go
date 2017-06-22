package postgres

import (
	"bytes"
	"database/sql/driver"
	"fmt"
)

func HStore(data interface{}) (Value, error) {
	k := &pgHStore{}
	return k, k.Scan(data)
}

type pgHStore struct {
	m     map[string]Value
	valid bool
}

func (k *pgHStore) Scan(src interface{}) (err error) {
	// reset
	k.m = make(map[string]Value)
	// check null
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	// get src into a valid type
	var keyvals map[string]string
	switch s := src.(type) {
	case []byte:
		// do the parsing
		keyvals, err = parseHStore(s)
		if err != nil {
			return err
		}
	case map[string]string:
		keyvals = s
	default:
		return fmt.Errorf("cannot set HSTORE value with %T -> %v", src, src)
	}
	for key, val := range keyvals {
		vx, err := Text(val)
		if err != nil {
			return err
		}
		k.m[key] = vx
	}
	return nil
}

func (k *pgHStore) IsNull() bool {
	return !k.valid
}

func (k *pgHStore) Value() (driver.Value, error) {
	if !k.valid {
		return nullBytes, nil
	}
	return k.bytes()
}

func (k *pgHStore) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

// return all hstore values
func (k *pgHStore) Map() map[string]Value {
	return k.m
}

func (k *pgHStore) ValueBy(name string) Value {
	if v, ok := k.m[name]; ok {
		return v
	}
	return nil
}

func (k *pgHStore) Get(name string) interface{} {
	return k.ValueBy(name).Val()
}

func (k *pgHStore) Set(name string, src interface{}) error {
	return k.ValueBy(name).Scan(src)
}

func (k *pgHStore) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make(map[string]string)
	for key, v := range k.m {
		vals[key] = v.Val().(string)
	}
	return vals
}

// TODO: this was just a quick test.. does not quote fields!
func (k *pgHStore) bytes() ([]byte, error) {
	buf := make([][]byte, len(k.m))
	i := 0
	for key, val := range k.m {
		buf[i] = []byte(fmt.Sprintf(`"%s" => "%s"`, key, val))
		i++
	}
	return bytes.Join(buf, []byte(`,`)), nil
}

func parseHStore(s []byte) (map[string]string, error) {
	m := make(map[string]string)
	st := 0 // 0=waiting-for-key, 1=inkey 2=waiting-for-val 3=inval
	ka := -1
	kz := -1
	va := -1
	vz := -1
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case b == '\\':
			i++
		case st == 0:
			switch {
			case b == '"':
				ka = i + 1
				st++
			}
		case st == 1:
			switch {
			case b == '"':
				kz = i - 1
				st++
			}
		case st == 2:
			switch {
			case b == 'N' && s[i+1] == 'U' && s[i+2] == 'L' && s[i+3] == 'L':
				va = i
				vz = i + 3
				st = 0
			case b == '"':
				va = i + 1
				st++
			}
		case st == 3:
			switch {
			case b == '"':
				vz = i - 1
				st = 0
			}
		}
		if kz != -1 && vz != -1 {
			k := s[ka : kz+1]
			v := s[va : vz+1]
			if string(v) == "NULL" {
				// do something? .. for now just ignore NULL value
			} else {
				k = bytes.Replace(k, []byte(`\\`), []byte(`\`), -1)
				k = bytes.Replace(k, []byte(`\"`), []byte(`"`), -1)
				v = bytes.Replace(v, []byte(`\\`), []byte(`\`), -1)
				v = bytes.Replace(v, []byte(`\"`), []byte(`"`), -1)
				m[string(k)] = string(v)
			}
			ka = -1
			kz = -1
			va = -1
			vz = -1
		}
	}
	return m, nil
}
