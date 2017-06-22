package postgres

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
)

var nullBytes = []byte("NULL")

// quote a byte string
// where t is the string quoting type
// 		0 = none
// 		1 = double quoted escaped \" (array style)
// 		2 = double quoted escaped "" (row style)
func escape(s []byte, t int) []byte {
	if t == 0 {
		return s
	}
	// replace all \ with \\
	s = bytes.Replace(s, []byte(`\`), []byte(`\\`), -1)
	switch t {
	case 1:
		// replace all " with \"
		s = bytes.Replace(s, []byte(`"`), []byte(`\"`), -1)
	case 2:
		// replace all " with ""
		s = bytes.Replace(s, []byte(`"`), []byte(`""`), -1)
	}
	return s
}

// converts n to int64
// returns error if n does not fit into the int bitsize
func fitInt(v interface{}, bitSize int) (r int64, err error) {
	// convert to int64
	switch n := v.(type) {
	case int:
		r = int64(n)
	case int8:
		r = int64(n)
	case int16:
		r = int64(n)
	case int32:
		r = int64(n)
	case int64:
		r = n
	case uint8:
		r = int64(n)
	case uint16:
		r = int64(n)
	case uint32:
		r = int64(n)
	case uint:
		if n < math.MaxInt64 {
			r = int64(n)
		} else {
			return 0, fmt.Errorf("Cannot fit %v into int%d", n, bitSize)
		}
	case uint64:
		if n < math.MaxInt64 {
			r = int64(n)
		} else {
			return 0, fmt.Errorf("Cannot fit %v into int%d", n, bitSize)
		}
	}
	// check fits
	ok := false
	switch bitSize {
	case 8:
		ok = r < math.MaxInt8
	case 16: // INT2
		ok = r < math.MaxInt16
	case 32: // INT4
		ok = r < math.MaxInt32
	case 64: // INT8
		ok = true
	default:
		return 0, fmt.Errorf("invalid bitSize %d", bitSize)
	}
	if !ok {
		return 0, fmt.Errorf("Cannot fit %v into int%d", r, bitSize)
	}
	return r, nil
}

func srcToBytes(src interface{}) (b []byte, err error) {
	switch x := src.(type) {
	case string:
		b = []byte(x)
	case []byte:
		b = x
	default:
		err = fmt.Errorf("Cannot parse %T into Value expected []byte or string", src)
	}
	return
}

// take a byte representation of an array or row and return
// each element unescaped
// will also decode any hex bytea fields (although not sure if that should be done here really)
func split(s []byte) ([][]byte, error) {
	parts := make([][]byte, 0)
	ignore := false
	dep := 0
	var mode byte // }=array )=record
	var closer byte
	a := -1
	z := -1
	for i, b := range s {
		switch {
		// sanity check
		case i == 0:
			switch b {
			case '{':
				mode = '}'
			case '(':
				mode = ')'
			default:
				return nil, fmt.Errorf("cannot split data. Unknown format: %s", string(s))
			}
		// if not inside value
		case a == -1:
			switch {
			// skip whitespace
			case b == ' ' || b == ',':
			// consume whitespace or commas
			// mark val wrapped in { }
			case b == '{':
				a = i
				dep++
				closer = '}'
			// mark val wrapped in "
			case b == '"':
				a = i + 1
				closer = '"'
			// anything else mark
			default:
				a = i
				closer = ','
			}
		// EOF
		case i == len(s)-1:
			if b != mode {
				return nil, fmt.Errorf("cannot split data. missing '%s': %s", string([]byte{mode}), string(s))
			}
			z = i - 1
		// start collecting val
		case a != -1:
			switch {
			// skip esc char and mark next char as unimportant (for array escaping)
			case !ignore && mode == '}' && b == '\\':
				ignore = true
			// treat "" as " (for row escaping)
			case !ignore && mode == ')' && b == '"' && s[i+1] == '"':
				ignore = true
			// this byte will not cause end
			case ignore:
				ignore = false
			// mark end of array
			case closer == '}' && (b == '{' || b == '}'):
				switch {
				case b == '{':
					dep++
				case b == '}':
					dep--
					if dep == 0 {
						z = i
					}
				}
			// mark end of quoted
			case closer == '"' && b == closer:
				z = i - 1
			// mark end of simple , val
			case closer == ',' && b == closer:
				z = i - 1
			}
		}
		// check for end
		if z != -1 {
			part := s[a : z+1]
			// unescape
			part = bytes.Replace(part, []byte(`\\`), []byte(`\`), -1)
			if mode == '}' {
				part = bytes.Replace(part, []byte(`\"`), []byte(`"`), -1)
			} else if mode == ')' {
				part = bytes.Replace(part, []byte(`""`), []byte(`"`), -1)
			}
			// check if it looks like a hex bytea in here and try to decode it
			if len(part) >= 2 && part[0] == '\\' && part[1] == 'x' {
				part, _ = hex.DecodeString(string(part[2:]))
			}
			parts = append(parts, part)
			a = -1
			z = -1
			dep = 0
		}
	}
	return parts, nil
}
