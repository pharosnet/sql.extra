package postgres

import (
	"fmt"
	"strconv"
)

// load all the known standard oids/Valstructors into the type map
// this map is used by DB to convert col info into a ToValue
var typs = map[uint32]func(args ...string) (ToValue, error){

	16: func(args ...string) (ToValue, error) {
		return Bool, nil
	},

	17: func(args ...string) (ToValue, error) {
		return Bytes, nil
	},

	18: func(args ...string) (ToValue, error) {
		vs, err := argsToInts(args, 1)
		if err != nil {
			return nil, err
		}
		return Char(vs[0]), nil
	},

	20: func(args ...string) (ToValue, error) {
		return BigInt, nil
	},

	21: func(args ...string) (ToValue, error) {
		return SmallInt, nil
	},

	23: func(args ...string) (ToValue, error) {
		return Integer, nil
	},

	25: func(args ...string) (ToValue, error) {
		return Text, nil
	},

	26: func(args ...string) (ToValue, error) {
		return Integer, nil
	},

	700: func(args ...string) (ToValue, error) {
		return Real, nil
	},

	701: func(args ...string) (ToValue, error) {
		return Double, nil
	},

	1042: func(args ...string) (ToValue, error) {
		vs, err := argsToInts(args, 1)
		if err != nil {
			return nil, err
		}
		return Char(vs[0]), nil
	},

	1043: func(args ...string) (ToValue, error) {
		vs, err := argsToInts(args, 1)
		if err != nil {
			return nil, err
		}
		return VarChar(vs[0]), nil
	},

	1114: func(args ...string) (ToValue, error) {
		return Timestamp, nil
	},

	1184: func(args ...string) (ToValue, error) {
		return Timestamp, nil
	},

	1700: func(args ...string) (ToValue, error) {
		vs, err := argsToInts(args, 1)
		if err != nil {
			return nil, err
		}
		if len(vs) < 2 {
			vs = append(vs, 2)
		}
		return Numeric(vs[0], vs[1]), nil
	},
}

func argsToInts(args []string, need int) ([]int, error) {
	if len(args) < need {
		return nil, fmt.Errorf("need at least %d args", need)
	}
	ints := make([]int, len(args))
	for i, v := range args {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, err
		}
		ints[i] = int(n)
	}
	return ints, nil
}
