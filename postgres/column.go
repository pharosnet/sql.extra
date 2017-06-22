package postgres

import (
	"fmt"
	"strings"
)

func Col(name string, k ToValue) *col {
	c := new(col)
	c.k = k
	c.name = name
	return c
}

type col struct {
	k       ToValue // the Value kind
	typ     string  // the pg_type name for casting
	oid     uint32  // the pg_type oid (if available)
	name    string  // name of this col
	refT    string  // name of referenced relation (if any)
	refF    string  // name of field in referenced relation (if any)
	pk      bool    // is col a primary key
	notNull bool    // is col marked as notNull
}

type refKind uint

const (
	ref_hasOne = iota
	ref_hasMany
)

// struct to hold foreign reference info on *Relation
type ref struct {
	name string    // relationship name
	kind refKind   //relationship type
	rel  *Relation // relation
	col  *col      // column with foreign key details
}

// Relation holds column and reference info about a relation.
// Usually inferred from the database. See Relation methods on DB
type Relation struct {
	Name string
	k    ToValue
	cols []*col
	refs []*ref
}

// return a new RecordValue that represents a row
// from this relation
func (r *Relation) New(data interface{}) (RecordValue, error) {
	v, err := r.k(data)
	if err != nil {
		return nil, err
	}
	k := v.(RecordValue)
	k.SetRelation(r)
	return k, nil
}

// csv list of column names for this relation.
// If pk is false then the primary key will not appear in the list.
func (r *Relation) fields(pk bool) string {
	if r.cols == nil {
		panic("Cols not defined?")
	}
	n := len(r.cols)
	if !pk {
		n--
	}
	cols := make([]string, n)
	i := 0
	for _, c := range r.cols {
		if c.pk && !pk {
			continue
		}
		cols[i] = c.name
		i++
	}
	return strings.Join(cols, ",")
}

func (r *Relation) bindings(pk bool, set bool) (string, int) {
	n := len(r.cols)
	if !pk {
		n--
	}
	ss := make([]string, n)
	i := 0
	for _, c := range r.cols {
		if c.pk && !pk {
			continue
		}
		bnd := fmt.Sprintf("$%d", i+1)
		if c.typ != "" {
			bnd = fmt.Sprintf("cast(%s as %s)\n", bnd, c.typ)
		}
		if set {
			bnd = fmt.Sprintf("%s = %s", c.name, bnd)
		}
		ss[i] = bnd
		i++
	}
	return strings.Join(ss, ","), i
}

func (r *Relation) pk() *col {
	if r.cols == nil {
		return nil
	}
	for _, c := range r.cols {
		if c.pk {
			return c
		}
	}
	return nil
}

func (r *Relation) valArgs(v RecordValue, update bool) []interface{} {
	n := len(r.cols)
	if !update {
		n--
	}
	infs := make([]interface{}, n)
	i := 0
	var pk *col
	for _, c := range r.cols {
		if c.pk {
			pk = c
			continue
		}
		infs[i] = v.ValueBy(c.name)
		i++
	}
	if update {
		infs[i] = v.ValueBy(pk.name)
		i++
	}
	return infs
}

// return list of column data in the order postgres expects them
func (r *Relation) Cols() []*col {
	return r.cols
}
