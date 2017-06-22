package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type queryer interface {
	Query(string, ...interface{}) (*Rows, error)
	Relations() (map[string]*Relation, error)
}

type Rows struct {
	*sql.Rows
}

// Similar to sql.Rows#Scan but scans all values into a RecordValue
func (rs *Rows) ScanRecord(v RecordValue) error {
	// get list of vals as interface
	vals := make([]interface{}, len(v.Values()))
	for i, v := range v.Values() {
		vals[i] = v
	}
	err := rs.Scan(vals...)
	if err != nil {
		return err
	}
	return nil
}

type Query struct {
	tx          queryer
	from        *Relation
	where       []string
	whereParams []interface{}
	order       string
	limit       int
	offset      int
	err         error // some errors are defered until a call the Fetch(), Update() etc
}

func (q *Query) cp() *Query {
	if q.err != nil {
		panic("cp should not be called when there is a pending error")
	}
	return &Query{
		q.tx,
		q.from,
		q.where,
		q.whereParams,
		q.order,
		q.limit,
		q.offset,
		q.err,
	}
}

// Return a new Query based on this query with an additional
// (WHERE) filter.
func (q *Query) Where(w string, params ...interface{}) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	q2.where = append(q2.where, w)
	q2.whereParams = append(q2.whereParams, params...)
	return q2
}

func (q *Query) And(w string, params ...interface{}) *Query {
	return q.Where(w, params...)
}

// Return a new Query with
func (q *Query) For(v RecordValue) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	vrel := v.Relation()
	if vrel == nil {
		q2.err = errors.New("RecordValue given to For() does not belong to a relation.")
		return q2
	}
	if ref := q.refFor(ref_hasOne, q.from, vrel); ref != nil {
		fkv := v.ValueBy(ref.col.name)
		if fkv == nil {
			q2.err = fmt.Errorf("No column %s for %s", ref.col.name, vrel.Name)
			return q2
		}
		if fkv.IsNull() {
			q2.err = fmt.Errorf("RecordValue for %s has a NULL foreign key", vrel.Name)
			return q2
		}
		pk := q.from.pk()
		if pk == nil {
			q2.err = fmt.Errorf("%s must have a primary key to use in For query",
				q.from.Name)
			return q2
		}
		return q2.Where(fmt.Sprintf(`%s = $1`, pk.name), fkv)
	}
	// check for a ref on v that can be used (has many)
	// select * from x where fk = v.id
	if ref := q.refFor(ref_hasMany, q.from, vrel); ref != nil {
		pk := vrel.pk()
		if pk == nil {
			q2.err = fmt.Errorf("RecordValue for %s must have a primary key to use in For query",
				vrel.Name)
			return q2
		}
		pkv := v.ValueBy(pk.name)
		if pkv.IsNull() {
			q2.err = fmt.Errorf("RecordValue for %s has a NULL primary key", vrel.Name)
			return q2
		}
		return q2.Where(fmt.Sprintf(`%s = $1`, ref.col.name), pkv)
	}
	q2.err = fmt.Errorf("No reference columns between %s and %s", q.from.Name, vrel.Name)
	return q2
}

// find a column
func (q *Query) refFor(kind refKind, target *Relation, within *Relation) *ref {
	if within.refs == nil {
		return nil
	}
	for _, ref := range within.refs {
		if ref.rel == target && ref.kind == kind {
			return ref
		}
	}
	return nil
}

func (q *Query) Limit(n int) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	q2.limit = n
	return q2
}

func (q *Query) Offset(n int) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	q2.offset = n
	return q2
}

func (q *Query) rows(s string, params ...interface{}) (*Rows, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.tx.Query(s, params...)
}

func (q *Query) query(s string, params ...interface{}) ([]RecordValue, error) {
	rs, err := q.rows(s, params...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	all := make([]RecordValue, 0)
	for rs.Next() {
		vx, err := q.from.k(nil)
		if err != nil {
			return nil, err
		}
		v, ok := vx.(RecordValue)
		if !ok {
			return nil, fmt.Errorf("%T is not a RecordValue", vx)
		}
		v.SetRelation(q.from)
		err = rs.ScanRecord(v)
		if err != nil {
			return nil, err
		}
		all = append(all, v)
	}
	return all, nil
}

// perform a SELECT for the current query and
// return a slice of RecordValues
func (q *Query) Fetch() ([]RecordValue, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.query(q.selectSql(), q.selectArgs()...)
}

// perform a SELECT and return a single RecordValue for this query
// will return nil if no rows where returned
func (q *Query) FetchOne() (RecordValue, error) {
	rs, err := q.Limit(1).Fetch()
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	return rs[0], nil
}

// create a new Query with a WHERE filter for the relation's
// primary key and the call FetchOne
func (q *Query) Get(pk interface{}) (RecordValue, error) {
	if q.err != nil {
		return nil, q.err
	}
	pkcol := q.from.pk()
	if pkcol == nil {
		return nil, fmt.Errorf("No primary key found for relation %s", q.from.Name)
	}
	s := fmt.Sprintf(`%s = $1`, pkcol.name)
	return q.Where(s, pk).FetchOne()
}

func (q *Query) agg(sel string, v Value, vals ...interface{}) error {
	if q.err != nil {
		return q.err
	}
	rs, err := q.rows(q.selectSql(sel), q.selectArgs()...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		err = rs.Scan(v)
		if err != nil {
			return err
		}
	}
	err = rs.Err()
	if err != nil {
		return err
	}
	return rs.Close()
}

// perform a "SELECT count(*)" query for this Query
func (q *Query) Count() (int64, error) {
	v, _ := BigInt(0)
	err := q.agg("count(*)", v)
	if err != nil {
		return 0, err
	}
	return v.Val().(int64), nil
}

// perform a "SELECT sum(x)" query
func (q *Query) Sum(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := c.k(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("sum(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use sum(%s) unknown column name: %s", name, name)
}

// perform a "SELECT avg(x)" query
func (q *Query) Avg(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := Double(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("avg(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use avg(%s) unknown column name: %s", name, name)
}

// perform a "SELECT avg(x)" query
func (q *Query) Min(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := c.k(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("min(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use min(%s) unknown column name: %s", name, name)
}

// perform a "SELECT max(x)" query
func (q *Query) Max(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := c.k(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("max(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use max(%s) unknown column name: %s", name, name)
}

// perform a "SELECT array_agg(x)" query. Returns an array value
func (q *Query) ArrayAgg(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := Array(c.k)(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("array_agg(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use array_agg(%s) unknown column name: %s", name, name)
}

// generate SQL string for a SELECT
// optionally pass in a list of column names to
// override the SELECT args
func (q *Query) selectSql(names ...string) string {
	cols := strings.Join(names, ",")
	if cols == "" {
		cols = q.from.fields(true)
	}
	return fmt.Sprintf(`SELECT %s FROM %s %s %s %s`,
		cols,
		q.from.Name,
		q.whereExpr(),
		q.limitExpr(),
		q.offsetExpr())
}

// regexp to match the $X placeholders in queries
var placePat = regexp.MustCompile(`(?:[^\\]\$)(\d+)`)

// convert all the where expressions into a single one
func (q *Query) whereExpr() string {
	if len(q.where) == 0 {
		return ""
	}
	sts := make([]string, len(q.where))
	var i int64
	for idx, st := range q.where {
		if i == 0 { // find the bigest $X in this string
			matches := placePat.FindAllStringSubmatch(st, -1)
			if len(matches) == 0 {
				continue
			}
			for _, m := range matches {
				n, err := strconv.ParseInt(m[1], 10, 64)
				if err != nil {
					panic(fmt.Sprintf("could not convert %s to int", m[1]))
				}
				if n > i {
					i = n
				}
			}
		} else { // update each $X we find by adding i to it
			st = placePat.ReplaceAllStringFunc(st, func(m string) string {
				n, err := strconv.ParseInt(m[2:], 10, 64)
				if err != nil {
					panic(fmt.Sprintf("could not convert %s to int", m[2:]))
				}
				return fmt.Sprintf(`%s%d`, m[0:2], n+1)
			})
		}
		sts[idx] = st
	}
	return fmt.Sprintf(`WHERE %s`, strings.Join(sts, " AND "))
}

func (q *Query) limitExpr() string {
	if q.limit == 0 {
		return ""
	}
	return fmt.Sprintf(`LIMIT %d`, q.limit)
}

func (q *Query) offsetExpr() string {
	if q.offset == 0 {
		return ""
	}
	return fmt.Sprintf(`OFFSET %d`, q.offset)
}

// return the vals to bind to placholders for selectSql
func (q *Query) selectArgs() []interface{} {
	vals := make([]interface{}, 0)
	vals = append(vals, q.whereParams...)
	return vals
}
