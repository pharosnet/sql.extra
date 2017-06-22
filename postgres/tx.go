package postgres

import (
	"database/sql"
	"errors"
	"fmt"
)

// wrapper type around sql.Tx
// Adds methods for INSERT, UPDATE and DELETE
// RecordValues
type Tx struct {
	*sql.Tx
	db *DB
}

func (tx *Tx) Relations() (rels map[string]*Relation, err error) {
	return tx.db.Relations()
}

// Create a Query for a named relation
// any errors are defered until an actual query is performed
func (tx *Tx) From(name string) *Query {
	// TODO: stop loading ALL relations just to get one
	q := new(Query)
	rel, err := tx.db.Relation(name)
	if err != nil {
		q.err = err
		return q
	}
	q.from = rel
	q.tx = tx
	return q
}

// perform query q and update values in v from the first RETURNING result
func (tx *Tx) queryAndUpdate(q string, v RecordValue, update bool) error {
	rs, err := tx.Query(q, v.Relation().valArgs(v, update)...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		err := rs.ScanRecord(v)
		if err != nil {
			return err
		}
	}
	return rs.Close()
}

// INSERT RecordValue(s)
func (tx *Tx) Insert(vs ...RecordValue) error {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return errors.New("RecordValue does not have a relation set")
		}
		bnds, _ := rel.bindings(false, false)
		s := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) RETURNING %s`,
			rel.Name,
			rel.fields(false),
			bnds,
			rel.fields(true))
		err := tx.queryAndUpdate(s, v, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// UPDATE RecordValue(s)
func (tx *Tx) Update(vs ...RecordValue) error {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return errors.New("RecordValue does not have a relation set")
		}
		pk := rel.pk()
		if pk == nil {
			return errors.New("Relation must have a primary key to use Update")
		}
		bnds, n := rel.bindings(false, true)
		s := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = $%d RETURNING %s`,
			rel.Name,
			bnds,
			pk.name,
			n+1,
			rel.fields(true))
		err := tx.queryAndUpdate(s, v, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// UPDATE or INSERT RecordValue(s)
func (tx *Tx) Upsert(vs ...RecordValue) (err error) {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return errors.New("RecordValue does not have a relation set")
		}
		pk := rel.pk()
		if pk == nil {
			return errors.New("Relation has no primary key")
		}
		pkv := v.ValueBy(pk.name)
		if pkv == nil || pkv.IsNull() {
			err = tx.Insert(v)
		} else {
			err = tx.Update(v)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DELETE RecordValue(s)
func (tx *Tx) Delete(vs ...RecordValue) error {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return errors.New("RecordValue does not have a relation set")
		}
		pk := rel.pk()
		if pk == nil {
			return errors.New("Relation has no primary key")
		}
		pkv := v.ValueBy(pk.name)
		if pkv == nil {
			return errors.New("Value must have a primary key set")
		}
		s := fmt.Sprintf(`DELETE FROM %s WHERE %s = $1`,
			rel.Name,
			pk.name)
		rs, err := tx.Tx.Query(s, pkv)
		if err != nil {
			return err
		}
		rs.Close()
	}
	return nil
}

// like sql.Tx.Query only returns a *Rows rather than *sql.Rows
func (tx *Tx) Query(q string, vals ...interface{}) (*Rows, error) {
	rows, err := tx.Tx.Query(q, vals...)
	if err != nil {
		return nil, err
	}
	rs := new(Rows)
	rs.Rows = rows
	return rs, nil
}
