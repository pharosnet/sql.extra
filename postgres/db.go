package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	// SQL to list relations with oid
	selectRelsSql = `
		SELECT
			pgc.oid,
			pgc.relname
		FROM pg_class pgc, pg_namespace pgn
		WHERE pgc.relnamespace = pgn.oid
		AND pg_table_is_visible(pgc.oid)
		AND pgc.relkind IN ('r','v','c')
		AND pgc.relpersistence != 't'
		AND pgn.nspname = 'public'
	`
	// SQL to fetch col info for a relation
	// along with foreign key data notnull, primary key info
	selectColsSql = `
		SELECT DISTINCT
			a.attnum as num,
			a.attname as name,
			COALESCE(
				format_type(a.atttypid, a.atttypmod),
				''
			) as typ,
			a.atttypid as toid,
			a.attnotnull as notnull,
			COALESCE(i.indisprimary,false) as pk,
			COALESCE(fks.fktable, ''),
			COALESCE(fks.fkfield, ''),
			COALESCE(regexp_replace(
				regexp_replace(
					format_type(a.atttypid, a.atttypmod),
					E'^(.*?\\(|[^\\(]+$)',
					''
				),
				E'\\).*',
				''
			),'') as args
		FROM pg_attribute a JOIN pg_class pgc ON pgc.oid = a.attrelid
		LEFT JOIN pg_index i ON pgc.oid = i.indrelid AND i.indkey[0] = a.attnum
		LEFT JOIN (
			select
				att2.attname as name,
				cl.relname as fktable,
				att.attname as fkfield,
				con.relname as relname
			from
				(select
					unnest(con1.conkey) as "parent",
					unnest(con1.confkey) as "child",
					con1.confrelid,
					con1.conrelid,
					cl.relname as relname
				from
					pg_class cl
					join pg_namespace ns on cl.relnamespace = ns.oid
					join pg_constraint con1 on con1.conrelid = cl.oid
				where
					con1.contype = 'f'
				) con
			join pg_attribute att on
				att.attrelid = con.confrelid and att.attnum = con.child
			join pg_class cl on
				cl.oid = con.confrelid
			join pg_attribute att2 on
				att2.attrelid = con.conrelid and att2.attnum = con.parent
		) fks ON fks.name = a.attname AND fks.relname = pgc.relname
		WHERE a.attnum > 0 AND pgc.oid = a.attrelid
		AND pgc.oid = $1
		AND pg_table_is_visible(pgc.oid)
		AND NOT a.attisdropped
		ORDER BY a.attnum
	`
	// SQL to list pg_type info
	selectTypeSql = `
		SELECT
			typname,
			typtype,
			typdelim,
			typrelid,
			typelem,
			typarray,
			typnotnull,
			typbasetype,
			typtypmod,
			typndims
		FROM pg_type
		WHERE oid = $1
		AND typisdefined = true
	`

	// SQL to fetch list of enum labels
	selectEnumSql = `
		SELECT enumlabel
		FROM pg_enum
		WHERE enumtypid = $1
		ORDER BY enumsortorder
	`
)

// wrapper type around sql.DB
type DB struct {
	*sql.DB
	rels      map[string]*Relation
	getRels   *sql.Stmt
	getCols   *sql.Stmt
	getType   *sql.Stmt
	getLabels *sql.Stmt
}

// Analog of sql.Open that returns a *DB
// requires a "postgres" driver (lib/pq) is registered
func Open(dataSourceName string) (*DB, error) {
	rawdb, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	return newDB(rawdb)
}

// init *DB by preparing any stmts we might need
func newDB(rawdb *sql.DB) (db *DB, err error) {
	db = new(DB)
	db.DB = rawdb
	db.getRels, err = db.DB.Prepare(selectRelsSql)
	if err != nil {
		return
	}
	db.getCols, err = db.DB.Prepare(selectColsSql)
	if err != nil {
		return
	}
	db.getType, err = db.DB.Prepare(selectTypeSql)
	if err != nil {
		return
	}
	db.getLabels, err = db.DB.Prepare(selectEnumSql)
	if err != nil {
		return
	}
	return
}

// Create a new RecordValue for the named relation
func (db *DB) New(name string, args interface{}) (RecordValue, error) {
	rel, err := db.Relation(name)
	if err != nil {
		return nil, err
	}
	return rel.New(args)
}

// Return all the Relations from the database
func (db *DB) Relations() (rels map[string]*Relation, err error) {
	if db.rels == nil {
		rels, err = db.relations()
		if err != nil {
			return nil, err
		}
		db.rels = rels
	}
	return db.rels, err
}

// Create a Query for a named relation
func (db *DB) From(name string) *Query {
	// TODO: stop loading ALL relations just to get one
	q := new(Query)
	rel, err := db.Relation(name)
	if err != nil {
		q.err = err
		return q
	}
	q.from = rel
	q.tx = db
	return q
}

// Get Relation info by name
func (db *DB) Relation(name string) (*Relation, error) {
	// TODO: stop loading ALL relations just to get one
	rels, err := db.Relations()
	if err != nil {
		return nil, err
	}
	rel, ok := rels[name]
	if !ok {
		return nil, fmt.Errorf("No relation found: %s", name)
	}
	return rel, nil
}

func (db *DB) Query(q string, vals ...interface{}) (*Rows, error) {
	rows, err := db.DB.Query(q, vals...)
	if err != nil {
		return nil, err
	}
	rs := new(Rows)
	rs.Rows = rows
	return rs, nil
}

func (db *DB) Begin() (*Tx, error) {
	rawtx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{rawtx, db}, nil
}

func (db *DB) Insert(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Insert(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Update(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Update(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Upsert(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Upsert(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Delete(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Delete(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) relations() (map[string]*Relation, error) {
	rels := make(map[string]*Relation)
	rows, err := db.getRels.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var (
			oid  uint32
			name string
		)
		err = rows.Scan(&oid, &name)
		if err != nil {
			return nil, err
		}
		rel, err := db.relation(name, oid)
		if err != nil {
			return nil, err
		}
		rels[name] = rel
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	for _, rel := range rels {
		for _, c := range rel.cols {
			if c.refT == "" {
				continue
			}
			frel, ok := rels[c.refT]
			if !ok {
				return nil, fmt.Errorf("expected to find referenced relation: %s", c.refT)
			}
			rx := regexp.MustCompile(`_(id|sku|key)$`)
			hasOneName := rx.ReplaceAllString(c.name, "")
			rel.refs = append(rel.refs, &ref{hasOneName, ref_hasOne, frel, c})
			// NOTE:
			// if there are multiple local keys pointing to the foreign model
			// then the relation will be setup to look at ALL of the keys
			// ie if you have a table (person) with two foreign keys (locate_a_id, locate_b_id)
			// then the has_many side of that relationship will lookup like:
			// SELECT * FROM locate WHERE id = locate_a_id OR id = locate_b_id
			hasManyName := rel.Name
			frel.refs = append(frel.refs, &ref{hasManyName, ref_hasMany, rel, c})
		}
	}
	return rels, rows.Close()
}

// return list of cols for a pg_class oid
func (db *DB) cols(reloid uint32) ([]*col, error) {
	rows, err := db.getCols.Query(reloid)
	if err != nil {
		return nil, err
	}
	cols := make([]*col, 0)
	for rows.Next() {
		c := new(col)
		var argstr string
		var num int
		err = rows.Scan(&num, &c.name, &c.typ, &c.oid, &c.notNull,
			&c.pk, &c.refT, &c.refF, &argstr)
		if err != nil {
			return nil, err
		}
		var args []string
		if argstr != "" {
			args = strings.Split(argstr, ",")
		}
		c.k, err = db.kind(c.oid, args...)
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return cols, rows.Close()
}

// create a new Relation from the db
func (db *DB) relation(name string, oid uint32) (r *Relation, err error) {
	r = new(Relation)
	r.Name = name
	r.cols, err = db.cols(oid)
	r.k = Record(r.cols...)
	return r, err
}

func (db *DB) kind(oid uint32, args ...string) (ToValue, error) {
	if f, ok := typs[oid]; ok {
		return f(args...)
	}
	return db.complexKind(oid, args...)
}

func (db *DB) complexKind(oid uint32, args ...string) (ToValue, error) {
	rows, err := db.getType.Query(oid)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, fmt.Errorf("No pg_type with oid %d", oid)
	}
	var (
		name     string // the string representation
		typ      string // b=base c=composite d=domain e=enum p=pseudo
		delim    string // delimeter when array=0
		relid    uint32 // pg_class oid when typ=c
		elem     uint32 // the pg_type oid of the element (or 0 if not array)
		array    uint32 // the pg_type oid of the array version of type (or 0 if is an array)
		notnull  bool   // rejects nulls
		basetype uint32 // pg_type oid of base type when typ=d
		typmod   int32  // type-specific data supplied at table creation time
		ndims    int32  // num of array dimension when typ=d
	)
	err = rows.Scan(
		&name, &typ, &delim, &relid, &elem, &array,
		&notnull, &basetype, &typmod, &ndims,
	)
	if err != nil {
		return nil, err
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	switch typ {
	// base types
	case "b":
		switch array {
		// handle array
		case 0:
			elk, err := db.kind(elem, args...)
			if err != nil {
				return nil, err
			}
			return Array(elk), nil
		// handle other base types
		default:
			switch name {
			// auto-register hstore oid
			case "hstore":
				typs[oid] = func(args ...string) (ToValue, error) {
					return HStore, nil
				}
				return HStore, nil
			// other (unknown) base types
			default:
				return nil, fmt.Errorf("base type %s with oid %d is not implimented", name, oid)
			}
		}
	// composite types
	case "c":
		cols, err := db.cols(relid)
		if err != nil {
			return nil, err
		}
		return Record(cols...), nil
	// domain types
	case "d":
		return nil, errors.New("domain types not implimented yet")
	// enum types
	case "e":
		labels, err := db.enumLabelsFor(oid)
		if err != nil {
			return nil, err
		}
		if len(labels) == 0 {
			return nil, fmt.Errorf("No labels found for Enum type %s", name)
		}
		return Enum(labels...), nil
	// psuedo types
	default:
		return nil, errors.New("psuedo pg_types cannot be supported")
	}
	panic("unreachable")
}

func (db *DB) enumLabelsFor(oid uint32) ([]string, error) {
	rows, err := db.getLabels.Query(oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	labels := make([]string, 0)
	for rows.Next() {
		var label string
		err = rows.Scan(&label)
		if err != nil {
			return nil, err
		}
		labels = append(labels, label)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return labels, rows.Close()
}
