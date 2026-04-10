// Copyright 2026 Hanzo AI. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//go:build ignore

package dbx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// CompatEngine wraps a *DB to provide xorm-style fluent query methods.
// This enables mechanical migration from xorm without rewriting every call site.
//
// Usage:
//
//	engine := dbx.NewCompatEngine(db)
//	engine.Where("owner=?", "admin").Find(&users)
//	engine.ID([]interface{}{"admin","app1"}).Get(&app)
//	engine.Insert(&user)
//	engine.ID(pk).Update(&user)
//	engine.ID(pk).Delete(&User{})
type CompatEngine struct {
	db         *DB
	where      string
	whereArgs  []interface{}
	orderBy    string
	limit      int
	offset     int
	id         []interface{}
	useBool    bool
	allCols    bool
	mustCols   []string
	tableName  string
}

// NewCompatEngine creates a xorm-compatible wrapper around a dbx.DB.
func NewCompatEngine(db *DB) *CompatEngine {
	return &CompatEngine{db: db}
}

func (e *CompatEngine) clone() *CompatEngine {
	return &CompatEngine{
		db:        e.db,
		where:     e.where,
		whereArgs: e.whereArgs,
		orderBy:   e.orderBy,
		limit:     e.limit,
		offset:    e.offset,
		id:        e.id,
		useBool:   e.useBool,
		allCols:   e.allCols,
		mustCols:  e.mustCols,
		tableName: e.tableName,
	}
}

func (e *CompatEngine) reset() {
	e.where = ""
	e.whereArgs = nil
	e.orderBy = ""
	e.limit = 0
	e.offset = 0
	e.id = nil
	e.useBool = false
	e.allCols = false
	e.mustCols = nil
	e.tableName = ""
}

// Where adds a WHERE condition. Chainable.
func (e *CompatEngine) Where(cond string, args ...interface{}) *CompatEngine {
	c := e.clone()
	if c.where != "" {
		c.where += " AND " + cond
		c.whereArgs = append(c.whereArgs, args...)
	} else {
		c.where = cond
		c.whereArgs = args
	}
	return c
}

// ID sets the primary key for Get/Update/Delete. Accepts single value or []interface{} for composite PKs.
func (e *CompatEngine) ID(id interface{}) *CompatEngine {
	c := e.clone()
	switch v := id.(type) {
	case []interface{}:
		c.id = v
	default:
		c.id = []interface{}{v}
	}
	return c
}

// Desc adds ORDER BY ... DESC.
func (e *CompatEngine) Desc(cols ...string) *CompatEngine {
	c := e.clone()
	for _, col := range cols {
		if c.orderBy != "" {
			c.orderBy += ", "
		}
		c.orderBy += col + " DESC"
	}
	return c
}

// Asc adds ORDER BY ... ASC.
func (e *CompatEngine) Asc(cols ...string) *CompatEngine {
	c := e.clone()
	for _, col := range cols {
		if c.orderBy != "" {
			c.orderBy += ", "
		}
		c.orderBy += col + " ASC"
	}
	return c
}

// Limit sets LIMIT and OFFSET.
func (e *CompatEngine) Limit(limit int, offset ...int) *CompatEngine {
	c := e.clone()
	c.limit = limit
	if len(offset) > 0 {
		c.offset = offset[0]
	}
	return c
}

// Table sets an explicit table name.
func (e *CompatEngine) Table(name string) *CompatEngine {
	c := e.clone()
	c.tableName = name
	return c
}

// UseBool is a no-op for compatibility (dbx handles bools natively).
func (e *CompatEngine) UseBool(cols ...string) *CompatEngine {
	c := e.clone()
	c.useBool = true
	return c
}

// AllCols marks all columns for update (default in dbx).
func (e *CompatEngine) AllCols() *CompatEngine {
	c := e.clone()
	c.allCols = true
	return c
}

// MustCols forces specific columns in update even if zero value.
func (e *CompatEngine) MustCols(cols ...string) *CompatEngine {
	c := e.clone()
	c.mustCols = append(c.mustCols, cols...)
	return c
}

// Cols is an alias for MustCols.
func (e *CompatEngine) Cols(cols ...string) *CompatEngine {
	return e.MustCols(cols...)
}

// Find queries all matching rows into a slice pointer.
func (e *CompatEngine) Find(rowsSlicePtr interface{}, conds ...interface{}) error {
	defer e.reset()
	table := e.resolveTable(rowsSlicePtr)
	q := e.db.Select().From(table)
	q = e.applyConditions(q)

	// Additional conditions from args
	for _, cond := range conds {
		if hm, ok := cond.(map[string]interface{}); ok {
			for k, v := range hm {
				q = q.Where(HashExp{k: v})
			}
		}
	}

	return q.All(rowsSlicePtr)
}

// Get queries a single row by PK or conditions.
func (e *CompatEngine) Get(bean interface{}) (bool, error) {
	defer e.reset()
	table := e.resolveTable(bean)
	q := e.db.Select().From(table)
	q = e.applyConditions(q)

	err := q.One(bean)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Insert inserts a struct into its table.
func (e *CompatEngine) Insert(beans ...interface{}) (int64, error) {
	defer e.reset()
	var total int64
	for _, bean := range beans {
		_, err := e.db.Model(bean).Insert()
		if err != nil {
			return total, err
		}
		total++
	}
	return total, nil
}

// Update updates a row by PK or conditions.
func (e *CompatEngine) Update(bean interface{}, conds ...interface{}) (int64, error) {
	defer e.reset()
	table := e.resolveTable(bean)

	// Build SET clause from struct fields
	sv := e.db.NewStructValue(bean, nil)
	if sv == nil {
		return 0, fmt.Errorf("cannot resolve struct for update")
	}

	sets := make(map[string]interface{})
	v := reflect.ValueOf(bean)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get(DbTag)
		if tag == "-" || tag == "pk" || strings.HasPrefix(tag, "pk,") {
			continue
		}
		fv := v.Field(i)
		// Include if AllCols or non-zero or in MustCols
		if e.allCols || !fv.IsZero() || e.inMustCols(tag, field.Name) {
			colName := tag
			if colName == "" {
				colName = defaultFieldName(field.Name)
			}
			sets[colName] = fv.Interface()
		}
	}

	if len(sets) == 0 {
		return 0, nil
	}

	// Build WHERE from ID or conditions
	where, args := e.buildWhere(bean)

	// Build UPDATE SQL
	var setCols []string
	var setArgs []interface{}
	i := 0
	for col, val := range sets {
		setCols = append(setCols, fmt.Sprintf(`"%s"=?`, col))
		setArgs = append(setArgs, val)
		i++
	}

	sql := fmt.Sprintf(`UPDATE "%s" SET %s`, table, strings.Join(setCols, ", "))
	if where != "" {
		sql += " WHERE " + where
		setArgs = append(setArgs, args...)
	}

	result, err := e.db.NewQuery(sql).Execute(setArgs...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

// Delete deletes rows by PK or conditions.
func (e *CompatEngine) Delete(bean interface{}) (int64, error) {
	defer e.reset()
	table := e.resolveTable(bean)
	where, args := e.buildWhere(bean)

	sql := fmt.Sprintf(`DELETE FROM "%s"`, table)
	if where != "" {
		sql += " WHERE " + where
	}

	result, err := e.db.NewQuery(sql).Execute(args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

// Count returns the number of matching rows.
func (e *CompatEngine) Count(bean ...interface{}) (int64, error) {
	defer e.reset()
	var table string
	if len(bean) > 0 {
		table = e.resolveTable(bean[0])
	} else if e.tableName != "" {
		table = e.tableName
	} else {
		return 0, fmt.Errorf("Count requires a bean or Table()")
	}

	sql := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table)
	var args []interface{}
	if e.where != "" {
		sql += " WHERE " + e.where
		args = e.whereArgs
	}

	var count int64
	err := e.db.NewQuery(sql).Row(&count, args...)
	return count, err
}

// Exec executes raw SQL.
func (e *CompatEngine) Exec(sql string, args ...interface{}) (sql.Result, error) {
	return e.db.NewQuery(sql).Execute(args...)
}

// Sync creates or updates tables from struct definitions.
func (e *CompatEngine) Sync(beans ...interface{}) error {
	return e.db.Sync(beans...)
}

// Sync2 is an alias for Sync (xorm compat).
func (e *CompatEngine) Sync2(beans ...interface{}) error {
	return e.db.Sync(beans...)
}

// QueryString executes raw SQL and returns string maps.
func (e *CompatEngine) QueryString(sql string, args ...interface{}) ([]map[string]string, error) {
	rows, err := e.db.NewQuery(sql).Rows(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var results []map[string]string

	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		rows.Scan(ptrs...)

		row := make(map[string]string)
		for i, col := range cols {
			row[col] = fmt.Sprintf("%v", values[i])
		}
		results = append(results, row)
	}
	return results, nil
}

// ShowSQL is a no-op for compatibility.
func (e *CompatEngine) ShowSQL(show ...bool) {}

// Ping checks the database connection.
func (e *CompatEngine) Ping() error {
	return e.db.DB().Ping()
}

// RawDB returns the underlying *sql.DB.
func (e *CompatEngine) RawDB() *sql.DB {
	return e.db.DB()
}

// DB returns the underlying dbx.DB.
func (e *CompatEngine) DBX() *DB {
	return e.db
}

// --- helpers ---

func (e *CompatEngine) resolveTable(bean interface{}) string {
	if e.tableName != "" {
		return e.tableName
	}
	t := reflect.TypeOf(bean)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	return defaultFieldName(t.Name())
}

func (e *CompatEngine) applyConditions(q *SelectQuery) *SelectQuery {
	if e.id != nil {
		// Build WHERE from PK columns
		// For composite PKs, we need the struct info — use positional matching
		// This is a simplification; real code should use struct PK names
		// For now, use owner + name convention
		switch len(e.id) {
		case 1:
			q = q.Where(HashExp{"id": e.id[0]})
		case 2:
			q = q.Where(HashExp{"owner": e.id[0], "name": e.id[1]})
		case 3:
			q = q.Where(HashExp{"owner": e.id[0], "name": e.id[1], "application": e.id[2]})
		}
	}
	if e.where != "" {
		q = q.Where(NewExp(e.where, e.whereArgs...))
	}
	if e.orderBy != "" {
		q = q.OrderBy(e.orderBy)
	}
	if e.limit > 0 {
		q = q.Limit(int64(e.limit))
	}
	if e.offset > 0 {
		q = q.Offset(int64(e.offset))
	}
	return q
}

func (e *CompatEngine) buildWhere(bean interface{}) (string, []interface{}) {
	if e.id != nil {
		switch len(e.id) {
		case 1:
			return `"id"=?`, e.id
		case 2:
			return `"owner"=? AND "name"=?`, e.id
		case 3:
			return `"owner"=? AND "name"=? AND "application"=?`, e.id
		}
	}
	if e.where != "" {
		return e.where, e.whereArgs
	}
	return "", nil
}

func (e *CompatEngine) inMustCols(tag, fieldName string) bool {
	for _, mc := range e.mustCols {
		if mc == tag || mc == fieldName || mc == defaultFieldName(fieldName) {
			return true
		}
	}
	return false
}

// NewExp creates a raw SQL expression with positional args.
func NewExp(expr string, args ...interface{}) Expression {
	return &RawExpression{expr: expr, args: args}
}

// RawExpression implements Expression for raw SQL.
type RawExpression struct {
	expr string
	args []interface{}
}

func (e *RawExpression) Build(db *DB, params Params) string {
	sql := e.expr
	for i, arg := range e.args {
		key := fmt.Sprintf("_raw_%d", i)
		params[key] = arg
		sql = strings.Replace(sql, "?", "{:"+key+"}", 1)
	}
	return sql
}
