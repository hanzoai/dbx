// Copyright 2026 Hanzo AI. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dbx

import (
	"fmt"
	"reflect"
	"strings"
)

// SyncOptions configures the Sync behavior.
type SyncOptions struct {
	// FieldMapper maps struct field names to DB column names.
	// If nil, uses the db tag or defaults to the field name.
	FieldMapper FieldMapFunc

	// TableMapper maps struct types to table names.
	// If nil, uses snake_case of the struct name.
	TableMapper TableMapFunc
}

// Sync creates or updates tables to match the given struct definitions.
// Similar to xorm's Sync2() — reads struct fields and tags, generates
// CREATE TABLE IF NOT EXISTS and ALTER TABLE ADD COLUMN statements.
//
// Supported db tags:
//   - `db:"pk"` or `db:"pk,column_name"` — primary key
//   - `db:"column_name"` — explicit column name
//   - `db:"-"` — skip field
//
// Extended tags for schema (optional, ignored if not present):
//   - `db:"pk" type:"varchar(100)"` — explicit SQL type
//   - `db:"column_name" notnull:"true"` — NOT NULL constraint
//   - `db:"column_name" unique:"true"` — UNIQUE constraint
//   - `db:"column_name" default:"value"` — DEFAULT value
//
// Usage:
//
//	db.Sync(User{}, Organization{}, Application{})
func (db *DB) Sync(models ...interface{}) error {
	for _, model := range models {
		if err := db.syncOne(model, nil); err != nil {
			return err
		}
	}
	return nil
}

// SyncWith creates or updates tables with custom options.
func (db *DB) SyncWith(opts SyncOptions, models ...interface{}) error {
	for _, model := range models {
		if err := db.syncOne(model, &opts); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) syncOne(model interface{}, opts *SyncOptions) error {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("dbx.Sync: expected struct, got %s", t.Kind())
	}

	tableName := structToTableName(t, opts)
	columns := extractColumns(t, opts)

	if len(columns) == 0 {
		return fmt.Errorf("dbx.Sync: struct %s has no exported fields", t.Name())
	}

	// Check if table exists
	exists, err := db.tableExists(tableName)
	if err != nil {
		return fmt.Errorf("dbx.Sync: check table %s: %w", tableName, err)
	}

	if !exists {
		return db.createTable(tableName, columns)
	}

	// Table exists — add any missing columns
	return db.addMissingColumns(tableName, columns)
}

type columnDef struct {
	Name       string
	SQLType    string
	PK         bool
	NotNull    bool
	Unique     bool
	Default    string
	HasDefault bool
}

func extractColumns(t reflect.Type, opts *SyncOptions) []columnDef {
	var cols []columnDef
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get(DbTag)
		if tag == "-" {
			continue
		}

		// Handle embedded structs
		if field.Anonymous && field.Type.Kind() == reflect.Struct && !isNestedStruct(field.Type) {
			// Skip non-nested embedded structs (they're flattened by dbx)
			continue
		}
		if field.Anonymous && field.Type.Kind() == reflect.Struct && isNestedStruct(field.Type) {
			embedded := extractColumns(field.Type, opts)
			cols = append(cols, embedded...)
			continue
		}

		col := columnDef{}

		// Parse db tag
		dbName, isPK := parseTag(tag)
		if dbName == "" {
			if opts != nil && opts.FieldMapper != nil {
				dbName = opts.FieldMapper(field.Name)
			} else {
				dbName = defaultFieldName(field.Name)
			}
		}
		col.Name = dbName
		col.PK = isPK

		// Parse extended tags
		if sqlType := field.Tag.Get("type"); sqlType != "" {
			col.SQLType = sqlType
		} else {
			col.SQLType = goTypeToSQL(field.Type)
		}

		if field.Tag.Get("notnull") == "true" {
			col.NotNull = true
		}
		if field.Tag.Get("unique") == "true" {
			col.Unique = true
		}
		if def := field.Tag.Get("default"); def != "" {
			col.Default = def
			col.HasDefault = true
		}

		cols = append(cols, col)
	}
	return cols
}

func structToTableName(t reflect.Type, opts *SyncOptions) string {
	if opts != nil && opts.TableMapper != nil {
		return opts.TableMapper(reflect.New(t).Elem().Interface())
	}
	return defaultFieldName(t.Name())
}

func defaultFieldName(name string) string {
	// CamelCase → snake_case
	var result strings.Builder
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				prev := name[i-1]
				if prev >= 'a' && prev <= 'z' {
					result.WriteByte('_')
				}
			}
			result.WriteRune(r + 32) // toLower
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func goTypeToSQL(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "INTEGER"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.Bool:
		return "INTEGER" // SQLite: 0/1
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BLOB"
		}
		return "TEXT" // JSON-serialized
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return "TEXT" // ISO 8601
		}
		return "TEXT" // JSON-serialized
	default:
		return "TEXT"
	}
}

func (db *DB) tableExists(name string) (bool, error) {
	var count int
	q := db.NewQuery("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name={:name}")
	q.Bind(Params{"name": name})
	if err := q.Row(&count); err != nil {
		// Try information_schema for PostgreSQL/MySQL
		q2 := db.NewQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name={:name}")
		q2.Bind(Params{"name": name})
		if err2 := q2.Row(&count); err2 != nil {
			return false, fmt.Errorf("check table exists: sqlite=%w, pg=%w", err, err2)
		}
	}
	return count > 0, nil
}

func (db *DB) existingColumns(tableName string) (map[string]bool, error) {
	cols := make(map[string]bool)

	// Try PRAGMA for SQLite
	rows, err := db.NewQuery(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(tableName))).Rows()
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var cid int
			var name, typ string
			var notNull, pk int
			var dflt *string
			if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
				continue
			}
			cols[name] = true
		}
		if len(cols) > 0 {
			return cols, nil
		}
	}

	// Fallback: information_schema for PostgreSQL/MySQL
	q := db.NewQuery("SELECT column_name FROM information_schema.columns WHERE table_name={:table}")
	q.Bind(Params{"table": tableName})
	rows2, err := q.Rows()
	if err != nil {
		return cols, nil // empty map, best effort
	}
	defer rows2.Close()
	for rows2.Next() {
		var name string
		rows2.Scan(&name)
		cols[name] = true
	}
	return cols, nil
}

func (db *DB) createTable(tableName string, columns []columnDef) error {
	var pks []string
	var colDefs []string

	for _, col := range columns {
		def := fmt.Sprintf("%s %s", quoteIdent(col.Name), col.SQLType)
		if col.NotNull {
			def += " NOT NULL"
		}
		if col.Unique {
			def += " UNIQUE"
		}
		if col.HasDefault {
			def += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		colDefs = append(colDefs, def)
		if col.PK {
			pks = append(pks, quoteIdent(col.Name))
		}
	}

	if len(pks) > 0 {
		colDefs = append(colDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ", ")))
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		quoteIdent(tableName),
		strings.Join(colDefs, ",\n  "))

	_, err := db.NewQuery(sql).Execute()
	return err
}

func (db *DB) addMissingColumns(tableName string, columns []columnDef) error {
	existing, err := db.existingColumns(tableName)
	if err != nil {
		return err
	}

	for _, col := range columns {
		if existing[col.Name] {
			continue
		}
		// ALTER TABLE ADD COLUMN
		def := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			quoteIdent(tableName), quoteIdent(col.Name), col.SQLType)
		if col.HasDefault {
			def += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if _, err := db.NewQuery(def).Execute(); err != nil {
			return fmt.Errorf("add column %s.%s: %w", tableName, col.Name, err)
		}
	}
	return nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
