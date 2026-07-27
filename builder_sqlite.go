// Copyright 2016 Qiang Xue. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dbx

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var _ QueryBuilder = &SqliteQueryBuilder{}

// SqliteQueryBuilder is the query builder for SQLite databases.
type SqliteQueryBuilder struct {
	*BaseQueryBuilder
}

// BuildUnion generates a UNION clause from the given union information.
//
// This is similar to BaseQueryBuilder.BuildUnion but without the parenthesis wrapping.
func (b *SqliteQueryBuilder) BuildUnion(unions []UnionInfo, params Params) string {
	if len(unions) == 0 {
		return ""
	}

	sql := ""

	for i, union := range unions {
		if i > 0 {
			sql += " "
		}

		for k, v := range union.Query.params {
			params[k] = v
		}

		u := "UNION"
		if union.All {
			u = "UNION ALL"
		}

		sql += fmt.Sprintf("%v %v", u, union.Query.sql)
	}

	return sql
}

// CombineUnion combines the nonempty unionClause with the provided sql string.
//
// This is similar to BaseQueryBuilder.CombineUnion but without the parenthesis wrapping.
func (q *SqliteQueryBuilder) CombineUnion(sql string, unionClause string) string {
	if unionClause == "" {
		return sql
	}

	return fmt.Sprintf("%v %v", sql, unionClause)
}

// -------------------------------------------------------------------

// SqliteBuilder is the builder for SQLite databases.
type SqliteBuilder struct {
	*BaseBuilder
	qb *SqliteQueryBuilder
}

var _ Builder = &SqliteBuilder{}

// NewSqliteBuilder creates a new SqliteBuilder instance.
func NewSqliteBuilder(db *DB, executor Executor) Builder {
	return &SqliteBuilder{
		NewBaseBuilder(db, executor),
		&SqliteQueryBuilder{NewBaseQueryBuilder(db)},
	}
}

// QueryBuilder returns the query builder supporting the current DB.
func (b *SqliteBuilder) QueryBuilder() QueryBuilder {
	return b.qb
}

// Select returns a new SelectQuery object that can be used to build a SELECT statement.
// The parameters to this method should be the list column names to be selected.
// A column name may have an optional alias name. For example, Select("id", "my_name AS name").
func (b *SqliteBuilder) Select(cols ...string) *SelectQuery {
	return NewSelectQuery(b, b.db).Select(cols...)
}

// Model returns a new ModelQuery object that can be used to perform model-based DB operations.
// The model passed to this method should be a pointer to a model struct.
func (b *SqliteBuilder) Model(model interface{}) *ModelQuery {
	return NewModelQuery(model, b.db.FieldMapper, b.db, b)
}

// QuoteSimpleTableName quotes a simple table name.
// A simple table name does not contain any schema prefix.
// Upsert creates a Query that represents an INSERT ... ON CONFLICT DO UPDATE
// statement.
//
// SQLite has supported this since 3.24 (2018), but SqliteBuilder never
// overrode it, so every SQLite caller fell through to BaseBuilder.Upsert and
// got a query carrying LastError = "Upsert is not supported". PgsqlBuilder
// implemented it all along. The result was that callers on SQLite wrote raw
// INSERT ... ON CONFLICT strings by hand and concluded the builder could not
// express it — 58 such sites in hanzoai/cloud alone — when the gap was one
// missing override in a package we own.
//
// The generated SQL matches the Postgres form, which is also SQLite's: the
// conflict target is optional, and omitting it means "any uniqueness
// constraint".
func (b *SqliteBuilder) Upsert(table string, cols Params, constraints ...string) *Query {
	q := b.Insert(table, cols)

	names := []string{}
	for name := range cols {
		names = append(names, name)
	}
	// Sorted so the statement is deterministic: an Upsert that renders a
	// different string each call defeats statement caching and makes tests
	// flaky for no reason. Map iteration order is not a feature.
	sort.Strings(names)

	lines := []string{}
	for _, name := range names {
		value := cols[name]
		name = b.db.QuoteColumnName(name)
		if e, ok := value.(Expression); ok {
			lines = append(lines, name+"="+e.Build(b.db, q.params))
		} else {
			lines = append(lines, fmt.Sprintf("%v={:p%v}", name, len(q.params)))
			q.params[fmt.Sprintf("p%v", len(q.params))] = value
		}
	}

	if len(constraints) > 0 {
		c := b.quoteColumns(constraints)
		q.sql += " ON CONFLICT (" + c + ") DO UPDATE SET " + strings.Join(lines, ", ")
	} else {
		q.sql += " ON CONFLICT DO UPDATE SET " + strings.Join(lines, ", ")
	}

	return b.NewQuery(q.sql).Bind(q.params)
}

func (b *SqliteBuilder) QuoteSimpleTableName(s string) string {
	if strings.ContainsAny(s, "`") {
		return s
	}
	return "`" + s + "`"
}

// QuoteSimpleColumnName quotes a simple column name.
// A simple column name does not contain any table prefix.
func (b *SqliteBuilder) QuoteSimpleColumnName(s string) string {
	if strings.Contains(s, "`") || s == "*" {
		return s
	}
	return "`" + s + "`"
}

// DropIndex creates a Query that can be used to remove the named index from a table.
func (b *SqliteBuilder) DropIndex(table, name string) *Query {
	sql := fmt.Sprintf("DROP INDEX %v", b.db.QuoteColumnName(name))
	return b.NewQuery(sql)
}

// TruncateTable creates a Query that can be used to truncate a table.
func (b *SqliteBuilder) TruncateTable(table string) *Query {
	sql := "DELETE FROM " + b.db.QuoteTableName(table)
	return b.NewQuery(sql)
}

// RenameTable creates a Query that can be used to rename a table.
func (b *SqliteBuilder) RenameTable(oldName, newName string) *Query {
	sql := fmt.Sprintf("ALTER TABLE %v RENAME TO %v", b.db.QuoteTableName(oldName), b.db.QuoteTableName(newName))
	return b.NewQuery(sql)
}

// AlterColumn creates a Query that can be used to change the definition of a table column.
func (b *SqliteBuilder) AlterColumn(table, col, typ string) *Query {
	q := b.NewQuery("")
	q.LastError = errors.New("SQLite does not support altering column")
	return q
}

// AddPrimaryKey creates a Query that can be used to specify primary key(s) for a table.
// The "name" parameter specifies the name of the primary key constraint.
func (b *SqliteBuilder) AddPrimaryKey(table, name string, cols ...string) *Query {
	q := b.NewQuery("")
	q.LastError = errors.New("SQLite does not support adding primary key")
	return q
}

// DropPrimaryKey creates a Query that can be used to remove the named primary key constraint from a table.
func (b *SqliteBuilder) DropPrimaryKey(table, name string) *Query {
	q := b.NewQuery("")
	q.LastError = errors.New("SQLite does not support dropping primary key")
	return q
}

// AddForeignKey creates a Query that can be used to add a foreign key constraint to a table.
// The length of cols and refCols must be the same as they refer to the primary and referential columns.
// The optional "options" parameters will be appended to the SQL statement. They can be used to
// specify options such as "ON DELETE CASCADE".
func (b *SqliteBuilder) AddForeignKey(table, name string, cols, refCols []string, refTable string, options ...string) *Query {
	q := b.NewQuery("")
	q.LastError = errors.New("SQLite does not support adding foreign keys")
	return q
}

// DropForeignKey creates a Query that can be used to remove the named foreign key constraint from a table.
func (b *SqliteBuilder) DropForeignKey(table, name string) *Query {
	q := b.NewQuery("")
	q.LastError = errors.New("SQLite does not support dropping foreign keys")
	return q
}
