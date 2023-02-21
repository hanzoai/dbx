// Copyright 2016 Qiang Xue. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dbx

import (
	"errors"
	"testing"

	"database/sql"

	"github.com/stretchr/testify/assert"
)

func TestSelectQuery(t *testing.T) {
	db := getDB()

	// minimal select query
	q := db.Select().From("users").Build()
	expected := "SELECT * FROM `users`"
	assert.Equal(t, q.SQL(), expected, "t1")
	assert.Equal(t, len(q.Params()), 0, "t2")

	// a full select query
	q = db.Select("id", "name").
		AndSelect("age").
		Distinct(true).
		SelectOption("CALC").
		From("users").
		Where(NewExp("age>30")).
		AndWhere(NewExp("status=1")).
		OrWhere(NewExp("type=2")).
		InnerJoin("profile", NewExp("user.id=profile.id")).
		LeftJoin("team", nil).
		RightJoin("dept", nil).
		OrderBy("age DESC", "type").
		AndOrderBy("id").
		GroupBy("id").
		AndGroupBy("age").
		Having(NewExp("id>10")).
		AndHaving(NewExp("id<20")).
		OrHaving(NewExp("type=3")).
		Limit(10).
		Offset(20).
		Bind(Params{"id": 1}).
		AndBind(Params{"age": 30}).
		Build()

	expected = "SELECT DISTINCT CALC `id`, `name`, `age` FROM `users` INNER JOIN `profile` ON user.id=profile.id LEFT JOIN `team` RIGHT JOIN `dept` WHERE ((age>30) AND (status=1)) OR (type=2) GROUP BY `id`, `age` HAVING ((id>10) AND (id<20)) OR (type=3) ORDER BY `age` DESC, `type`, `id` LIMIT 10 OFFSET 20"
	assert.Equal(t, q.SQL(), expected, "t3")
	assert.Equal(t, len(q.Params()), 2, "t4")

	q3 := db.Select().AndBind(Params{"id": 1}).Build()
	assert.Equal(t, len(q3.Params()), 1)

	// union
	q1 := db.Select().From("users").Build()
	q2 := db.Select().From("posts").Build()
	q = db.Select().From("profiles").Union(q1).UnionAll(q2).Build()
	expected = "(SELECT * FROM `profiles`) UNION (SELECT * FROM `users`) UNION ALL (SELECT * FROM `posts`)"
	assert.Equal(t, q.SQL(), expected, "t5")
}

func TestSelectQuery_Data(t *testing.T) {
	db := getPreparedDB()
	defer db.Close()

	q := db.Select("id", "email").From("customer").OrderBy("id")

	var customer Customer
	q.One(&customer)
	assert.Equal(t, customer.Email, "user1@example.com", "customer.Email")

	var customers []Customer
	q.All(&customers)
	assert.Equal(t, len(customers), 3, "len(customers)")

	rows, _ := q.Rows()
	customer.Email = ""
	rows.one(&customer)
	assert.Equal(t, customer.Email, "user1@example.com", "customer.Email")

	var id, email string
	q.Row(&id, &email)
	assert.Equal(t, id, "1", "id")
	assert.Equal(t, email, "user1@example.com", "email")

	var emails []string
	err := db.Select("email").From("customer").Column(&emails)
	if assert.Nil(t, err) {
		assert.Equal(t, 3, len(emails))
	}

	var e int
	err = db.Select().From("customer").One(&e)
	assert.NotNil(t, err)
	err = db.Select().From("customer").All(&e)
	assert.NotNil(t, err)
}

func TestSelectQuery_Model(t *testing.T) {
	db := getPreparedDB()
	defer db.Close()

	{
		// One without specifying FROM
		var customer CustomerPtr
		err := db.Select().OrderBy("id").One(&customer)
		if assert.Nil(t, err) {
			assert.Equal(t, "user1@example.com", *customer.Email)
		}
	}

	{
		// All without specifying FROM
		var customers []CustomerPtr
		err := db.Select().OrderBy("id").All(&customers)
		if assert.Nil(t, err) {
			assert.Equal(t, 3, len(customers))
		}
	}

	{
		// Model without specifying FROM
		var customer CustomerPtr
		err := db.Select().Model(2, &customer)
		if assert.Nil(t, err) {
			assert.Equal(t, "user2@example.com", *customer.Email)
		}
	}

	{
		// Model with WHERE
		var customer CustomerPtr
		err := db.Select().Where(HashExp{"id": 1}).Model(2, &customer)
		assert.Equal(t, sql.ErrNoRows, err)

		err = db.Select().Where(HashExp{"id": 2}).Model(2, &customer)
		assert.Nil(t, err)
	}

	{
		// errors
		var i int
		err := db.Select().Model(1, &i)
		assert.Equal(t, VarTypeError("must be a pointer to a struct"), err)

		var a struct {
			Name string
		}

		err = db.Select().Model(1, &a)
		assert.Equal(t, MissingPKError, err)
		var b struct {
			ID1 string `db:"pk"`
			ID2 string `db:"pk"`
		}
		err = db.Select().Model(1, &b)
		assert.Equal(t, CompositePKError, err)
	}
}

func TestSelectWithExecHook(t *testing.T) {
	db := getPreparedDB()
	defer db.Close()

	// error return
	{
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				return errors.New("test")
			}).
			Row()

		assert.Error(t, err)
	}

	// Row()
	{
		calls := 0
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				calls++
				return nil
			}).
			Row()
		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "Row()")
	}

	// Rows()
	{
		calls := 0
		_, err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				calls++
				return nil
			}).
			Rows()
		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "Rows()")
	}

	// One()
	{
		calls := 0
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				calls++
				return nil
			}).
			One(nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "One()")
	}

	// All()
	{
		calls := 0
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				calls++
				return nil
			}).
			All(nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "All()")
	}

	// Column()
	{
		calls := 0
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				calls++
				return nil
			}).
			Column(nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "Column()")
	}

	// op call
	{
		calls := 0
		var id int
		err := db.Select("id").
			From("user").
			WithExecHook(func(s *SelectQuery, op func() error) error {
				calls++
				return op()
			}).
			Where(HashExp{"id": 2}).
			Row(&id)
		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "op hook calls")
		assert.Equal(t, 2, id, "id mismatch")
	}
}

func TestSelectWithOneHook(t *testing.T) {
	db := getPreparedDB()
	defer db.Close()

	// error return
	{
		err := db.Select().
			WithOneHook(func(s *SelectQuery, a interface{}, op func(b interface{}) error) error {
				return errors.New("test")
			}).
			One(nil)

		assert.Error(t, err)
	}

	// hooks call order
	{
		hookCalls := []string{}
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				hookCalls = append(hookCalls, "exec")
				return op()
			}).
			WithOneHook(func(s *SelectQuery, a interface{}, op func(b interface{}) error) error {
				hookCalls = append(hookCalls, "one")
				return nil
			}).
			One(nil)

		assert.Nil(t, err)
		assert.Equal(t, hookCalls, []string{"exec", "one"})
	}

	// op call
	{
		calls := 0
		other := User{}
		err := db.Select("id").
			From("user").
			WithOneHook(func(s *SelectQuery, a interface{}, op func(b interface{}) error) error {
				calls++
				return op(&other)
			}).
			Where(HashExp{"id": 2}).
			One(nil)

		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "hook calls")
		assert.Equal(t, int64(2), other.ID, "replaced scan struct")
	}
}

func TestSelectWithAllHook(t *testing.T) {
	db := getPreparedDB()
	defer db.Close()

	// error return
	{
		err := db.Select().
			WithAllHook(func(s *SelectQuery, a interface{}, op func(b interface{}) error) error {
				return errors.New("test")
			}).
			All(nil)

		assert.Error(t, err)
	}

	// hooks call order
	{
		hookCalls := []string{}
		err := db.Select().
			WithExecHook(func(s *SelectQuery, op func() error) error {
				hookCalls = append(hookCalls, "exec")
				return op()
			}).
			WithAllHook(func(s *SelectQuery, a interface{}, op func(b interface{}) error) error {
				hookCalls = append(hookCalls, "all")
				return nil
			}).
			All(nil)

		assert.Nil(t, err)
		assert.Equal(t, hookCalls, []string{"exec", "all"})
	}

	// op call
	{
		calls := 0
		other := []User{}
		err := db.Select("id").
			From("user").
			WithAllHook(func(s *SelectQuery, a interface{}, op func(b interface{}) error) error {
				calls++
				return op(&other)
			}).
			OrderBy("id asc").
			All(nil)

		assert.Nil(t, err)
		assert.Equal(t, 1, calls, "hook calls")
		assert.Equal(t, 2, len(other), "users length")
		assert.Equal(t, int64(1), other[0].ID, "user 1 id check")
		assert.Equal(t, int64(2), other[1].ID, "user 2 id check")
	}
}
