package dbx

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type syncTestUser struct {
	Owner       string `db:"pk" type:"varchar(100)"`
	Name        string `db:"pk" type:"varchar(100)"`
	Email       string `db:"email" notnull:"true"`
	DisplayName string `db:"display_name"`
	Age         int    `db:"age" default:"0"`
	IsAdmin     bool   `db:"is_admin"`
	CreatedAt   time.Time
	Data        []byte `db:"data"`
	Ignored     string `db:"-"`
}

type syncTestApp struct {
	Owner    string `db:"pk" type:"varchar(100)"`
	Name     string `db:"pk" type:"varchar(100)"`
	ClientID string `db:"client_id" unique:"true"`
	Secret   string `db:"secret"`
}

func testSyncDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sync_test.db")
	db, err := DefaultSQLiteConnect(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		os.Remove(path)
	})
	return db
}

func TestSync_CreateTable(t *testing.T) {
	db := testSyncDB(t)

	err := db.Sync(syncTestUser{})
	if err != nil {
		t.Fatal(err)
	}

	// Verify table exists
	var count int
	db.NewQuery("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sync_test_user'").Row(&count)
	if count != 1 {
		t.Fatal("table sync_test_user not created")
	}

	// Verify columns
	cols, _ := db.existingColumns("sync_test_user")
	expected := []string{"owner", "name", "email", "display_name", "age", "is_admin", "created_at", "data"}
	for _, col := range expected {
		if !cols[col] {
			t.Fatalf("missing column: %s", col)
		}
	}

	// Verify ignored field is NOT a column
	if cols["ignored"] {
		t.Fatal("ignored field should not be a column")
	}
}

func TestSync_Idempotent(t *testing.T) {
	db := testSyncDB(t)

	// Call Sync twice — should not error
	if err := db.Sync(syncTestUser{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(syncTestUser{}); err != nil {
		t.Fatal(err)
	}
}

func TestSync_AddMissingColumn(t *testing.T) {
	db := testSyncDB(t)

	// Create table with fewer columns
	db.NewQuery(`CREATE TABLE "sync_test_user" ("owner" TEXT, "name" TEXT, PRIMARY KEY ("owner", "name"))`).Execute()

	// Sync should add missing columns
	if err := db.Sync(syncTestUser{}); err != nil {
		t.Fatal(err)
	}

	cols, _ := db.existingColumns("sync_test_user")
	if !cols["email"] {
		t.Fatal("email column should have been added")
	}
	if !cols["display_name"] {
		t.Fatal("display_name column should have been added")
	}
}

func TestSync_MultipleStructs(t *testing.T) {
	db := testSyncDB(t)

	err := db.Sync(syncTestUser{}, syncTestApp{})
	if err != nil {
		t.Fatal(err)
	}

	var count int
	db.NewQuery("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('sync_test_user','sync_test_app')").Row(&count)
	if count != 2 {
		t.Fatalf("expected 2 tables, got %d", count)
	}
}

func TestSync_InsertAndQuery(t *testing.T) {
	db := testSyncDB(t)

	db.Sync(syncTestUser{})

	// Insert a row
	db.NewQuery(`INSERT INTO "sync_test_user" ("owner","name","email","display_name","age","is_admin") VALUES ('admin','alice','alice@test.com','Alice',30,1)`).Execute()

	// Query it back
	var email string
	db.NewQuery(`SELECT "email" FROM "sync_test_user" WHERE "owner"='admin' AND "name"='alice'`).Row(&email)
	if email != "alice@test.com" {
		t.Fatalf("expected alice@test.com, got %s", email)
	}
}

func TestSync_CompositePK(t *testing.T) {
	db := testSyncDB(t)
	db.Sync(syncTestUser{})

	// Insert with composite PK
	db.NewQuery(`INSERT INTO "sync_test_user" ("owner","name","email") VALUES ('org1','user1','a@b.com')`).Execute()
	db.NewQuery(`INSERT INTO "sync_test_user" ("owner","name","email") VALUES ('org1','user2','c@d.com')`).Execute()
	db.NewQuery(`INSERT INTO "sync_test_user" ("owner","name","email") VALUES ('org2','user1','e@f.com')`).Execute()

	// Should have 3 distinct rows
	var count int
	db.NewQuery(`SELECT COUNT(*) FROM "sync_test_user"`).Row(&count)
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}

	// Duplicate PK should fail
	_, err := db.NewQuery(`INSERT INTO "sync_test_user" ("owner","name","email") VALUES ('org1','user1','dup@test.com')`).Execute()
	if err == nil {
		t.Fatal("duplicate PK should fail")
	}
}

func TestSync_SnakeCaseConversion(t *testing.T) {
	type CamelCaseTest struct {
		FirstName string `db:"pk"`
		LastName  string
		EmailAddr string
	}

	db := testSyncDB(t)
	db.Sync(CamelCaseTest{})

	cols, _ := db.existingColumns("camel_case_test")
	if !cols["first_name"] {
		t.Fatal("expected first_name column")
	}
	if !cols["last_name"] {
		t.Fatal("expected last_name column")
	}
	if !cols["email_addr"] {
		t.Fatal("expected email_addr column")
	}
}
