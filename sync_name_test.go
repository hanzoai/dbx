// Copyright 2026 Hanzo AI Inc. All Rights Reserved.

package dbx

import "testing"

// A table's columns and the statements that read them get their names from the
// same Go field, so the two derivations have to be one derivation. They were two,
// and they agreed everywhere except after a digit.
func TestSyncNamesColumnsTheWayQueriesDo(t *testing.T) {
	for _, field := range []string{
		"Fallback1Up", "Fallback2Up", "Owner", "ModelName", "CreatedTime",
		"CostInPerMillion", "ID", "URLPath", "Line1Address", "X2Y",
	} {
		if got, want := defaultFieldName(field), DefaultFieldMapFunc(field); got != want {
			t.Errorf("%s: Sync would create %q, statements would name %q", field, got, want)
		}
	}
}
