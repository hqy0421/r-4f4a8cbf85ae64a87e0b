package rowkey

import (
	"encoding/json"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
)

func mustTypes(t *testing.T, cols []manifest.Column) map[string]string {
	t.Helper()
	types := ColumnTypes(cols)
	if len(types) != len(cols) {
		t.Fatalf("ColumnTypes dropped entries: got %v for %d columns", types, len(cols))
	}
	return types
}

// TestOf_SeparatorInTextDoesNotCollide is the headline regression: two text
// PK columns whose values contain the old "|" separator must keep distinct
// identities. The previous bare-join rendered both pairs as "a|b|c|".
func TestOf_SeparatorInTextDoesNotCollide(t *testing.T) {
	cols := []manifest.Column{
		{Name: "k1", PgType: "text"},
		{Name: "k2", PgType: "text"},
	}
	types := mustTypes(t, cols)
	pks := []string{"k1", "k2"}

	rowA := Row{"k1": "a|b", "k2": "c"}
	rowB := Row{"k1": "a", "k2": "b|c"}

	keyA, err := Of(rowA, pks, types)
	if err != nil {
		t.Fatalf("Of rowA: %v", err)
	}
	keyB, err := Of(rowB, pks, types)
	if err != nil {
		t.Fatalf("Of rowB: %v", err)
	}

	if keyA == keyB {
		t.Fatalf("distinct composite keys collapsed:\n  A=%s\n  B=%s", keyA, keyB)
	}

	// The key must remain self-describing: decode it back and check the
	// original values survive verbatim.
	var comps []component
	if err := json.Unmarshal([]byte(keyA), &comps); err != nil {
		t.Fatalf("identity is not inspectable JSON: %v (%s)", err, keyA)
	}
	if len(comps) != 2 {
		t.Fatalf("got %d components, want 2", len(comps))
	}
	if comps[0].Type != "UTF8" || comps[0].Val != "a|b" || comps[1].Val != "c" {
		t.Fatalf("components did not round-trip: %+v", comps)
	}
}

// TestOf_OrderSensitive ensures column order is part of the identity.
func TestOf_OrderSensitive(t *testing.T) {
	cols := []manifest.Column{
		{Name: "k1", PgType: "text"},
		{Name: "k2", PgType: "text"},
	}
	types := mustTypes(t, cols)

	keyAB, _ := Of(Row{"k1": "x", "k2": "y"}, []string{"k1", "k2"}, types)
	keyBA, _ := Of(Row{"k1": "y", "k2": "x"}, []string{"k1", "k2"}, types)
	if keyAB == keyBA {
		t.Fatalf("swapped column values produced the same identity: %s", keyAB)
	}
}

// TestOf_SameIdentityAcrossRepresentations verifies that a value read back
// from Parquet (typed) and one arriving from the CDC decoder (string) resolve
// to the same identity — this is what lets base read-back and delta
// application match.
func TestOf_SameIdentityAcrossRepresentations(t *testing.T) {
	cols := []manifest.Column{
		{Name: "id", PgType: "bigint"},
		{Name: "active", PgType: "boolean"},
		{Name: "name", PgType: "text"},
	}
	types := mustTypes(t, cols)
	pks := []string{"id", "active", "name"}

	// As read back from a Parquet base/delta file.
	typed := Row{"id": int64(42), "active": true, "name": "n"}
	// As emitted by the pgoutput decoder (everything is text).
	textual := Row{"id": "42", "active": "t", "name": "n"}
	// Numeric PK also surfaces as int32/int/float64 on some read paths.
	altNumeric := Row{"id": float64(42), "active": bool(true), "name": []byte("n")}

	k1, err := Of(typed, pks, types)
	if err != nil {
		t.Fatalf("typed: %v", err)
	}
	k2, err := Of(textual, pks, types)
	if err != nil {
		t.Fatalf("textual: %v", err)
	}
	k3, err := Of(altNumeric, pks, types)
	if err != nil {
		t.Fatalf("altNumeric: %v", err)
	}

	if k1 != k2 || k2 != k3 {
		t.Fatalf("same logical PK produced different identities:\n  typed=%s\n  text=%s\n  alt =%s", k1, k2, k3)
	}
}

// TestOf_TypeTagsDistinguishEqualRenderings ensures values that stringify the
// same way but have different column types never share an identity.
func TestOf_TypeTagsDistinguishEqualRenderings(t *testing.T) {
	textCol := mustTypes(t, []manifest.Column{{Name: "k", PgType: "text"}})
	boolCol := mustTypes(t, []manifest.Column{{Name: "k", PgType: "boolean"}})
	intCol := mustTypes(t, []manifest.Column{{Name: "k", PgType: "bigint"}})

	textTrue, _ := Of(Row{"k": "true"}, []string{"k"}, textCol)
	boolTrue, _ := Of(Row{"k": true}, []string{"k"}, boolCol)
	intOne, _ := Of(Row{"k": int64(1)}, []string{"k"}, intCol)
	textOne, _ := Of(Row{"k": "1"}, []string{"k"}, textCol)

	identities := map[string]int{}
	for _, k := range []string{textTrue, boolTrue, intOne, textOne} {
		identities[k]++
	}
	if len(identities) != 4 {
		t.Fatalf("type-erased collision: identities=%v", identities)
	}
}

// TestOf_NullDoesNotCollideWithLiteralText covers the "<nil>" rendering bug:
// a missing value must not match the literal text "<nil>".
func TestOf_NullDoesNotCollideWithLiteralText(t *testing.T) {
	types := mustTypes(t, []manifest.Column{{Name: "k", PgType: "text"}})

	nullKey, err := Of(Row{"k": nil}, []string{"k"}, types)
	if err != nil {
		t.Fatalf("null: %v", err)
	}
	literalKey, err := Of(Row{"k": "<nil>"}, []string{"k"}, types)
	if err != nil {
		t.Fatalf("literal: %v", err)
	}
	if nullKey == literalKey {
		t.Fatalf("null PK collided with literal text \"<nil>\": %s", nullKey)
	}
}

// TestOf_ByteaUsesExactBytes checks binary PK components compare on bytes and
// stay distinct for different content with a type tag of BYTE_ARRAY.
func TestOf_ByteaUsesExactBytes(t *testing.T) {
	types := mustTypes(t, []manifest.Column{{Name: "k", PgType: "bytea"}})

	k1, err := Of(Row{"k": []byte{0x00, 0xff, 0x7c}}, []string{"k"}, types)
	if err != nil {
		t.Fatalf("bytea: %v", err)
	}
	k2, _ := Of(Row{"k": []byte{0x00, 0xff, 0x7d}}, []string{"k"}, types)
	if k1 == k2 {
		t.Fatalf("distinct bytea PKs collided: %s", k1)
	}

	// String and []byte representations of the same bytes must agree.
	k3, err := Of(Row{"k": string([]byte{0x00, 0xff, 0x7c})}, []string{"k"}, types)
	if err != nil {
		t.Fatalf("bytea string form: %v", err)
	}
	if k1 != k3 {
		t.Fatalf("bytea representations differ:\n  []byte=%s\n  string=%s", k1, k3)
	}

	var comps []component
	if err := json.Unmarshal([]byte(k1), &comps); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if comps[0].Type != "BYTE_ARRAY" || comps[0].Val != "hex:00ff7c" {
		t.Fatalf("unexpected bytea component: %+v", comps[0])
	}
}

// TestOf_DeterministicAndStable checks repeated calls and equal rows agree.
func TestOf_DeterministicAndStable(t *testing.T) {
	cols := []manifest.Column{
		{Name: "a", PgType: "integer"},
		{Name: "b", PgType: "uuid"},
	}
	types := mustTypes(t, cols)
	pks := []string{"a", "b"}
	row := Row{"a": int32(7), "b": "00000000-0000-0000-0000-000000000001"}

	first, err := Of(row, pks, types)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := Of(Row{"a": "7", "b": "00000000-0000-0000-0000-000000000001"}, pks, types)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first != second {
		t.Fatalf("identity not stable across calls/representations:\n  %s\n  %s", first, second)
	}
}

func TestOf_MissingSchemaColumnErrors(t *testing.T) {
	types := map[string]string{} // no entry for the PK
	if _, err := Of(Row{"k": "v"}, []string{"k"}, types); err == nil {
		t.Fatal("expected error for PK column missing from schema")
	}
}

func TestOf_NonIntegralFloatIntoIntegerColumnErrors(t *testing.T) {
	types := mustTypes(t, []manifest.Column{{Name: "k", PgType: "bigint"}})
	if _, err := Of(Row{"k": float64(1.5)}, []string{"k"}, types); err == nil {
		t.Fatal("expected error for non-integral float in integer PK column")
	}
}

// TestOf_DeleteTargetScenario models the exact reported failure: a DELETE for
// one composite key must not match a different row whose text values merely
// re-partition the separator characters.
func TestOf_DeleteTargetScenario(t *testing.T) {
	cols := []manifest.Column{
		{Name: "tenant", PgType: "text"},
		{Name: "code", PgType: "text"},
	}
	types := mustTypes(t, cols)
	pks := []string{"tenant", "code"}

	baseRow := Row{"tenant": "a", "code": "b|c"}
	deleteEvent := Row{"tenant": "a|b", "code": "c"}

	baseKey, _ := Of(baseRow, pks, types)
	delKey, _ := Of(deleteEvent, pks, types)

	if baseKey == delKey {
		t.Fatalf("delete for (%q,%q) would hit base row (%q,%q): key=%s",
			deleteEvent["tenant"], deleteEvent["code"], baseRow["tenant"], baseRow["code"], delKey)
	}
}
