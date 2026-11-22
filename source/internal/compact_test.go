package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/rowkey"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// uploadTestFile pushes a local Parquet file into the sink.
func uploadTestFile(ctx context.Context, sink *filesystem.Adapter, remote, local string) error {
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return sink.WriteFile(ctx, remote, f)
}

// readCompactedBase reads the newest base Parquet file for a table and indexes
// every row by the same rowkey identity used during compaction.
func readCompactedBase(ctx context.Context, sink *filesystem.Adapter, tablePath string) (map[string]writer.Row, error) {
	files, err := sink.ListFiles(ctx, filepath.Join(tablePath, "base")+"/")
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, os.ErrNotExist
	}
	rows, err := readParquetRows(ctx, sink, files[len(files)-1])
	if err != nil {
		return nil, err
	}

	// Pull types from the on-disk manifest so the test keys are built exactly
	// like production keys.
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return nil, err
	}
	var schema manifest.Schema
	for name, s := range m.Schemas {
		if m.Tables[name].Path == tablePath {
			schema = s
			break
		}
	}
	types := rowkey.ColumnTypes(schema.Columns)
	pks := func() []string {
		for _, t := range m.Tables {
			if t.Path == tablePath {
				return t.PrimaryKey
			}
		}
		return nil
	}()

	indexed := make(map[string]writer.Row, len(rows))
	for _, r := range rows {
		k, err := rowkey.Of(r, pks, types)
		if err != nil {
			return nil, err
		}
		indexed[k] = r
	}
	return indexed, nil
}

// rowKeyFor is a small test-only convenience wrapper around rowkey.Of.
func rowKeyFor(row writer.Row, pks []string, types map[string]string) (string, error) {
	return rowkey.Of(row, pks, types)
}

// TestCompact_CompositeTextPKWithSeparator proves the end-to-end fix through
// the real Parquet writer, filesystem sink and Compact pipeline. Two rows have
// composite text PKs whose values re-partition the old "|" join separator; a
// DELETE for one row must tombstone only that row, and an UPDATE must not
// overwrite the other.
func TestCompact_CompositeTextPKWithSeparator(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	sink, err := filesystem.New(root)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	const tableName = "public.pairs"
	tablePath := tableName
	cols := []manifest.Column{
		{Name: "k1", PgType: "text", ParquetType: "UTF8", Nullable: false},
		{Name: "k2", PgType: "text", ParquetType: "UTF8", Nullable: false},
		{Name: "note", PgType: "text", ParquetType: "UTF8", Nullable: true},
	}
	pks := []string{"k1", "k2"}

	// Two distinct composite keys that the bare "|" join would render
	// identically: both stringify to "a|b|c|".
	rowKeep := writer.Row{"k1": "a", "k2": "b|c", "note": "keep-me"}
	rowDelete := writer.Row{"k1": "a|b", "k2": "c", "note": "delete-me"}

	// Base snapshot (epoch 0).
	basePath := filepath.Join(root, tablePath, "base", "base_000000.parquet")
	if err := writer.WriteBaseParquet(basePath, []writer.Row{rowKeep, rowDelete}, cols); err != nil {
		t.Fatalf("write base: %v", err)
	}
	if err := uploadTestFile(ctx, sink, filepath.Join(tablePath, "base", "base_000000.parquet"), basePath); err != nil {
		t.Fatalf("upload base: %v", err)
	}

	// Epoch 1 delta: delete rowDelete, update rowKeep. Values arrive as
	// strings — exactly what the pgoutput decoder produces.
	deltaRows := []writer.Row{
		{"__op": "D", "k1": "a|b", "k2": "c", "note": "delete-me"},
		{"__op": "U", "k1": "a", "k2": "b|c", "note": "updated"},
	}
	deltaLocal := filepath.Join(root, "delta-1.parquet")
	if err := writer.WriteDeltaParquet(deltaLocal, deltaRows, cols, 1); err != nil {
		t.Fatalf("write delta: %v", err)
	}
	deltaRemote := filepath.Join(tablePath, "deltas", "epoch=000001.parquet")
	if err := uploadTestFile(ctx, sink, deltaRemote, deltaLocal); err != nil {
		t.Fatalf("upload delta: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			tableName: {
				Status:           "active",
				PrimaryKey:       pks,
				BaseEpoch:        0,
				LatestDeltaEpoch: 1,
				Path:             tablePath,
			},
		},
		Schemas: map[string]manifest.Schema{
			tableName: {Version: 1, Columns: cols},
		},
	}
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	if err := Compact(ctx, sink); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Read the compacted base back and index by the same identity function.
	out, err := readCompactedBase(ctx, sink, tablePath)
	if err != nil {
		t.Fatalf("read compacted base: %v", err)
	}

	types := map[string]string{"k1": "UTF8", "k2": "UTF8"}
	keepKey, err := rowKeyFor(rowKeep, pks, types)
	if err != nil {
		t.Fatal(err)
	}
	deleteKey, err := rowKeyFor(rowDelete, pks, types)
	if err != nil {
		t.Fatal(err)
	}

	kept, ok := out[keepKey]
	if !ok {
		t.Fatalf("row (%q,%q) missing after compaction — update/delete hit the wrong identity", rowKeep["k1"], rowKeep["k2"])
	}
	if got := kept["note"]; got != "updated" {
		t.Fatalf("kept row note = %v, want %q (update applied to wrong row)", got, "updated")
	}
	// A present-but-nil tombstone column (optional, definition level 0) must
	// not be mistaken for an actual deletion — mirror the nil-value check the
	// purge loop uses.
	if delAt := kept[btypes.ColDeletedAt]; delAt != nil {
		t.Fatalf("kept row was tombstoned: %v", kept)
	}

	deleted, ok := out[deleteKey]
	if !ok {
		t.Fatalf("deleted row missing entirely; expected a tombstone so purge TTL can manage it")
	}
	if delAt := deleted[btypes.ColDeletedAt]; delAt == nil {
		t.Fatalf("row (%q,%q) was not tombstoned — delete missed its target", rowDelete["k1"], rowDelete["k2"])
	}
}

// TestCompact_TypedPKBaseMatchesStringDelta covers the representation mismatch:
// a base file read back yields typed Go values (int64, bool) while delta rows
// carry CDC text strings. Both must resolve to one identity.
func TestCompact_TypedPKBaseMatchesStringDelta(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	sink, err := filesystem.New(root)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	const tableName = "public.mixed"
	tablePath := tableName
	cols := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "active", PgType: "boolean", ParquetType: "BOOLEAN", Nullable: false},
		{Name: "label", PgType: "text", ParquetType: "UTF8", Nullable: true},
	}
	pks := []string{"id", "active"}

	baseRows := []writer.Row{
		{"id": int64(7), "active": true, "label": "orig"},
		{"id": int64(7), "active": false, "label": "other"},
	}
	basePath := filepath.Join(root, tablePath, "base", "base_000000.parquet")
	if err := writer.WriteBaseParquet(basePath, baseRows, cols); err != nil {
		t.Fatalf("write base: %v", err)
	}
	if err := uploadTestFile(ctx, sink, filepath.Join(tablePath, "base", "base_000000.parquet"), basePath); err != nil {
		t.Fatalf("upload base: %v", err)
	}

	// Update arrives with text-encoded PK values from the decoder.
	deltaRows := []writer.Row{
		{"__op": "U", "id": "7", "active": "t", "label": "changed"},
	}
	deltaLocal := filepath.Join(root, "delta-1.parquet")
	if err := writer.WriteDeltaParquet(deltaLocal, deltaRows, cols, 1); err != nil {
		t.Fatalf("write delta: %v", err)
	}
	if err := uploadTestFile(ctx, sink, filepath.Join(tablePath, "deltas", "epoch=000001.parquet"), deltaLocal); err != nil {
		t.Fatalf("upload delta: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			tableName: {
				Status:           "active",
				PrimaryKey:       pks,
				BaseEpoch:        0,
				LatestDeltaEpoch: 1,
				Path:             tablePath,
			},
		},
		Schemas: map[string]manifest.Schema{tableName: {Version: 1, Columns: cols}},
	}
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatalf("manifest: %v", err)
	}

	if err := Compact(ctx, sink); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	out, err := readCompactedBase(ctx, sink, tablePath)
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	types := map[string]string{"id": "INT64", "active": "BOOLEAN"}

	targetKey, err := rowKeyFor(writer.Row{"id": int64(7), "active": true}, pks, types)
	if err != nil {
		t.Fatal(err)
	}
	target, ok := out[targetKey]
	if !ok {
		t.Fatalf("target row (7,true) not found after compaction")
	}
	if got := target["label"]; got != "changed" {
		t.Fatalf("label = %v, want %q — text PK in delta failed to match typed PK in base", got, "changed")
	}

	otherKey, err := rowKeyFor(writer.Row{"id": int64(7), "active": false}, pks, types)
	if err != nil {
		t.Fatal(err)
	}
	other, ok := out[otherKey]
	if !ok {
		t.Fatalf("non-target row (7,false) disappeared — update overmatched")
	}
	if got := other["label"]; got != "other" {
		t.Fatalf("non-target row label = %v, want %q — update hit the wrong boolean identity", got, "other")
	}
}
