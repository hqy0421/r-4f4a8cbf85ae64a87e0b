// Package rowkey builds collision-free, type-aware identities for rows keyed by
// a table's primary-key columns.
//
// The previous implementation joined stringified values with a bare "|":
//
//	fmt.Sprintf("%v|", row[pk])
//
// That collapses distinct composite keys whenever a text value itself contains
// the separator (e.g. ("a|b","c") and ("a","b|c") both render as "a|b|c|"),
// and it erases types (nil renders as "<nil>", bool as "true"/"false"), so an
// UPDATE or DELETE in a delta file could silently hit a different base row.
//
// The identity used here is a JSON array of typed components, one per PK
// column in manifest order:
//
//	[{"t":"UTF8","v":"a|b"},{"t":"UTF8","v":"c"}]
//
// JSON quoting makes every value self-delimiting — no separator can appear in
// the structure — and each component carries its canonical Parquet type tag,
// so values of different types never share an identity. The same extraction is
// used both when indexing the base snapshot and when applying historical
// deltas, which guarantees the two sides agree.
package rowkey

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"
)

// Row is the generic row container shared by the writer and compaction paths.
// It mirrors writer.Row so callers can pass either without conversion.
type Row = map[string]any

// component is one primary-key column inside a composite identity. The type
// tag is the canonical Parquet type (btypes.ParquetType output), making the
// identity type-distinguishable; the value is a canonical, deterministic
// rendering so equal logical values always produce equal components.
type component struct {
	Type string `json:"t"`
	Val  string `json:"v"`
}

// ColumnTypes builds the column-name → canonical Parquet type lookup used by
// Of from a manifest schema's column definitions.
func ColumnTypes(columns []manifest.Column) map[string]string {
	out := make(map[string]string, len(columns))
	for _, c := range columns {
		out[c.Name] = btypes.ParquetType(c.PgType)
	}
	return out
}

// Of builds the row identity for the given primary-key columns using the
// column-type lookup produced by ColumnTypes. Every PK column must have a
// schema entry.
//
// The same function must be used for base read-back and delta application so
// both sides of compaction resolve to the same identity.
func Of(row Row, pkCols []string, colTypes map[string]string) (string, error) {
	parts := make([]component, len(pkCols))
	for i, name := range pkCols {
		parquetType, ok := colTypes[name]
		if !ok {
			return "", fmt.Errorf("rowkey: primary key column %q missing from schema", name)
		}
		c, err := canonicalize(row[name], parquetType, name)
		if err != nil {
			return "", err
		}
		parts[i] = c
	}
	raw, err := json.Marshal(parts)
	if err != nil {
		return "", fmt.Errorf("rowkey: encode identity: %w", err)
	}
	return string(raw), nil
}

// canonicalize renders a single PK value under its canonical Parquet type.
// It accepts both the typed values produced when reading Parquet back
// (int64, bool, float64, []byte, …) and the string values emitted by the
// pgoutput decoder, so a value has one identity regardless of which path it
// arrived through.
func canonicalize(val any, parquetType, colName string) (component, error) {
	c := component{Type: parquetType}

	if val == nil {
		// Primary-key columns are NOT NULL in PostgreSQL; reaching here means
		// a malformed/missing column rather than real data. Encode it as an
		// explicit null component instead of letting it stringify to "<nil>",
		// which would collide with the literal text value "<nil>".
		c.Val = "\x00null"
		return c, nil
	}

	switch parquetType {
	case "INT32", "INT64":
		n, err := toInt64(val)
		if err != nil {
			return component{}, fmt.Errorf("rowkey: column %q: %w", colName, err)
		}
		c.Val = strconv.FormatInt(n, 10)
	case "BOOLEAN":
		b, err := toBool(val)
		if err != nil {
			return component{}, fmt.Errorf("rowkey: column %q: %w", colName, err)
		}
		c.Val = strconv.FormatBool(b)
	case "FLOAT", "DOUBLE", "DECIMAL":
		f, err := toFloat64(val)
		if err != nil {
			return component{}, fmt.Errorf("rowkey: column %q: %w", colName, err)
		}
		// -1 precision means the shortest representation that round-trips.
		c.Val = strconv.FormatFloat(f, 'g', -1, 64)
	case "BYTE_ARRAY":
		// bytea: compare the exact bytes. Hex is unambiguous and keeps the
		// JSON component human-checkable.
		b, err := toBytes(val)
		if err != nil {
			return component{}, fmt.Errorf("rowkey: column %q: %w", colName, err)
		}
		c.Val = "hex:" + hex.EncodeToString(b)
	default:
		// UTF8 and every string-backed logical type (UUID, JSON, temporal,
		// DATE, TIME_*, …) compare on their exact text representation.
		s, err := toText(val)
		if err != nil {
			return component{}, fmt.Errorf("rowkey: column %q: %w", colName, err)
		}
		c.Val = s
	}
	return c, nil
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case int:
		return int64(n), nil
	case float64:
		// Values read from a numeric/decimal Parquet column arrive as float64.
		// Reject non-integral floats for integer columns rather than silently
		// truncating and merging unrelated rows.
		if n != float64(int64(n)) {
			return 0, fmt.Errorf("non-integral value %v for integer primary key", n)
		}
		return int64(n), nil
	case float32:
		f := float64(n)
		if f != float64(int64(f)) {
			return 0, fmt.Errorf("non-integral value %v for integer primary key", n)
		}
		return int64(f), nil
	case json.Number:
		return n.Int64()
	case string:
		// pgoutput emits every column as text; the writer parses it, but a
		// value keyed before that coercion still arrives as a string.
		return strconv.ParseInt(n, 10, 64)
	case []byte:
		return strconv.ParseInt(string(n), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer value type %T", v)
	}
}

func toBool(v any) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		// Postgres text format for boolean is "t"/"f"; ParseBool covers those
		// plus "true"/"false"/"1"/"0".
		return strconv.ParseBool(b)
	default:
		return false, fmt.Errorf("unsupported boolean value type %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch f := v.(type) {
	case float64:
		return f, nil
	case float32:
		return float64(f), nil
	case int64:
		return float64(f), nil
	case int32:
		return float64(f), nil
	case int:
		return float64(f), nil
	case json.Number:
		return f.Float64()
	case string:
		return strconv.ParseFloat(f, 64)
	case []byte:
		return strconv.ParseFloat(string(f), 64)
	default:
		return 0, fmt.Errorf("unsupported numeric value type %T", v)
	}
}

func toBytes(v any) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case string:
		return []byte(b), nil
	default:
		return nil, fmt.Errorf("unsupported bytea value type %T", v)
	}
}

func toText(v any) (string, error) {
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		// A UTF8/string-backed column read back from Parquet as bytes.
		return string(s), nil
	default:
		return "", fmt.Errorf("unsupported text value type %T", v)
	}
}
