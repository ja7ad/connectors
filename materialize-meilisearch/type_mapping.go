package connector

import (
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

// fieldConfig is the per-field configuration that users can set on a binding's
// field selection. It controls the index settings that make a field usable in
// Meilisearch filters and sorts.
type fieldConfig struct {
	Filterable bool `json:"filterable,omitempty"`
	Sortable   bool `json:"sortable,omitempty"`
}

func (fc fieldConfig) Validate() error {
	return nil
}

func (fc fieldConfig) CastToString() bool {
	return false
}

// property describes a materialized field. Meilisearch is schemaless and stores
// any JSON value for any attribute, so there is no type to reconcile with an
// existing index: every field is compatible with every existing attribute.
type property struct {
	// Type is the JSON type of the field, informational only.
	Type       string
	Filterable bool
	Sortable   bool
}

func (p property) String() string {
	return p.Type
}

func (p property) Compatible(existing boilerplate.ExistingField) bool {
	return true
}

func (p property) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}

func propForProjection(p boilerplate.Projection, fc fieldConfig) property {
	types := slices.DeleteFunc(slices.Clone(p.Inference.Types), func(t string) bool { return t == "null" })
	typ := "json"
	if len(types) == 1 {
		typ = types[0]
	}

	return property{
		Type:       typ,
		Filterable: fc.Filterable,
		Sortable:   fc.Sortable,
	}
}

const (
	// primaryKeyAttribute is the attribute holding each document's ID, which
	// is set as the primary key of indexes created by the connector. It is
	// derived from the collection key so that a document can be found by its
	// key.
	primaryKeyAttribute = "flow_id"

	reservedFieldSuffix = "_flow"
)

// Meilisearch gives special meaning to these attributes, and rejects documents
// where they don't have the expected shape. A field that happens to have one
// of these names is renamed so that it is stored as a regular attribute.
//
// See https://www.meilisearch.com/docs/learn/resources/known_limitations
var reservedFields = []string{
	primaryKeyAttribute,
	"_geo",
	"_geojson",
	"_vectors",
	"_formatted",
	"_matchesPosition",
	"_rankingScore",
	"_rankingScoreDetails",
	"_federation",
}

// translateField returns the Meilisearch attribute name for a Flow field.
func translateField(f string) string {
	// Meilisearch interprets dots in attribute names as paths into nested
	// objects when filtering, sorting and configuring searchable attributes.
	// This isn't what we want, so dots are substituted with underscores.
	f = strings.ReplaceAll(f, ".", "_")

	if slices.Contains(reservedFields, f) {
		f = f + reservedFieldSuffix
	}

	return f
}
