package connector

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeIndexUID(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "not normalized", input: "some_Collection-1", want: "some_Collection-1"},
		{name: "collection path", input: "acmeCo/anvils/orders", want: "acmeCo_anvils_orders"},
		{name: "disallowed characters", input: `we.ird*<"\ |>?:#index`, want: "we_ird__________index"},
		{name: "non-ascii", input: "中文", want: "__"},
		{name: "truncated", input: strings.Repeat("a", 500), want: strings.Repeat("a", maxIndexUIDBytes)},
		{name: "empty", input: "", want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeIndexUID(tt.input))
		})
	}
}

func TestTranslateField(t *testing.T) {
	require.Equal(t, "canary", translateField("canary"))
	require.Equal(t, "nested_field", translateField("nested.field"))
	require.Equal(t, "_meta/op", translateField("_meta/op"))
	require.Equal(t, "flow_id_flow", translateField("flow_id"))
	require.Equal(t, "_geo_flow", translateField("_geo"))
	require.Equal(t, "_vectors_flow", translateField("_vectors"))
}

func TestVersionAtLeast(t *testing.T) {
	min := [2]int{1, 14}
	require.True(t, versionAtLeast("1.14.0", min))
	require.True(t, versionAtLeast("1.55.0", min))
	require.True(t, versionAtLeast("2.0.0", min))
	require.True(t, versionAtLeast("v1.14.1", min))
	require.False(t, versionAtLeast("1.13.3", min))
	require.False(t, versionAtLeast("0.30.0", min))
	// Unparseable versions are assumed to be recent.
	require.True(t, versionAtLeast("nightly", min))
}

func TestSettingsUpdate(t *testing.T) {
	desired := desiredSettings{
		searchable: []string{"id", "canary"},
		filterable: []string{"canary"},
		sortable:   []string{"canary"},
	}

	t.Run("new index", func(t *testing.T) {
		update, added := settingsUpdate(indexSettings{}, desired)
		require.Equal(t, indexSettings{
			searchable: []string{"id", "canary"},
			filterable: []any{"canary"},
			sortable:   []string{"canary"},
		}, update)
		require.Equal(t, []string{
			`searchable attributes "id", "canary"`,
			`filterable attributes "canary"`,
			`sortable attributes "canary"`,
		}, added)
	})

	t.Run("up to date", func(t *testing.T) {
		update, added := settingsUpdate(indexSettings{
			searchable: []string{"canary", "id"},
			filterable: []any{"canary"},
			sortable:   []string{"canary"},
		}, desired)
		require.Equal(t, indexSettings{}, update)
		require.Empty(t, added)
	})

	t.Run("all attributes searchable", func(t *testing.T) {
		update, added := settingsUpdate(indexSettings{
			searchable: []string{"*"},
			filterable: []any{"canary"},
			sortable:   []string{"canary"},
		}, desired)
		require.Equal(t, indexSettings{}, update)
		require.Empty(t, added)
	})

	t.Run("keeps other settings", func(t *testing.T) {
		pattern := map[string]any{"attributePatterns": []any{"tags*"}}
		update, added := settingsUpdate(indexSettings{
			searchable: []string{"title"},
			filterable: []any{pattern, "price"},
			sortable:   []string{"price"},
		}, desired)
		require.Equal(t, indexSettings{
			searchable: []string{"title", "id", "canary"},
			filterable: []any{pattern, "price", "canary"},
			sortable:   []string{"price", "canary"},
		}, update)
		require.Len(t, added, 3)
	})
}
