package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/meilisearch/meilisearch-go"
	"golang.org/x/sync/errgroup"
)

// Fetching documents by ID, which Load relies on, was added in Meilisearch
// v1.14.
var minimumVersion = [2]int{1, 14}

type driver struct{}

var _ boilerplate.Connector = &driver{}

// NewDriver builds the connector, and is its entry point: an importing
// caller is handed the assembled connector rather than its pieces.
func NewDriver() boilerplate.Connector { return new(driver) }

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Meilisearch Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Meilisearch Index", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-meilisearch", endpointSchema, resourceSchema)
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	// Index names are normalized, so different names may refer to the same
	// index.
	seen := make(map[string]string)
	for _, b := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		uid := normalizeIndexUID(res.Index)
		if other, ok := seen[uid]; ok {
			return nil, fmt.Errorf("index names %q and %q both refer to the index %q: each binding must use a different index", other, res.Index, uid)
		}
		seen[uid] = res.Index
	}

	return boilerplate.RunValidate(ctx, req, NewMaterializer)
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, NewMaterializer)
}

func (driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, NewMaterializer)
}

type materialization struct {
	cfg    config
	client *client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, property] = &materialization{}

func NewMaterializer(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, property], error) {
	client, err := cfg.toClient()
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &materialization{
		cfg:    cfg,
		client: client,
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		TranslateField:     translateField,
		ConcurrentApply:    true,
		NoCreateNamespaces: true,
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	indexes, err := d.client.listIndexes(ctx)
	if err != nil {
		return err
	}

	bound := make(map[string]bool)
	for _, p := range resourcePaths {
		bound[p[0]] = true
	}

	// Indexes have no schema, so the fields of an index are the attributes
	// that its documents have, or that its settings name. They are only
	// needed for the indexes of the materialization, which tells new
	// projections apart from existing ones.
	// PushField isn't safe for concurrent use.
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)
	for _, idx := range indexes {
		res := is.PushResource(idx.UID)
		if !bound[idx.UID] {
			continue
		}
		group.Go(func() error {
			attrs, err := d.client.existingAttributes(groupCtx, idx.UID)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, attr := range attrs {
				res.PushField(boilerplate.ExistingField{Name: attr, Nullable: true})
			}
			return nil
		})
	}

	return group.Wait()
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Unlike the health endpoint, listing indexes requires a valid API key.
	_, err := d.client.ms.ListIndexesWithContext(checkCtx, &meilisearch.IndexesQuery{Limit: 1})
	if err != nil {
		switch {
		case isUnreachable(err) || checkCtx.Err() != nil:
			errs.Err(fmt.Errorf("cannot connect to endpoint '%s': double check your configuration, and make sure Estuary's IP is allowed to connect to your instance: %w", d.cfg.Endpoint, err))
		case statusCode(err) == 401:
			errs.Err(fmt.Errorf("an API key is required to connect to this Meilisearch instance"))
		case statusCode(err) == 403:
			errs.Err(fmt.Errorf("invalid API key: the key was rejected or is not allowed to list indexes"))
		case statusCode(err) == 404:
			errs.Err(fmt.Errorf("could not connect to endpoint '%s': endpoint URL not found", d.cfg.Endpoint))
		default:
			errs.Err(fmt.Errorf("could not connect to endpoint '%s': %w", d.cfg.Endpoint, err))
		}
		return errs
	}

	// Every write is followed by reading the status of its task.
	if _, err := d.client.ms.GetTasksWithContext(checkCtx, &meilisearch.TasksQuery{Limit: 1}); err != nil {
		errs.Err(fmt.Errorf("getting tasks: make sure the API key is allowed the 'tasks.get' action: %w", err))
	}

	version, err := d.client.ms.VersionWithContext(checkCtx)
	if err != nil {
		errs.Err(fmt.Errorf("getting Meilisearch version: make sure the API key is allowed the 'version' action: %w", err))
	} else if !versionAtLeast(version.PkgVersion, minimumVersion) {
		errs.Err(fmt.Errorf("meilisearch version %s is not supported: version %d.%d or later is required", version.PkgVersion, minimumVersion[0], minimumVersion[1]))
	}

	return errs
}

// versionAtLeast reports whether a Meilisearch version such as "1.14.0" is at
// least the given major and minor version. Versions which can't be parsed are
// assumed to be recent enough.
func versionAtLeast(version string, min [2]int) bool {
	parts := strings.SplitN(strings.TrimPrefix(version, "v"), ".", 3)
	if len(parts) < 2 {
		return true
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return true
	}
	return major > min[0] || (major == min[0] && minor >= min[1])
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	_, isNumeric := m.AsFormattedNumeric(&p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (property, boilerplate.ElementConverter) {
	return propForProjection(p, fc), nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return "", nil
}

// desiredSettings are the index settings that a binding's field selection
// requires.
type desiredSettings struct {
	// Attributes to search. The root document is excluded since it repeats
	// the other attributes, and searching it would duplicate their effect on
	// ranking and the size of the index.
	searchable []string
	filterable []string
	sortable   []string
}

func desiredSettingsFor(projections []boilerplate.MappedProjection[property]) desiredSettings {
	var out desiredSettings
	for _, p := range projections {
		attr := translateField(p.Field)
		if !p.IsRootDocumentProjection() {
			out.searchable = append(out.searchable, attr)
		}
		if p.Mapped.Filterable {
			out.filterable = append(out.filterable, attr)
		}
		if p.Mapped.Sortable {
			out.sortable = append(out.sortable, attr)
		}
	}
	return out
}

// settingsUpdate computes the changes to make to the current settings of an
// index so that it has the desired settings. Attributes are only ever added,
// so that settings configured outside of the connector are kept. Only
// searchableAttributes set to the default of all attributes ("*") is left
// alone, since every attribute is then already searchable. The returned
// settings only have the kinds of settings that change set, and are empty if
// nothing changes.
func settingsUpdate(current indexSettings, desired desiredSettings) (update indexSettings, added []string) {
	if !slices.Equal(current.searchable, []string{"*"}) {
		if missing := missingAttributes(current.searchable, desired.searchable); len(missing) > 0 {
			update.searchable = append(slices.Clone(current.searchable), missing...)
			added = append(added, fmt.Sprintf("searchable attributes %s", quoteAll(missing)))
		}
	}

	var filterableNames []string
	for _, f := range current.filterable {
		// Filterable attributes configured with patterns are kept, but
		// aren't matched against the desired attributes.
		if s, ok := f.(string); ok {
			filterableNames = append(filterableNames, s)
		}
	}
	if missing := missingAttributes(filterableNames, desired.filterable); len(missing) > 0 {
		update.filterable = slices.Clone(current.filterable)
		if update.filterable == nil {
			update.filterable = []any{}
		}
		for _, f := range missing {
			update.filterable = append(update.filterable, f)
		}
		added = append(added, fmt.Sprintf("filterable attributes %s", quoteAll(missing)))
	}

	if missing := missingAttributes(current.sortable, desired.sortable); len(missing) > 0 {
		update.sortable = append(slices.Clone(current.sortable), missing...)
		added = append(added, fmt.Sprintf("sortable attributes %s", quoteAll(missing)))
	}

	return update, added
}

// missingAttributes returns the desired attributes which are not in current.
func missingAttributes(current, desired []string) []string {
	var out []string
	for _, d := range desired {
		if !slices.Contains(current, d) && !slices.Contains(out, d) {
			out = append(out, d)
		}
	}
	return out
}

func quoteAll(ss []string) string {
	quoted := make([]string, len(ss))
	for i, s := range ss {
		quoted[i] = strconv.Quote(s)
	}
	return strings.Join(quoted, ", ")
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, property]) (string, boilerplate.ActionApplyFn, error) {
	uid := res.ResourcePath[0]
	desired := desiredSettingsFor(res.SelectedProjections())

	// A new index searches all attributes by default, which is replaced by
	// the selected attributes.
	_, added := settingsUpdate(indexSettings{}, desired)

	desc := fmt.Sprintf("create index %q with primary key %q", uid, primaryKeyAttribute)
	if len(added) > 0 {
		desc += " and " + strings.Join(added, " and ")
	}

	return desc, func(ctx context.Context) error {
		if err := d.client.createIndex(ctx, uid); err != nil {
			return err
		}

		// The index may have been created by someone else in the meantime,
		// so the settings are merged with those it actually has.
		current, err := d.client.getSettings(ctx, uid)
		if err != nil {
			return err
		}
		if slices.Equal(current.searchable, []string{"*"}) {
			current.searchable = nil
		}
		update, _ := settingsUpdate(current, desired)
		return d.client.updateSettings(ctx, uid, update)
	}, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	uid := resourcePath[0]

	return fmt.Sprintf("delete index %q", uid), func(ctx context.Context) error {
		return d.client.deleteIndex(ctx, uid)
	}, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	uid := path[0]

	// Deleting all documents keeps the settings of the index, unlike
	// re-creating it.
	return fmt.Sprintf("delete all documents from index %q", uid), func(ctx context.Context) error {
		return d.client.deleteAllDocuments(ctx, uid)
	}, nil
}

func (d *materialization) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, property],
) (string, boilerplate.ActionApplyFn, error) {
	uid := resourcePath[0]

	current, err := d.client.getSettings(ctx, uid)
	if err != nil {
		return "", nil, err
	}

	// Attributes added to the searchable attributes are those of new
	// projections only, so that attributes which a user removed from them
	// aren't added back. Filterable and sortable attributes follow the field
	// configuration of every selected field, so that enabling them for an
	// existing field takes effect.
	desired := desiredSettingsFor(update.Binding.SelectedProjections())
	desired.searchable = desiredSettingsFor(update.NewProjections).searchable

	changes, added := settingsUpdate(current, desired)
	if len(added) == 0 {
		return "", nil, nil
	}

	return fmt.Sprintf("add %s to index %q", strings.Join(added, " and "), uid), func(ctx context.Context) error {
		return d.client.updateSettings(ctx, uid, changes)
	}, nil
}

func (d *materialization) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, property],
	be *m.BindingEvents,
) (m.Transactor, error) {
	var bindings []binding
	for _, b := range mappedBindings {
		var fields []string
		for _, p := range append(slices.Clone(b.Keys), b.Values...) {
			fields = append(fields, translateField(p.Field))
		}

		var docField string
		if b.Document != nil {
			docField = translateField(b.Document.Field)
		}

		// The runtime selects at most one of the fields with the same folded
		// name, so this should never happen, but two fields with the same
		// attribute would silently overwrite each other.
		seen := map[string]bool{primaryKeyAttribute: true}
		for _, f := range append(slices.Clone(fields), docField) {
			if f == "" {
				continue
			} else if seen[f] {
				return nil, fmt.Errorf("binding for index %q has more than one field for the attribute %q", b.ResourcePath[0], f)
			}
			seen[f] = true
		}

		bindings = append(bindings, newBinding(b.ResourcePath, b.DeltaUpdates, fields, docField))
	}

	return &transactor{
		cfg:      d.cfg,
		client:   d.client,
		bindings: bindings,
		be:       be,
	}, nil
}

func (d *materialization) ListTestTasks(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (d *materialization) CleanupTestTask(ctx context.Context, taskName string) error {
	return nil
}

func (d *materialization) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	uid := path[0]
	idx := d.client.ms.Index(uid)

	var docs []map[string]any
	const pageSize = 1000
	for offset := int64(0); ; offset += pageSize {
		var res meilisearch.DocumentsResult
		if err := idx.GetDocumentsWithContext(ctx, &meilisearch.DocumentsQuery{Offset: offset, Limit: pageSize}, &res); err != nil {
			return nil, nil, fmt.Errorf("getting documents from index %q: %w", uid, err)
		}
		for _, hit := range res.Results {
			doc := make(map[string]any, len(hit))
			for k, v := range hit {
				// Numbers are decoded exactly, rather than as float64, so
				// that the snapshot shows the stored value.
				dec := json.NewDecoder(bytes.NewReader(v))
				dec.UseNumber()
				var val any
				if err := dec.Decode(&val); err != nil {
					return nil, nil, fmt.Errorf("decoding attribute %q: %w", k, err)
				}
				doc[k] = val
			}
			docs = append(docs, doc)
		}
		if len(res.Results) < pageSize {
			break
		}
	}

	if len(docs) == 0 {
		return nil, nil, nil
	}

	colSet := make(map[string]struct{})
	for _, doc := range docs {
		for k := range doc {
			colSet[k] = struct{}{}
		}
	}
	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	var rows [][]any
	for _, doc := range docs {
		row := make([]any, len(columns))
		for i, col := range columns {
			v, ok := doc[col]
			if !ok {
				continue
			}
			// IDs of delta updates documents without the root document are
			// random, so they would differ between test runs.
			if s, isString := v.(string); col == primaryKeyAttribute && isString && isUUID(s) {
				v = "<uuid>"
			}
			row[i] = v
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		for c := range columns {
			vi, _ := json.Marshal(rows[i][c])
			vj, _ := json.Marshal(rows[j][c])
			if string(vi) != string(vj) {
				return string(vi) < string(vj)
			}
		}
		return false
	})

	return columns, rows, nil
}

func isUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil && len(s) == 36
}

func (d *materialization) Close(ctx context.Context) {
	if d.client != nil {
		d.client.close()
	}
}
