package connector

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpecification(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(index string, delta bool) resource {
		return resource{Index: index, DeltaUpdates: delta}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn)
	})

	// Meilisearch indexes are schemaless, so there are no column types to
	// migrate and the migration test is not applicable.
}
