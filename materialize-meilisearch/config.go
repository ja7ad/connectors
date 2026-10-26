package connector

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/estuary/connectors/go/common"
)

var featureFlagDefaults = map[string]common.FlagDefault{}

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type config struct {
	Endpoint   string `json:"endpoint" jsonschema:"title=Endpoint,description=URL of the Meilisearch instance. Must start with http:// or https://. For Meilisearch Cloud this is the project URL. For example https://ms-abc123.meilisearch.io" jsonschema_extras:"order=0,pattern=^(http://|https://).+$"`
	APIKey     string `json:"apiKey,omitempty" jsonschema:"title=API Key,description=API key used to authenticate with Meilisearch. The key must be allowed the indexes.*\\, documents.*\\, settings.*\\, tasks.get and version actions on the materialized indexes. Leave blank only for instances that run without a master key." jsonschema_extras:"secret=true,order=1"`
	HardDelete bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete). Deletions are skipped for indexes with delta updates when this is enabled.,default=false" jsonschema_extras:"order=2,nonsensitive=true"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

func (c config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("missing 'endpoint'")
	} else if !strings.HasPrefix(c.Endpoint, "http://") && !strings.HasPrefix(c.Endpoint, "https://") {
		return fmt.Errorf("endpoint '%s' is invalid: must start with either http:// or https://", c.Endpoint)
	} else if u, err := url.Parse(c.Endpoint); err != nil {
		return fmt.Errorf("endpoint '%s' is invalid: %w", c.Endpoint, err)
	} else if u.Host == "" {
		return fmt.Errorf("endpoint '%s' is invalid: missing host", c.Endpoint)
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return ""
}

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type resource struct {
	Index        string `json:"index" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates bool   `json:"delta_updates" jsonschema:"default=false" jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`
}

func (r resource) Validate() error {
	if r.Index == "" {
		return fmt.Errorf("missing Index")
	}

	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Index":
		return "Name of the Meilisearch index to store the materialization results. Characters other than letters, numbers, '-' and '_' are replaced with '_'."
	case "DeltaUpdates":
		return "Should updates to this index be done via delta updates. Default is false. Each delta update is stored as a separate document."
	default:
		return ""
	}
}

func (r resource) WithDefaults(cfg config) resource {
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	uid := normalizeIndexUID(r.Index)
	if len(uid) == 0 {
		return nil, false, fmt.Errorf("index name '%s' is invalid: must contain at least 1 character", r.Index)
	}

	return []string{uid}, r.DeltaUpdates, nil
}

// Index UIDs are limited to 400 bytes.
// See https://www.meilisearch.com/docs/learn/getting_started/indexes#index-uid
const maxIndexUIDBytes = 400

// normalizeIndexUID replaces every character that is not allowed in a
// Meilisearch index UID (anything other than ASCII alphanumerics, '-' and '_')
// with an underscore, and truncates the result to the maximum UID length. Index
// UIDs are case-sensitive, so the case is preserved.
func normalizeIndexUID(index string) string {
	var b strings.Builder
	for _, r := range index {
		if !isIdentifierRune(r) {
			r = '_'
		}
		if b.Len() >= maxIndexUIDBytes {
			break
		}
		b.WriteRune(r)
	}

	return b.String()
}

// isIdentifierRune reports whether r may appear in an index UID or a document
// ID.
func isIdentifierRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_'
}
