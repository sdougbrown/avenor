// Package configfile reads structured configuration files (Team, Loop,
// Roster, and future Workflow/Controller definitions) and decodes them into
// caller-supplied Go values. JSON, YAML, and TOML are supported; the format
// is detected from the file extension.
//
// Non-JSON formats are normalized through a JSON intermediate so that
// DisallowUnknownFields semantics and json struct tags are applied uniformly
// across all formats. This keeps deferred or misspelled fields from silently
// becoming no-ops regardless of how the author chose to write the file.
package configfile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	gotoml "github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

// Format identifies the encoding of a config file.
type Format int

const (
	// FormatJSON is the default and requires no external dependencies.
	FormatJSON Format = iota
	// FormatYAML covers .yaml and .yml extensions.
	FormatYAML
	// FormatTOML covers .toml extensions.
	FormatTOML
)

// DetectFormat infers the config format from the file extension. Unknown
// extensions default to JSON so that existing callers that pass paths
// without an explicit extension continue to work.
func DetectFormat(path string) Format {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		return FormatYAML
	case ".toml":
		return FormatTOML
	default:
		return FormatJSON
	}
}

// Load reads a config file and decodes it into dst. The format is detected
// from the file extension: .json (default), .yaml/.yml, or .toml. Unknown
// fields are rejected in all formats. For non-JSON formats the content is
// first decoded into a generic value and re-encoded as JSON so that the
// strict json.Decoder path and json struct tags apply uniformly.
func Load(path string, dst any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	return Decode(path, data, dst)
}

// Decode decodes raw config bytes into dst. The path is used only for format
// detection. This is useful when the caller has already read the file (for
// example to apply a pre-processing step) or is working with embedded data.
func Decode(path string, data []byte, dst any) error {
	jsonBytes, err := normalizeToJSON(path, data)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode config %s: %w", path, err)
	}

	// Reject trailing data (multiple documents / values) consistently across
	// all formats.
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode config %s: multiple top-level values", path)
		}
		return fmt.Errorf("decode config %s: trailing data: %w", path, err)
	}
	return nil
}

// normalizeToJSON converts non-JSON config data into a JSON byte slice suitable
// for the strict json.Decoder path. JSON data is returned as-is so that line
// numbers and byte offsets in decode errors stay accurate.
func normalizeToJSON(path string, data []byte) ([]byte, error) {
	switch DetectFormat(path) {
	case FormatYAML:
		return yamlToJSON(path, data)
	case FormatTOML:
		return tomlToJSON(path, data)
	default:
		return data, nil
	}
}

func yamlToJSON(path string, data []byte) ([]byte, error) {
	// yaml.v3 supports multi-document streams; we reject anything after the
	// first document to match the single-value contract of the JSON path.
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var node any
	if err := dec.Decode(&node); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("decode config %s: empty YAML document", path)
		}
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("decode config %s: %w", path, err)
		}
		return nil, fmt.Errorf("decode config %s: multiple YAML documents", path)
	}
	out, err := json.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("normalize YAML config %s: %w", path, err)
	}
	return out, nil
}

func tomlToJSON(path string, data []byte) ([]byte, error) {
	var node any
	if err := gotoml.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	out, err := json.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("normalize TOML config %s: %w", path, err)
	}
	return out, nil
}
