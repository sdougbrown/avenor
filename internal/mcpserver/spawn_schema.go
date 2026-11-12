package mcpserver

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// publishedSpawnInputSchema is schemas/spawn.input.json reduced to the fields
// this server publishes, with the one fact this server owns added: which of
// them it requires. Filtering matters: the file also describes dir and wait,
// which only the JS surfaces publish, and a schema that advertised them would
// accept a call the strict decoder then refuses. The required list is derived
// from spawnArgs, where "no omitempty" is exactly what the decoder will not
// default.
func publishedSpawnInputSchema() json.RawMessage {
	var source struct {
		Schema               string                     `json:"$schema"`
		ID                   string                     `json:"$id"`
		Title                string                     `json:"title"`
		Description          string                     `json:"description"`
		Type                 string                     `json:"type"`
		AdditionalProperties bool                       `json:"additionalProperties"`
		Properties           map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(spawnInputSchemaJSON), &source); err != nil {
		panic(fmt.Sprintf("spawn input schema: %v", err))
	}

	properties := make(map[string]json.RawMessage, len(source.Properties))
	for name, raw := range source.Properties {
		var meta struct {
			Surfaces []string `json:"x-avenor-surfaces"`
		}
		if err := json.Unmarshal(raw, &meta); err != nil {
			panic(fmt.Sprintf("spawn input schema: property %q: %v", name, err))
		}
		for _, surface := range meta.Surfaces {
			if surface == "go" {
				properties[name] = raw
				break
			}
		}
	}

	published := struct {
		Schema               string                     `json:"$schema"`
		ID                   string                     `json:"$id"`
		Title                string                     `json:"title"`
		Description          string                     `json:"description"`
		Type                 string                     `json:"type"`
		AdditionalProperties bool                       `json:"additionalProperties"`
		Required             []string                   `json:"required"`
		Properties           map[string]json.RawMessage `json:"properties"`
	}{
		Schema:               source.Schema,
		ID:                   source.ID,
		Title:                source.Title,
		Description:          source.Description,
		Type:                 source.Type,
		AdditionalProperties: source.AdditionalProperties,
		Required:             requiredSpawnFields(),
		Properties:           properties,
	}

	encoded, err := json.Marshal(published)
	if err != nil {
		panic(fmt.Sprintf("spawn input schema: %v", err))
	}
	return encoded
}

// requiredSpawnFields is the field names spawnArgs decodes without a default,
// which is the set the published schema marks required.
func requiredSpawnFields() []string {
	typ := reflect.TypeOf(spawnArgs{})
	required := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "" || name == "-" || strings.Contains(tag, "omitempty") {
			continue
		}
		required = append(required, name)
	}
	return required
}
