package agy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

const rpcFixtureDir = "testdata/rpc/v1.1.5"

func readRPCFixture(t *testing.T, name string, dst any) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(rpcFixtureDir, name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	return data
}

func TestRPC115FixtureCorpus(t *testing.T) {
	var heartbeat struct {
		Request struct {
			Path        string `json:"path"`
			ContentType string `json:"content_type"`
			BodyHex     string `json:"body_hex"`
		} `json:"request"`
		Response struct {
			Status int `json:"status"`
			Frames []struct {
				Flag    string `json:"flag_hex"`
				Trailer string `json:"trailer"`
			} `json:"frames"`
		} `json:"response"`
	}
	readRPCFixture(t, "heartbeat-grpcweb.json", &heartbeat)
	if heartbeat.Request.Path != "/exa.language_server_pb.LanguageServerService/Heartbeat" || heartbeat.Request.ContentType != "application/grpc-web+proto" || heartbeat.Request.BodyHex != "0000000000" {
		t.Fatalf("heartbeat fixture does not describe the validated grpc-web protobuf request: %+v", heartbeat.Request)
	}
	if heartbeat.Response.Status != 200 || len(heartbeat.Response.Frames) != 2 || heartbeat.Response.Frames[0].Flag != "0x00" || heartbeat.Response.Frames[1].Flag != "0x80" || heartbeat.Response.Frames[1].Trailer != "grpc-status: 0\r\n" {
		t.Fatalf("heartbeat fixture does not describe the validated data/trailer framing: %+v", heartbeat.Response)
	}

	var unary struct {
		Service       string   `json:"service"`
		ContentType   string   `json:"content_type"`
		RequestFrame  string   `json:"request_frame"`
		ResponseFrame []string `json:"response_frames"`
		Methods       []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	readRPCFixture(t, "unary-shapes.json", &unary)
	if unary.Service != "exa.language_server_pb.LanguageServerService" || unary.ContentType != "application/grpc-web+proto" || unary.RequestFrame != "0x00 + uint32be length + protobuf" {
		t.Fatalf("unary fixture has unexpected protocol identity: %+v", unary)
	}
	wantMethods := map[string]bool{
		"Heartbeat": true, "GetConversationMetadata": true, "GetAvailableModels": true,
		"GetAllCascadeTrajectories": true, "GetCascadeTrajectorySteps": true, "StartCascade": true,
		"SendUserCascadeMessage": true, "HandleCascadeUserInteraction": true,
		"CancelCascadeSteps": true, "ForceStopCascadeTree": true,
	}
	if len(unary.Methods) != len(wantMethods) {
		t.Fatalf("method count = %d, want %d", len(unary.Methods), len(wantMethods))
	}
	for _, method := range unary.Methods {
		if !wantMethods[method.Name] {
			t.Fatalf("unexpected or duplicate method fixture %q", method.Name)
		}
		delete(wantMethods, method.Name)
	}
	if len(wantMethods) != 0 {
		t.Fatalf("missing method fixtures: %v", wantMethods)
	}

	var stream struct {
		FixtureKind string `json:"fixture_kind"`
		Service     string `json:"service"`
		Method      string `json:"method"`
		ContentType string `json:"content_type"`
		Request     struct {
			Fields []string `json:"fields"`
		} `json:"request"`
		Response struct {
			DataFrame    string   `json:"data_frame"`
			TrailerFrame string   `json:"trailer_frame"`
			Fields       []string `json:"fields"`
		} `json:"response"`
	}
	readRPCFixture(t, "stream-agent-state-updates-structural.json", &stream)
	if stream.FixtureKind != "redacted structural fixture, not a raw live capture" || stream.Service != languageServerService || stream.Method != "StreamAgentStateUpdates" || stream.ContentType != grpcWebContentType || len(stream.Request.Fields) != 1 || stream.Request.Fields[0] != "conversation_id=1:string" || stream.Response.DataFrame != "0x00 protobuf StreamAgentStateUpdatesResponse" || stream.Response.TrailerFrame != "0x80 ASCII grpc-status trailer" || len(stream.Response.Fields) != 4 {
		t.Fatalf("stream structural fixture does not retain the validated typed boundary: %+v", stream)
	}

	var rpcErrors struct {
		MalformedMetadata struct {
			HTTPStatus int    `json:"http_status"`
			GRPCStatus string `json:"grpc_status"`
			DataFrames int    `json:"data_frames"`
		} `json:"malformed_metadata"`
	}
	readRPCFixture(t, "errors.json", &rpcErrors)
	if rpcErrors.MalformedMetadata.HTTPStatus != 200 || rpcErrors.MalformedMetadata.GRPCStatus != "2" || rpcErrors.MalformedMetadata.DataFrames != 0 {
		t.Fatalf("error fixture must retain HTTP-200 gRPC failure semantics: %+v", rpcErrors.MalformedMetadata)
	}

	var listeners struct {
		Candidates []struct {
			Transport string `json:"transport"`
		} `json:"candidates"`
		Selection string `json:"selection"`
	}
	readRPCFixture(t, "listener-roles.json", &listeners)
	if len(listeners.Candidates) != 2 || listeners.Candidates[0].Transport != "tls" || listeners.Candidates[1].Transport != "plain_http" || listeners.Selection == "" {
		t.Fatalf("listener fixture must retain both validated transport roles: %+v", listeners)
	}
}

func TestRPC115SchemaProvenancePinsUnretainedCompleteClosure(t *testing.T) {
	var provenance struct {
		AgyVersion       string            `json:"agy_version"`
		EvidenceSHA256   map[string]string `json:"evidence_sha256"`
		DescriptorSHA256 map[string]string `json:"descriptor_sha256"`
		CompleteClosure  struct {
			RecoveredLocally                  bool   `json:"recovered_locally"`
			DescriptorCount                   int    `json:"descriptor_count"`
			SHA256                            string `json:"sha256"`
			ValidatedMissingImports           int    `json:"validated_missing_imports"`
			ValidatedUnresolvedTypeReferences int    `json:"validated_unresolved_type_references"`
			Retained                          bool   `json:"retained"`
		} `json:"complete_closure"`
	}
	readRPCFixture(t, "schema-provenance.json", &provenance)
	if provenance.AgyVersion != "1.1.5" || !provenance.CompleteClosure.RecoveredLocally || provenance.CompleteClosure.DescriptorCount != 102 || provenance.CompleteClosure.SHA256 != "7e3073643388963f21bf269c51fa30a13f37ba3a25b3ddd7b49edbbf91557a21" || provenance.CompleteClosure.ValidatedMissingImports != 0 || provenance.CompleteClosure.ValidatedUnresolvedTypeReferences != 0 || provenance.CompleteClosure.Retained {
		t.Fatalf("schema fixture must pin the locally recovered but unretained complete closure: %+v", provenance)
	}
	for name, sum := range provenance.EvidenceSHA256 {
		if _, err := hex.DecodeString(sum); err != nil || len(sum) != sha256.Size*2 {
			t.Fatalf("invalid evidence SHA-256 for %s: %q", name, sum)
		}
	}
	for name, sum := range provenance.DescriptorSHA256 {
		if _, err := hex.DecodeString(sum); err != nil || len(sum) != sha256.Size*2 {
			t.Fatalf("invalid descriptor SHA-256 for %s: %q", name, sum)
		}
	}
	if len(provenance.EvidenceSHA256) < 10 || len(provenance.DescriptorSHA256) != 6 {
		t.Fatalf("provenance corpus unexpectedly incomplete: %+v", provenance)
	}
}

func TestRPC115FixturesAreRedacted(t *testing.T) {
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`(?i)/(users|home)/`),
		regexp.MustCompile(`(?i)-----BEGIN (CERTIFICATE|.*PRIVATE KEY)-----`),
		regexp.MustCompile(`\b127\.0\.0\.1:[0-9]+\b`),
		regexp.MustCompile(`\[::1\]:[0-9]+`),
		regexp.MustCompile(`(?i)\b(pid|process id)\s*[:=]?\s*[0-9]+\b`),
	}
	entries, err := os.ReadDir(rpcFixtureDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(rpcFixtureDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, pattern := range forbidden {
			if pattern.Match(data) {
				t.Fatalf("fixture %s matches forbidden redaction pattern %q", entry.Name(), pattern)
			}
		}
	}
}
