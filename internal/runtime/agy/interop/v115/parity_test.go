package agyv115

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestParityCheckerSafeFixture(t *testing.T) {
	manifest, err := loadParityManifest()
	if err != nil {
		t.Fatal(err)
	}
	fixture := parityFixture(t, manifest)
	data, err := proto.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	manifest.DescriptorSHA256 = hex.EncodeToString(sum[:])
	if err := checkParity(data, manifest); err != nil {
		t.Fatal(err)
	}

	mutated := false
	for _, file := range fixture.File {
		if len(file.MessageType) > 0 && len(file.MessageType[0].Field) > 0 {
			file.MessageType[0].Field[0].Number = proto.Int32(99)
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("fixture has no field to mutate")
	}
	data, err = proto.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	sum = sha256.Sum256(data)
	manifest.DescriptorSHA256 = hex.EncodeToString(sum[:])
	if err := checkParity(data, manifest); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("number mutation error = %v, want mismatch", err)
	}

	fixture = parityFixture(t, manifest)
	fixture.File[len(fixture.File)-1].Service[0].Method[0].OutputType = proto.String(".missing.Output")
	data, err = proto.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	sum = sha256.Sum256(data)
	manifest.DescriptorSHA256 = hex.EncodeToString(sum[:])
	if err := checkParity(data, manifest); err == nil || !strings.Contains(err.Error(), "method mismatch") {
		t.Fatalf("method mutation error = %v, want method mismatch", err)
	}
}

func TestParityAgainstSuppliedClosure(t *testing.T) {
	path := os.Getenv("AGY_DESCRIPTOR_SET")
	if path == "" {
		t.Skip("set AGY_DESCRIPTOR_SET to run the private closure parity check")
	}
	if err := CheckParityFile(path); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedMessagesPreserveUnknownFields(t *testing.T) {
	wire := []byte{0x98, 0x06, 0x01} // field 99, varint 1: intentionally omitted locally.
	var message HeartbeatResponse
	if err := proto.Unmarshal(wire, &message); err != nil {
		t.Fatal(err)
	}
	got, err := proto.Marshal(&message)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(wire) {
		t.Fatalf("unknown field was not preserved: got %x, want %x", got, wire)
	}
}

func TestFieldShapesAndServicePaths(t *testing.T) {
	steps := (&StepsUpdate{}).ProtoReflect().Descriptor()
	if !steps.Fields().ByName("indices").IsList() || steps.Fields().ByName("indices").Kind() != protoreflect.Uint32Kind || !steps.Fields().ByName("steps").IsList() {
		t.Fatal("steps update repeated field shape changed")
	}
	models := (&FetchAvailableModelsResponse{}).ProtoReflect().Descriptor().Fields().ByName("models")
	if !models.IsMap() || models.MapKey().Kind() != protoreflect.StringKind || models.MapValue().Message().FullName() != "avenor.agy.v115.ModelDetails" {
		t.Fatal("models map shape changed")
	}
	step := (&Step{}).ProtoReflect().Descriptor()
	if step.Fields().ByName("planner_response").ContainingOneof() != step.Fields().ByName("list_directory").ContainingOneof() {
		t.Fatal("step alternatives must share a oneof")
	}
	if CascadeRunStatus_CASCADE_RUN_STATUS_IDLE.Number() != 1 || CortexStepStatus_CORTEX_STEP_STATUS_DONE.Number() != 3 || CortexTrajectorySource_CORTEX_TRAJECTORY_SOURCE_CLI.Number() != 17 {
		t.Fatal("validated enum values changed")
	}
	service := File_internal_runtime_agy_interop_v115_agy_proto.Services().ByName("LanguageServerService")
	if service == nil || service.Methods().Len() != 11 {
		t.Fatal("minimal service methods changed")
	}
	stream := service.Methods().ByName("StreamAgentStateUpdates")
	if stream == nil || !stream.IsStreamingServer() || stream.IsStreamingClient() || stream.Input().FullName() != "avenor.agy.v115.StreamAgentStateUpdatesRequest" || stream.Output().FullName() != "avenor.agy.v115.StreamAgentStateUpdatesResponse" {
		t.Fatal("stream method descriptor changed")
	}
}

func TestStage9FixtureCompatibility(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "rpc", "v1.1.5", "unary-shapes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	service := File_internal_runtime_agy_interop_v115_agy_proto.Services().ByName("LanguageServerService")
	for _, method := range fixture.Methods {
		if service.Methods().ByName(protoreflect.Name(method.Name)) == nil {
			t.Fatalf("fixture method %s absent from schema", method.Name)
		}
	}
}

func TestDeterministicGenerationClean(t *testing.T) {
	if os.Getenv("AGY_VERIFY_GENERATION") == "" {
		t.Skip("set AGY_VERIFY_GENERATION=1 to regenerate and verify the checked-in bindings")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(filepath.Join(root, "scripts", "generate-agy-interop-proto.sh"))
	command.Dir = root
	command.Env = os.Environ()
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("regenerate: %v\n%s", err, output)
	}
	check := exec.Command("git", "diff", "--exit-code", "--", "internal/runtime/agy/interop/v115/agy.pb.go", "internal/runtime/agy/interop/v115/snapshot.pb.go")
	check.Dir = root
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("generated bindings are not clean:\n%s", output)
	}
}

func parityFixture(t *testing.T, manifest parityManifest) *descriptorpb.FileDescriptorSet {
	t.Helper()
	types := map[string]string{}
	for _, m := range manifest.Messages {
		types[m.Local] = m.Original
	}
	for _, e := range manifest.Enums {
		types[e.Local] = e.Original
	}
	set := &descriptorpb.FileDescriptorSet{}
	for _, m := range manifest.Messages {
		local, err := localMessage(m.Local)
		if err != nil {
			t.Fatal(err)
		}
		file := protodesc.ToFileDescriptorProto(local.ParentFile())
		message := proto.Clone(findMessage(file.GetMessageType(), string(local.Name()))).(*descriptorpb.DescriptorProto)
		message.Name = proto.String(lastName(m.Original))
		rewriteFields(message, types)
		set.File = append(set.File, &descriptorpb.FileDescriptorProto{Name: proto.String("fixture-" + m.Local + ".proto"), Package: proto.String(parentName(m.Original)), MessageType: []*descriptorpb.DescriptorProto{message}, Syntax: proto.String("proto3")})
	}
	for _, e := range manifest.Enums {
		local, err := localEnum(e.Local)
		if err != nil {
			t.Fatal(err)
		}
		file := protodesc.ToFileDescriptorProto(local.ParentFile())
		enum := proto.Clone(findEnum(file.GetEnumType(), string(local.Name()))).(*descriptorpb.EnumDescriptorProto)
		enum.Name = proto.String(lastName(e.Original))
		set.File = append(set.File, &descriptorpb.FileDescriptorProto{Name: proto.String("fixture-" + e.Local + ".proto"), Package: proto.String(parentName(e.Original)), EnumType: []*descriptorpb.EnumDescriptorProto{enum}, Syntax: proto.String("proto3")})
	}
	serviceFile := &descriptorpb.FileDescriptorProto{Name: proto.String("fixture-service.proto"), Package: proto.String(parentName(manifest.Service.Original)), Syntax: proto.String("proto3")}
	serviceFile.Service = []*descriptorpb.ServiceDescriptorProto{{Name: proto.String(lastName(manifest.Service.Original))}}
	localService := File_internal_runtime_agy_interop_v115_agy_proto.Services().ByName("LanguageServerService")
	for _, pair := range manifest.Service.Methods {
		method := localService.Methods().ByName(protoreflect.Name(pair[0]))
		serviceFile.Service[0].Method = append(serviceFile.Service[0].Method, &descriptorpb.MethodDescriptorProto{Name: proto.String(pair[1]), InputType: proto.String("." + types[string(method.Input().FullName())]), OutputType: proto.String("." + types[string(method.Output().FullName())]), ServerStreaming: proto.Bool(method.IsStreamingServer()), ClientStreaming: proto.Bool(method.IsStreamingClient())})
	}
	set.File = append(set.File, serviceFile)
	return set
}
func findMessage(messages []*descriptorpb.DescriptorProto, name string) *descriptorpb.DescriptorProto {
	for _, m := range messages {
		if m.GetName() == name {
			return m
		}
	}
	return nil
}
func findEnum(enums []*descriptorpb.EnumDescriptorProto, name string) *descriptorpb.EnumDescriptorProto {
	for _, e := range enums {
		if e.GetName() == name {
			return e
		}
	}
	return nil
}
func rewriteFields(message *descriptorpb.DescriptorProto, types map[string]string) {
	for _, field := range message.Field {
		if target, ok := types[strings.TrimPrefix(field.GetTypeName(), ".")]; ok {
			field.TypeName = proto.String("." + target)
		}
	}
	for _, nested := range message.NestedType {
		rewriteFields(nested, types)
	}
}
func lastName(name string) string   { return name[strings.LastIndex(name, ".")+1:] }
func parentName(name string) string { return name[:strings.LastIndex(name, ".")] }
