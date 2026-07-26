package agyv115

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

//go:embed parity-manifest.json
var parityManifestJSON []byte

type parityManifest struct {
	DescriptorSHA256   string          `json:"descriptor_sha256"`
	DescriptorSHA256es []string        `json:"descriptor_sha256es"`
	Messages           []messageParity `json:"messages"`
	Enums              []enumParity    `json:"enums"`
	Service            serviceParity   `json:"service"`
}
type messageParity struct {
	Local, Original string
	Fields          [][]string `json:"fields"`
}
type enumParity struct {
	Local, Original string
	Values          [][]any `json:"values"`
}
type serviceParity struct {
	Local, Original string
	Methods         [][]string `json:"methods"`
}

// CheckParityFile verifies this generated schema against a caller-supplied
// complete descriptor set. The private descriptor set is never retained here.
func CheckParityFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	manifest, err := loadParityManifest()
	if err != nil {
		return err
	}
	return checkParity(data, manifest)
}

func loadParityManifest() (parityManifest, error) {
	var manifest parityManifest
	return manifest, json.Unmarshal(parityManifestJSON, &manifest)
}

func checkParity(data []byte, manifest parityManifest) error {
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	allowed := append([]string{manifest.DescriptorSHA256}, manifest.DescriptorSHA256es...)
	for _, want := range allowed {
		if got == want {
			goto descriptorOK
		}
	}
	return fmt.Errorf("descriptor SHA-256 = %s, want supported agy 1.1.5 or 1.1.7 closure", got)

descriptorOK:
	set := new(descriptorpb.FileDescriptorSet)
	if err := proto.Unmarshal(data, set); err != nil {
		return fmt.Errorf("unmarshal descriptor set: %w", err)
	}
	originals := indexOriginals(set)
	types := map[protoreflect.FullName]string{}
	for _, mapping := range manifest.Messages {
		local, err := localMessage(mapping.Local)
		if err != nil {
			return err
		}
		if originals.messages[mapping.Original] == nil {
			return fmt.Errorf("mapped original message %q not found", mapping.Original)
		}
		types[local.FullName()] = mapping.Original
	}
	for _, mapping := range manifest.Enums {
		local, err := localEnum(mapping.Local)
		if err != nil {
			return err
		}
		if originals.enums[mapping.Original] == nil {
			return fmt.Errorf("mapped original enum %q not found", mapping.Original)
		}
		types[local.FullName()] = mapping.Original
	}
	for _, mapping := range manifest.Messages {
		local, _ := localMessage(mapping.Local)
		if err := checkMessage(local, originals.messages[mapping.Original], mapping, types); err != nil {
			return err
		}
	}
	for _, mapping := range manifest.Enums {
		local, _ := localEnum(mapping.Local)
		if err := checkEnum(local, originals.enums[mapping.Original], mapping); err != nil {
			return err
		}
	}
	if err := checkCoverage(manifest); err != nil {
		return err
	}
	return checkService(manifest.Service, originals, types)
}

type originalIndex struct {
	messages map[string]*descriptorpb.DescriptorProto
	enums    map[string]*descriptorpb.EnumDescriptorProto
	services map[string]*descriptorpb.ServiceDescriptorProto
}

func indexOriginals(set *descriptorpb.FileDescriptorSet) originalIndex {
	out := originalIndex{map[string]*descriptorpb.DescriptorProto{}, map[string]*descriptorpb.EnumDescriptorProto{}, map[string]*descriptorpb.ServiceDescriptorProto{}}
	for _, file := range set.File {
		var walk func([]*descriptorpb.DescriptorProto, string)
		walk = func(messages []*descriptorpb.DescriptorProto, parent string) {
			for _, message := range messages {
				name := joinName(parent, message.GetName())
				out.messages[name] = message
				for _, enum := range message.GetEnumType() {
					out.enums[joinName(name, enum.GetName())] = enum
				}
				walk(message.GetNestedType(), name)
			}
		}
		walk(file.GetMessageType(), file.GetPackage())
		for _, enum := range file.GetEnumType() {
			out.enums[joinName(file.GetPackage(), enum.GetName())] = enum
		}
		for _, service := range file.GetService() {
			out.services[joinName(file.GetPackage(), service.GetName())] = service
		}
	}
	return out
}
func joinName(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "." + name
}

func localMessage(name string) (protoreflect.MessageDescriptor, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, err
	}
	m, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("local %q is not a message", name)
	}
	return m, nil
}
func localEnum(name string) (protoreflect.EnumDescriptor, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, err
	}
	e, ok := d.(protoreflect.EnumDescriptor)
	if !ok {
		return nil, fmt.Errorf("local %q is not an enum", name)
	}
	return e, nil
}

func checkMessage(local protoreflect.MessageDescriptor, original *descriptorpb.DescriptorProto, mapping messageParity, types map[protoreflect.FullName]string) error {
	if len(mapping.Fields) != local.Fields().Len() {
		return fmt.Errorf("%s maps %d fields, has %d", mapping.Local, len(mapping.Fields), local.Fields().Len())
	}
	seen := map[protoreflect.Name]bool{}
	for _, pair := range mapping.Fields {
		if len(pair) != 2 {
			return fmt.Errorf("%s has invalid field mapping", mapping.Local)
		}
		lf := local.Fields().ByName(protoreflect.Name(pair[0]))
		if lf == nil || seen[lf.Name()] {
			return fmt.Errorf("%s maps local field %q incorrectly", mapping.Local, pair[0])
		}
		seen[lf.Name()] = true
		of := fieldByName(original.GetField(), pair[1])
		if of == nil {
			return fmt.Errorf("%s.%s maps missing original field %s", mapping.Local, pair[0], pair[1])
		}
		if err := checkField(lf, of, original, types); err != nil {
			return fmt.Errorf("%s.%s: %w", mapping.Local, pair[0], err)
		}
	}
	return nil
}
func fieldByName(fields []*descriptorpb.FieldDescriptorProto, name string) *descriptorpb.FieldDescriptorProto {
	for _, f := range fields {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func checkField(local protoreflect.FieldDescriptor, original *descriptorpb.FieldDescriptorProto, parent *descriptorpb.DescriptorProto, types map[protoreflect.FullName]string) error {
	if int32(local.Number()) != original.GetNumber() || descriptorpb.FieldDescriptorProto_Label(local.Cardinality()) != original.GetLabel() || descriptorpb.FieldDescriptorProto_Type(local.Kind()) != original.GetType() {
		return fmt.Errorf("number/type/cardinality mismatch")
	}
	localOneof := local.ContainingOneof()
	if (localOneof != nil) != (original.OneofIndex != nil) {
		return fmt.Errorf("oneof presence mismatch")
	}
	if localOneof != nil && localOneof.Name() != protoreflect.Name(parent.GetOneofDecl()[original.GetOneofIndex()].GetName()) {
		return fmt.Errorf("oneof name mismatch")
	}
	if local.IsMap() != originalIsMap(original, parent) {
		return fmt.Errorf("map shape mismatch")
	}
	if local.IsMap() {
		return checkMapField(local, original, parent, types)
	}
	return checkReferencedType(local, original.GetTypeName(), types)
}

func originalIsMap(field *descriptorpb.FieldDescriptorProto, parent *descriptorpb.DescriptorProto) bool {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED || field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		return false
	}
	name := field.GetTypeName()
	for _, nested := range parent.GetNestedType() {
		if "."+nested.GetName() == name || hasSuffix(name, "."+nested.GetName()) {
			return nested.GetOptions().GetMapEntry()
		}
	}
	return false
}
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func checkMapField(local protoreflect.FieldDescriptor, original *descriptorpb.FieldDescriptorProto, parent *descriptorpb.DescriptorProto, types map[protoreflect.FullName]string) error {
	var entry *descriptorpb.DescriptorProto
	for _, nested := range parent.GetNestedType() {
		if hasSuffix(original.GetTypeName(), "."+nested.GetName()) {
			entry = nested
			break
		}
	}
	if entry == nil {
		return fmt.Errorf("original map entry missing")
	}
	key := local.MapKey()
	value := local.MapValue()
	originalKey := fieldByName(entry.GetField(), "key")
	originalValue := fieldByName(entry.GetField(), "value")
	if originalKey == nil || originalValue == nil || descriptorpb.FieldDescriptorProto_Type(key.Kind()) != originalKey.GetType() || descriptorpb.FieldDescriptorProto_Type(value.Kind()) != originalValue.GetType() {
		return fmt.Errorf("map key/value type mismatch")
	}
	return checkReferencedType(value, originalValue.GetTypeName(), types)
}

func checkReferencedType(local protoreflect.FieldDescriptor, originalType string, types map[protoreflect.FullName]string) error {
	if local.Kind() != protoreflect.MessageKind && local.Kind() != protoreflect.EnumKind {
		return nil
	}
	var name protoreflect.FullName
	if local.Kind() == protoreflect.MessageKind {
		name = local.Message().FullName()
	} else {
		name = local.Enum().FullName()
	}
	if types[name] != trimDot(originalType) {
		return fmt.Errorf("referenced type = %q, want %q", types[name], trimDot(originalType))
	}
	return nil
}
func trimDot(name string) string { return string(name[1:]) }

func checkEnum(local protoreflect.EnumDescriptor, original *descriptorpb.EnumDescriptorProto, mapping enumParity) error {
	if len(mapping.Values) != local.Values().Len() {
		return fmt.Errorf("%s maps %d values, has %d", mapping.Local, len(mapping.Values), local.Values().Len())
	}
	for _, pair := range mapping.Values {
		if len(pair) != 2 {
			return fmt.Errorf("%s has invalid value mapping", mapping.Local)
		}
		name, ok := pair[0].(string)
		if !ok {
			return fmt.Errorf("%s has non-string enum value", mapping.Local)
		}
		number, ok := pair[1].(float64)
		if !ok {
			return fmt.Errorf("%s has non-number enum value", mapping.Local)
		}
		lv := local.Values().ByName(protoreflect.Name(name))
		ov := enumValueByName(original.GetValue(), name)
		if lv == nil || ov == nil || int32(lv.Number()) != ov.GetNumber() || int32(number) != ov.GetNumber() {
			return fmt.Errorf("%s.%s value mismatch", mapping.Local, name)
		}
	}
	return nil
}
func enumValueByName(values []*descriptorpb.EnumValueDescriptorProto, name string) *descriptorpb.EnumValueDescriptorProto {
	for _, v := range values {
		if v.GetName() == name {
			return v
		}
	}
	return nil
}

func checkCoverage(manifest parityManifest) error {
	messages, enums := map[protoreflect.FullName]bool{}, map[protoreflect.FullName]bool{}
	for _, m := range manifest.Messages {
		messages[protoreflect.FullName(m.Local)] = true
	}
	for _, e := range manifest.Enums {
		enums[protoreflect.FullName(e.Local)] = true
	}
	for _, file := range []protoreflect.FileDescriptor{File_internal_runtime_agy_interop_v115_agy_proto, File_internal_runtime_agy_interop_v115_snapshot_proto} {
		var unmapped string
		var walk func(protoreflect.MessageDescriptors)
		walk = func(list protoreflect.MessageDescriptors) {
			for i := 0; i < list.Len(); i++ {
				m := list.Get(i)
				if !m.IsMapEntry() && !messages[m.FullName()] && unmapped == "" {
					unmapped = string(m.FullName())
				}
				walk(m.Messages())
			}
		}
		walk(file.Messages())
		if unmapped != "" {
			return fmt.Errorf("unmapped local message %s", unmapped)
		}
		for i := 0; i < file.Enums().Len(); i++ {
			if !enums[file.Enums().Get(i).FullName()] {
				return fmt.Errorf("unmapped local enum %s", file.Enums().Get(i).FullName())
			}
		}
	}
	return nil
}

func checkService(mapping serviceParity, originals originalIndex, types map[protoreflect.FullName]string) error {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(mapping.Local))
	if err != nil {
		return err
	}
	local, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return fmt.Errorf("local %q is not a service", mapping.Local)
	}
	original := originals.services[mapping.Original]
	if original == nil {
		return fmt.Errorf("mapped original service %q not found", mapping.Original)
	}
	if len(mapping.Methods) != local.Methods().Len() {
		return fmt.Errorf("%s method coverage mismatch", mapping.Local)
	}
	seenLocal, seenOriginal := map[protoreflect.Name]bool{}, map[string]bool{}
	for _, pair := range mapping.Methods {
		if len(pair) != 2 || seenLocal[protoreflect.Name(pair[0])] || seenOriginal[pair[1]] {
			return fmt.Errorf("invalid or duplicate method mapping")
		}
		seenLocal[protoreflect.Name(pair[0])] = true
		seenOriginal[pair[1]] = true
		lm := local.Methods().ByName(protoreflect.Name(pair[0]))
		if lm == nil {
			return fmt.Errorf("local method %q missing", pair[0])
		}
		om := methodByName(original.GetMethod(), pair[1])
		if om == nil {
			return fmt.Errorf("original method %q missing", pair[1])
		}
		if lm.IsStreamingClient() != om.GetClientStreaming() || lm.IsStreamingServer() != om.GetServerStreaming() || types[lm.Input().FullName()] != trimDot(om.GetInputType()) || types[lm.Output().FullName()] != trimDot(om.GetOutputType()) {
			return fmt.Errorf("%s method mismatch", pair[0])
		}
	}
	return nil
}
func methodByName(methods []*descriptorpb.MethodDescriptorProto, name string) *descriptorpb.MethodDescriptorProto {
	for _, m := range methods {
		if m.GetName() == name {
			return m
		}
	}
	return nil
}
