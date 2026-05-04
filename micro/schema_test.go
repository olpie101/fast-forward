package micro_test

import (
	"encoding/json"
	"testing"

	"github.com/invopop/jsonschema"
	"github.com/olpie101/fast-forward/micro"
)

type schemaReq struct {
	A string
}

type schemaRes struct {
	B int
}

func TestGenerateJsonSchemaPointerInputs(t *testing.T) {
	s, err := micro.GenerateJsonSchema(jsonschema.Reflector{}, &schemaReq{}, &schemaRes{})
	if err != nil {
		t.Fatalf("GenerateJsonSchema err: %v", err)
	}
	assertTitle(t, s.Request, "schemaReq")
	assertTitle(t, s.Response, "schemaRes")
}

func TestGenerateJsonSchemaValueInputs(t *testing.T) {
	s, err := micro.GenerateJsonSchema(jsonschema.Reflector{}, schemaReq{}, schemaRes{})
	if err != nil {
		t.Fatalf("GenerateJsonSchema err: %v", err)
	}
	assertTitle(t, s.Request, "schemaReq")
	assertTitle(t, s.Response, "schemaRes")
}

func TestGenerateJsonSchemaAnonymousStructHasEmptyTitle(t *testing.T) {
	s, err := micro.GenerateJsonSchema(jsonschema.Reflector{}, struct{ A string }{}, struct{ B int }{})
	if err != nil {
		t.Fatalf("GenerateJsonSchema err: %v", err)
	}
	assertTitle(t, s.Request, "")
	assertTitle(t, s.Response, "")
}

func assertTitle(t *testing.T, raw, want string) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("schema not valid JSON: %v\n%s", err, raw)
	}
	got, _ := m["title"].(string)
	if got != want {
		t.Errorf("title: want %q, got %q (raw=%s)", want, got, raw)
	}
}
