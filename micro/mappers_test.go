package micro_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/olpie101/fast-forward/micro"
)

func TestUUIDMapperBasicField(t *testing.T) {
	type S struct {
		UserID uuid.UUID `jsonschema:"description=user id"`
	}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if len(out) != 1 {
		t.Fatalf("want 1 field, got %d", len(out))
	}
	if out[0].Name != "user_id" {
		t.Errorf("Name: want user_id, got %q", out[0].Name)
	}
	if out[0].Type != reflect.TypeOf("") {
		t.Errorf("Type: want string, got %v", out[0].Type)
	}
	tag := string(out[0].Tag)
	wantClause := `jsonschema:"description=user id,format=uuid"`
	if !strings.Contains(tag, wantClause) {
		t.Errorf("tag missing appended clause %q: %q", wantClause, tag)
	}
}

func TestUUIDMapperUntaggedField(t *testing.T) {
	type S struct {
		UUID uuid.UUID
	}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if len(out) != 1 {
		t.Fatalf("want 1 field, got %d", len(out))
	}
	tag := string(out[0].Tag)
	if !strings.Contains(tag, `jsonschema:",format=uuid"`) {
		t.Errorf("tag should contain `jsonschema:\",format=uuid\"`, got %q", tag)
	}
}

func TestUUIDMapperSkipsNonUUID(t *testing.T) {
	type S struct {
		ID   uuid.UUID
		Name string
	}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if len(out) != 1 {
		t.Fatalf("want 1 field, got %d", len(out))
	}
	if out[0].Name != "id" {
		t.Errorf("only UUID field should remain, got %q", out[0].Name)
	}
}

func TestUUIDMapperKebabReplacement(t *testing.T) {
	type S struct {
		// jsonschema.ToSnakeCase produces "tenant_id"; the dash-replacement
		// guard at mappers.go:20 only matters if ToSnakeCase emits a dash.
		Tenant_ID uuid.UUID
	}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if len(out) != 1 {
		t.Fatalf("want 1 field, got %d", len(out))
	}
	if strings.Contains(out[0].Name, "-") {
		t.Errorf("Name must contain no dashes, got %q", out[0].Name)
	}
}

type aliasUUID = uuid.UUID
type wrapUUID uuid.UUID

func TestUUIDMapperAliasIsMapped(t *testing.T) {
	type S struct {
		ID aliasUUID
	}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if len(out) != 1 {
		t.Fatalf("Go type alias is identical type; want 1 field, got %d", len(out))
	}
}

func TestUUIDMapperDefinedWrapperIsNotMapped(t *testing.T) {
	type S struct {
		ID wrapUUID
	}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if len(out) != 0 {
		t.Errorf("defined-type wrapper is a distinct type; want 0 fields, got %d", len(out))
	}
}

func TestUUIDMapperEmptyStructReturnsNil(t *testing.T) {
	type S struct{}
	out := micro.UUIDMapper(reflect.TypeOf(S{}))
	if out != nil {
		t.Errorf("want nil, got %v", out)
	}
}
