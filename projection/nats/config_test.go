package nats_test

import (
	"encoding/json"
	"reflect"
	"testing"

	pnats "github.com/olpie101/fast-forward/projection/nats"
)

func fullConfig() pnats.ProjectionConfig {
	return pnats.ProjectionConfig{
		Name:       "proj",
		Enabled:    true,
		Durable:    true,
		QueueGroup: "qg",
		KVName:     "kv",
		KVNames:    map[string]string{"a": "x", "b": "y"},
		NoKV:       true,
	}
}

func TestProjectionConfigJSONRoundTripFull(t *testing.T) {
	want := fullConfig()
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pnats.ProjectionConfig
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestProjectionConfigJSONRoundTripSparse(t *testing.T) {
	want := pnats.ProjectionConfig{Name: "n", KVName: "kv"}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pnats.ProjectionConfig
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestProjectionConfigJSONZeroValueOmitempty(t *testing.T) {
	b, err := json.Marshal(pnats.ProjectionConfig{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != "{}" {
		t.Errorf("zero-value marshal: got %s want {}", string(b))
	}
}

func TestProjectionConfigJSONOmitemptyPerField(t *testing.T) {
	cases := []struct {
		field      string
		mutate     func(*pnats.ProjectionConfig) // sets the field to its zero
		jsonKey    string
	}{
		{"Name", func(c *pnats.ProjectionConfig) { c.Name = "" }, "name"},
		{"Enabled", func(c *pnats.ProjectionConfig) { c.Enabled = false }, "enabled"},
		{"Durable", func(c *pnats.ProjectionConfig) { c.Durable = false }, "durable"},
		{"QueueGroup", func(c *pnats.ProjectionConfig) { c.QueueGroup = "" }, "queue_group"},
		{"KVName", func(c *pnats.ProjectionConfig) { c.KVName = "" }, "kv_name"},
		{"KVNames", func(c *pnats.ProjectionConfig) { c.KVNames = nil }, "kv_names"},
		{"NoKV", func(c *pnats.ProjectionConfig) { c.NoKV = false }, "no_kv"},
	}
	allKeys := []string{"name", "enabled", "durable", "queue_group", "kv_name", "kv_names", "no_kv"}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			c := fullConfig()
			tc.mutate(&c)
			b, err := json.Marshal(c)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatalf("unmarshal map: %v", err)
			}
			if _, present := m[tc.jsonKey]; present {
				t.Errorf("expected %q absent (zero-value omitempty), got %s", tc.jsonKey, string(b))
			}
			for _, k := range allKeys {
				if k == tc.jsonKey {
					continue
				}
				if _, present := m[k]; !present {
					t.Errorf("expected peer %q present, got %s", k, string(b))
				}
			}
		})
	}
}

func TestProjectionConfigJSONUnknownFieldsIgnored(t *testing.T) {
	var got pnats.ProjectionConfig
	if err := json.Unmarshal([]byte(`{"name":"x","unknown":1}`), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Name != "x" {
		t.Errorf("Name: got %q want x", got.Name)
	}
}

func TestProjectionConfigStructTags(t *testing.T) {
	want := []struct {
		field string
		tag   string // identical for both json and mapstructure namespaces
	}{
		{"Name", "name,omitempty"},
		{"Enabled", "enabled,omitempty"},
		{"Durable", "durable,omitempty"},
		{"QueueGroup", "queue_group,omitempty"},
		{"KVName", "kv_name,omitempty"},
		{"KVNames", "kv_names,omitempty"},
		{"NoKV", "no_kv,omitempty"},
	}
	rt := reflect.TypeFor[pnats.ProjectionConfig]()
	for _, w := range want {
		f, ok := rt.FieldByName(w.field)
		if !ok {
			t.Errorf("field %s missing", w.field)
			continue
		}
		if got := f.Tag.Get("json"); got != w.tag {
			t.Errorf("%s json tag: got %q want %q", w.field, got, w.tag)
		}
		if got := f.Tag.Get("mapstructure"); got != w.tag {
			t.Errorf("%s mapstructure tag: got %q want %q", w.field, got, w.tag)
		}
	}
	if rt.NumField() != len(want) {
		t.Errorf("field count: got %d want %d (extra/missing fields not asserted)", rt.NumField(), len(want))
	}
}

func TestProjectionConfigMapstructureDecodeSkipped(t *testing.T) {
	t.Skip("mapstructure not in go.mod; live decode skipped — projection/nats/config.go:3-11 carries mapstructure tags consumed only by downstream nexus-baas. Tag values are asserted via reflection in TestProjectionConfigStructTags. Wire a live decode test if/when fast-forward depends on mapstructure directly.")
}
