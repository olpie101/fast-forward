package kv

import (
	"bytes"
	"testing"
)

func TestNilValue(t *testing.T) {
	t.Run("MarshalValue returns nil/nil", func(t *testing.T) {
		var v *NilValue
		b, err := v.MarshalValue()
		if err != nil {
			t.Fatalf("MarshalValue err = %v, want nil", err)
		}
		if b != nil {
			t.Fatalf("MarshalValue bytes = %v, want nil", b)
		}
	})

	t.Run("UnmarshalValue returns nil", func(t *testing.T) {
		var v *NilValue
		if err := v.UnmarshalValue(nil); err != nil {
			t.Fatalf("UnmarshalValue err = %v, want nil", err)
		}
	})
}

func TestUInt64Value(t *testing.T) {
	t.Run("MarshalValue produces JSON", func(t *testing.T) {
		v := &UInt64Value{Value: 7}
		b, err := v.MarshalValue()
		if err != nil {
			t.Fatalf("MarshalValue err = %v", err)
		}
		want := []byte(`{"value":7}`)
		if !bytes.Equal(b, want) {
			t.Fatalf("MarshalValue = %q, want %q", b, want)
		}
	})

	t.Run("UnmarshalValue round-trips", func(t *testing.T) {
		dst := &UInt64Value{}
		if err := dst.UnmarshalValue([]byte(`{"value":7}`)); err != nil {
			t.Fatalf("UnmarshalValue err = %v", err)
		}
		if dst.Value != 7 {
			t.Fatalf("UnmarshalValue Value = %d, want 7", dst.Value)
		}
	})
}

func TestStringValue(t *testing.T) {
	t.Run("MarshalValue raw bytes", func(t *testing.T) {
		v := &StringValue{Value: "hi"}
		b, err := v.MarshalValue()
		if err != nil {
			t.Fatalf("MarshalValue err = %v", err)
		}
		if !bytes.Equal(b, []byte("hi")) {
			t.Fatalf("MarshalValue = %q, want %q", b, "hi")
		}
	})

	t.Run("MarshalValue on typed nil returns empty bytes", func(t *testing.T) {
		var v *StringValue
		b, err := v.MarshalValue()
		if err != nil {
			t.Fatalf("MarshalValue err = %v", err)
		}
		if !bytes.Equal(b, []byte("")) {
			t.Fatalf("MarshalValue typed-nil = %q, want empty", b)
		}
	})

	t.Run("UnmarshalValue assigns string", func(t *testing.T) {
		dst := &StringValue{}
		if err := dst.UnmarshalValue([]byte("hi")); err != nil {
			t.Fatalf("UnmarshalValue err = %v", err)
		}
		if dst.Value != "hi" {
			t.Fatalf("UnmarshalValue Value = %q, want %q", dst.Value, "hi")
		}
	})
}
