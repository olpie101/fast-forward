package kv

import "testing"

func TestResolve(t *testing.T) {
	t.Run("typed-nil pointer returns non-nil zero pointer", func(t *testing.T) {
		var p *UInt64Value
		got := resolve(p)
		out, ok := got.(*UInt64Value)
		if !ok {
			t.Fatalf("resolve type = %T, want *UInt64Value", got)
		}
		if out == nil {
			t.Fatal("resolve returned nil pointer; want non-nil")
		}
		if out.Value != 0 {
			t.Fatalf("resolve Value = %d, want 0", out.Value)
		}
	})

	t.Run("nil slice returns empty non-nil slice", func(t *testing.T) {
		var s []UInt64Value
		got := resolve(s)
		out, ok := got.([]UInt64Value)
		if !ok {
			t.Fatalf("resolve type = %T, want []UInt64Value", got)
		}
		if out == nil {
			t.Fatal("resolve returned nil slice; want non-nil")
		}
		if len(out) != 0 || cap(out) != 0 {
			t.Fatalf("resolve len/cap = %d/%d, want 0/0", len(out), cap(out))
		}
	})

	t.Run("non-pointer value returns zero of type", func(t *testing.T) {
		got := resolve(StringValue{Value: "x"})
		out, ok := got.(StringValue)
		if !ok {
			t.Fatalf("resolve type = %T, want StringValue", got)
		}
		if out.Value != "" {
			t.Fatalf("resolve Value = %q, want empty", out.Value)
		}
	})
}
