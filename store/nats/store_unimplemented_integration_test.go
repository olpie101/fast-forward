package nats

import "testing"

// TestFindIntegration_Skip documents the unimplemented Store.Find surface.
// store/nats/store.go:142-144 panics with "not implemented". Desired: return
// the event with the given uuid via JetStream lookup, or expose a codified
// ErrNotImplemented sentinel until the feature lands. Would-be assertion:
// after inserting one event, Find(ctx, evt.ID()) returns an Event whose
// ID matches.
func TestFindIntegration_Skip(t *testing.T) {
	t.Skip("store/nats/store.go:142-144 Find panics with 'not implemented'; desired: return event by uuid via JetStream lookup or codified ErrNotImplemented sentinel")
}

// TestDeleteIntegration_Skip documents the unimplemented Store.Delete surface.
// store/nats/store.go:188-190 panics with "not implemented". Desired: purge
// matching subjects via js.DeleteMsg or codified ErrNotImplemented sentinel.
// Would-be assertion: after Insert + Delete, Query for the same aggregate
// returns 0 events.
func TestDeleteIntegration_Skip(t *testing.T) {
	t.Skip("store/nats/store.go:188-190 Delete panics with 'not implemented'; desired: purge matching subjects via js.DeleteMsg or codified ErrNotImplemented sentinel")
}
