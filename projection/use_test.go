package projection_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/aggregate"
	"github.com/modernice/goes/event"
	goesprojection "github.com/modernice/goes/projection"
	ffprojection "github.com/olpie101/fast-forward/projection"
)

// fakeJob satisfies goes/projection.Job. Use only ever exercises the embedded
// context.Context (passed to newFn/getFn/useFn/saveFn). The other six methods
// panic if invoked, which guards against future changes that begin calling
// them.
type fakeJob struct {
	context.Context
}

func (fakeJob) Events(context.Context, ...event.Query) (<-chan event.Event, <-chan error, error) {
	panic("unused")
}
func (fakeJob) EventsOf(context.Context, ...string) (<-chan event.Event, <-chan error, error) {
	panic("unused")
}
func (fakeJob) EventsFor(context.Context, goesprojection.Target[any]) (<-chan event.Event, <-chan error, error) {
	panic("unused")
}
func (fakeJob) Aggregates(context.Context, ...string) (<-chan aggregate.Ref, <-chan error, error) {
	panic("unused")
}
func (fakeJob) Aggregate(context.Context, string) (uuid.UUID, error) {
	panic("unused")
}
func (fakeJob) Apply(context.Context, goesprojection.Target[any], ...goesprojection.ApplyOption) error {
	panic("unused")
}

// fakeUseTarget is a minimal ProgressAwareTarget. ApplyEvent and Progress must
// not be invoked by Use; they panic to enforce that.
type fakeUseTarget struct {
	key   string
	dirty bool
}

func (f *fakeUseTarget) ApplyEvent(event.Event)             { panic("unused by Use") }
func (f *fakeUseTarget) Progress() (time.Time, []uuid.UUID) { panic("unused by Use") }
func (f *fakeUseTarget) Key() string                        { return f.key }
func (f *fakeUseTarget) Dirty() bool                        { return f.dirty }

type useCallRecord struct {
	newCount  int
	getCount  int
	getKeys   []string
	useCount  int
	useArgs   []*fakeUseTarget
	saveCount int
	saveArgs  []*fakeUseTarget
}

func runUse(t *testing.T, newTgt *fakeUseTarget, existing *fakeUseTarget, getErr error,
	newErr, useErr, saveErr error) (*useCallRecord, error) {
	t.Helper()
	rec := &useCallRecord{}
	newFn := func(ctx context.Context) (*fakeUseTarget, error) {
		rec.newCount++
		if newErr != nil {
			return nil, newErr
		}
		return newTgt, nil
	}
	getFn := func(ctx context.Context, key string) (*fakeUseTarget, error) {
		rec.getCount++
		rec.getKeys = append(rec.getKeys, key)
		if getErr != nil {
			return nil, getErr
		}
		return existing, nil
	}
	useFn := func(ctx context.Context, v *fakeUseTarget) error {
		rec.useCount++
		rec.useArgs = append(rec.useArgs, v)
		return useErr
	}
	saveFn := func(ctx context.Context, v *fakeUseTarget) error {
		rec.saveCount++
		rec.saveArgs = append(rec.saveArgs, v)
		return saveErr
	}
	err := ffprojection.Use[*fakeUseTarget](
		fakeJob{Context: context.Background()},
		newFn, getFn, saveFn, useFn,
	)
	return rec, err
}

func TestUseExistingFoundDirtyCallsAllInOrder(t *testing.T) {
	newT := &fakeUseTarget{key: "k1"}
	existing := &fakeUseTarget{key: "k1", dirty: true}
	rec, err := runUse(t, newT, existing, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Use err: %v", err)
	}
	if rec.newCount != 1 || rec.getCount != 1 || rec.useCount != 1 || rec.saveCount != 1 {
		t.Fatalf("call counts: %+v", rec)
	}
	if rec.getKeys[0] != "k1" {
		t.Errorf("getFn key: want k1, got %q", rec.getKeys[0])
	}
	if rec.useArgs[0] != existing {
		t.Errorf("useFn arg should be existing, got %v", rec.useArgs[0])
	}
	if rec.saveArgs[0] != existing {
		t.Errorf("saveFn arg should be existing, got %v", rec.saveArgs[0])
	}
}

func TestUseExistingFoundNotDirtySkipsSave(t *testing.T) {
	newT := &fakeUseTarget{key: "k"}
	existing := &fakeUseTarget{key: "k", dirty: false}
	rec, err := runUse(t, newT, existing, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Use err: %v", err)
	}
	if rec.saveCount != 0 {
		t.Errorf("saveFn must not be called when not dirty, count=%d", rec.saveCount)
	}
	if rec.newCount != 1 || rec.getCount != 1 || rec.useCount != 1 {
		t.Errorf("counts: %+v", rec)
	}
}

func TestUseNotFoundFallbackUsesNewTarget(t *testing.T) {
	newT := &fakeUseTarget{key: "k", dirty: true}
	rec, err := runUse(t, newT, nil, ffprojection.ErrNotFound, nil, nil, nil)
	if err != nil {
		t.Fatalf("Use err: %v", err)
	}
	if rec.useArgs[0] != newT {
		t.Errorf("useFn arg should be newTarget when not found")
	}
	if rec.saveCount != 1 || rec.saveArgs[0] != newT {
		t.Errorf("saveFn should be called with newTarget when dirty")
	}
}

func TestUseNotFoundFallbackNotDirtySkipsSave(t *testing.T) {
	newT := &fakeUseTarget{key: "k", dirty: false}
	rec, err := runUse(t, newT, nil, ffprojection.ErrNotFound, nil, nil, nil)
	if err != nil {
		t.Fatalf("Use err: %v", err)
	}
	if rec.saveCount != 0 {
		t.Errorf("saveFn must not be called when not dirty, count=%d", rec.saveCount)
	}
}

func TestUseNewFnError(t *testing.T) {
	errBoom := errors.New("boom")
	rec, err := runUse(t, nil, nil, nil, errBoom, nil, nil)
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
	if rec.getCount != 0 || rec.useCount != 0 || rec.saveCount != 0 {
		t.Errorf("downstream callbacks must not run: %+v", rec)
	}
}

func TestUseGetFnNonNotFoundError(t *testing.T) {
	newT := &fakeUseTarget{key: "k"}
	errBoom := errors.New("boom")
	rec, err := runUse(t, newT, nil, errBoom, nil, nil, nil)
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
	if rec.useCount != 0 || rec.saveCount != 0 {
		t.Errorf("useFn/saveFn must not run on getFn err: %+v", rec)
	}
}

func TestUseGetFnWrappedNotFound(t *testing.T) {
	newT := &fakeUseTarget{key: "k", dirty: true}
	wrapped := fmt.Errorf("wrap: %w", ffprojection.ErrNotFound)
	rec, err := runUse(t, newT, nil, wrapped, nil, nil, nil)
	if err != nil {
		t.Fatalf("Use err: %v", err)
	}
	if rec.useArgs[0] != newT {
		t.Errorf("useFn should run with newTarget when wrapped ErrNotFound")
	}
}

func TestUseUseFnError(t *testing.T) {
	newT := &fakeUseTarget{key: "k"}
	existing := &fakeUseTarget{key: "k", dirty: true}
	errBoom := errors.New("boom")
	rec, err := runUse(t, newT, existing, nil, nil, errBoom, nil)
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
	if rec.saveCount != 0 {
		t.Errorf("saveFn must not run after useFn err, count=%d", rec.saveCount)
	}
}

func TestUseSaveFnError(t *testing.T) {
	newT := &fakeUseTarget{key: "k"}
	existing := &fakeUseTarget{key: "k", dirty: true}
	errBoom := errors.New("boom")
	_, err := runUse(t, newT, existing, nil, nil, nil, errBoom)
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
}

func TestUseKeyPlumbedToGetFn(t *testing.T) {
	newT := &fakeUseTarget{key: "projection-key-xyz"}
	existing := &fakeUseTarget{key: "projection-key-xyz"}
	rec, err := runUse(t, newT, existing, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Use err: %v", err)
	}
	if len(rec.getKeys) != 1 || rec.getKeys[0] != "projection-key-xyz" {
		t.Errorf("getFn key: want projection-key-xyz, got %v", rec.getKeys)
	}
}
