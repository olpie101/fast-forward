package projection_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/olpie101/fast-forward/projection"
)

// fakeProgressTarget is a minimal ProgressAwareTarget used by Filter tests.
// Only Progress() is exercised; the other methods are unused stubs.
type fakeProgressTarget struct {
	progress time.Time
	ids      []uuid.UUID
}

func (f fakeProgressTarget) ApplyEvent(event.Event)               {}
func (f fakeProgressTarget) Progress() (time.Time, []uuid.UUID)   { return f.progress, f.ids }
func (f fakeProgressTarget) Key() string                          { return "" }
func (f fakeProgressTarget) Dirty() bool                          { return false }

func evtAt(name string, t time.Time) event.Event {
	return event.New[any](name, struct{}{}, event.Time(t)).Any()
}

func evtAtWithID(name string, t time.Time, id uuid.UUID) event.Event {
	return event.New[any](name, struct{}{}, event.Time(t), event.ID(id)).Any()
}

func TestFilterDropsBeforeProgressKeepsAtAndAfter(t *testing.T) {
	T := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	tgt := fakeProgressTarget{progress: T, ids: []uuid.UUID{}}

	before := evtAt("e", T.Add(-time.Nanosecond))
	at := evtAt("e", T)
	after := evtAt("e", T.Add(time.Nanosecond))

	out, err := projection.Filter(tgt, []event.Event{before, at, after})
	if err != nil {
		t.Fatalf("Filter returned err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 events, got %d", len(out))
	}
	if out[0].ID() != at.ID() {
		t.Errorf("out[0] should be the @T event")
	}
	if out[1].ID() != after.ID() {
		t.Errorf("out[1] should be the @T+1ns event")
	}
}

func TestFilterExcludesAtProgressWhenIDInList(t *testing.T) {
	T := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	idA := uuid.New()
	tgt := fakeProgressTarget{progress: T, ids: []uuid.UUID{idA}}

	a := evtAtWithID("e", T, idA)
	b := evtAt("e", T)
	c := evtAt("e", T.Add(time.Nanosecond))

	out, err := projection.Filter(tgt, []event.Event{a, b, c})
	if err != nil {
		t.Fatalf("Filter returned err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 events, got %d", len(out))
	}
	if out[0].ID() != b.ID() {
		t.Errorf("out[0] should be evtB")
	}
	if out[1].ID() != c.ID() {
		t.Errorf("out[1] should be evtC")
	}
}

func TestFilterEmptyInputProducesEmptyOutput(t *testing.T) {
	tgt := fakeProgressTarget{progress: time.Now(), ids: []uuid.UUID{}}
	out, err := projection.Filter(tgt, []event.Event{})
	if err != nil {
		t.Fatalf("Filter returned err: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("want empty output, got %d", len(out))
	}
}

func TestGroupsKeysPreservesOrder(t *testing.T) {
	g := projection.Groups[string, int]{
		{Key: "a", Value: []int{1, 2}},
		{Key: "b", Value: []int{3}},
	}
	keys := g.Keys()
	want := []string{"a", "b"}
	if !reflect.DeepEqual(keys, want) {
		t.Errorf("want %v, got %v", want, keys)
	}
}

func TestGroupsMapPreservesValues(t *testing.T) {
	g := projection.Groups[string, int]{
		{Key: "a", Value: []int{1, 2}},
		{Key: "b", Value: []int{3}},
	}
	m := g.Map()
	if len(m) != 2 {
		t.Fatalf("want 2 entries, got %d", len(m))
	}
	if !reflect.DeepEqual(m["a"].Value, []int{1, 2}) {
		t.Errorf("unexpected a value: %v", m["a"].Value)
	}
	if !reflect.DeepEqual(m["b"].Value, []int{3}) {
		t.Errorf("unexpected b value: %v", m["b"].Value)
	}
}

func TestGroupsEmpty(t *testing.T) {
	var g projection.Groups[string, int]
	keys := g.Keys()
	if keys == nil {
		t.Errorf("Keys() should be non-nil on empty Groups")
	}
	if len(keys) != 0 {
		t.Errorf("want empty keys, got %v", keys)
	}
	m := g.Map()
	if m == nil {
		t.Errorf("Map() should be non-nil on empty Groups")
	}
	if len(m) != 0 {
		t.Errorf("want empty map, got %v", m)
	}
}

func TestGroupByFuncSplitsInputByKey(t *testing.T) {
	in := make(chan int, 4)
	in <- 1
	in <- 2
	in <- 3
	in <- 4
	close(in)

	groupFn := func(v int) (string, error) {
		if v%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	}

	got, err := projection.GroupByFunc(context.Background(), groupFn, (<-chan int)(in))
	if err != nil {
		t.Fatalf("GroupByFunc err: %v", err)
	}
	gm := projection.Groups[string, int](got).Map()
	if len(gm) != 2 {
		t.Fatalf("want 2 groups, got %d", len(gm))
	}
	if !reflect.DeepEqual(gm["odd"].Value, []int{1, 3}) {
		t.Errorf("odd: want [1 3], got %v", gm["odd"].Value)
	}
	if !reflect.DeepEqual(gm["even"].Value, []int{2, 4}) {
		t.Errorf("even: want [2 4], got %v", gm["even"].Value)
	}
}

func TestGroupByFuncReturnsGroupFnError(t *testing.T) {
	in := make(chan int, 2)
	in <- 1
	in <- 2
	close(in)

	errBoom := errors.New("boom")
	calls := 0
	groupFn := func(v int) (string, error) {
		calls++
		if calls == 2 {
			return "", errBoom
		}
		return "k", nil
	}

	got, err := projection.GroupByFunc(context.Background(), groupFn, (<-chan int)(in))
	if !errors.Is(err, errBoom) {
		t.Errorf("want errBoom, got %v", err)
	}
	if got != nil {
		t.Errorf("want nil result on err, got %v", got)
	}
}

func TestGroupByFuncReturnsExternalError(t *testing.T) {
	in := make(chan int)
	close(in)

	errFanIn := errors.New("fanin")
	errCh := make(chan error, 1)
	errCh <- errFanIn
	close(errCh)

	groupFn := func(v int) (string, error) { return "k", nil }

	got, err := projection.GroupByFunc(
		context.Background(),
		groupFn,
		(<-chan int)(in),
		(<-chan error)(errCh),
	)
	// FanIn (goes/helper/streams/fanin.go:24-47) performs a blocking send of
	// errFanIn before close, so even when the GroupByFunc select picks <-in
	// first the next iteration MUST receive errFanIn before any close signal.
	if !errors.Is(err, errFanIn) {
		t.Errorf("want errFanIn, got %v", err)
	}
	if got != nil {
		t.Errorf("want nil result on err, got %v", got)
	}
}

func TestGroupByFuncCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan int)
	groupFn := func(v int) (string, error) { return "k", nil }

	got, err := projection.GroupByFunc(ctx, groupFn, (<-chan int)(in))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", err)
	}
	if got != nil {
		t.Errorf("want nil result, got %v", got)
	}
}

func TestDeduplicateSingleWindowSetEquality(t *testing.T) {
	in := make(chan string, 3)
	out := projection.Deduplicate[string](in, 50*time.Millisecond)

	in <- "a"
	in <- "a"
	in <- "b"
	close(in)

	got := map[string]struct{}{}
	for v := range out {
		got[v] = struct{}{}
	}
	want := map[string]struct{}{"a": {}, "b": {}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestDeduplicateMultiWindowDeterministicOrder(t *testing.T) {
	in := make(chan string, 2)
	out := projection.Deduplicate[string](in, 30*time.Millisecond)

	in <- "a"
	// Wait past the window so the timer fires and pushes "a" to out.
	time.Sleep(40 * time.Millisecond)

	// Drain "a" from out before sending "b" so the second window cannot race.
	select {
	case v := <-out:
		if v != "a" {
			t.Fatalf("want 'a', got %q", v)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first window flush")
	}

	in <- "b"
	close(in)

	var rest []string
	for v := range out {
		rest = append(rest, v)
	}
	if !reflect.DeepEqual(rest, []string{"b"}) {
		t.Errorf("want [b], got %v", rest)
	}
}

func TestDeduplicateEmptyInput(t *testing.T) {
	in := make(chan string)
	out := projection.Deduplicate[string](in, 50*time.Millisecond)

	close(in)

	count := 0
	for range out {
		count++
	}
	if count != 0 {
		t.Errorf("want empty drain, got %d values", count)
	}
}
