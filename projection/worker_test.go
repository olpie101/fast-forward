package projection_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/modernice/goes/codec"
	"github.com/modernice/goes/event/eventbus"
	goesprojection "github.com/modernice/goes/projection"
	ffprojection "github.com/olpie101/fast-forward/projection"
)

// captureSchedule is a goes projection.Schedule that records the handler the
// worker subscribes with, so tests can run jobs through it directly.
type captureSchedule struct {
	mu      sync.Mutex
	handler func(goesprojection.Job) error
}

func (s *captureSchedule) Subscribe(_ context.Context, apply func(goesprojection.Job) error, _ ...goesprojection.SubscribeOption) (<-chan error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = apply
	return make(chan error), nil
}

func (s *captureSchedule) Trigger(context.Context, ...goesprojection.TriggerOption) error {
	return nil
}

func (s *captureSchedule) run(t *testing.T, job goesprojection.Job) error {
	t.Helper()
	s.mu.Lock()
	h := s.handler
	s.mu.Unlock()
	if h == nil {
		t.Fatal("worker did not subscribe")
	}
	return h(job)
}

// recordingProjector records the jobs it handles and returns err.
type recordingProjector struct {
	schedules ffprojection.Schedules
	err       error
	jobs      []goesprojection.Job
}

func (p *recordingProjector) Schedules() ffprojection.Schedules { return p.schedules }

func (p *recordingProjector) HandleJob(job goesprojection.Job) error {
	p.jobs = append(p.jobs, job)
	return p.err
}

// valueJob is a job whose context carries an extra value, as a tracing
// middleware would (span in the context).
type valueJob struct {
	goesprojection.Job
	key, val any
}

func (j valueJob) Value(key any) any {
	if key == j.key {
		return j.val
	}
	return j.Job.Value(key)
}

type ctxKey string

func startWorker(t *testing.T, p *recordingProjector, opts ...ffprojection.WorkerOption) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := ffprojection.New(p, eventbus.New(), codec.New(), opts...)
	if _, err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestWorkerJobMiddleware(t *testing.T) {
	sched := &captureSchedule{}
	p := &recordingProjector{schedules: ffprojection.Schedules{
		"kyb_initialiser": {Schedule: sched},
	}}

	var calls []string
	mw := func(label string) ffprojection.JobMiddleware {
		return func(name string, next ffprojection.JobHandler) ffprojection.JobHandler {
			return func(job goesprojection.Job) error {
				calls = append(calls, label+">"+name)
				err := next(job)
				calls = append(calls, label+"<")
				return err
			}
		}
	}
	// The inner middleware replaces the job, as a tracing middleware does.
	replace := func(_ string, next ffprojection.JobHandler) ffprojection.JobHandler {
		return func(job goesprojection.Job) error {
			return next(valueJob{Job: job, key: ctxKey("span"), val: "s1"})
		}
	}

	startWorker(t, p,
		ffprojection.WithJobMiddleware(mw("a"), mw("b")),
		ffprojection.WithJobMiddleware(replace), // appended: innermost
	)

	if err := sched.run(t, fakeJob{Context: context.Background()}); err != nil {
		t.Fatalf("job: %v", err)
	}

	want := []string{"a>kyb_initialiser", "b>kyb_initialiser", "b<", "a<"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
	if len(p.jobs) != 1 {
		t.Fatalf("projector handled %d jobs, want 1", len(p.jobs))
	}
	if got := p.jobs[0].Value(ctxKey("span")); got != "s1" {
		t.Fatalf("projector got job without the middleware's value: %v", got)
	}
}

func TestWorkerWithoutMiddlewareCallsHandleJob(t *testing.T) {
	sched := &captureSchedule{}
	p := &recordingProjector{schedules: ffprojection.Schedules{"w": {Schedule: sched}}}
	startWorker(t, p)

	job := fakeJob{Context: context.Background()}
	if err := sched.run(t, job); err != nil {
		t.Fatalf("job: %v", err)
	}
	if len(p.jobs) != 1 || p.jobs[0] != goesprojection.Job(job) {
		t.Fatalf("projector jobs = %v, want the original job", p.jobs)
	}
}

func TestWorkerErrorFuncCalledOnFailedJob(t *testing.T) {
	sched := &captureSchedule{}
	boom := errors.New("boom")
	p := &recordingProjector{schedules: ffprojection.Schedules{"w": {Schedule: sched}}, err: boom}

	var gotJob goesprojection.Job
	var gotErr error
	startWorker(t, p, ffprojection.WithErrorFunc(func(job goesprojection.Job, err error) {
		gotJob, gotErr = job, err
	}))

	job := fakeJob{Context: context.Background()}
	if err := sched.run(t, job); !errors.Is(err, boom) {
		t.Fatalf("job error = %v, want %v", err, boom)
	}
	if !errors.Is(gotErr, boom) || gotJob != goesprojection.Job(job) {
		t.Fatalf("error func got (%v, %v), want (job, %v)", gotJob, gotErr, boom)
	}
}

func TestWorkerErrorFuncNotCalledOnSuccess(t *testing.T) {
	sched := &captureSchedule{}
	p := &recordingProjector{schedules: ffprojection.Schedules{"w": {Schedule: sched}}}

	called := false
	startWorker(t, p, ffprojection.WithErrorFunc(func(goesprojection.Job, error) { called = true }))

	if err := sched.run(t, fakeJob{Context: context.Background()}); err != nil {
		t.Fatalf("job: %v", err)
	}
	if called {
		t.Fatal("error func called for a successful job")
	}
}
