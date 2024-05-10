package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/modernice/goes/helper/streams"
	"github.com/nats-io/nats.go"
)

type contextKey string

var (
	contextKeyStopOnZero      = contextKey("stop-on-zero")
	contextKeyNoErrOnNotFound = contextKey("no-err-on-not-found")
)

type Op = nats.KeyValueOp

type KeyValuer[T MarshalerUnmarshaler] interface {
	Keys(ctx context.Context) ([]string, error)
	Get(ctx context.Context, key string) (T, uint64, error)
	GetAll(ctx context.Context, keys []string) ([]T, error)
	Create(ctx context.Context, key string, value T) (revision uint64, err error)
	Put(ctx context.Context, key string, value T) (revision uint64, err error)
	Update(ctx context.Context, key string, value T, lastRevision uint64) (revision uint64, err error)
	// Status(ctx context.Context, key string) (nats.KeyValueStatus, error)
	LastRevision(context.Context, string) (uint64, error)
	Delete(ctx context.Context, key string, opts ...nats.DeleteOpt) error
	WatchAll(ctx context.Context, opts ...nats.WatchOpt) (<-chan WatchValue[T], <-chan error, error)
	Watch(ctx context.Context, sub string, opts ...nats.WatchOpt) (<-chan WatchValue[T], <-chan error, error)
}

type WatchValue[T MarshalerUnmarshaler] struct {
	Op    Op
	Value T
	Key   string
}

type KeyValue[T MarshalerUnmarshaler] struct {
	kv nats.KeyValue
}

func New[T MarshalerUnmarshaler](kv nats.KeyValue) KeyValuer[T] {
	return &KeyValue[T]{
		kv: kv,
	}
}

func (s *KeyValue[T]) Keys(ctx context.Context) ([]string, error) {
	keys, err := s.kv.Keys()
	if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
		return nil, err
	}
	return keys, nil
}

func (s *KeyValue[T]) Get(ctx context.Context, key string) (T, uint64, error) {
	var out T
	out = resolve(out).(T)

	kve, err := s.kv.Get(key)
	if err != nil {
		return out, 0, err
	}

	err = out.UnmarshalValue(kve.Value())
	if err != nil {
		return out, 0, err
	}
	return out, kve.Revision(), nil
}

func (s *KeyValue[T]) GetAll(ctx context.Context, keys []string) ([]T, error) {
	var wg sync.WaitGroup
	wg.Add(len(keys))
	res := make(chan T, len(keys))
	errs := make(chan error, 1)
	go func() {
		wg.Wait()
		close(res)
	}()
	noErrOnNotFound := isNoErrorOnNotFound(ctx)

	for _, id := range keys {
		go func(wg *sync.WaitGroup, id string, res chan<- T) {
			defer wg.Done()
			kve, _, err := s.Get(ctx, id)
			if err != nil {
				if errors.Is(err, nats.ErrKeyNotFound) && noErrOnNotFound {
					return
				}
				errs <- fmt.Errorf("error getting key (%s): %w", id, err)
				return
			}
			res <- kve
		}(&wg, id, res)
	}

	out := make([]T, 0, len(keys))
	var outErr error
L:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err, ok := <-errs:
			if !ok {
				continue
			}
			outErr = err
			break L
		case a, ok := <-res:
			if !ok {
				break L
			}
			out = append(out, a)
		}
	}

	return out, outErr
}
func (s *KeyValue[T]) Create(ctx context.Context, key string, value T) (revision uint64, err error) {
	b, err := value.MarshalValue()
	if err != nil {
		return 0, err
	}
	return s.kv.Create(key, b)
}
func (s *KeyValue[T]) Put(ctx context.Context, key string, value T) (revision uint64, err error) {
	b, err := value.MarshalValue()
	if err != nil {
		return 0, err
	}
	return s.kv.Put(key, b)
}
func (s *KeyValue[T]) Update(ctx context.Context, key string, value T, lastRevision uint64) (revision uint64, err error) {
	b, err := value.MarshalValue()
	if err != nil {
		return 0, err
	}
	return s.kv.Update(key, b, lastRevision)
}

func (s *KeyValue[T]) LastRevision(ctx context.Context, key string) (uint64, error) {
	kve, err := s.kv.Get(key)
	if err != nil {
		return 0, err
	}
	return uint64(kve.Revision()), nil
}
func (s *KeyValue[T]) Delete(ctx context.Context, key string, opts ...nats.DeleteOpt) error {
	return s.kv.Delete(key, opts...)
}

func (s *KeyValue[T]) WatchAll(ctx context.Context, opts ...nats.WatchOpt) (<-chan WatchValue[T], <-chan error, error) {
	o := []nats.WatchOpt{
		nats.Context(ctx),
	}

	o = append(o, opts...)

	w, err := s.kv.WatchAll(o...)
	if err != nil {
		return nil, nil, err
	}

	vals, errs := s.watch(ctx, w)
	return vals, errs, nil
}

func (s *KeyValue[T]) Watch(ctx context.Context, sub string, opts ...nats.WatchOpt) (<-chan WatchValue[T], <-chan error, error) {
	o := []nats.WatchOpt{
		nats.Context(ctx),
	}

	o = append(o, opts...)

	w, err := s.kv.Watch(sub, o...)
	if err != nil {
		return nil, nil, err
	}

	vals, errs := s.watch(ctx, w)
	return vals, errs, nil
}

func (s *KeyValue[T]) watch(ctx context.Context, kw nats.KeyWatcher) (<-chan WatchValue[T], <-chan error) {
	out := make(chan WatchValue[T])
	errs := make(chan error)
	go func() {
		for {
			select {
			case <-ctx.Done():
				err := kw.Stop()
				if err != nil {
					errs <- err
				}
				return
			case kve := <-kw.Updates():
				var v T
				v = resolve(v).(T)

				// A delete/purge operation happened
				if kve.Operation() != nats.KeyValuePut {
					out <- WatchValue[T]{
						Op:  kve.Operation(),
						Key: kve.Key(),
					}
					continue
				}

				err := v.UnmarshalValue(kve.Value())
				if err != nil {
					errs <- err
					fmt.Println("kv err", kve.Key(), kve.Operation())
					continue
				}

				out <- WatchValue[T]{
					Op:    kve.Operation(),
					Value: v,
					Key:   kve.Key(),
				}
			}
		}
	}()
	return out, errs
}

// A Marshaler can encode itself into bytes. aggregates must implement Marshaler
// & Unmarshaler for Snapshots to work.
//
// Example using encoding/gob:
//
//	type foo struct {
//		aggregate.Aggregate
//		state
//	}
//
//	type state struct {
//		Name string
//		Age uint8
//	}
//
//	func (f *foo) MarshalValue() ([]byte, error) {
//		var buf bytes.Buffer
//		err := gob.NewEncoder(&buf).Encode(f.state)
//		return buf.Bytes(), err
//	}
//
//	func (f *foo) UnmarshalValue(p []byte) error {
//		return gob.NewDecoder(bytes.NewReader(p)).Decode(&f.state)
//	}
type Marshaler interface {
	MarshalValue() ([]byte, error)
}

// An Unmarshaler can decode itself from bytes.
type Unmarshaler interface {
	UnmarshalValue([]byte) error
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type NilValue struct{}

func (*NilValue) MarshalValue() ([]byte, error) {
	return nil, nil
}

func (*NilValue) UnmarshalValue([]byte) error {
	return nil
}

type UInt64Value struct {
	Value uint64 `json:"value"`
}

func (v *UInt64Value) MarshalValue() ([]byte, error) {
	return json.Marshal(v)
}

func (v *UInt64Value) UnmarshalValue(b []byte) error {
	return json.Unmarshal(b, v)
}

type StringValue struct {
	Value string `json:"value"`
}

func (v *StringValue) MarshalValue() ([]byte, error) {
	var out string
	if v != nil {
		out = v.Value
	}
	return []byte(out), nil
}

func (v *StringValue) UnmarshalValue(b []byte) error {
	v.Value = string(b)
	return nil
}

func resolve(p any) any {
	rt := reflect.TypeOf(p)
	var rv reflect.Value
	if rt.Kind() == reflect.Ptr {
		rv = reflect.New(rt.Elem())
	} else if rt.Kind() == reflect.Slice {
		rv = reflect.MakeSlice(rt, 0, 0)
	} else {
		rv = reflect.New(rt).Elem()
	}
	return rv.Interface()
}

func WithStopOnZero(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyStopOnZero, true)
}

func isStopOnZero(ctx context.Context) bool {
	return ctx.Value(contextKeyStopOnZero) != nil
}

func WithNoErrorOnNotFound(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyNoErrOnNotFound, true)
}

func isNoErrorOnNotFound(ctx context.Context) bool {
	return ctx.Value(contextKeyNoErrOnNotFound) != nil
}

func UnwrapValues[T MarshalerUnmarshaler](ctx context.Context, in <-chan WatchValue[T], errs <-chan error) (<-chan T, <-chan error, error) {
	valChan, valPush, valClose := streams.NewConcurrentContext[T](ctx)
	errConChan, errPush, errClose := streams.NewConcurrentContext[error](ctx)
	errOut := make(chan error)
	errChan, stop := streams.FanIn(errConChan, errOut)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer func() {
			cancel()
			stop()
			valClose()
			errClose()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-in:
				var err error
				if v.Key == "" {
					if isStopOnZero(ctx) {
						return
					}
					v.Value = resolve(v.Value).(T)
				}
				err = valPush(v.Value)
				if err != nil {
					errOut <- err
				}
			case e := <-errs:
				err := errPush(e)
				if err != nil {
					errOut <- err
				}
			}

		}
	}()

	return valChan, errChan, nil
}
