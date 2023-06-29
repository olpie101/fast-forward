package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/nats-io/nats.go"
)

type KeyValuer[T any] interface {
	Keys(ctx context.Context) ([]string, error)
	Get(ctx context.Context, key string) (T, uint64, error)
	GetAll(ctx context.Context, keys []string) ([]T, error)
	Create(ctx context.Context, key string, value T) (revision uint64, err error)
	Put(ctx context.Context, key string, value T) (revision uint64, err error)
	Update(ctx context.Context, key string, value T, lastRevision uint64) (revision uint64, err error)
	// Status(ctx context.Context, key string) (nats.KeyValueStatus, error)
	LastRevision(context.Context, string) (uint64, error)
	Delete(ctx context.Context, key string, opts ...nats.DeleteOpt) error
}

type KeyValue[T MarshalerUnmarshaler] struct {
	kv nats.KeyValue
}

func New[T MarshalerUnmarshaler](kv nats.KeyValue) KeyValuer[T] {
	return &KeyValue[T]{
		kv: kv,
	}
}

// func (s *KeyValue[T]) Get(ctx context.Context, key string) (T, error) {
// 	kve, err := s.kv.Get(key)
// 	var v T
// 	if err != nil {
// 		return v, err
// 	}

// 	err = v.UnmarshalValue(kve.Value())
// 	if err != nil {
// 		return v, err
// 	}
// 	return v, nil
// }

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
	for _, id := range keys {
		go func(wg *sync.WaitGroup, id string, res chan<- T) {
			defer wg.Done()
			kve, err := s.kv.Get(id)
			if err != nil {
				fmt.Println(">>>>>>>failed to get")
				// TODO: @olpie101 handle this error
				// wg.Done()
				return
			}
			var v T
			v = resolve(v).(T)
			// resolve(f)
			// if m := g.(T); ok {
			// v = m
			err = v.UnmarshalValue(kve.Value())
			if err != nil {
				fmt.Println(">>>>>>>failed to get", err)
				// TODO: @olpie101 handle this error
				// wg.Done()
				return
			}
			// if r.debug {
			// 	log.Printf("[goes/codec.Registry@Unmarshal] unmarshaling type %T (%s) using custom Unmarshaler", resolve(ptr), name)
			// }

			// if err := m.Unmarshal(b); err != nil {
			// 	return nil, err
			// }

			// return resolve(ptr), nil
			res <- v
			// }

			// err = v.UnmarshalValue(kve.Value())
			// if err != nil {
			// 	fmt.Println(">>>>>>>failed to get", err)
			// 	// TODO: @olpie101 handle this error
			// 	// wg.Done()
			// 	return
			// }
		}(&wg, id, res)
	}
	sig := make(chan struct{})
	go func() {
		wg.Wait()
		close(sig)
	}()
	out := make([]T, 0, len(keys))
L:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case a := <-res:
			out = append(out, a)
		case <-sig:
			break L
		}
	}
	return out, nil
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
	return []byte(v.Value), nil
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
