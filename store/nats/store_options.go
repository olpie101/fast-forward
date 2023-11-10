package nats

// Code generated by github.com/launchdarkly/go-options.  DO NOT EDIT.

import "fmt"

import (
	"go.uber.org/zap"
	"time"
)

import "github.com/google/go-cmp/cmp"

type ApplyStoreOptionFunc func(c *Store) error

func (f ApplyStoreOptionFunc) apply(c *Store) error {
	return f(c)
}

func applyStoreOptions(c *Store, options ...StoreOption) error {
	for _, o := range options {
		if err := o.apply(c); err != nil {
			return err
		}
	}
	return nil
}

type StoreOption interface {
	apply(*Store) error
}

type withLoggerImpl struct {
	o *zap.SugaredLogger
}

func (o withLoggerImpl) apply(c *Store) error {
	c.logger = o.o
	return nil
}

func (o withLoggerImpl) Equal(v withLoggerImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withLoggerImpl) String() string {
	name := "WithLogger"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

func WithLogger(o *zap.SugaredLogger) StoreOption {
	return withLoggerImpl{
		o: o,
	}
}

type withRetryCountImpl struct {
	o uint
}

func (o withRetryCountImpl) apply(c *Store) error {
	c.retryCount = o.o
	return nil
}

func (o withRetryCountImpl) Equal(v withRetryCountImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withRetryCountImpl) String() string {
	name := "WithRetryCount"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

func WithRetryCount(o uint) StoreOption {
	return withRetryCountImpl{
		o: o,
	}
}

type withPullExpiryImpl struct {
	o time.Duration
}

func (o withPullExpiryImpl) apply(c *Store) error {
	c.pullExpiry = o.o
	return nil
}

func (o withPullExpiryImpl) Equal(v withPullExpiryImpl) bool {
	switch {
	case !cmp.Equal(o.o, v.o):
		return false
	}
	return true
}

func (o withPullExpiryImpl) String() string {
	name := "WithPullExpiry"

	// hack to avoid go vet error about passing a function to Sprintf
	var value interface{} = o.o
	return fmt.Sprintf("%s: %+v", name, value)
}

func WithPullExpiry(o time.Duration) StoreOption {
	return withPullExpiryImpl{
		o: o,
	}
}