package kv

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func bucket(conn *nats.Conn, name string) (nats.KeyValue, error) {
	jsc, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	return jsc.KeyValue(name)
}

func BucketKeyValuer[T MarshalerUnmarshaler](conn *nats.Conn, name string) (KeyValuer[T], error) {
	bucket, err := bucket(conn, name)
	if err != nil {
		return nil, fmt.Errorf("kv: %s - %w", name, err)
	}
	return New[T](bucket), nil
}
