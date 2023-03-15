package nats

import (
	"errors"
	"fmt"

	"github.com/modernice/goes/event"
	"github.com/nats-io/nats.go"
)

func Retry[D any](action func(event.Of[D]) error, maxRetries uint) func(event.Of[D]) {
	return func(e event.Of[D]) {
		fmt.Println("retry enter")
		for i := uint(0); i < maxRetries; i++ {
			fmt.Println("retry loop", i)
			err := action(e)
			if err != nil && isMismatchErr(err) {
				continue
			}
			if err == nil {
				return
			}
			// TODO what happens on retry failure
		}
		fmt.Println("retry Exit")
	}
}

func isMismatchErr(e error) bool {
	err := &nats.APIError{}
	if errors.As(e, &err) {
		return err.ErrorCode == 10071
	}
	return false
}

func IsMismatchErr(e error) bool {
	return isMismatchErr(e)
}

func isKeyNotFoundError(e error) bool {
	return errors.Is(e, nats.ErrKeyNotFound)
}

func IsKeyNotFoundError(e error) bool {
	return isKeyNotFoundError(e)
}

func IsRetryable(e error) bool {
	return isMismatchErr(e) || isKeyNotFoundError(e)
}
