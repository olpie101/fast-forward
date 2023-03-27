package lookup

import (
	"time"

	"github.com/modernice/goes/event"
)

func DebounceReady(debounceTime time.Duration, ready chan struct{}) func(evt event.Event, original func(event.Event)) {
	t := time.NewTicker(debounceTime)
	fn := func(evt event.Event, original func(event.Event)) {
		t.Reset(debounceTime)
		original(evt)
	}

	go func() {
		<-t.C
		close(ready)
	}()

	return fn
}
