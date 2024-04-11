package projection

import (
	"time"

	"github.com/google/uuid"
	"github.com/modernice/goes/event"
	"github.com/modernice/goes/projection"
)

type ProgressAwareTarget interface {
	ApplyEvent(event.Event)
	Progress() (time.Time, []uuid.UUID)
	Key() string
	Dirty() bool
}

type ProjectionProgressor struct {
	*projection.Progressor
	dirty bool
}

func NewProgressor() *ProjectionProgressor {
	return &ProjectionProgressor{
		Progressor: projection.NewProgressor(),
		dirty:      false,
	}
}

func (p *ProjectionProgressor) SetProgress(t time.Time, ids ...uuid.UUID) {
	p.Progressor.SetProgress(t, ids...)
	p.dirty = true
}

func (p *ProjectionProgressor) Dirty() bool {
	return p.dirty
}
