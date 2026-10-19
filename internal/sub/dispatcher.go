package sub

import (
	"container/heap"
	"time"
)

// entry is the in-memory view of one non-terminal message.
type entry struct {
	p          pos
	ref        string // message ID that holds the payload
	key        string
	headers    map[string]string
	priority   string
	producedAt time.Time
	rec        record
	queued     bool   // in the scheduler's ready structures
	band       int    // base priority band index, set by the scheduler
	value      string // fairness value, set by the scheduler
	holdsKey   bool   // this entry is its key's head under per-key ordering
}

func (e *entry) fairness(header string) string {
	if header == "" {
		return ""
	}
	return e.headers[header]
}

// timerItem is a lazily invalidated heap item.
type timerItem struct {
	at  time.Time
	p   pos
	tag uint64 // lease ID for lease items, 0 for delay items
}

type timerHeap []timerItem

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)        { *h = append(*h, x.(timerItem)) }
func (h *timerHeap) Pop() any          { o := *h; it := o[len(o)-1]; *h = o[:len(o)-1]; return it }
func (h *timerHeap) push(it timerItem) { heap.Push(h, it) }
func (h timerHeap) peek() (timerItem, bool) {
	if len(h) == 0 {
		return timerItem{}, false
	}
	return h[0], true
}
func (h *timerHeap) pop() timerItem { return heap.Pop(h).(timerItem) }
