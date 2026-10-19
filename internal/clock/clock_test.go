package clock

import (
	"testing"
	"time"
)

func TestFakeClockAdvances(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	c := NewFake(start)
	c.Advance(1500 * time.Millisecond)
	if got := c.Now(); !got.Equal(start.Add(1500 * time.Millisecond)) {
		t.Fatalf("got %v", got)
	}
}
