package provider

import (
	"context"
	"sync/atomic"
	"time"
)

// stallDetector bounds the silent gap between upstream stream frames. The
// adapter kicks it after every frame; when no kick arrives within the stall
// window the derived context is canceled, which tears down the in-flight
// response body and unblocks a read parked in the frame loop. A stall window
// of 0 disables detection: every method degrades to a no-op and the derived
// context is the parent's.
//
// The window also covers everything before the first frame (connection,
// response headers, time to first token), so a stalled upstream is noticed
// per frame instead of at the request's total deadline.
type stallDetector struct {
	ctx    context.Context
	cancel context.CancelFunc
	kick   chan struct{}
	done   chan struct{}
	fired  atomic.Bool
}

func newStallDetector(parent context.Context, stall time.Duration) *stallDetector {
	if stall <= 0 {
		return &stallDetector{ctx: parent}
	}
	ctx, cancel := context.WithCancel(parent)
	d := &stallDetector{
		ctx: ctx, cancel: cancel,
		kick: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go d.watch(stall)
	return d
}

// watch cancels the derived context once the window elapses without a kick.
// The kick channel replaces the per-frame timer: the watchdog goroutine owns
// the timer exclusively, so Stop/Reset never race with a firing callback.
func (d *stallDetector) watch(stall time.Duration) {
	defer close(d.done)
	timer := time.NewTimer(stall)
	defer timer.Stop()
	for {
		select {
		case <-d.kick:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(stall)
		case <-timer.C:
			d.fired.Store(true)
			d.cancel()
			return
		case <-d.ctx.Done():
			return
		}
	}
}

// frame records one arrived frame and re-arms the window. It never blocks;
// a kick still pending is coalesced.
func (d *stallDetector) frame() {
	if d.kick == nil {
		return
	}
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// stalled reports whether the window elapsed without a frame.
func (d *stallDetector) stalled() bool { return d.fired.Load() }

// err converts the detector's context state into the adapter's timeout
// error: stall-specific when the window fired, the generic cancellation
// message when the client or the parent deadline won the race.
func (d *stallDetector) err() *Error {
	if d.stalled() {
		return &Error{Class: ClassTimeout, Msg: "upstream stalled: no frame within the stall window"}
	}
	return &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
}

// stop releases the watchdog goroutine; it must be called exactly once when
// the stream attempt ends.
func (d *stallDetector) stop() {
	if d.kick == nil {
		return
	}
	d.cancel()
	<-d.done
}
