package limiter

import (
	"context"
	"testing"
	"time"
)

func TestRateWindow(t *testing.T) {
	l := New(2, 10)
	now := time.Now()
	for i := 0; i < 2; i++ {
		ok, _, release, _ := l.Allow(context.Background(), "s", now)
		if !ok {
			t.Fatalf("request %d denied", i)
		}
		release()
	}
	if ok, _, _, _ := l.Allow(context.Background(), "s", now); ok {
		t.Error("third request in window should be denied")
	}
	if ok, _, _, _ := l.Allow(context.Background(), "s", now.Add(time.Minute)); !ok {
		t.Error("next window should allow again")
	}
}

func TestConcurrency(t *testing.T) {
	l := New(100, 1)
	now := time.Now()
	ok, _, release, _ := l.Allow(context.Background(), "s", now)
	if !ok {
		t.Fatal("first request denied")
	}
	if ok, _, _, _ := l.Allow(context.Background(), "s", now); ok {
		t.Error("second concurrent request should be denied")
	}
	release()
	if ok, _, _, _ := l.Allow(context.Background(), "s", now); !ok {
		t.Error("slot should be released")
	}
}
