package singleflight

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for caller")
		var zero T
		return zero
	}
}

// Holding the producer and observing registration avoids scheduler-dependent
// sleeps when testing that all synchronous waiters share the same flight.
func waitForDuplicates(t *testing.T, g *Group, key string, count int) {
	t.Helper()
	s := g.shardFor(key)
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		c := s.m[key]
		ready := c != nil && c.dups >= count
		s.mu.Unlock()
		if ready {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("waiters did not register")
}

func assertEmpty(t *testing.T, g *Group) {
	t.Helper()
	for i := range g.shards {
		s := &g.shards[i]
		s.mu.Lock()
		n := len(s.m)
		s.mu.Unlock()
		if n != 0 {
			t.Errorf("shard %d retained %d flights", i, n)
		}
	}
}

func forEachWaitMode(t *testing.T, test func(*testing.T, WaitMode)) {
	t.Helper()
	for _, tc := range []struct {
		name string
		mode WaitMode
	}{
		{"WaitGroup", WaitGroupMode},
		{"Channel", ChannelMode},
	} {
		t.Run(tc.name, func(t *testing.T) { test(t, tc.mode) })
	}
}

func newTestGroup(mode WaitMode, count int) *Group {
	return NewWithOptions(count, Options{WaitMode: mode})
}

func TestDoSequential(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		for _, key := range []string{"", "key", "한글", strings.Repeat("long", 1024)} {
			t.Run(fmt.Sprintf("length=%d", len(key)), func(t *testing.T) {
				g := newTestGroup(mode, DefaultShards)
				for i := 0; i < 3; i++ {
					wantErr := errors.New("producer error")
					v, err, shared := g.Do(key, func() (any, error) { return i, wantErr })
					if v != i || err != wantErr || shared {
						t.Fatalf("got (%v, %v, %v), want (%d, original error, false)", v, err, shared, i)
					}
				}
				assertEmpty(t, g)
			})
		}
	})
}

func TestDoDuplicates(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		const waiters = 32
		started, release := make(chan struct{}), make(chan struct{})
		results := make(chan Result, waiters+1)
		wantErr := errors.New("shared error")
		wantValue := new(int)
		var calls atomic.Int32
		go func() {
			v, err, shared := g.Do("key", func() (any, error) {
				calls.Add(1)
				close(started)
				<-release
				return wantValue, wantErr
			})
			results <- Result{v, err, shared}
		}()
		receive(t, started)
		for i := 0; i < waiters; i++ {
			go func() {
				v, err, shared := g.Do("key", func() (any, error) {
					calls.Add(1)
					return nil, nil
				})
				results <- Result{v, err, shared}
			}()
		}
		waitForDuplicates(t, g, "key", waiters)
		close(release)
		for i := 0; i <= waiters; i++ {
			r := receive(t, results)
			if r.Val != wantValue || r.Err != wantErr || !r.Shared {
				t.Fatalf("unexpected shared outcome: %+v", r)
			}
		}
		if calls.Load() != 1 {
			t.Fatalf("executed %d producers", calls.Load())
		}
		assertEmpty(t, g)
	})
}

func TestDoChanAndDoShare(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		release := make(chan struct{})
		first := g.DoChan("key", func() (any, error) { <-release; return 42, nil })
		second := g.DoChan("key", func() (any, error) { t.Error("duplicate producer ran"); return nil, nil })
		synchronous := make(chan Result, 1)
		go func() {
			v, err, shared := g.Do("key", func() (any, error) { t.Error("duplicate Do ran"); return nil, nil })
			synchronous <- Result{v, err, shared}
		}()
		waitForDuplicates(t, g, "key", 2)
		close(release)
		for _, ch := range []<-chan Result{first, second, synchronous} {
			r := receive(t, ch)
			if r.Val != 42 || r.Err != nil || !r.Shared {
				t.Fatalf("unexpected outcome: %+v", r)
			}
		}
		select {
		case _, ok := <-first:
			t.Fatalf("result channel had another value or was closed: open=%v", ok)
		default:
		}
		assertEmpty(t, g)
	})
}

func TestDoChanAbandonedReceiver(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		finished := make(chan struct{})
		_ = g.DoChan("abandoned", func() (any, error) {
			defer close(finished)
			return 1, nil
		})
		receive(t, finished)
		// The producer's function has returned, but publication can still be in
		// progress. Joining or starting anew must both complete without a receiver.
		r := receive(t, g.DoChan("abandoned", func() (any, error) { return 1, nil }))
		if r.Val != 1 || r.Err != nil {
			t.Fatalf("unexpected outcome: %+v", r)
		}
		assertEmpty(t, g)
	})
}

func TestDifferentKeysSameShard(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, 1) // Force a hash collision; exact string keys still stay separate.
		started, release := make(chan struct{}), make(chan struct{})
		slow := g.DoChan("slow", func() (any, error) { close(started); <-release; return 1, nil })
		receive(t, started)
		fast := g.DoChan("fast", func() (any, error) { return 2, nil })
		if r := receive(t, fast); r.Val != 2 || r.Shared {
			t.Fatalf("different key was shared: %+v", r)
		}
		close(release)
		if r := receive(t, slow); r.Val != 1 || r.Shared {
			t.Fatalf("unexpected slow outcome: %+v", r)
		}
		assertEmpty(t, g)
	})
}

func TestForgetKeepsReplacement(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		oldRelease, newRelease := make(chan struct{}), make(chan struct{})
		old := g.DoChan("key", func() (any, error) { <-oldRelease; return "old", nil })
		oldWaiter := g.DoChan("key", func() (any, error) { t.Error("old duplicate ran"); return nil, nil })
		g.Forget("key")
		replacement := g.DoChan("key", func() (any, error) { <-newRelease; return "new", nil })
		close(oldRelease)
		for _, ch := range []<-chan Result{old, oldWaiter} {
			if r := receive(t, ch); r.Val != "old" || !r.Shared {
				t.Fatalf("unexpected old outcome: %+v", r)
			}
		}
		// Old completion must not delete a newer entry for the same key.
		newWaiter := g.DoChan("key", func() (any, error) { t.Error("replacement was deleted"); return nil, nil })
		close(newRelease)
		for _, ch := range []<-chan Result{replacement, newWaiter} {
			if r := receive(t, ch); r.Val != "new" || !r.Shared {
				t.Fatalf("unexpected new outcome: %+v", r)
			}
		}
		g.Forget("missing")
		assertEmpty(t, g)
	})
}

func TestInitialization(t *testing.T) {
	for _, count := range []int{1, 16, DefaultShards, 256} {
		g := New(count)
		if len(g.shards) != count {
			t.Fatalf("got %d shards, want %d", len(g.shards), count)
		}
		for i := range g.shards {
			if g.shards[i].m == nil {
				t.Fatalf("shard %d map was not initialized", i)
			}
		}
	}
	for _, count := range []int{0, -1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("New accepted a non-positive shard count")
				}
			}()
			New(count)
		})
	}

	var g Group
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err, _ := g.Do(fmt.Sprint(i), func() (any, error) { return i, nil })
			if v != i || err != nil {
				t.Errorf("unexpected result for %d: %v, %v", i, v, err)
			}
		}(i)
	}
	wg.Wait()
	if len(g.shards) != DefaultShards {
		t.Fatalf("zero Group initialized %d shards", len(g.shards))
	}
	assertEmpty(t, &g)
}

func TestWaitModeOptions(t *testing.T) {
	// The typed parameter checks New's original (non-variadic) signature.
	construct := func(constructor func(int) *Group) *Group { return constructor(1) }
	for name, g := range map[string]*Group{
		"zero Group":   new(Group),
		"New":          construct(New),
		"zero Options": NewWithOptions(1, Options{}),
	} {
		t.Run(name, func(t *testing.T) {
			if g.waitMode != WaitGroupMode {
				t.Fatal("default mode is not WaitGroup")
			}
			c := g.newCall()
			if c.done != nil {
				t.Fatal("WaitGroup allocated a completion channel")
			}
			c.finish()
			c.wait()
		})
	}

	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		for _, count := range []int{1, 16, DefaultShards, 256} {
			options := Options{WaitMode: mode}
			g := NewWithOptions(count, options)
			options.WaitMode = WaitMode(255) // Caller mutation must not affect g.
			if g.waitMode != mode || len(g.shards) != count {
				t.Fatal("constructor did not retain the selected mode and shard count")
			}
			c := g.newCall()
			if (c.done != nil) != (mode == ChannelMode) {
				t.Fatal("constructor selected the wrong completion signal")
			}
			c.finish()
			c.wait()
		}
	})

	for _, tc := range []struct {
		name    string
		count   int
		options Options
	}{
		{"zero shards", 0, Options{}},
		{"negative shards", -1, Options{WaitMode: ChannelMode}},
		{"unknown mode", 1, Options{WaitMode: WaitMode(255)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("NewWithOptions accepted invalid configuration")
				}
			}()
			NewWithOptions(tc.count, tc.options)
		})
	}
}

func TestDoPanic(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		for _, panicValue := range []any{errors.New("original panic"), "string panic", nil} {
			t.Run(fmt.Sprintf("%T", panicValue), func(t *testing.T) {
				g := newTestGroup(mode, DefaultShards)
				started, release := make(chan struct{}), make(chan struct{})
				panics := make(chan any, 9)
				invoke := func(fn func() (any, error)) {
					defer func() { panics <- recover() }()
					if _, err, _ := g.Do("key", fn); err != nil {
						t.Errorf("unexpected returned error: %v", err)
					}
				}
				go invoke(func() (any, error) { close(started); <-release; panic(panicValue) })
				receive(t, started)
				for i := 0; i < 8; i++ {
					go invoke(func() (any, error) { t.Error("duplicate panic producer ran"); return nil, nil })
				}
				waitForDuplicates(t, g, "key", 8)
				close(release)
				for i := 0; i < 9; i++ {
					p := receive(t, panics)
					e, ok := p.(error)
					if !ok {
						t.Fatalf("panic was not propagated as an error: %v", p)
					}
					if want, ok := panicValue.(error); ok && !errors.Is(e, want) {
						t.Fatalf("original panic error not preserved: %v", e)
					}
					if !strings.Contains(e.Error(), "TestDoPanic") {
						t.Fatalf("producer stack missing: %v", e)
					}
				}
				assertEmpty(t, g)
				v, err, shared := g.Do("key", func() (any, error) { return "recovered", nil })
				if v != "recovered" || err != nil || shared {
					t.Fatal("panic left a stale flight")
				}
			})
		}
	})
}

func TestDoGoexit(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		started, release := make(chan struct{}), make(chan struct{})
		exited := make(chan bool, 9)
		invoke := func(fn func() (any, error)) {
			returned := false
			defer func() { exited <- returned }()
			if _, err, _ := g.Do("key", fn); err != nil {
				t.Errorf("unexpected returned error: %v", err)
			}
			returned = true
		}
		go invoke(func() (any, error) { close(started); <-release; runtime.Goexit(); return nil, nil })
		receive(t, started)
		for i := 0; i < 8; i++ {
			go invoke(func() (any, error) { t.Error("duplicate Goexit producer ran"); return nil, nil })
		}
		waitForDuplicates(t, g, "key", 8)
		close(release)
		for i := 0; i < 9; i++ {
			if receive(t, exited) {
				t.Fatal("Do returned instead of terminating its goroutine")
			}
		}
		assertEmpty(t, g)
		if v, _, _ := g.Do("key", func() (any, error) { return 42, nil }); v != 42 {
			t.Fatal("Goexit left a stale flight")
		}
	})
}

func TestDoChanGoexit(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		exited := make(chan struct{})
		ch := g.DoChan("key", func() (any, error) {
			// This outer defer belongs to fn; registry cleanup occurs afterward.
			defer close(exited)
			runtime.Goexit()
			return nil, nil
		})
		receive(t, exited)
		s := g.shardFor("key")
		deadline := time.Now().Add(testTimeout)
		for {
			s.mu.Lock()
			_, exists := s.m["key"]
			s.mu.Unlock()
			if !exists {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Goexit flight was not reclaimed")
			}
			runtime.Gosched()
		}
		select {
		case <-ch:
			t.Fatal("Goexit must not publish a channel result")
		default:
		}
		assertEmpty(t, g)
	})
}

func TestDoChanPanicProcess(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		const marker = "singleflight-channel-panic-marker"
		if panicMode := os.Getenv("TOYBOX_SINGLEFLIGHT_PANIC"); panicMode != "" {
			if os.Getenv("TOYBOX_SINGLEFLIGHT_WAIT_MODE") != fmt.Sprint(mode) {
				return // The child only exercises the requested wait mode.
			}
			g := newTestGroup(mode, DefaultShards)
			if panicMode == "mixed" {
				started, release := make(chan struct{}), make(chan struct{})
				go func() {
					defer func() { _ = recover() }()
					if _, err, _ := g.Do("key", func() (any, error) { close(started); <-release; panic(marker) }); err != nil {
						t.Errorf("unexpected returned error: %v", err)
					}
				}()
				<-started
				_ = g.DoChan("key", func() (any, error) { return nil, nil })
				close(release)
			} else {
				_ = g.DoChan("key", func() (any, error) { panic(marker) })
			}
			select {}
		}
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		for _, panicMode := range []string{"channel", "mixed"} {
			t.Run(panicMode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, exe, "-test.run=^TestDoChanPanicProcess$")
				cmd.Env = append(os.Environ(), "TOYBOX_SINGLEFLIGHT_PANIC="+panicMode, "TOYBOX_SINGLEFLIGHT_WAIT_MODE="+fmt.Sprint(mode))
				out, err := cmd.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatalf("panic child did not terminate: %v", ctx.Err())
				}
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || !strings.Contains(string(out), marker) {
					t.Fatalf("expected unrecoverable producer panic, got %v\n%s", err, out)
				}
			})
		}
	})
}

// Completion must release Do callers even while another operation holds the
// shard mutex. This is the behavior the old Cond-based wait could not provide.
func TestDoWaiterReturnsWithoutShardLock(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, 1)
		s := &g.shards[0]
		c := g.newCall()
		s.m["key"] = c
		result := make(chan Result, 1)
		go func() {
			v, err, shared := g.Do("key", func() (any, error) {
				t.Error("duplicate producer ran")
				return nil, nil
			})
			result <- Result{v, err, shared}
		}()
		waitForDuplicates(t, g, "key", 1)
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			c.val = 42
			delete(s.m, "key")
			c.finish()
			// Intentionally keep the shard locked until the waiter has returned.
			r := receive(t, result)
			if r.Val != 42 || r.Err != nil || !r.Shared {
				t.Fatalf("unexpected outcome: %+v", r)
			}
		}()
		assertEmpty(t, g)
	})
}

func TestCompletedFlightMultipleWaits(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, DefaultShards)
		release := make(chan struct{})
		wantErr := errors.New("completed outcome")
		ch := g.DoChan("key", func() (any, error) { <-release; return 42, wantErr })
		s := g.shardFor("key")
		s.mu.Lock()
		c := s.m["key"]
		s.mu.Unlock()
		close(release)
		receive(t, ch) // Completion already occurred before the waits below.
		finished := make(chan bool, 64)
		for i := 0; i < cap(finished); i++ {
			go func() {
				c.wait()
				c.wait()
				finished <- c.val == 42 && c.err == wantErr
			}()
		}
		for i := 0; i < cap(finished); i++ {
			if !receive(t, finished) {
				t.Fatal("completion did not publish the original outcome")
			}
		}
		assertEmpty(t, g)
	})
}

func TestDoCompletionBoundary(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		g := newTestGroup(mode, 1)
		wantErr := errors.New("boundary error")
		finished := make(chan bool, 16)
		for worker := 0; worker < cap(finished); worker++ {
			go func() {
				ok := true
				for i := 0; i < 256; i++ {
					v, err, _ := g.Do("key", func() (any, error) {
						if i%2 == 0 {
							runtime.Gosched()
						}
						return 42, wantErr
					})
					ok = ok && v == 42 && err == wantErr
				}
				finished <- ok
			}()
		}
		for i := 0; i < cap(finished); i++ {
			if !receive(t, finished) {
				t.Fatal("completion boundary exposed an incomplete outcome")
			}
		}
		assertEmpty(t, g)
	})
}

func TestCompletionSignalOnEveryExit(t *testing.T) {
	forEachWaitMode(t, func(t *testing.T, mode WaitMode) {
		for _, exit := range []string{"return", "error", "panic", "goexit"} {
			t.Run(exit, func(t *testing.T) {
				g := newTestGroup(mode, DefaultShards)
				started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				wantErr := errors.New("terminal outcome")
				go func() {
					defer close(finished)
					defer func() { _ = recover() }()
					_, err, _ := g.Do("key", func() (any, error) {
						close(started)
						<-release
						switch exit {
						case "panic":
							panic(wantErr)
						case "goexit":
							runtime.Goexit()
						case "error":
							return 42, wantErr
						}
						return 42, nil
					})
					if exit == "error" && err != wantErr {
						t.Errorf("original error not returned: %v", err)
					}
				}()
				receive(t, started)
				s := g.shardFor("key")
				s.mu.Lock()
				c := s.m["key"]
				s.mu.Unlock()
				if mode == ChannelMode {
					select {
					case <-c.done:
						t.Fatal("completion signaled before producer finished")
					default:
					}
				} else if c.done != nil {
					t.Fatal("WaitGroup mode allocated a completion channel")
				}
				unblock()
				waited := make(chan struct{})
				go func() { c.wait(); close(waited) }()
				receive(t, waited)
				if mode == ChannelMode {
					// Closed without a send: repeated receives stay ready with ok=false.
					if _, open := <-c.done; open {
						t.Fatal("completion channel sent a value instead of closing")
					}
				}
				switch exit {
				case "return":
					if c.val != 42 || c.err != nil {
						t.Fatalf("invalid outcome: %v, %v", c.val, c.err)
					}
				case "error":
					if c.val != 42 || c.err != wantErr {
						t.Fatalf("invalid error outcome: %v, %v", c.val, c.err)
					}
				case "panic":
					if _, ok := c.err.(*panicError); !ok || !errors.Is(c.err, wantErr) {
						t.Fatalf("panic outcome not published: %v", c.err)
					}
				case "goexit":
					if c.err != errGoexit {
						t.Fatalf("Goexit outcome not published: %v", c.err)
					}
				}
				receive(t, finished)
				assertEmpty(t, g)
			})
		}
	})
}
