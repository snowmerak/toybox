// Copyright 2013 The Go Authors. All rights reserved.
// Portions of the panic/Goexit handling are derived from golang.org/x/sync.
// Use of those portions is governed by the BSD-style license in LICENSE.

// Package singleflight suppresses overlapping function calls for the same key.
// Keys are hashed into independent registries to reduce contention between
// unrelated calls. Results are shared, not cached.
package singleflight

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"runtime"
	"runtime/debug"
	"sync"
)

// DefaultShards is the number of registries used by a zero-value Group.
const DefaultShards = 255

// WaitMode selects the completion signal used by duplicate Do callers.
type WaitMode uint8

const (
	// WaitGroupMode uses a per-call sync.WaitGroup and is the default.
	WaitGroupMode WaitMode = iota
	// ChannelMode uses a per-call chan struct{} closed by the producer.
	ChannelMode
)

// Options configures a Group at construction. Its zero value uses WaitGroupMode.
type Options struct {
	WaitMode WaitMode
}

// Group is a namespace for duplicate suppression. Its zero value is ready to
// use and initializes DefaultShards registries with WaitGroupMode on first use.
//
// A Group must not be copied after first use. Equal keys must identify
// equivalent work, including any tenant or authorization scope. Returned
// mutable values are shared; callers must synchronize access or copy them.
type Group struct {
	once     sync.Once
	seed     maphash.Seed
	shards   []shard
	waitMode WaitMode // Immutable after construction; zero means WaitGroupMode.
}

type shard struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	// Exactly one signal is initialized before map publication and completed
	// by the producer. A call is never reused; all duplicates may wait safely.
	wg   sync.WaitGroup
	done chan struct{} // Non-nil only in ChannelMode; no values are sent.

	// Protected by the shard mutex until removal, then immutable.
	dups  int
	chans []chan<- Result

	// Written before Done/close; read by duplicates after completion.
	val any
	err error
}

// Result is the outcome received from DoChan.
type Result struct {
	Val    any
	Err    error
	Shared bool
}

// New constructs a Group with shardCount independent maps and WaitGroupMode.
// It panics if shardCount is not positive.
func New(shardCount int) *Group {
	return NewWithOptions(shardCount, Options{})
}

// NewWithOptions constructs a Group with the selected completion signal.
// It panics if shardCount is not positive or WaitMode is unknown.
// Options are copied; the mode cannot be changed after construction.
// DoChan's public result channel behavior is identical in both modes.
func NewWithOptions(shardCount int, options Options) *Group {
	if shardCount <= 0 {
		panic("singleflight: shard count must be positive")
	}
	if options.WaitMode != WaitGroupMode && options.WaitMode != ChannelMode {
		panic("singleflight: unknown wait mode")
	}
	g := &Group{waitMode: options.WaitMode}
	g.initialize(shardCount)
	return g
}

func (g *Group) newCall() *call {
	c := new(call)
	if g.waitMode == ChannelMode {
		c.done = make(chan struct{})
	} else {
		c.wg.Add(1) // Must precede publication and every duplicate's Wait.
	}
	return c
}

func (c *call) wait() {
	if c.done != nil {
		<-c.done
	} else {
		c.wg.Wait()
	}
}

func (c *call) finish() {
	if c.done != nil {
		close(c.done)
	} else {
		c.wg.Done()
	}
}

func (g *Group) initialize(count int) {
	g.once.Do(func() {
		g.seed = maphash.MakeSeed()
		g.shards = make([]shard, count)
		for i := range g.shards {
			g.shards[i].m = make(map[string]*call)
		}
	})
}

func (g *Group) shardFor(key string) *shard {
	g.initialize(DefaultShards)
	return &g.shards[maphash.String(g.seed, key)%uint64(len(g.shards))]
}

// Do executes fn unless an overlapping call for key is already running.
// Duplicate callers wait for that call and receive the same value and error.
// shared reports whether the outcome was shared with another caller.
//
// fn runs outside the registry lock. A panic is propagated to all Do callers
// as an error containing the original value and stack; an original error can
// be inspected with errors.Is/As. runtime.Goexit terminates all Do callers.
func (g *Group) Do(key string, fn func() (any, error)) (v any, err error, shared bool) {
	s := g.shardFor(key)
	s.mu.Lock()
	if c, ok := s.m[key]; ok {
		c.dups++
		s.mu.Unlock()
		c.wait() // No shard lock is reacquired after completion.
		if e, ok := c.err.(*panicError); ok {
			panic(e)
		}
		if c.err == errGoexit {
			runtime.Goexit()
		}
		return c.val, c.err, true
	}
	c := g.newCall()
	s.m[key] = c
	s.mu.Unlock()

	g.doCall(s, c, key, fn)
	return c.val, c.err, c.dups > 0
}

// DoChan is like Do, but runs a new producer in a goroutine and returns a
// buffered channel immediately. The channel receives one result and is never
// closed. Not receiving the result does not block the producer.
//
// As with golang.org/x/sync/singleflight, a producer panic with any channel
// subscribers is unrecoverable and crashes the process. runtime.Goexit cleans
// up the flight but sends no channel result. DoChan does not cancel work when
// a caller stops waiting; use select with a context to cancel only the wait.
func (g *Group) DoChan(key string, fn func() (any, error)) <-chan Result {
	ch := make(chan Result, 1)
	s := g.shardFor(key)
	s.mu.Lock()
	if c, ok := s.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)
		s.mu.Unlock()
		return ch
	}
	c := g.newCall()
	c.chans = []chan<- Result{ch}
	s.m[key] = c
	s.mu.Unlock()

	go g.doCall(s, c, key, fn)
	return ch
}

// Forget removes key from the registry without canceling any running call.
// Existing waiters still receive the old outcome; subsequent callers can
// start a new flight even while the old one is running.
func (g *Group) Forget(key string) {
	s := g.shardFor(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

var errGoexit = errors.New("runtime.Goexit was called")

type panicError struct {
	value any
	stack []byte
}

func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func (p *panicError) Unwrap() error {
	err, _ := p.value.(error)
	return err
}

func newPanicError(v any) error {
	stack := debug.Stack()
	// Drop the goroutine status line, which may be stale when re-panicked.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

func (g *Group) doCall(s *shard, c *call, key string, fn func() (any, error)) {
	normalReturn, recovered := false, false
	// A nested defer distinguishes a recovered panic from runtime.Goexit.
	defer func() {
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		s.mu.Lock()
		// Forget may already have installed a replacement flight.
		if s.m[key] == c {
			delete(s.m, key)
		}
		// Publish the outcome on every exit, including panic and Goexit.
		c.finish()
		shared := c.dups > 0
		s.mu.Unlock()

		if e, ok := c.err.(*panicError); ok {
			if len(c.chans) > 0 {
				go func() { panic(e) }()
				select {} // Preserve the producer in the crash dump.
			}
			panic(e)
		}
		if c.err == errGoexit {
			return // Already unwinding runtime.Goexit.
		}
		// No caller can attach after removal. Buffered sends do not block;
		// keep delivery outside the registry lock for unrelated keys.
		for _, ch := range c.chans {
			ch <- Result{Val: c.val, Err: c.err, Shared: shared}
		}
	}()

	func() {
		defer func() {
			if !normalReturn {
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()
		c.val, c.err = fn()
		normalReturn = true
	}()
	if !normalReturn {
		recovered = true
	}
}
