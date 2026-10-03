// Frozen pre-channel WaitGroup implementation for benchmarks only.
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

// waitGroupDefaultShards is the number of registries used by a zero-value waitGroupBaseline.
const waitGroupDefaultShards = 255

// waitGroupBaseline is a namespace for duplicate suppression. Its zero value is ready to
// use and initializes waitGroupDefaultShards registries on first use.
//
// A waitGroupBaseline must not be copied after first use. Equal keys must identify
// equivalent work, including any tenant or authorization scope. Returned
// mutable values are shared; callers must synchronize access or copy them.
type waitGroupBaseline struct {
	once   sync.Once
	seed   maphash.Seed
	shards []waitGroupShard
}

type waitGroupShard struct {
	mu sync.Mutex
	m  map[string]*waitGroupCall
}

type waitGroupCall struct {
	// One producer is added before publication in the waitGroupShard map. The waitGroupCall
	// is never reused: all duplicate callers can safely Wait concurrently.
	wg sync.WaitGroup

	// Protected by the waitGroupShard mutex until removal, then immutable.
	dups  int
	chans []chan<- waitGroupResult

	// Written by the producer before Done; read by duplicates after Wait.
	val any
	err error
}

// waitGroupResult is the outcome received from DoChan.
type waitGroupResult struct {
	Val    any
	Err    error
	Shared bool
}

// newWaitGroupBaseline constructs a waitGroupBaseline with shardCount independent maps.
// It panics if shardCount is not positive.
func newWaitGroupBaseline(shardCount int) *waitGroupBaseline {
	if shardCount <= 0 {
		panic("singleflight: waitGroupShard count must be positive")
	}
	g := new(waitGroupBaseline)
	g.initialize(shardCount)
	return g
}

func (g *waitGroupBaseline) initialize(count int) {
	g.once.Do(func() {
		g.seed = maphash.MakeSeed()
		g.shards = make([]waitGroupShard, count)
		for i := range g.shards {
			g.shards[i].m = make(map[string]*waitGroupCall)
		}
	})
}

func (g *waitGroupBaseline) shardFor(key string) *waitGroupShard {
	g.initialize(waitGroupDefaultShards)
	return &g.shards[maphash.String(g.seed, key)%uint64(len(g.shards))]
}

// Do executes fn unless an overlapping waitGroupCall for key is already running.
// Duplicate callers wait for that waitGroupCall and receive the same value and error.
// shared reports whether the outcome was shared with another caller.
//
// fn runs outside the registry lock. A panic is propagated to all Do callers
// as an error containing the original value and stack; an original error can
// be inspected with errors.Is/As. runtime.Goexit terminates all Do callers.
func (g *waitGroupBaseline) Do(key string, fn func() (any, error)) (v any, err error, shared bool) {
	s := g.shardFor(key)
	s.mu.Lock()
	if c, ok := s.m[key]; ok {
		c.dups++
		s.mu.Unlock()
		c.wg.Wait() // No waitGroupShard lock is reacquired after completion.
		if e, ok := c.err.(*waitGroupPanicError); ok {
			panic(e)
		}
		if c.err == errWaitGroupGoexit {
			runtime.Goexit()
		}
		return c.val, c.err, true
	}
	c := new(waitGroupCall)
	c.wg.Add(1) // Initialize before any duplicate can observe the waitGroupCall.
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
func (g *waitGroupBaseline) DoChan(key string, fn func() (any, error)) <-chan waitGroupResult {
	ch := make(chan waitGroupResult, 1)
	s := g.shardFor(key)
	s.mu.Lock()
	if c, ok := s.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)
		s.mu.Unlock()
		return ch
	}
	c := &waitGroupCall{chans: []chan<- waitGroupResult{ch}}
	c.wg.Add(1)
	s.m[key] = c
	s.mu.Unlock()

	go g.doCall(s, c, key, fn)
	return ch
}

// Forget removes key from the registry without canceling any running waitGroupCall.
// Existing waiters still receive the old outcome; subsequent callers can
// start a new flight even while the old one is running.
func (g *waitGroupBaseline) Forget(key string) {
	s := g.shardFor(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

var errWaitGroupGoexit = errors.New("runtime.Goexit was called")

type waitGroupPanicError struct {
	value any
	stack []byte
}

func (p *waitGroupPanicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func (p *waitGroupPanicError) Unwrap() error {
	err, _ := p.value.(error)
	return err
}

func newWaitGroupPanicError(v any) error {
	stack := debug.Stack()
	// Drop the goroutine status line, which may be stale when re-panicked.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &waitGroupPanicError{value: v, stack: stack}
}

func (g *waitGroupBaseline) doCall(s *waitGroupShard, c *waitGroupCall, key string, fn func() (any, error)) {
	normalReturn, recovered := false, false
	// A nested defer distinguishes a recovered panic from runtime.Goexit.
	defer func() {
		if !normalReturn && !recovered {
			c.err = errWaitGroupGoexit
		}

		s.mu.Lock()
		// Forget may already have installed a replacement flight.
		if s.m[key] == c {
			delete(s.m, key)
		}
		// Publish the outcome on every exit, including panic and Goexit.
		c.wg.Done()
		shared := c.dups > 0
		s.mu.Unlock()

		if e, ok := c.err.(*waitGroupPanicError); ok {
			if len(c.chans) > 0 {
				go func() { panic(e) }()
				select {} // Preserve the producer in the crash dump.
			}
			panic(e)
		}
		if c.err == errWaitGroupGoexit {
			return // Already unwinding runtime.Goexit.
		}
		// No caller can attach after removal. Buffered sends do not block;
		// keep delivery outside the registry lock for unrelated keys.
		for _, ch := range c.chans {
			ch <- waitGroupResult{Val: c.val, Err: c.err, Shared: shared}
		}
	}()

	func() {
		defer func() {
			if !normalReturn {
				if r := recover(); r != nil {
					c.err = newWaitGroupPanicError(r)
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
