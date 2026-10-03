// Frozen pre-WaitGroup Cond baseline, renamed only for same-run benchmarks.
// Test-only: it is never compiled into the production package.
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

// condDefaultShards is the number of registries used by a zero-value condGroup.
const condDefaultShards = 255

// condGroup is a namespace for duplicate suppression. Its zero value is ready to
// use and initializes condDefaultShards registries on first use.
//
// A condGroup must not be copied after first use. Equal keys must identify
// equivalent work, including any tenant or authorization scope. Returned
// mutable values are shared; callers must synchronize access or copy them.
type condGroup struct {
	once   sync.Once
	seed   maphash.Seed
	shards []condShard
}

type condShard struct {
	mu sync.Mutex
	m  map[string]*condCall
}

type condCall struct {
	// cond, done, dups and chans are protected by the condShard mutex. The Cond
	// is allocated only when a synchronous caller actually needs to wait.
	cond  *sync.Cond
	done  bool
	dups  int
	chans []chan<- condResult

	// Written by the producer, then published under the condShard mutex.
	val any
	err error
}

// condResult is the outcome received from DoChan.
type condResult struct {
	Val    any
	Err    error
	Shared bool
}

// New constructs a condGroup with shardCount independent maps.
// It panics if shardCount is not positive.
func newCondGroup(shardCount int) *condGroup {
	if shardCount <= 0 {
		panic("singleflight: condShard count must be positive")
	}
	g := new(condGroup)
	g.initialize(shardCount)
	return g
}

func (g *condGroup) initialize(count int) {
	g.once.Do(func() {
		g.seed = maphash.MakeSeed()
		g.shards = make([]condShard, count)
		for i := range g.shards {
			g.shards[i].m = make(map[string]*condCall)
		}
	})
}

func (g *condGroup) shardFor(key string) *condShard {
	g.initialize(condDefaultShards)
	return &g.shards[maphash.String(g.seed, key)%uint64(len(g.shards))]
}

// Do executes fn unless an overlapping condCall for key is already running.
// Duplicate callers wait for that condCall and receive the same value and error.
// shared reports whether the outcome was shared with another caller.
//
// fn runs outside the registry lock. A panic is propagated to all Do callers
// as an error containing the original value and stack; an original error can
// be inspected with errors.Is/As. runtime.Goexit terminates all Do callers.
func (g *condGroup) Do(key string, fn func() (any, error)) (v any, err error, shared bool) {
	s := g.shardFor(key)
	s.mu.Lock()
	if c, ok := s.m[key]; ok {
		c.dups++
		if c.cond == nil {
			c.cond = sync.NewCond(&s.mu)
		}
		for !c.done {
			c.cond.Wait()
		}
		s.mu.Unlock()
		if e, ok := c.err.(*condPanicError); ok {
			panic(e)
		}
		if c.err == errCondGoexit {
			runtime.Goexit()
		}
		return c.val, c.err, true
	}
	c := new(condCall)
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
func (g *condGroup) DoChan(key string, fn func() (any, error)) <-chan condResult {
	ch := make(chan condResult, 1)
	s := g.shardFor(key)
	s.mu.Lock()
	if c, ok := s.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)
		s.mu.Unlock()
		return ch
	}
	c := &condCall{chans: []chan<- condResult{ch}}
	s.m[key] = c
	s.mu.Unlock()

	go g.doCall(s, c, key, fn)
	return ch
}

// Forget removes key from the registry without canceling any running condCall.
// Existing waiters still receive the old outcome; subsequent callers can
// start a new flight even while the old one is running.
func (g *condGroup) Forget(key string) {
	s := g.shardFor(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

var errCondGoexit = errors.New("runtime.Goexit was called")

type condPanicError struct {
	value any
	stack []byte
}

func (p *condPanicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func (p *condPanicError) Unwrap() error {
	err, _ := p.value.(error)
	return err
}

func newCondPanicError(v any) error {
	stack := debug.Stack()
	// Drop the goroutine status line, which may be stale when re-panicked.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &condPanicError{value: v, stack: stack}
}

func (g *condGroup) doCall(s *condShard, c *condCall, key string, fn func() (any, error)) {
	normalReturn, recovered := false, false
	// A nested defer distinguishes a recovered panic from runtime.Goexit.
	defer func() {
		if !normalReturn && !recovered {
			c.err = errCondGoexit
		}

		s.mu.Lock()
		c.done = true
		// Forget may already have installed a replacement flight.
		if s.m[key] == c {
			delete(s.m, key)
		}
		if c.cond != nil {
			c.cond.Broadcast()
		}
		shared := c.dups > 0
		s.mu.Unlock()

		if e, ok := c.err.(*condPanicError); ok {
			if len(c.chans) > 0 {
				go func() { panic(e) }()
				select {} // Preserve the producer in the crash dump.
			}
			panic(e)
		}
		if c.err == errCondGoexit {
			return // Already unwinding runtime.Goexit.
		}
		// No caller can attach after removal. Buffered sends do not block;
		// keep delivery outside the registry lock for unrelated keys.
		for _, ch := range c.chans {
			ch <- condResult{Val: c.val, Err: c.err, Shared: shared}
		}
	}()

	func() {
		defer func() {
			if !normalReturn {
				if r := recover(); r != nil {
					c.err = newCondPanicError(r)
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
