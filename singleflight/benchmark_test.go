package singleflight

import (
	"fmt"
	"hash/maphash"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	xsingleflight "golang.org/x/sync/singleflight"
)

type benchmarkCaller func(string, func() (any, error)) (any, error, bool)

type benchmarkImplementation struct {
	name string
	new  func(maphash.Seed) benchmarkCaller
}

func implementations(async bool) []benchmarkImplementation {
	return []benchmarkImplementation{
		{
			name: "Sharded255Channel",
			new: func(seed maphash.Seed) benchmarkCaller {
				g := NewWithOptions(DefaultShards, Options{WaitMode: ChannelMode})
				g.seed = seed // Same key-to-shard mapping for all sharded variants.
				if async {
					return func(key string, fn func() (any, error)) (any, error, bool) {
						r := <-g.DoChan(key, fn)
						return r.Val, r.Err, r.Shared
					}
				}
				return g.Do
			},
		},
		{
			name: "Sharded255WaitGroup",
			new: func(seed maphash.Seed) benchmarkCaller {
				g := New(DefaultShards)
				g.seed = seed
				if async {
					return func(key string, fn func() (any, error)) (any, error, bool) {
						r := <-g.DoChan(key, fn)
						return r.Val, r.Err, r.Shared
					}
				}
				return g.Do
			},
		},
		{
			name: "Sharded255Cond",
			new: func(seed maphash.Seed) benchmarkCaller {
				g := newCondGroup(DefaultShards)
				g.seed = seed
				if async {
					return func(key string, fn func() (any, error)) (any, error, bool) {
						r := <-g.DoChan(key, fn)
						return r.Val, r.Err, r.Shared
					}
				}
				return g.Do
			},
		},
		{
			name: "XSync",
			new: func(_ maphash.Seed) benchmarkCaller {
				g := new(xsingleflight.Group)
				if async {
					return func(key string, fn func() (any, error)) (any, error, bool) {
						r := <-g.DoChan(key, fn)
						return r.Val, r.Err, r.Shared
					}
				}
				return g.Do
			},
		},
	}
}

func cheapWork() (any, error) { return uint64(42), nil }

// A data-dependent loop makes CPU work visible to the compiler. No sleep or
// wall-clock spin is used, so timer granularity cannot determine its duration.
func cpuWork() uint64 {
	v := uint64(42)
	for i := 0; i < 1024; i++ {
		v = v*1664525 + 1013904223
	}
	return v
}

func BenchmarkSerial(b *testing.B) {
	for _, keyCase := range []struct {
		name string
		key  string
	}{
		{"ShortKey", "key"},
		{"LongKey4096", strings.Repeat("k", 4096)},
	} {
		b.Run(keyCase.name, func(b *testing.B) {
			seed := maphash.MakeSeed()
			for _, impl := range implementations(false) {
				b.Run(impl.name, func(b *testing.B) {
					do := impl.new(seed)
					if v, err, shared := do(keyCase.key, cheapWork); v != uint64(42) || err != nil || shared {
						b.Fatal("invalid warmup outcome")
					} // Exclude one-time initialization.
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						v, err, shared := do(keyCase.key, cheapWork)
						if v != uint64(42) || err != nil || shared {
							b.Fatalf("invalid outcome: %v, %v, %v", v, err, shared)
						}
					}
				})
			}
		})
	}
}

type parallelWorkload struct {
	name        string
	keys        int
	independent bool
	yield       bool
	cpu         bool
}

var parallelWorkloads = []parallelWorkload{
	{name: "IndependentCheap", independent: true},
	{name: "Uniform256Cheap", keys: 256},
	{name: "HotCheap", keys: 1},
	{name: "HotYield", keys: 1, yield: true},
	{name: "Uniform16Yield", keys: 16, yield: true},
	{name: "IndependentCPU", independent: true, cpu: true},
	{name: "HotCPU", keys: 1, cpu: true},
}

func runParallelBenchmark(b *testing.B, do benchmarkCaller, workload parallelWorkload) {
	// RunParallel starts GOMAXPROCS workers at its default parallelism.
	keyCount := workload.keys
	if workload.independent {
		keyCount = runtime.GOMAXPROCS(0)
	}
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%08d", i)
	}
	want := uint64(42)
	if workload.cpu {
		want = cpuWork()
	}
	if v, err, shared := do("warmup", cheapWork); v != uint64(42) || err != nil || shared {
		b.Fatal("invalid warmup outcome")
	}
	var workerID, totalExecutions, totalShared atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := int(workerID.Add(1) - 1)
		index := id % keyCount
		var executions, sharedCount int64
		fn := func() (any, error) {
			executions++
			if workload.yield {
				runtime.Gosched() // Deliberate overlap, not backend latency.
			}
			if workload.cpu {
				return cpuWork(), nil
			}
			return uint64(42), nil
		}
		for pb.Next() {
			v, err, shared := do(keys[index], fn)
			if v != want || err != nil {
				b.Errorf("invalid outcome: %v, %v", v, err)
			}
			if shared {
				sharedCount++
			}
			if !workload.independent {
				index = (index + 1) % keyCount
			}
		}
		// All executions initiated by this worker have completed before this
		// addition, including async producers, which publish via their channel.
		totalExecutions.Add(executions)
		totalShared.Add(sharedCount)
	})
	b.StopTimer()
	b.ReportMetric(float64(totalExecutions.Load())/float64(b.N), "backend/op")
	b.ReportMetric(float64(totalShared.Load())/float64(b.N), "shared/op")
}

func BenchmarkParallel(b *testing.B) {
	for _, workload := range parallelWorkloads {
		b.Run(workload.name, func(b *testing.B) {
			seed := maphash.MakeSeed()
			for _, impl := range implementations(false) {
				b.Run(impl.name, func(b *testing.B) {
					runParallelBenchmark(b, impl.new(seed), workload)
				})
			}
		})
	}
}

func BenchmarkDoChan(b *testing.B) {
	for _, workload := range []parallelWorkload{
		{name: "IndependentCheap", independent: true},
		{name: "HotYield", keys: 1, yield: true},
	} {
		b.Run(workload.name, func(b *testing.B) {
			seed := maphash.MakeSeed()
			for _, impl := range implementations(true) {
				b.Run(impl.name, func(b *testing.B) {
					runParallelBenchmark(b, impl.new(seed), workload)
				})
			}
		})
	}
}

// Initialization costs are deliberately separate from steady-state calls.
func BenchmarkColdGroup(b *testing.B) {
	b.Run("Sharded255Channel", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			g := NewWithOptions(DefaultShards, Options{WaitMode: ChannelMode})
			v, err, shared := g.Do("key", cheapWork)
			if v != uint64(42) || err != nil || shared {
				b.Fatal("invalid cold result")
			}
		}
	})
	b.Run("Sharded255WaitGroup", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var g Group
			v, err, shared := g.Do("key", cheapWork)
			if v != uint64(42) || err != nil || shared {
				b.Fatal("invalid cold result")
			}
		}
	})
	b.Run("Sharded255Cond", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var g condGroup
			v, err, shared := g.Do("key", cheapWork)
			if v != uint64(42) || err != nil || shared {
				b.Fatal("invalid cold result")
			}
		}
	})
	b.Run("XSync", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var g xsingleflight.Group
			v, err, shared := g.Do("key", cheapWork)
			if v != uint64(42) || err != nil || shared {
				b.Fatal("invalid cold result")
			}
		}
	})
}

// Optional tuning experiment, kept separate from the upstream comparison.
func BenchmarkShardCount(b *testing.B) {
	for _, count := range []int{1, 16, 255, 256, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			runParallelBenchmark(b, New(count).Do, parallelWorkload{independent: true})
		})
	}
}

// Compare the selectable WaitGroup path with the frozen single-mode version.
func BenchmarkWaitModeOverhead(b *testing.B) {
	for _, workload := range []parallelWorkload{
		{name: "IndependentCheap", independent: true},
		{name: "HotYield", keys: 1, yield: true},
	} {
		b.Run(workload.name, func(b *testing.B) {
			seed := maphash.MakeSeed()
			old := newWaitGroupBaseline(DefaultShards)
			old.seed = seed
			current := New(DefaultShards)
			current.seed = seed
			b.Run("FrozenWaitGroup", func(b *testing.B) {
				runParallelBenchmark(b, old.Do, workload)
			})
			b.Run("SelectableWaitGroup", func(b *testing.B) {
				runParallelBenchmark(b, current.Do, workload)
			})
		})
	}
}
