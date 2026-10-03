package singleflight

import (
	"fmt"
	"hash/maphash"
	"math/bits"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	xsingleflight "golang.org/x/sync/singleflight"
)

const (
	serviceTraceSeed   = uint64(0x5eed20261004)
	serviceTraceLength = 1024
	serviceKeyCount    = 4096
)

type serviceProfile struct {
	name          string
	uniquePercent int
	zipf          bool
	hotset        bool
}

var serviceProfiles = []serviceProfile{
	{name: "Uniform4096New50", uniquePercent: 50},
	{name: "Zipf4096New10", uniquePercent: 10, zipf: true},
	{name: "Hot32New20", uniquePercent: 20, hotset: true},
}

type serviceTraceEntry struct {
	keyID  int // -1 means a never-reused key, not merely a large fixed pool.
	doChan bool
}

// One immutable, seeded trace per client. Replaying a trace never reuses its
// unique keys: those incorporate the client's request sequence, not the slot.
func makeServiceTrace(profile serviceProfile, client int) []serviceTraceEntry {
	rng := rand.New(rand.NewPCG(serviceTraceSeed, uint64(client)+1))
	var zipf *rand.Zipf
	if profile.zipf {
		zipf = rand.NewZipf(rng, 1.2, 1, serviceKeyCount-1)
	}
	trace := make([]serviceTraceEntry, serviceTraceLength)
	for i := range trace {
		entry := serviceTraceEntry{keyID: -1, doChan: rng.IntN(5) == 0}
		if rng.IntN(100) >= profile.uniquePercent {
			switch {
			case profile.zipf:
				entry.keyID = int(zipf.Uint64())
			case profile.hotset && rng.IntN(10) < 9:
				entry.keyID = rng.IntN(32)
			default:
				entry.keyID = rng.IntN(serviceKeyCount)
			}
		}
		trace[i] = entry
	}
	return trace
}

type serviceKey struct {
	key   string
	token uint64
}

func makeServiceKeys() []serviceKey {
	keys := make([]serviceKey, serviceKeyCount)
	for i := range keys {
		// Values above the runtime's small-integer boxing cache.
		token := uint64(i) + 256
		keys[i] = serviceKey{key: fmt.Sprintf("item:%016x", token), token: token}
	}
	return keys
}

func uniqueServiceKey(client, sequence, clients int) serviceKey {
	// The high bit separates unique keys from the reusable pool. Fixed-width
	// hexadecimal keys have the same length in both populations.
	token := uint64(1)<<63 | (uint64(sequence)*uint64(clients) + uint64(client))
	var buf [32]byte
	copy(buf[:], "item:")
	key := string(strconv.AppendUint(buf[:5], token, 16))
	return serviceKey{key: key, token: token}
}

type serviceCallers struct {
	do     benchmarkCaller
	doChan benchmarkCaller // Blocks for the public result, on the same Group.
}

type serviceImplementation struct {
	name string
	new  func(maphash.Seed) serviceCallers
}

func serviceGroupCallers(g *Group) serviceCallers {
	return serviceCallers{
		do: g.Do,
		doChan: func(key string, fn func() (any, error)) (any, error, bool) {
			r := <-g.DoChan(key, fn)
			return r.Val, r.Err, r.Shared
		},
	}
}

func serviceImplementations() []serviceImplementation {
	return []serviceImplementation{
		{
			name: "WaitGroup",
			new: func(seed maphash.Seed) serviceCallers {
				g := New(DefaultShards)
				g.seed = seed
				return serviceGroupCallers(g)
			},
		},
		{
			name: "Channel",
			new: func(seed maphash.Seed) serviceCallers {
				g := NewWithOptions(DefaultShards, Options{WaitMode: ChannelMode})
				g.seed = seed
				return serviceGroupCallers(g)
			},
		},
		{
			name: "Cond",
			new: func(seed maphash.Seed) serviceCallers {
				g := newCondGroup(DefaultShards)
				g.seed = seed
				return serviceCallers{
					do: g.Do,
					doChan: func(key string, fn func() (any, error)) (any, error, bool) {
						r := <-g.DoChan(key, fn)
						return r.Val, r.Err, r.Shared
					},
				}
			},
		},
		{
			name: "XSync",
			new: func(_ maphash.Seed) serviceCallers {
				g := new(xsingleflight.Group)
				return serviceCallers{
					do: g.Do,
					doChan: func(key string, fn func() (any, error)) (any, error, bool) {
						r := <-g.DoChan(key, fn)
						return r.Val, r.Err, r.Shared
					},
				}
			},
		},
	}
}

// Bounded-memory histogram: 16 subdivisions per power of two. Percentiles
// report bucket upper bounds (at most ~6.25% rounding), not exact samples.
type serviceLatency struct {
	buckets [1 + 64*16]uint64
	count   uint64
}

func (h *serviceLatency) add(ns uint64) {
	bucket := 0
	if ns != 0 {
		exponent := bits.Len64(ns) - 1
		base := uint64(1) << exponent
		var fraction uint64
		if exponent < 4 {
			fraction = (ns - base) << (4 - exponent)
		} else {
			fraction = (ns - base) >> (exponent - 4)
		}
		bucket = 1 + exponent*16 + int(fraction)
	}
	h.buckets[bucket]++
	h.count++
}

func (h *serviceLatency) merge(other *serviceLatency) {
	for i, n := range other.buckets {
		h.buckets[i] += n
	}
	h.count += other.count
}

func serviceLatencyUpperBound(bucket int) uint64 {
	if bucket == 0 {
		return 0
	}
	exponent, fraction := (bucket-1)/16, uint64((bucket-1)%16)
	base := uint64(1) << exponent
	if exponent < 4 {
		return base + (fraction >> (4 - exponent))
	}
	step := base >> 4
	return base + fraction*step + step - 1
}

func (h *serviceLatency) percentile(percent uint64) uint64 {
	if h.count == 0 {
		return 0
	}
	rank := (h.count*percent + 99) / 100
	var seen uint64
	for i, n := range h.buckets {
		seen += n
		if seen >= rank {
			return serviceLatencyUpperBound(i)
		}
	}
	panic("singleflight benchmark: invalid latency histogram")
}

type serviceClientStats struct {
	requests, backend, shared, unique, async, invalid, slowBackend uint64
	backendNs                                                      uint64
	latency, backendLatency                                        serviceLatency
}

func runServiceBenchmark(b *testing.B, callers serviceCallers, profile serviceProfile, clients int, io bool) {
	b.Helper()
	keys := makeServiceKeys()
	traces := make([][]serviceTraceEntry, clients)
	stats := make([]serviceClientStats, clients)
	for id := range traces {
		traces[id] = makeServiceTrace(profile, id)
	}
	// Initialize the registries and exercise both APIs before timing.
	for _, do := range []benchmarkCaller{callers.do, callers.doChan} {
		if v, err, shared := do("service-warmup", cheapWork); v != uint64(42) || err != nil || shared {
			b.Fatal("invalid service warmup")
		}
	}
	cpuResult := cpuWork()
	var ready, done sync.WaitGroup
	ready.Add(clients)
	done.Add(clients)
	start := make(chan struct{})
	for id := 0; id < clients; id++ {
		go func() {
			defer done.Done()
			s := &stats[id]
			var current serviceKey
			// This client starts at most one producer at a time. Both Do and the
			// public DoChan result synchronize its backend stats before reuse.
			fn := func() (any, error) {
				token := current.token
				began := time.Now()
				s.backend++
				if io {
					delay := time.Millisecond
					if token%10 == 0 {
						delay = 5 * time.Millisecond
						s.slowBackend++
					}
					time.Sleep(delay) // Requested delay; actual time is measured.
				} else {
					token ^= cpuWork()
				}
				ns := uint64(time.Since(began))
				s.backendNs += ns
				s.backendLatency.add(ns)
				return token, nil
			}
			ready.Done()
			<-start
			// Exactly b.N calls, divided statically. Each implementation sees
			// the same per-client prefix for equal N; scheduler order may vary.
			for sequence, i := 0, id; i < b.N; sequence, i = sequence+1, i+clients {
				entry := traces[id][sequence%serviceTraceLength]
				if entry.keyID < 0 {
					current = uniqueServiceKey(id, sequence, clients)
					s.unique++
				} else {
					current = keys[entry.keyID]
				}
				want := current.token
				if !io {
					want ^= cpuResult
				}
				do := callers.do
				if entry.doChan {
					do = callers.doChan
					s.async++
				}
				began := time.Now()
				v, err, shared := do(current.key, fn)
				s.latency.add(uint64(time.Since(began)))
				if v != want || err != nil {
					s.invalid++
				}
				if shared {
					s.shared++
				}
				s.requests++
			}
		}()
	}
	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	var total serviceClientStats
	for i := range stats {
		s := &stats[i]
		total.requests += s.requests
		total.backend += s.backend
		total.shared += s.shared
		total.unique += s.unique
		total.async += s.async
		total.invalid += s.invalid
		total.slowBackend += s.slowBackend
		total.backendNs += s.backendNs
		total.latency.merge(&s.latency)
		total.backendLatency.merge(&s.backendLatency)
	}
	if total.requests != uint64(b.N) || total.latency.count != total.requests ||
		total.backendLatency.count != total.backend || total.backend < total.unique ||
		total.backend > total.requests || total.invalid != 0 {
		b.Fatalf("invalid service run: N=%d requests=%d backend=%d unique=%d invalid=%d",
			b.N, total.requests, total.backend, total.unique, total.invalid)
	}
	n := float64(total.requests)
	b.ReportMetric(n/b.Elapsed().Seconds(), "requests/s")
	b.ReportMetric(float64(clients), "clients")
	b.ReportMetric(float64(total.backend)/n, "backend/op")
	b.ReportMetric(float64(total.shared)/n, "shared/op")
	b.ReportMetric(float64(total.unique)/n, "unique/op")
	b.ReportMetric(float64(total.async)/n, "dochan/op")
	b.ReportMetric(float64(total.invalid)/n, "errors/op")
	b.ReportMetric(float64(total.slowBackend)/n, "slow-backend/op")
	b.ReportMetric(float64(total.latency.buckets[0])/n, "latency-zero/op")
	b.ReportMetric(float64(total.backendLatency.buckets[0])/float64(total.backend), "backend-zero/exec")
	b.ReportMetric(float64(total.backendNs)/float64(total.backend), "backend-mean-ns")
	b.ReportMetric(float64(total.backendLatency.percentile(95)), "backend-p95-ns")
	for _, p := range []uint64{50, 95, 99} {
		b.ReportMetric(float64(total.latency.percentile(p)), fmt.Sprintf("p%d-ns", p))
	}
}

// Saturated, bounded, closed-loop clients, not an open-loop arrival-rate test.
// Every client chooses another key immediately after its previous result.
func BenchmarkServiceFlood(b *testing.B) {
	for _, profile := range serviceProfiles {
		b.Run(profile.name, func(b *testing.B) {
			for _, backend := range []struct {
				name string
				io   bool
			}{
				{"CPU", false},
				{"IOMixed", true},
			} {
				b.Run(backend.name, func(b *testing.B) {
					for _, clients := range []int{64, 1024} {
						b.Run(fmt.Sprintf("Clients%d", clients), func(b *testing.B) {
							seed := maphash.MakeSeed()
							for _, impl := range serviceImplementations() {
								b.Run(impl.name, func(b *testing.B) {
									runServiceBenchmark(b, impl.new(seed), profile, clients, backend.io)
								})
							}
						})
					}
				})
			}
		})
	}
}
