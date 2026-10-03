package singleflight

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestServiceTrace(t *testing.T) {
	for _, profile := range serviceProfiles {
		t.Run(profile.name, func(t *testing.T) {
			a := makeServiceTrace(profile, 7)
			if !reflect.DeepEqual(a, makeServiceTrace(profile, 7)) {
				t.Fatal("trace is not reproducible")
			}
			if reflect.DeepEqual(a, makeServiceTrace(profile, 8)) {
				t.Fatal("clients have identical traces")
			}
			var unique, async, hot int
			for _, entry := range a {
				if entry.keyID < -1 || entry.keyID >= serviceKeyCount {
					t.Fatalf("invalid key index: %d", entry.keyID)
				}
				if entry.keyID == -1 {
					unique++
				} else if entry.keyID < 32 {
					hot++
				}
				if entry.doChan {
					async++
				}
			}
			// Broad deterministic guards, not statistical quality claims.
			if unique < serviceTraceLength*profile.uniquePercent/200 ||
				unique > serviceTraceLength*profile.uniquePercent/100+serviceTraceLength/10 ||
				async < serviceTraceLength/10 || async > serviceTraceLength*3/10 {
				t.Fatalf("unexpected trace composition: unique=%d async=%d", unique, async)
			}
			if (profile.zipf || profile.hotset) && hot < serviceTraceLength/3 {
				t.Fatalf("missing popular-key skew: hot=%d", hot)
			}
		})
	}
}

func TestServiceUniqueKeys(t *testing.T) {
	seen := make(map[string]bool)
	for _, key := range makeServiceKeys() {
		seen[key.key] = true
	}
	// More than one trace cycle; no unique key may turn into a duplicate.
	for sequence := 0; sequence < serviceTraceLength+1; sequence++ {
		for client := 0; client < 4; client++ {
			key := uniqueServiceKey(client, sequence, 4)
			if seen[key.key] || len(key.key) != 21 || key.token>>63 != 1 {
				t.Fatalf("non-unique or invalid key: %+v", key)
			}
			seen[key.key] = true
		}
	}
}

func TestServiceLatency(t *testing.T) {
	var empty serviceLatency
	if empty.percentile(99) != 0 {
		t.Fatal("empty histogram")
	}
	for _, ns := range []uint64{0, 1, 2, 15, 16, 17, 31, 32, 1000, 1_000_000, math.MaxInt64, math.MaxUint64} {
		var h serviceLatency
		h.add(ns)
		upper := h.percentile(99)
		if upper < ns || (ns > 0 && float64(upper)/float64(ns) > 1.0625) {
			t.Fatalf("invalid bucket upper bound: ns=%d upper=%d", ns, upper)
		}
	}
	var a, b serviceLatency
	for i := uint64(1); i <= 100; i++ {
		if i <= 50 {
			a.add(i)
		} else {
			b.add(i)
		}
	}
	a.merge(&b)
	if a.count != 100 || a.percentile(50) != 51 || a.percentile(95) != 95 || a.percentile(99) != 99 {
		t.Fatalf("invalid merged percentiles: count=%d p50=%d p95=%d p99=%d",
			a.count, a.percentile(50), a.percentile(95), a.percentile(99))
	}
}

// Diagnostic only: low clock resolution is not a library correctness failure.
func TestServiceClockResolution(t *testing.T) {
	last := time.Now()
	minimum := time.Duration(math.MaxInt64)
	var zero int
	const samples = 100_000
	for i := 0; i < samples; i++ {
		now := time.Now()
		delta := now.Sub(last)
		if delta == 0 {
			zero++
		} else if delta > 0 && delta < minimum {
			minimum = delta
		}
		last = now
	}
	t.Logf("time.Now consecutive reads: samples=%d zero=%d minimum-positive=%s", samples, zero, minimum)
}
