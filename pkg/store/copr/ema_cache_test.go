// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"testing"
	"time"

	copr_metrics "github.com/pingcap/tidb/pkg/store/copr/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestEMACacheSharesPointerForSameKey(t *testing.T) {
	c := newEMACache(time.Second)
	a := c.GetOrCreate("plan-A", 4194304)
	b := c.GetOrCreate("plan-A", 1048576) // seed bytes ignored on hit
	require.Same(t, a, b, "same key returns shared pointer")
	require.Equal(t, uint64(4194304), a.Predict(),
		"second seed value is ignored — convergence carries over")
}

func TestEMACacheBypassesOnEmptyKey(t *testing.T) {
	c := newEMACache(time.Second)
	a := c.GetOrCreate("", 4194304)
	b := c.GetOrCreate("", 4194304)
	require.NotSame(t, a, b,
		"empty key bypasses cache — each call gets a fresh EMA")
	require.Zero(t, c.size(), "bypassed calls do not insert entries")
}

func TestEMACacheDistinctKeysAreIsolated(t *testing.T) {
	c := newEMACache(time.Second)
	a := c.GetOrCreate("plan-A", 4194304)
	b := c.GetOrCreate("plan-B", 1048576)
	require.NotSame(t, a, b)
	require.Equal(t, uint64(4194304), a.Predict())
	require.Equal(t, uint64(1048576), b.Predict())
}

func TestEMACacheReplacesExpiredOnAccess(t *testing.T) {
	c := newEMACache(50 * time.Millisecond)
	e := c.GetOrCreate("plan-A", 4194304)

	// Force lastObsAt into the past so the entry is older than the TTL.
	e.mu.Lock()
	e.lastObsAt = time.Now().Add(-time.Hour)
	e.mu.Unlock()

	// Next access for the same key should drop the stale entry and re-seed.
	fresh := c.GetOrCreate("plan-A", 1048576)
	require.NotSame(t, e, fresh, "expired entry replaced on next access")
	require.Equal(t, uint64(1048576), fresh.Predict(),
		"replacement is re-seeded with the new caller's seed")
	require.Equal(t, 1, c.size(), "single entry per key")
}

func TestEMACacheKeepsActiveEntries(t *testing.T) {
	c := newEMACache(time.Hour)
	e := c.GetOrCreate("plan-A", 1<<20)
	e.Observe(2<<20, time.Now())
	again := c.GetOrCreate("plan-A", 0)
	require.Same(t, e, again, "fresh activity keeps the entry alive")
}

func TestEMACacheCountersTickEachBranch(t *testing.T) {
	// Snapshot process-global counters, call GetOrCreate across all 4
	// outcomes, then assert the delta on each pre-bound counter. Counters
	// are monotonic so we only need before/after.
	snap := func() (hit, absent, expired, bypass float64) {
		return testutil.ToFloat64(copr_metrics.CoprEMACacheHit),
			testutil.ToFloat64(copr_metrics.CoprEMACacheMissAbsent),
			testutil.ToFloat64(copr_metrics.CoprEMACacheMissExpired),
			testutil.ToFloat64(copr_metrics.CoprEMACacheMissBypass)
	}
	h0, a0, e0, b0 := snap()

	c := newEMACache(50 * time.Millisecond)
	_ = c.GetOrCreate("", 1<<20)        // miss_bypass
	_ = c.GetOrCreate("plan-A", 1<<20)  // miss_absent
	_ = c.GetOrCreate("plan-A", 1<<20)  // hit
	// Force expiry on plan-A then look it up again.
	c.mu.Lock()
	c.m["plan-A"].ema.mu.Lock()
	c.m["plan-A"].ema.lastObsAt = time.Now().Add(-time.Hour)
	c.m["plan-A"].ema.mu.Unlock()
	c.mu.Unlock()
	_ = c.GetOrCreate("plan-A", 1<<20) // miss_expired

	h1, a1, e1, b1 := snap()
	require.Equal(t, 1.0, h1-h0, "hit tick")
	require.Equal(t, 1.0, a1-a0, "miss_absent tick")
	require.Equal(t, 1.0, e1-e0, "miss_expired tick")
	require.Equal(t, 1.0, b1-b0, "miss_bypass tick")
}
