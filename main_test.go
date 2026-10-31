package main

import (
	"reflect"
	"testing"
	"time"

	m "github.com/plusev-terminal/go-plugin-common/meta"
)

// TestRateLimitsPinned pins the manifest rate-limit table so the declared
// numbers can't silently drift. Kraken public endpoints: 1 request per
// second, rate limited by IP (support.kraken.com, retrieved 2026-10-31).
func TestRateLimitsPinned(t *testing.T) {
	targets := (&KrakenOHLCVPlugin{}).GetMeta().Resources.AllowedNetworkTargets
	if len(targets) != 1 {
		t.Fatalf("expected 1 network target, got %d", len(targets))
	}

	tgt := targets[0]
	if tgt.Pattern != "https://api.kraken.com/*" {
		t.Errorf("pattern = %q, want api.kraken.com", tgt.Pattern)
	}

	if len(tgt.Limits) != 1 {
		t.Fatalf("expected 1 limit rule, got %d", len(tgt.Limits))
	}

	// 1 req/s with the default burst (= Limit = 1): the doc gives a
	// per-second frequency with no bucket size, so no extra burst headroom.
	want := m.NewRateLimitRule("public", 1, time.Second).WithLabel("Public endpoints")
	got := tgt.Limits[0]
	if !reflect.DeepEqual(got, want) {
		t.Errorf("limit rule = %+v, want %+v", got, want)
	}

	// Manifest limits declared => legacy per-command limits must be an
	// explicit opt-out (empty, not nil).
	if limits := (&KrakenOHLCVPlugin{}).GetRateLimits(); limits == nil || len(limits) != 0 {
		t.Errorf("GetRateLimits() = %v, want empty non-nil slice", limits)
	}
}
