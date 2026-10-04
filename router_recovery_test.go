package router

import (
	"errors"
	"path/filepath"
	"testing"
)

func stateFile(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "router-state.json")
}

func recoveryReport(t *testing.T, router *Router, id string) RecoveryReport {
	t.Helper()
	for _, report := range router.RecoveryReports() {
		if report.SubscriptionID == id {
			return report
		}
	}
	t.Fatalf("no recovery report for %q", id)
	return RecoveryReport{}
}

// A paused slow subscriber's progress survives a restart: the recovery
// report names exactly what can still be replayed and which messages the
// retention policy evicted, each gap keeping the reason, the matched
// rules, and the policy in effect when it was created. The fast
// subscriber and the publisher are never held back by the slow one.
func TestRestartRecoversPausedSubscription(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{
		{ID: "all", Topic: "t"},
		{ID: "crit", Topic: "t", Attrs: map[string]string{"sev": "crit"}},
	}, WithSubscriptionBufferSize(3), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Pause("slow"); err != nil {
		t.Fatal(err)
	}
	ids := []string{"m1", "m2", "m3", "m4", "m5"}
	for _, id := range ids {
		if err := router.Publish(mustMessage(t, id, "t", map[string]string{"sev": "crit"})); err != nil {
			t.Fatal(err)
		}
	}
	// The publisher never waited on the paused slow subscriber: the fast
	// one received everything already.
	if got := receive(t, fast, 5); len(got) != 5 {
		t.Fatalf("fast deliveries = %d, want 5", len(got))
	}

	// Simulate a restart: a brand-new router over the same state file.
	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	report := recoveryReport(t, restarted, "slow")
	if !report.Paused {
		t.Fatal("slow subscription should still be paused after recovery")
	}
	wantReplay := []string{"m3", "m4", "m5"}
	if len(report.Replayable) != len(wantReplay) {
		t.Fatalf("replayable = %v, want %v", report.Replayable, wantReplay)
	}
	for index, id := range wantReplay {
		if report.Replayable[index] != id {
			t.Fatalf("replayable %d = %q, want %q", index, report.Replayable[index], id)
		}
	}
	if len(report.Gaps) != 2 {
		t.Fatalf("gaps = %#v, want m1 and m2", report.Gaps)
	}
	for index, id := range []string{"m1", "m2"} {
		gap := report.Gaps[index]
		if gap.MessageID != id {
			t.Fatalf("gap %d = %q, want %q", index, gap.MessageID, id)
		}
		if gap.Code != "subscriber_buffer_evicted" || gap.Reason == "" {
			t.Fatalf("gap %q missing reason: %#v", id, gap)
		}
		if gap.Policy != KeepNewest {
			t.Fatalf("gap %q policy = %q, want keep_newest", id, gap.Policy)
		}
		if len(gap.RuleIDs) != 2 || gap.RuleIDs[0] != "all" || gap.RuleIDs[1] != "crit" {
			t.Fatalf("gap %q rules = %v, want [all crit]", id, gap.RuleIDs)
		}
		if gap.RuleVersion != 1 {
			t.Fatalf("gap %q rule version = %d, want 1", id, gap.RuleVersion)
		}
	}

	// The recovered subscription is still paused; Resume replays exactly
	// the recovered backlog on the live channel, and its report agrees
	// with the recovery report.
	sub, ok := restarted.Subscription("slow")
	if !ok {
		t.Fatal("recovered slow subscription not found")
	}
	report2, err := restarted.Resume("slow")
	if err != nil {
		t.Fatal(err)
	}
	if len(report2.Replayable) != 3 || len(report2.Evicted) != 2 || len(report2.Gaps) != 2 {
		t.Fatalf("resume report after restart = %#v", report2)
	}
	replayed := receive(t, sub, 3)
	for index, id := range wantReplay {
		if replayed[index].Message.ID() != id {
			t.Fatalf("replayed %d = %q, want %q", index, replayed[index].Message.ID(), id)
		}
		if replayed[index].RuleVersion != 1 {
			t.Fatalf("replayed %q rule version = %d, want 1", id, replayed[index].RuleVersion)
		}
	}
	if err := restarted.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
	_ = slow
}

// Changing the retention policy while paused must not rewrite gaps that
// already exist: each gap keeps the policy that produced it, across a
// restart.
func TestRestartKeepsGapPolicyHistory(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(2), WithOverflowPolicy(KeepOldest)); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	// KeepOldest: m1, m2 buffered; m3 dropped under keep_oldest.
	for _, id := range []string{"m1", "m2", "m3"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.SetOverflowPolicy("sub", KeepNewest); err != nil {
		t.Fatal(err)
	}
	// KeepNewest: m4 evicts m1 under keep_newest.
	if err := router.Publish(mustMessage(t, "m4", "t", nil)); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	report := recoveryReport(t, restarted, "sub")
	if len(report.Gaps) != 2 {
		t.Fatalf("gaps = %#v, want m3 and m1", report.Gaps)
	}
	if report.Gaps[0].MessageID != "m3" || report.Gaps[0].Policy != KeepOldest ||
		report.Gaps[0].Code != "subscriber_buffer_full" {
		t.Fatalf("first gap rewritten by policy switch: %#v", report.Gaps[0])
	}
	if report.Gaps[1].MessageID != "m1" || report.Gaps[1].Policy != KeepNewest ||
		report.Gaps[1].Code != "subscriber_buffer_evicted" {
		t.Fatalf("second gap = %#v, want m1 evicted under keep_newest", report.Gaps[1])
	}
	if len(report.Replayable) != 2 || report.Replayable[0] != "m2" || report.Replayable[1] != "m4" {
		t.Fatalf("replayable = %v, want [m2 m4]", report.Replayable)
	}
	if err := restarted.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}

// A cancelled subscription is a hard boundary across restarts too: it is
// not revived, and a same-name resubscribe after the restart inherits
// nothing from the old backlog.
func TestRestartDoesNotReviveCancelledSubscription(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	first, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old1", "old2"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if got := drain(first); len(got) != 0 {
		t.Fatalf("cancelled subscription delivered: %#v", got)
	}

	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if reports := restarted.RecoveryReports(); len(reports) != 0 {
		t.Fatalf("cancelled subscription revived: %#v", reports)
	}
	if _, err := restarted.Resume("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("resume of cancelled subscription = %v, want ErrSubscriptionGone", err)
	}

	// A same-name resubscribe is fresh: version 1, no inherited backlog.
	second, err := restarted.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Publish(mustMessage(t, "new1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	got := receive(t, second, 1)
	if got[0].Message.ID() != "new1" || got[0].RuleVersion != 1 {
		t.Fatalf("resubscribed delivery = %#v, want new1 at version 1", got[0])
	}
	if err := restarted.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}

// Messages already handed to the consumer are the consumer's: after a
// restart only the still-queued messages are replayed, never the ones
// already received.
func TestRestartReplaysOnlyUnreceivedBacklog(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	// m1 and m2 are received; m3 and m4 are still queued.
	if got := receive(t, sub, 2); got[0].Message.ID() != "m1" || got[1].Message.ID() != "m2" {
		t.Fatalf("pre-restart deliveries = %#v, want m1 and m2", got)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	report := recoveryReport(t, restarted, "sub")
	if len(report.Replayable) != 2 || report.Replayable[0] != "m3" || report.Replayable[1] != "m4" {
		t.Fatalf("replayable = %v, want only the unreceived [m3 m4]", report.Replayable)
	}
	if len(report.Gaps) != 0 {
		t.Fatalf("unexpected gaps: %#v", report.Gaps)
	}
	sub, ok := restarted.Subscription("sub")
	if !ok {
		t.Fatal("recovered subscription not found")
	}
	resumeReport, err := restarted.Resume("sub")
	if err != nil {
		t.Fatal(err)
	}
	if len(resumeReport.Replayable) != 2 {
		t.Fatalf("resume replayable = %v, want [m3 m4]", resumeReport.Replayable)
	}
	got := receive(t, sub, 2)
	if got[0].Message.ID() != "m3" || got[1].Message.ID() != "m4" {
		t.Fatalf("replayed deliveries = %#v, want m3 and m4", got)
	}
	if err := restarted.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}
