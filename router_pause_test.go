package router

import (
	"errors"
	"testing"
)

// The cancellation boundary is exact: every message the consumer received
// before Unsubscribe returns stays received, and not a single further
// message is delivered afterwards — the unreceived backlog is discarded
// and accounted for, while other subscriptions keep their own progress.
func TestCancellationBoundaryWithFastAndSlowReceivers(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}

	// The slow receiver pauses; the fast one keeps up with every message.
	if err := router.Pause("slow"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	fastGot := receive(t, fast, 3)
	for index, id := range []string{"m1", "m2", "m3"} {
		if fastGot[index].Message.ID() != id {
			t.Fatalf("fast delivery %d = %q, want %q", index, fastGot[index].Message.ID(), id)
		}
	}

	// Cancelling the paused slow subscription discards its whole backlog:
	// nothing is delivered after Unsubscribe returns, and every discarded
	// message is explicitly recorded.
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if got := drain(slow); len(got) != 0 {
		t.Fatalf("slow deliveries after cancellation = %#v, want none", got)
	}
	cancelled := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Kind == RecordDropped && record.Code == "subscription_cancelled" {
			cancelled[record.MessageID] = true
			if record.SubscriptionID != "slow" {
				t.Fatalf("cancel record subscription = %q, want slow", record.SubscriptionID)
			}
		}
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		if !cancelled[id] {
			t.Fatalf("message %q missing from cancellation records: %v", id, cancelled)
		}
	}

	// The fast subscription is untouched by the slow one's cancellation.
	if err := router.Publish(mustMessage(t, "m4", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, fast, 1); got[0].Message.ID() != "m4" {
		t.Fatalf("fast delivery after slow cancellation = %q, want m4", got[0].Message.ID())
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
}

// Pause, publish, resume: the replay report names exactly what can be
// replayed and which messages the subscription's own retention policy
// evicted during the pause, and the replayed messages then arrive in
// order on the original channel.
func TestPauseResumeReplayReportMatchesDelivery(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(3), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}

	// One message is received before the pause; it is the consumer's and
	// never appears in any report again.
	if err := router.Publish(mustMessage(t, "before", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, sub, 1); got[0].Message.ID() != "before" {
		t.Fatalf("pre-pause delivery = %q, want before", got[0].Message.ID())
	}

	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"p1", "p2", "p3", "p4", "p5"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	report, err := router.Resume("sub")
	if err != nil {
		t.Fatal(err)
	}
	wantReplay := []string{"p3", "p4", "p5"}
	if len(report.Replayable) != len(wantReplay) {
		t.Fatalf("replayable = %v, want %v", report.Replayable, wantReplay)
	}
	for index, id := range wantReplay {
		if report.Replayable[index] != id {
			t.Fatalf("replayable %d = %q, want %q", index, report.Replayable[index], id)
		}
	}
	if len(report.Evicted) != 2 || report.Evicted[0] != "p1" || report.Evicted[1] != "p2" {
		t.Fatalf("evicted = %v, want [p1 p2]", report.Evicted)
	}

	got := receive(t, sub, 3)
	for index, id := range wantReplay {
		if got[index].Message.ID() != id {
			t.Fatalf("replayed delivery %d = %q, want %q", index, got[index].Message.ID(), id)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	// The evicted gaps are also in the audit history with their reason.
	evicted := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Kind == RecordDropped && record.Code == "subscriber_buffer_evicted" {
			evicted[record.MessageID] = true
		}
	}
	if !evicted["p1"] || !evicted["p2"] || len(evicted) != 2 {
		t.Fatalf("eviction records = %v, want p1 and p2", evicted)
	}
}

// Pause, resume, and policy changes interleaved with cancellation: a
// resubscription under the same ID is a brand-new subscription — no
// leftover message from the old one migrates into it, the rule version
// restarts at 1, and the old backlog is recorded as cancelled.
func TestCancelResubscribeDoesNotCarryBacklog(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	first, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.SetOverflowPolicy("sub", KeepNewest); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old1", "old2"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	// Cancel while paused: the paused backlog is discarded, never replayed.
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if got := drain(first); len(got) != 0 {
		t.Fatalf("first subscription delivered after cancellation: %#v", got)
	}

	second, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "new1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	got := receive(t, second, 1)
	if got[0].Message.ID() != "new1" {
		t.Fatalf("resubscribed delivery = %q, want new1 only", got[0].Message.ID())
	}
	if got[0].RuleVersion != 1 {
		t.Fatalf("resubscribed rule version = %d, want fresh version 1", got[0].RuleVersion)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	cancelled := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Kind == RecordDropped && record.Code == "subscription_cancelled" {
			cancelled[record.MessageID] = true
		}
	}
	if !cancelled["old1"] || !cancelled["old2"] || len(cancelled) != 2 {
		t.Fatalf("cancelled backlog = %v, want old1 and old2", cancelled)
	}
}

// A paused slow subscriber never blocks the publisher or other
// subscribers, even while its buffer overflows.
func TestPausedSlowSubscriberNeverBlocksPublisher(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(1))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := router.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(64))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("slow"); err != nil {
		t.Fatal(err)
	}
	const total = 32
	for index := 0; index < total; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if got := receive(t, fast, total); len(got) != total {
		t.Fatalf("fast deliveries = %d, want %d", len(got), total)
	}
	report, err := router.Resume("slow")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Replayable) != 1 || report.Replayable[0] != "a" {
		t.Fatalf("replayable = %v, want [a] under keep_oldest", report.Replayable)
	}
	if len(report.Evicted) != total-1 {
		t.Fatalf("evicted = %d, want %d", len(report.Evicted), total-1)
	}
	if got := receive(t, slow, 1); got[0].Message.ID() != "a" {
		t.Fatalf("slow replay = %q, want a", got[0].Message.ID())
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
}

// Pause and Resume validate their targets and state.
func TestPauseResumeValidation(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause(""); err == nil {
		t.Fatal("empty pause id unexpectedly accepted")
	}
	if _, err := router.Resume(""); err == nil {
		t.Fatal("empty resume id unexpectedly accepted")
	}
	if err := router.Pause("missing"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("pause missing error = %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.Resume("missing"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("resume missing error = %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.Resume("sub"); !errors.Is(err, ErrSubscriptionNotPaused) {
		t.Fatalf("resume active error = %v, want ErrSubscriptionNotPaused", err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); !errors.Is(err, ErrSubscriptionPaused) {
		t.Fatalf("double pause error = %v, want ErrSubscriptionPaused", err)
	}
	if _, err := router.Resume("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("pause after cancel error = %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.Resume("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("resume after cancel error = %v, want ErrSubscriptionGone", err)
	}

	rejects := 0
	for _, record := range router.Records() {
		if record.Kind == RecordRejected {
			rejects++
		}
	}
	if rejects != 8 {
		t.Fatalf("reject records = %d, want 8", rejects)
	}
}

// A process restart means a brand-new router: nothing survives, and a
// subscription with the same ID sees only messages published after the
// restart, with rule versions back at 1.
func TestProcessRestartStartsClean(t *testing.T) {
	boot := func() *Router {
		router, err := NewRouter()
		if err != nil {
			t.Fatal(err)
		}
		return router
	}

	first := boot()
	sub, err := first.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(mustMessage(t, "before-restart", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := first.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if got := drain(sub); len(got) != 0 {
		t.Fatalf("deliveries after cancellation = %#v, want none", got)
	}

	// Restart: a new router has no memory of the previous process.
	second := boot()
	restarted, err := second.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Publish(mustMessage(t, "after-restart", "t", nil)); err != nil {
		t.Fatal(err)
	}
	got := receive(t, restarted, 1)
	if got[0].Message.ID() != "after-restart" || got[0].RuleVersion != 1 {
		t.Fatalf("restarted delivery = %q at version %d, want after-restart at 1",
			got[0].Message.ID(), got[0].RuleVersion)
	}
	if err := second.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}
