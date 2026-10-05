package router

import (
	"errors"
	"path/filepath"
	"testing"
)

func bootStateful(t *testing.T, name string) (*Router, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+`.jsonl`)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	return router, path
}

func reopenStateful(t *testing.T, path string) *Router {
	t.Helper()
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	return router
}

// Paused state survives restart, including retained messages and the exact
// matched rule snapshot/reason for each historical gap. A later retention
// change cannot reinterpret an existing gap.
func TestPausedSubscriptionRestoresBacklogAndImmutableGaps(t *testing.T) {
	first, path := bootStateful(t, "pause")
	_, err := first.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(1), WithOverflowPolicy(KeepOldest))
	if err != nil {
		t.Fatal(err)
	}
	fast, err := first.Subscribe("fast", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Pause("slow"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		if err := first.Publish(mustMessage(t, id, "t", map[string]string{"env": "prod"})); err != nil {
			t.Fatal(err)
		}
	}
	if got := receive(t, fast, 4); len(got) != 4 {
		t.Fatalf("fast received %d messages, want 4", len(got))
	}
	if err := first.SetOverflowPolicy("slow", KeepNewest); err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(mustMessage(t, "m5", "t", map[string]string{"env": "prod"})); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, fast, 1); len(got) != 1 {
		t.Fatalf("fast received %d messages, want 1", len(got))
	}
	if err := first.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := reopenStateful(t, path)
	restored, err := second.Restore("slow")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Subscribe("slow", []Rule{{ID: "other", Topic: "x"}}); !errors.Is(err, ErrSubscriptionExists) {
		t.Fatalf("restored duplicate Subscribe error = %v, want ErrSubscriptionExists", err)
	}
	report, err := second.Resume("slow")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Replayable) != 1 || report.Replayable[0] != "m5" {
		t.Fatalf("replayable after restart = %v, want [m5]", report.Replayable)
	}
	wantGaps := map[string]string{
		"m1": "subscriber_buffer_evicted",
		"m2": "subscriber_buffer_full",
		"m3": "subscriber_buffer_full",
		"m4": "subscriber_buffer_full",
	}
	if len(report.Gaps) != len(wantGaps) {
		t.Fatalf("gaps = %#v, want %d", report.Gaps, len(wantGaps))
	}
	for _, gap := range report.Gaps {
		wantCode, ok := wantGaps[gap.Delivery.Message.ID()]
		if !ok {
			t.Fatalf("unexpected gap %#v", gap)
		}
		if gap.Code != wantCode || gap.Delivery.RuleVersion != 1 || len(gap.Delivery.RuleIDs) != 1 || gap.Delivery.RuleIDs[0] != "topic" {
			t.Fatalf("gap metadata was not restored: %#v, want code %q", gap, wantCode)
		}
		if value, _ := gap.Delivery.Message.Attribute("env"); value != "prod" || gap.Reason == "" {
			t.Fatalf("gap message/reason was not restored: %#v", gap)
		}
	}
	got := receive(t, restored, 1)
	if got[0].Message.ID() != "m5" {
		t.Fatalf("restored deliveries = %#v, want m5", got)
	}
	if err := second.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
}

// A completed cancellation is a durable tombstone. Reopening cannot restore
// it, while a later same-ID subscription starts as a distinct instance and
// inherits none of the old backlog.
func TestCancellationTombstoneAndSameNameResubscriptionAfterRestart(t *testing.T) {
	first, path := bootStateful(t, "cancel")
	old, err := first.Subscribe("sub", []Rule{{ID: "old", Topic: "old"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(mustMessage(t, "old1", "old", nil)); err != nil {
		t.Fatal(err)
	}
	if err := first.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if got := drain(old); len(got) != 0 {
		t.Fatalf("old channel delivered %#v after cancellation", got)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := reopenStateful(t, path)
	if _, err := second.Restore("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("Restore canceled error = %v, want ErrSubscriptionGone", err)
	}
	fresh, err := second.Subscribe("sub", []Rule{{ID: "new", Topic: "new"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Publish(mustMessage(t, "new1", "new", nil)); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, fresh, 1); got[0].Message.ID() != "new1" || got[0].RuleVersion != 1 {
		t.Fatalf("fresh delivery = %#v, want new1 at rule version 1", got)
	}
	if err := second.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	third := reopenStateful(t, path)
	if _, err := third.Restore("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("Restore second canceled error = %v, want ErrSubscriptionGone", err)
	}
}

// An active slow subscriber's unreceived queue also survives a restart; a
// message already handed to a consumer is not persisted again.
func TestActiveSlowSubscriberRestoresOnlyUnreceivedQueue(t *testing.T) {
	first, path := bootStateful(t, "active")
	sub, err := first.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		if err := first.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	got := receive(t, sub, 1)
	if got[0].Message.ID() != "m1" {
		t.Fatalf("received %q, want m1", got[0].Message.ID())
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := reopenStateful(t, path)
	restored, err := second.Restore("slow")
	if err != nil {
		t.Fatal(err)
	}
	got = receive(t, restored, 3)
	want := []string{"m2", "m3", "m4"}
	for index, id := range want {
		if got[index].Message.ID() != id {
			t.Fatalf("restored delivery %d = %q, want %q: %#v", index, got[index].Message.ID(), id, got)
		}
	}
	if err := second.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
}

// Close does not wait for a slow consumer to take the handoff message. That
// message remains unreceived and is therefore replayed after reopening.
func TestCloseRetainsInFlightMessageForSlowConsumer(t *testing.T) {
	first, path := bootStateful(t, "handoff")
	sub, err := first.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	_ = sub
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := reopenStateful(t, path)
	restored, err := second.Restore("slow")
	if err != nil {
		t.Fatal(err)
	}
	if got := receive(t, restored, 1); got[0].Message.ID() != "m1" {
		t.Fatalf("restored handoff delivery = %#v, want m1", got)
	}
	if err := second.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
}
