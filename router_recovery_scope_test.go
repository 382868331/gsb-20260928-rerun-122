package router

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func pauseAndPublish(t *testing.T, router *Router, subID string, ids ...string) {
	t.Helper()
	if err := router.Pause(subID); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := router.Publish(mustMessage(t, id, "t", map[string]string{"sev": "crit"})); err != nil {
			t.Fatal(err)
		}
	}
}

func wantIDs(t *testing.T, got, want []string, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
	for index, id := range want {
		if got[index] != id {
			t.Fatalf("%s %d = %q, want %q", what, index, got[index], id)
		}
	}
}

// Previewing a recovery scope shows exactly what can still be replayed and
// the historical gaps, and it is read-only: nothing is marked delivered,
// so a restart still replays the whole backlog afterwards.
func TestPreviewRecoveryScopeIsReadOnly(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(3), WithOverflowPolicy(KeepNewest)); err != nil {
		t.Fatal(err)
	}
	pauseAndPublish(t, router, "sub", "m1", "m2", "m3", "m4", "m5")

	scope, err := router.PreviewRecovery("sub")
	if err != nil {
		t.Fatal(err)
	}
	var replayable []string
	for _, candidate := range scope.Replayable {
		replayable = append(replayable, candidate.MessageID)
		if candidate.EnqueuedAt.IsZero() {
			t.Fatalf("candidate %q has no enqueue time", candidate.MessageID)
		}
	}
	wantIDs(t, replayable, []string{"m3", "m4", "m5"}, "preview replayable")
	if len(scope.Gaps) != 2 || scope.Gaps[0].MessageID != "m1" || scope.Gaps[1].MessageID != "m2" {
		t.Fatalf("preview gaps = %#v, want m1 and m2", scope.Gaps)
	}

	// The preview itself delivered nothing: after a restart the very same
	// backlog is still replayable.
	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	report := recoveryReport(t, restarted, "sub")
	wantIDs(t, report.Replayable, []string{"m3", "m4", "m5"}, "replayable after restart")
	if len(report.Abandoned) != 0 || len(report.Decisions) != 0 {
		t.Fatalf("preview must not record decisions, got %#v / %#v", report.Abandoned, report.Decisions)
	}
}

// A confirmed selection restores only the chosen rules' messages; the rest
// of the in-scope backlog is abandoned with the user's reason, auditable
// both in the live records and after a restart — clearly distinct from
// retention-policy gaps.
func TestConfirmRecoverySelectsRulesAndAbandonsRemainder(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{
		{ID: "crit", Topic: "t", Attrs: map[string]string{"sev": "crit"}},
		{ID: "all", Topic: "t"},
	}, WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"c1", "c2"} {
		if err := router.Publish(mustMessage(t, id, "t", map[string]string{"sev": "crit"})); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"i1", "i2"} {
		if err := router.Publish(mustMessage(t, id, "t", map[string]string{"sev": "info"})); err != nil {
			t.Fatal(err)
		}
	}

	scope, err := router.PreviewRecovery("sub")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		RuleIDs:   []string{"crit"},
		Remainder: RemainderAbandon,
		Reason:    "only critical alerts matter after the outage",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs(t, outcome.Selected, []string{"c1", "c2"}, "selected")
	wantIDs(t, outcome.Abandoned, []string{"i1", "i2"}, "abandoned")

	// Only the selected messages are replayed on resume.
	if _, err := router.Resume("sub"); err != nil {
		t.Fatal(err)
	}
	got := receive(t, sub, 2)
	for index, id := range []string{"c1", "c2"} {
		if got[index].Message.ID() != id {
			t.Fatalf("replayed %d = %q, want %q", index, got[index].Message.ID(), id)
		}
	}

	// The abandonment is audited with the user's reason.
	found := 0
	for _, record := range router.Records() {
		if record.Code == "recovery_abandoned" {
			found++
			if !strings.Contains(record.Reason, "only critical alerts matter after the outage") {
				t.Fatalf("abandon record reason = %q", record.Reason)
			}
		}
	}
	if found != 2 {
		t.Fatalf("recovery_abandoned records = %d, want 2", found)
	}

	// After a restart the decision and the manual abandonment are still
	// explainable, and distinct from retention gaps.
	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	report := recoveryReport(t, restarted, "sub")
	if len(report.Abandoned) != 2 {
		t.Fatalf("abandoned after restart = %#v, want i1 and i2", report.Abandoned)
	}
	for index, id := range []string{"i1", "i2"} {
		abandoned := report.Abandoned[index]
		if abandoned.MessageID != id || abandoned.Reason != "only critical alerts matter after the outage" {
			t.Fatalf("abandoned %d = %#v", index, abandoned)
		}
	}
	if len(report.Gaps) != 0 {
		t.Fatalf("gaps = %#v, want none (abandonment is not a retention gap)", report.Gaps)
	}
	if len(report.Decisions) != 1 {
		t.Fatalf("decisions = %#v, want exactly one", report.Decisions)
	}
	decision := report.Decisions[0]
	wantIDs(t, decision.Selected, []string{"c1", "c2"}, "decision selected")
	wantIDs(t, decision.Abandoned, []string{"i1", "i2"}, "decision abandoned")
	if decision.Token != scope.Token || decision.Remainder != RemainderAbandon {
		t.Fatalf("decision = %#v", decision)
	}
}

// The remainder can also be kept as pending instead of abandoned: it is
// then delivered normally when the subscription resumes.
func TestConfirmRecoveryKeepsRemainderPending(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{
		{ID: "crit", Topic: "t", Attrs: map[string]string{"sev": "crit"}},
	}, WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	pauseAndPublish(t, router, "sub", "m1", "m2", "m3")

	scope, err := router.PreviewRecovery("sub")
	if err != nil {
		t.Fatal(err)
	}
	// Select only the messages enqueued from now on: nothing in the
	// current backlog qualifies, so everything stays pending.
	outcome, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		Since:     time.Now().Add(time.Hour),
		Remainder: RemainderKeepPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Selected) != 0 {
		t.Fatalf("selected = %v, want none", outcome.Selected)
	}
	wantIDs(t, outcome.KeptPending, []string{"m1", "m2", "m3"}, "kept pending")

	if _, err := router.Resume("sub"); err != nil {
		t.Fatal(err)
	}
	got := receive(t, sub, 3)
	for index, id := range []string{"m1", "m2", "m3"} {
		if got[index].Message.ID() != id {
			t.Fatalf("replayed %d = %q, want %q", index, got[index].Message.ID(), id)
		}
	}
}

// A confirmation only applies to the backlog seen at preview time: a
// message that arrives between preview and confirm is outside the scope
// and is handled by the subscription's normal conventions, never
// abandoned by the stale selection.
func TestConfirmRecoveryDoesNotTouchLaterArrivals(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	pauseAndPublish(t, router, "sub", "old1", "old2")

	scope, err := router.PreviewRecovery("sub")
	if err != nil {
		t.Fatal(err)
	}
	// A new message arrives after the preview but before the confirm.
	if err := router.Publish(mustMessage(t, "new1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	outcome, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		Since:     time.Now().Add(time.Hour), // selects nothing in scope
		Remainder: RemainderAbandon,
		Reason:    "stale backlog no longer relevant",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs(t, outcome.Abandoned, []string{"old1", "old2"}, "abandoned")
	if len(outcome.KeptPending) != 0 || len(outcome.Selected) != 0 {
		t.Fatalf("outcome = %#v", outcome)
	}

	// The late arrival survived the stale-token abandon and is delivered.
	if _, err := router.Resume("sub"); err != nil {
		t.Fatal(err)
	}
	got := receive(t, sub, 1)
	if got[0].Message.ID() != "new1" {
		t.Fatalf("replayed = %q, want new1", got[0].Message.ID())
	}
}

// Cancelling a subscription and resubscribing under the same name starts a
// completely fresh instance: earlier recovery decisions and abandonments
// do not leak into it, and a stale token cannot abandon its backlog.
func TestResubscribeDoesNotInheritRecoveryDecisions(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(8)); err != nil {
		t.Fatal(err)
	}
	pauseAndPublish(t, router, "sub", "a1", "a2")
	scope, err := router.PreviewRecovery("sub")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		RuleIDs:   []string{"all"},
		Remainder: RemainderAbandon,
		Reason:    "resetting before shutdown",
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}

	// Same-name resubscribe is a new instance with a clean slate.
	sub, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	pauseAndPublish(t, router, "sub", "b1", "b2")

	// The old instance's token is meaningless for the new backlog: an
	// abandon-everything confirm scoped to it touches nothing.
	outcome, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		Remainder: RemainderAbandon,
		Reason:    "stale token from the previous instance",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Abandoned) != 0 || len(outcome.Selected) != 0 || len(outcome.KeptPending) != 0 {
		t.Fatalf("stale token outcome = %#v, want empty", outcome)
	}
	if _, err := router.Resume("sub"); err != nil {
		t.Fatal(err)
	}
	got := receive(t, sub, 2)
	for index, id := range []string{"b1", "b2"} {
		if got[index].Message.ID() != id {
			t.Fatalf("replayed %d = %q, want %q", index, got[index].Message.ID(), id)
		}
	}

	// A restart shows only the new instance: no inherited abandonments or
	// decisions from the cancelled one.
	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	report := recoveryReport(t, restarted, "sub")
	if len(report.Abandoned) != 0 || len(report.Decisions) != 1 {
		t.Fatalf("recovered abandoned/decisions = %#v / %#v", report.Abandoned, report.Decisions)
	}
	wantIDs(t, report.Decisions[0].Abandoned, nil, "new instance decision abandoned")
}

// Scope selection requires a paused subscription and a valid remainder
// policy; abandoning without a reason is rejected.
func TestRecoveryScopeValidation(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("sub", []Rule{{ID: "all", Topic: "t"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := router.PreviewRecovery("sub"); !errors.Is(err, ErrSubscriptionNotPaused) {
		t.Fatalf("preview on running subscription = %v", err)
	}
	if _, err := router.PreviewRecovery("missing"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("preview on missing subscription = %v", err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	scope, err := router.PreviewRecovery("sub")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		Remainder: RemainderAbandon,
	}); err == nil {
		t.Fatal("abandon without reason should be rejected")
	}
	if _, err := router.ConfirmRecovery("sub", RecoverySelection{
		Token:     scope.Token,
		Remainder: RemainderPolicy("bogus"),
	}); err == nil {
		t.Fatal("unknown remainder policy should be rejected")
	}
	if _, err := router.ConfirmRecovery("missing", RecoverySelection{
		Token:     scope.Token,
		Remainder: RemainderKeepPending,
	}); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("confirm on missing subscription = %v", err)
	}
}
