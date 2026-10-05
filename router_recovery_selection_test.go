package router

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The recovery-selection flow: preview the paused backlog and its gaps,
// confirm a selection that replays only some rules, and abandon the rest
// with a recorded reason. The preview itself changes nothing.
func TestRecoverySelectionPreviewDoesNotDeliver(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("alerts", []Rule{
		{ID: "all", Topic: "t"},
		{ID: "crit", Topic: "t", Attrs: map[string]string{"sev": "crit"}},
	}, WithSubscriptionBufferSize(4), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("alerts"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3", "m4", "m5", "m6"} {
		if err := router.Publish(mustMessage(t, id, "t", map[string]string{"sev": "crit"})); err != nil {
			t.Fatal(err)
		}
	}

	preview, err := router.PendingRecovery("alerts")
	if err != nil {
		t.Fatal(err)
	}
	wantReplay := []string{"m3", "m4", "m5", "m6"}
	if strings.Join(preview.Replayable, ",") != strings.Join(wantReplay, ",") {
		t.Fatalf("preview replayable = %v, want %v", preview.Replayable, wantReplay)
	}
	if len(preview.Gaps) != 2 || preview.Gaps[0].MessageID != "m1" || preview.Gaps[1].MessageID != "m2" {
		t.Fatalf("preview gaps = %+v, want m1 and m2", preview.Gaps)
	}
	// Previewing must not resume or mark anything delivered: the channel
	// stays silent until a selection is confirmed.
	select {
	case d := <-sub.Deliveries:
		t.Fatalf("preview delivered %q", d.Message.ID())
	case <-time.After(50 * time.Millisecond):
	}
	// A second preview sees the same snapshot: nothing was consumed.
	again, err := router.PendingRecovery("alerts")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(again.Replayable, ",") != strings.Join(wantReplay, ",") {
		t.Fatalf("second preview replayable = %v, want %v", again.Replayable, wantReplay)
	}
}

// A confirmed selection replays only the chosen rule's messages; the
// unselected remainder is abandoned with the caller's reason, visible in
// the report, the audit log, and the persisted decision log.
func TestResumeSelectiveAbandonsRemainderWithReason(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("alerts", []Rule{
		{ID: "crit", Topic: "t", Attrs: map[string]string{"sev": "crit"}},
		{ID: "info", Topic: "t", Attrs: map[string]string{"sev": "info"}},
	}, WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("alerts"); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "c1", "t", map[string]string{"sev": "crit"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "i1", "t", map[string]string{"sev": "info"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "c2", "t", map[string]string{"sev": "crit"})); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "i2", "t", map[string]string{"sev": "info"})); err != nil {
		t.Fatal(err)
	}

	report, err := router.ResumeSelective("alerts", RecoverySelection{
		RuleIDs:   []string{"crit"},
		Remainder: RemainderAbandon,
		Reason:    "operator dropped informational backlog",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(report.Replayable, ",") != "c1,c2" {
		t.Fatalf("replayable = %v, want [c1 c2]", report.Replayable)
	}
	if strings.Join(report.Abandoned, ",") != "i1,i2" {
		t.Fatalf("abandoned = %v, want [i1 i2]", report.Abandoned)
	}
	if len(report.Gaps) != 2 {
		t.Fatalf("gaps = %+v, want 2 abandonment gaps", report.Gaps)
	}
	for _, gap := range report.Gaps {
		if gap.Code != "recovery_abandoned" || gap.Reason != "operator dropped informational backlog" {
			t.Fatalf("gap = %+v, want recovery_abandoned with the operator reason", gap)
		}
	}

	got := receive(t, sub, 2)
	if got[0].Message.ID() != "c1" || got[1].Message.ID() != "c2" {
		t.Fatalf("delivered = %q,%q, want c1,c2", got[0].Message.ID(), got[1].Message.ID())
	}

	// The abandonment is auditable with the user's reason.
	var auditFound int
	for _, record := range router.Records() {
		if record.Code == "recovery_abandoned" {
			auditFound++
			if record.Reason != "operator dropped informational backlog" {
				t.Fatalf("audit reason = %q", record.Reason)
			}
		}
	}
	if auditFound != 2 {
		t.Fatalf("recovery_abandoned audit records = %d, want 2", auditFound)
	}

	// The decision survives a restart: old retention gaps and deliberate
	// abandonments stay explainable and distinguishable.
	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	decisions := restarted.RecoveryDecisions("alerts")
	if len(decisions) != 1 {
		t.Fatalf("decisions = %+v, want exactly one", decisions)
	}
	decision := decisions[0]
	if strings.Join(decision.Replayed, ",") != "c1,c2" ||
		strings.Join(decision.Abandoned, ",") != "i1,i2" ||
		decision.Reason != "operator dropped informational backlog" {
		t.Fatalf("decision = %+v", decision)
	}
	recovery := recoveryReport(t, restarted, "alerts")
	if len(recovery.Decisions) != 1 {
		t.Fatalf("recovery report decisions = %+v, want the persisted selection", recovery.Decisions)
	}
}

// A time-range selection replays only messages enqueued inside the
// window; the kept-pending remainder stays in the backlog and is
// delivered in order after the selected ones.
func TestResumeSelectiveTimeRangeKeepsRemainderPending(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("s", []Rule{{ID: "all", Topic: "t"}},
		WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("s"); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "old", "t", nil)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	since := time.Now()
	time.Sleep(20 * time.Millisecond)
	if err := router.Publish(mustMessage(t, "new", "t", nil)); err != nil {
		t.Fatal(err)
	}

	report, err := router.ResumeSelective("s", RecoverySelection{Since: since})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(report.Replayable, ",") != "old,new" {
		t.Fatalf("replayable = %v, want [old new] (kept remainder stays pending)", report.Replayable)
	}
	decisions := router.RecoveryDecisions("s")
	if len(decisions) != 1 {
		t.Fatalf("decisions = %+v, want one", decisions)
	}
	if strings.Join(decisions[0].Replayed, ",") != "new" ||
		strings.Join(decisions[0].KeptPending, ",") != "old" {
		t.Fatalf("decision = %+v, want replayed=[new] kept=[old]", decisions[0])
	}
	got := receive(t, sub, 2)
	if got[0].Message.ID() != "old" || got[1].Message.ID() != "new" {
		t.Fatalf("delivered = %q,%q, want original order old,new",
			got[0].Message.ID(), got[1].Message.ID())
	}
}

// Messages that arrive after a selection is confirmed are not covered by
// it: they follow the subscription's normal matching and delivery rules,
// even when they would not have matched the selection's filter.
func TestRecoverySelectionDoesNotBindLaterArrivals(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("s", []Rule{
		{ID: "crit", Topic: "t", Attrs: map[string]string{"sev": "crit"}},
		{ID: "info", Topic: "t", Attrs: map[string]string{"sev": "info"}},
	}, WithSubscriptionBufferSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("s"); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "c1", "t", map[string]string{"sev": "crit"})); err != nil {
		t.Fatal(err)
	}
	report, err := router.ResumeSelective("s", RecoverySelection{
		RuleIDs:   []string{"crit"},
		Remainder: RemainderAbandon,
		Reason:    "only critical backlog wanted",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(report.Replayable, ",") != "c1" {
		t.Fatalf("replayable = %v, want [c1]", report.Replayable)
	}
	// This arrival is outside the confirmed selection: it must be
	// delivered normally, not abandoned by the old filter.
	if err := router.Publish(mustMessage(t, "i1", "t", map[string]string{"sev": "info"})); err != nil {
		t.Fatal(err)
	}
	got := receive(t, sub, 2)
	if got[0].Message.ID() != "c1" || got[1].Message.ID() != "i1" {
		t.Fatalf("delivered = %q,%q, want c1,i1", got[0].Message.ID(), got[1].Message.ID())
	}
	for _, record := range router.Records() {
		if record.Code == "recovery_abandoned" && record.MessageID == "i1" {
			t.Fatal("later arrival must not be abandoned by the earlier selection")
		}
	}
}

// Cancelling and resubscribing under the same name creates a fresh
// instance: earlier recovery decisions never apply to it.
func TestRecoveryDecisionsDoNotMigrateToResubscribedInstance(t *testing.T) {
	path := stateFile(t)
	router, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	rules := []Rule{{ID: "all", Topic: "t"}}
	if _, err := router.Subscribe("s", rules, WithSubscriptionBufferSize(4)); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("s"); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := router.ResumeSelective("s", RecoverySelection{
		Remainder: RemainderAbandon,
		Reason:    "stale backlog dropped",
	}); err != nil {
		t.Fatal(err)
	}
	if len(router.RecoveryDecisions("s")) != 1 {
		t.Fatal("expected one recorded decision before cancellation")
	}
	if err := router.Unsubscribe("s"); err != nil {
		t.Fatal(err)
	}

	sub, err := router.Subscribe("s", rules)
	if err != nil {
		t.Fatal(err)
	}
	if decisions := router.RecoveryDecisions("s"); len(decisions) != 0 {
		t.Fatalf("resubscribed instance inherited decisions: %+v", decisions)
	}
	// The new instance works on its own terms.
	if err := router.Publish(mustMessage(t, "m2", "t", nil)); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, sub, 1); got[0].Message.ID() != "m2" {
		t.Fatalf("delivered = %q, want m2", got[0].Message.ID())
	}

	// A restart over the same state file cannot resurrect the old
	// instance's decisions either.
	restarted, err := NewRouter(WithStateFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if decisions := restarted.RecoveryDecisions("s"); len(decisions) != 0 {
		t.Fatalf("decisions leaked across cancel/resubscribe: %+v", decisions)
	}
}

func TestResumeSelectiveValidation(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Subscribe("s", []Rule{{ID: "all", Topic: "t"}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("s"); err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m1", "t", nil)); err != nil {
		t.Fatal(err)
	}

	if _, err := router.ResumeSelective("s", RecoverySelection{
		Remainder: RemainderAbandon,
	}); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("abandon without reason: %v, want ErrInvalidSubscription", err)
	}
	if _, err := router.ResumeSelective("s", RecoverySelection{
		Remainder: "bogus",
	}); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("unknown remainder: %v, want ErrInvalidSubscription", err)
	}
	if _, err := router.ResumeSelective("ghost", RecoverySelection{}); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("missing subscription: %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.PendingRecovery("ghost"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("preview missing subscription: %v, want ErrSubscriptionGone", err)
	}
	// Failed attempts change nothing: the backlog is still intact.
	preview, err := router.PendingRecovery("s")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(preview.Replayable, ",") != "m1" {
		t.Fatalf("replayable after failed selections = %v, want [m1]", preview.Replayable)
	}
	if len(router.RecoveryDecisions("s")) != 0 {
		t.Fatal("failed selections must not be logged")
	}

	if _, err := router.ResumeSelective("s", RecoverySelection{}); err != nil {
		t.Fatal(err)
	}
	if _, err := router.ResumeSelective("s", RecoverySelection{}); !errors.Is(err, ErrSubscriptionNotPaused) {
		t.Fatalf("double resume: %v, want ErrSubscriptionNotPaused", err)
	}
	if _, err := router.PendingRecovery("s"); !errors.Is(err, ErrSubscriptionNotPaused) {
		t.Fatalf("preview after resume: %v, want ErrSubscriptionNotPaused", err)
	}
}
