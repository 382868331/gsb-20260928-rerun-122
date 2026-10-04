package router

import (
	"errors"
	"testing"
	"time"
)

func receiveAll(t *testing.T, sub Subscription, count int) []Delivery {
	t.Helper()
	var deliveries []Delivery
	for index := 0; index < count; index++ {
		select {
		case delivery := <-sub.Deliveries:
			deliveries = append(deliveries, delivery)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for delivery %d of %d", index+1, count)
		}
	}
	return deliveries
}

func gapIDs(gaps []Gap) []string {
	ids := make([]string, len(gaps))
	for index, gap := range gaps {
		ids[index] = gap.MessageID
	}
	return ids
}

func equalIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// While paused nothing is delivered; on resume the report states exactly
// what can be redelivered and which messages the retention policy
// discarded, and the redeliverable backlog then arrives in order.
func TestPauseResumeKeepNewestRedeliversRetainedAndReportsGaps(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(3), WithOverflowPolicy(KeepNewest))
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3", "m4", "m5"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}

	// Nothing is delivered while paused.
	select {
	case delivery := <-sub.Deliveries:
		t.Fatalf("delivered %q while paused", delivery.Message.ID())
	case <-time.After(50 * time.Millisecond):
	}

	report, err := router.Resume("sub")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"m3", "m4", "m5"}; !equalIDs(report.Redeliverable, want) {
		t.Fatalf("redeliverable = %v, want %v", report.Redeliverable, want)
	}
	if want := []string{"m1", "m2"}; !equalIDs(gapIDs(report.Gaps), want) {
		t.Fatalf("gaps = %v, want %v", gapIDs(report.Gaps), want)
	}
	for _, gap := range report.Gaps {
		if gap.Code != "subscriber_buffer_evicted" || gap.RuleVersion != 1 {
			t.Fatalf("unexpected gap: %#v", gap)
		}
	}

	deliveries := receiveAll(t, sub, 3)
	for index, id := range []string{"m3", "m4", "m5"} {
		if deliveries[index].Message.ID() != id {
			t.Fatalf("delivery %d = %q, want %q", index, deliveries[index].Message.ID(), id)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}

// KeepOldest while paused: the retained backlog is the oldest messages and
// the gaps are the newer ones dropped against the full buffer.
func TestPauseResumeKeepOldestDropsNewest(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(2))
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	report, err := router.Resume("sub")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"m1", "m2"}; !equalIDs(report.Redeliverable, want) {
		t.Fatalf("redeliverable = %v, want %v", report.Redeliverable, want)
	}
	if want := []string{"m3", "m4"}; !equalIDs(gapIDs(report.Gaps), want) {
		t.Fatalf("gaps = %v, want %v", gapIDs(report.Gaps), want)
	}
	for _, gap := range report.Gaps {
		if gap.Code != "subscriber_buffer_full" {
			t.Fatalf("gap code = %q, want subscriber_buffer_full", gap.Code)
		}
	}

	deliveries := receiveAll(t, sub, 2)
	for index, id := range []string{"m1", "m2"} {
		if deliveries[index].Message.ID() != id {
			t.Fatalf("delivery %d = %q, want %q", index, deliveries[index].Message.ID(), id)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}

// Switching the retention policy during a pause only changes how later
// offers are discarded; the already retained backlog is not rewritten.
func TestPolicySwitchDuringPauseKeepsBacklog(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(2))
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3"} { // KeepOldest: m3 dropped
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.SetOverflowPolicy("sub", KeepNewest); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m4", "m5"} { // KeepNewest: evicts m1, m2
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}

	report, err := router.Resume("sub")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"m4", "m5"}; !equalIDs(report.Redeliverable, want) {
		t.Fatalf("redeliverable = %v, want %v", report.Redeliverable, want)
	}
	if want := []string{"m3", "m1", "m2"}; !equalIDs(gapIDs(report.Gaps), want) {
		t.Fatalf("gaps = %v, want %v (discard order)", gapIDs(report.Gaps), want)
	}

	deliveries := receiveAll(t, sub, 2)
	for index, id := range []string{"m4", "m5"} {
		if deliveries[index].Message.ID() != id {
			t.Fatalf("delivery %d = %q, want %q", index, deliveries[index].Message.ID(), id)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}

// Cancelling a paused subscription discards its retained backlog at the
// cancellation boundary; the messages are never moved into a later
// subscription with the same ID.
func TestCancelWhilePausedDiscardsBacklog(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2"} {
		if err := router.Publish(mustMessage(t, id, "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if _, open := <-sub.Deliveries; open {
		t.Fatal("channel remains open after cancelling a paused subscription")
	}
	if _, err := router.Resume("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("resume after cancel = %v, want ErrSubscriptionGone", err)
	}

	cancelled := make(map[string]bool)
	for _, record := range router.Records() {
		if record.Code == "subscription_cancelled" {
			cancelled[record.MessageID] = true
		}
	}
	if !cancelled["m1"] || !cancelled["m2"] {
		t.Fatalf("cancelled = %v, want m1 and m2", cancelled)
	}

	// A new subscription under the same ID receives only new messages.
	fresh, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Publish(mustMessage(t, "m3", "t", nil)); err != nil {
		t.Fatal(err)
	}
	deliveries := receiveAll(t, fresh, 1)
	if deliveries[0].Message.ID() != "m3" {
		t.Fatalf("fresh subscription got %q, want only m3", deliveries[0].Message.ID())
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
}

// A paused slow subscriber never blocks Publish and never affects other
// subscribers, however much accumulates against its bounded buffer.
func TestPausedSlowSubscriberDoesNotBlockPublisherOrOthers(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	slow, err := router.Subscribe("slow", []Rule{{ID: "topic", Topic: "t"}},
		WithSubscriptionBufferSize(2))
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

	const total = 50
	for index := 0; index < total; index++ {
		if err := router.Publish(mustMessage(t, string(rune('a'+index%26)), "t", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(receiveAll(t, fast, total)); got != total {
		t.Fatalf("fast received %d, want %d", got, total)
	}

	report, err := router.Resume("slow")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Redeliverable) != 2 {
		t.Fatalf("redeliverable = %v, want exactly the retained 2", report.Redeliverable)
	}
	if len(report.Gaps) != total-2 {
		t.Fatalf("gaps = %d, want %d", len(report.Gaps), total-2)
	}
	if got := len(receiveAll(t, slow, 2)); got != 2 {
		t.Fatalf("slow received %d after resume, want 2", got)
	}
	if err := router.Unsubscribe("slow"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("fast"); err != nil {
		t.Fatal(err)
	}
}

// Pause/Resume validate their targets and states.
func TestPauseResumeValidation(t *testing.T) {
	router, err := NewRouter()
	if err != nil {
		t.Fatal(err)
	}
	sub, err := router.Subscribe("sub", []Rule{{ID: "topic", Topic: "t"}})
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Pause(""); err == nil {
		t.Fatal("empty id pause unexpectedly accepted")
	}
	if _, err := router.Resume(""); err == nil {
		t.Fatal("empty id resume unexpectedly accepted")
	}
	if err := router.Pause("missing"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("pause missing = %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.Resume("missing"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("resume missing = %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.Resume("sub"); !errors.Is(err, ErrSubscriptionNotPaused) {
		t.Fatalf("resume unpaused = %v, want ErrSubscriptionNotPaused", err)
	}
	if err := router.Pause("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); !errors.Is(err, ErrSubscriptionPaused) {
		t.Fatalf("double pause = %v, want ErrSubscriptionPaused", err)
	}
	if _, err := router.Resume("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Unsubscribe("sub"); err != nil {
		t.Fatal(err)
	}
	if err := router.Pause("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("pause after cancel = %v, want ErrSubscriptionGone", err)
	}
	if _, err := router.Resume("sub"); !errors.Is(err, ErrSubscriptionGone) {
		t.Fatalf("resume after cancel = %v, want ErrSubscriptionGone", err)
	}
	_ = sub
}
