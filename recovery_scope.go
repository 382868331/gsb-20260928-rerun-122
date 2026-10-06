package router

import (
	"fmt"
	"time"
)

// RemainderPolicy decides what a confirmed recovery selection does with
// the in-scope backlog messages the user did not select for replay.
type RemainderPolicy string

const (
	// RemainderKeepPending leaves the unselected backlog in the buffer as
	// still-pending messages: they are delivered by a later Resume exactly
	// as if no selection had been made.
	RemainderKeepPending RemainderPolicy = "keep_pending"
	// RemainderAbandon drops the unselected backlog for good. Every
	// dropped message is recorded with the user-supplied reason so it
	// stays distinguishable from retention-policy gaps after a restart.
	RemainderAbandon RemainderPolicy = "abandon"
)

// RecoveryCandidate is one still-replayable backlog message as shown by
// PreviewRecovery, with the information needed to choose a scope: which
// rules matched and when the message entered the backlog.
type RecoveryCandidate struct {
	MessageID   string
	Topic       string
	RuleIDs     []string
	RuleVersion int
	EnqueuedAt  time.Time
}

// RecoveryScope is a frozen view of a paused subscription's backlog and
// historical gaps at preview time. Token identifies exactly this view: a
// later ConfirmRecovery only applies to the messages visible here, never
// to messages that arrive after the preview.
type RecoveryScope struct {
	SubscriptionID string
	Token          int64
	Replayable     []RecoveryCandidate
	Gaps           []Gap
}

// RecoverySelection chooses which part of a previewed backlog to restore.
// A message from the previewed scope is selected when it matches every
// given filter: RuleIDs (empty means any rule), and the enqueue-time
// window [Since, Until] (zero values leave the side unbounded). The
// in-scope messages that are not selected are handled according to
// Remainder; Reason is required when Remainder is RemainderAbandon and is
// stored with every abandoned message.
type RecoverySelection struct {
	Token     int64
	RuleIDs   []string
	Since     time.Time
	Until     time.Time
	Remainder RemainderPolicy
	Reason    string
}

// RecoveryOutcome reports what a confirmed selection did, in message IDs.
type RecoveryOutcome struct {
	SubscriptionID string
	// Selected are the in-scope messages kept for replay.
	Selected []string
	// KeptPending are the in-scope messages left as pending backlog.
	KeptPending []string
	// Abandoned are the in-scope messages dropped for good.
	Abandoned []string
}

// AbandonedRecord explains one message a recovery selection gave up on
// purpose: the user-supplied reason plus the rules that had matched it.
// It is persisted, so after a restart it remains distinguishable from a
// retention-policy Gap.
type AbandonedRecord struct {
	Time        time.Time
	MessageID   string
	Topic       string
	Code        string
	Reason      string
	RuleIDs     []string
	RuleVersion int
}

// RecoveryDecision is the persisted audit trail of one confirmed recovery
// selection: the scope token it applied to, the filters, and the outcome.
type RecoveryDecision struct {
	Time        time.Time
	Token       int64
	RuleIDs     []string
	Since       time.Time
	Until       time.Time
	Remainder   RemainderPolicy
	Reason      string
	Selected    []string
	KeptPending []string
	Abandoned   []string
}

func copyAbandoned(record AbandonedRecord) AbandonedRecord {
	record.RuleIDs = append([]string(nil), record.RuleIDs...)
	return record
}

func copyDecision(decision RecoveryDecision) RecoveryDecision {
	decision.RuleIDs = append([]string(nil), decision.RuleIDs...)
	decision.Selected = append([]string(nil), decision.Selected...)
	decision.KeptPending = append([]string(nil), decision.KeptPending...)
	decision.Abandoned = append([]string(nil), decision.Abandoned...)
	return decision
}

// PreviewRecovery snapshots the still-replayable backlog and the
// historical gaps of a paused subscription so the user can choose a
// recovery scope before resuming. The preview is read-only: it marks
// nothing as delivered and changes no state beyond noting the current
// offer sequence as the returned Token. The subscription must be paused.
func (r *Router) PreviewRecovery(id string) (RecoveryScope, error) {
	if id == "" {
		err := fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "invalid_subscription",
			Reason: err.Error(),
		})
		return RecoveryScope{}, err
	}
	r.mu.RLock()
	entry, ok := r.subscribers[id]
	r.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("%w: %s", ErrSubscriptionGone, id)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_gone",
			Reason:         err.Error(),
			SubscriptionID: id,
		})
		return RecoveryScope{}, err
	}
	entry.mu.Lock()
	if !entry.paused {
		entry.mu.Unlock()
		err := fmt.Errorf("%w: %s", ErrSubscriptionNotPaused, id)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_not_paused",
			Reason:         err.Error(),
			SubscriptionID: id,
		})
		return RecoveryScope{}, err
	}
	scope := RecoveryScope{SubscriptionID: id, Token: r.currentSeq()}
	if entry.pending != nil {
		scope.Replayable = append(scope.Replayable, candidateOf(*entry.pending))
	}
	for index := 0; index < entry.count; index++ {
		scope.Replayable = append(scope.Replayable,
			candidateOf(entry.queued[(entry.head+index)%len(entry.queued)]))
	}
	for _, gap := range entry.gaps {
		scope.Gaps = append(scope.Gaps, copyGap(gap))
	}
	entry.mu.Unlock()
	return scope, nil
}

func candidateOf(delivery Delivery) RecoveryCandidate {
	return RecoveryCandidate{
		MessageID:   delivery.Message.id,
		Topic:       delivery.Message.topic,
		RuleIDs:     append([]string(nil), delivery.RuleIDs...),
		RuleVersion: delivery.RuleVersion,
		EnqueuedAt:  delivery.enqueuedAt,
	}
}

// ConfirmRecovery applies a recovery selection to the backlog exactly as
// PreviewRecovery showed it: only messages offered at or before the
// selection's Token are in scope, so messages that arrive between preview
// and confirm are left to the subscription's normal conventions. Selected
// messages stay queued for replay; the remainder is either kept as
// pending backlog or abandoned outright with the given reason. Nothing is
// marked delivered here — delivery still only happens through the pump on
// Resume. The decision and every abandonment are persisted, and the
// subscription must be paused.
func (r *Router) ConfirmRecovery(id string, selection RecoverySelection) (RecoveryOutcome, error) {
	if id == "" {
		err := fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "invalid_subscription",
			Reason: err.Error(),
		})
		return RecoveryOutcome{}, err
	}
	switch selection.Remainder {
	case RemainderKeepPending:
	case RemainderAbandon:
		if selection.Reason == "" {
			err := fmt.Errorf("%w: abandoning backlog requires a reason", ErrInvalidSubscription)
			r.addRecord(Record{
				Kind:           RecordRejected,
				Code:           "invalid_recovery_selection",
				Reason:         err.Error(),
				SubscriptionID: id,
			})
			return RecoveryOutcome{}, err
		}
	default:
		err := fmt.Errorf("%w: unknown remainder policy %q", ErrInvalidSubscription, selection.Remainder)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "invalid_recovery_selection",
			Reason:         err.Error(),
			SubscriptionID: id,
		})
		return RecoveryOutcome{}, err
	}

	r.mu.RLock()
	entry, ok := r.subscribers[id]
	r.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("%w: %s", ErrSubscriptionGone, id)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_gone",
			Reason:         err.Error(),
			SubscriptionID: id,
		})
		return RecoveryOutcome{}, err
	}

	entry.mu.Lock()
	if !entry.paused {
		entry.mu.Unlock()
		err := fmt.Errorf("%w: %s", ErrSubscriptionNotPaused, id)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_not_paused",
			Reason:         err.Error(),
			SubscriptionID: id,
		})
		return RecoveryOutcome{}, err
	}

	// Collect the backlog in delivery order, split it by the selection,
	// and rebuild the ring with whatever survives. Messages with a
	// sequence number above the token arrived after the preview and are
	// always kept untouched.
	var kept []Delivery
	outcome := RecoveryOutcome{SubscriptionID: id}
	decision := RecoveryDecision{
		Time:      time.Now(),
		Token:     selection.Token,
		RuleIDs:   append([]string(nil), selection.RuleIDs...),
		Since:     selection.Since,
		Until:     selection.Until,
		Remainder: selection.Remainder,
		Reason:    selection.Reason,
	}
	abandon := func(delivery Delivery) {
		record := AbandonedRecord{
			Time:        decision.Time,
			MessageID:   delivery.Message.id,
			Topic:       delivery.Message.topic,
			Code:        "recovery_abandoned",
			Reason:      selection.Reason,
			RuleIDs:     append([]string(nil), delivery.RuleIDs...),
			RuleVersion: delivery.RuleVersion,
		}
		entry.abandoned = append(entry.abandoned, record)
		outcome.Abandoned = append(outcome.Abandoned, delivery.Message.id)
		r.addRecord(Record{
			Time:           record.Time,
			Kind:           RecordDropped,
			Code:           "recovery_abandoned",
			Reason:         fmt.Sprintf("abandoned by recovery selection: %s", selection.Reason),
			MessageID:      delivery.Message.id,
			Topic:          delivery.Message.topic,
			SubscriptionID: id,
			RuleIDs:        append([]string(nil), delivery.RuleIDs...),
			RuleVersion:    delivery.RuleVersion,
		})
	}
	classify := func(delivery Delivery) {
		if delivery.seq > selection.Token {
			kept = append(kept, delivery)
			return
		}
		if selectedBy(selection, delivery) {
			kept = append(kept, delivery)
			outcome.Selected = append(outcome.Selected, delivery.Message.id)
			return
		}
		if selection.Remainder == RemainderAbandon {
			abandon(delivery)
			return
		}
		kept = append(kept, delivery)
		outcome.KeptPending = append(outcome.KeptPending, delivery.Message.id)
	}
	if entry.pending != nil {
		pending := *entry.pending
		if pending.seq <= selection.Token &&
			!selectedBy(selection, pending) &&
			selection.Remainder == RemainderAbandon {
			abandon(pending)
			entry.pending = nil
			entry.gen++
		} else if pending.seq <= selection.Token {
			if selectedBy(selection, pending) {
				outcome.Selected = append(outcome.Selected, pending.Message.id)
			} else {
				outcome.KeptPending = append(outcome.KeptPending, pending.Message.id)
			}
		}
	}
	for index := 0; index < entry.count; index++ {
		classify(entry.queued[(entry.head+index)%len(entry.queued)])
	}
	for index := range entry.queued {
		entry.queued[index] = Delivery{}
	}
	entry.head = 0
	entry.count = 0
	for _, delivery := range kept {
		entry.queued[(entry.head+entry.count)%len(entry.queued)] = delivery
		entry.count++
	}
	decision.Selected = append([]string(nil), outcome.Selected...)
	decision.KeptPending = append([]string(nil), outcome.KeptPending...)
	decision.Abandoned = append([]string(nil), outcome.Abandoned...)
	entry.decisions = append(entry.decisions, decision)
	entry.signal()
	entry.mu.Unlock()
	r.persist()
	return outcome, nil
}

// selectedBy reports whether a backlog delivery matches every filter of
// the selection: the rule filter (empty matches any) and the enqueue-time
// window (zero bounds are open).
func selectedBy(selection RecoverySelection, delivery Delivery) bool {
	if len(selection.RuleIDs) > 0 {
		matched := false
		for _, want := range selection.RuleIDs {
			for _, got := range delivery.RuleIDs {
				if want == got {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}
	if !selection.Since.IsZero() && delivery.enqueuedAt.Before(selection.Since) {
		return false
	}
	if !selection.Until.IsZero() && delivery.enqueuedAt.After(selection.Until) {
		return false
	}
	return true
}
