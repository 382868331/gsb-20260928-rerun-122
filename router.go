package router

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalidMessage        = errors.New("invalid message")
	ErrInvalidSubscription   = errors.New("invalid subscription")
	ErrInvalidRule           = errors.New("invalid rule")
	ErrSubscriptionExists    = errors.New("subscription already exists")
	ErrSubscriptionGone      = errors.New("subscription not found")
	ErrSubscriptionPaused    = errors.New("subscription already paused")
	ErrSubscriptionNotPaused = errors.New("subscription not paused")
)

const (
	defaultBufferSize  = 16
	defaultHistorySize = 128
)

// Message is an immutable local message envelope. Construct messages with
// NewMessage so attribute maps cannot be mutated outside the router.
type Message struct {
	id    string
	topic string
	attrs map[string]string
}

func NewMessage(id, topic string, attrs map[string]string) (Message, error) {
	if id == "" {
		return Message{}, fmt.Errorf("%w: message id is required", ErrInvalidMessage)
	}
	if topic == "" {
		return Message{}, fmt.Errorf("%w: topic is required", ErrInvalidMessage)
	}

	copied := make(map[string]string, len(attrs))
	for key, value := range attrs {
		if key == "" {
			return Message{}, fmt.Errorf("%w: attribute key is required", ErrInvalidMessage)
		}
		copied[key] = value
	}
	return Message{id: id, topic: topic, attrs: copied}, nil
}

func (m Message) ID() string { return m.id }

func (m Message) Topic() string { return m.topic }

func (m Message) Attribute(key string) (string, bool) {
	value, ok := m.attrs[key]
	return value, ok
}

// Rule matches a message when the topic is equal and every rule attribute is
// present on the message with the same value.
type Rule struct {
	ID    string
	Topic string
	Attrs map[string]string
}

type Delivery struct {
	Message        Message
	SubscriptionID string
	RuleIDs        []string
	// RuleVersion is the version of the subscription's rule set that matched
	// the message. The initial Subscribe set is version 1; every successful
	// UpdateRules call advances it by one. A Publish call matches against a
	// single atomic snapshot, so this value identifies exactly which revision
	// of the rules produced the delivery.
	RuleVersion int
}

// OverflowPolicy decides what happens when a slow subscriber's bounded
// buffer is full and a new message matches.
type OverflowPolicy string

const (
	// KeepOldest retains buffered messages and drops the newest message.
	// This is the default policy.
	KeepOldest OverflowPolicy = "keep_oldest"
	// KeepNewest evicts the oldest buffered message to make room for the
	// newest one. The evicted message is recorded with reason
	// "subscriber_buffer_evicted".
	KeepNewest OverflowPolicy = "keep_newest"
)

type RecordKind string

const (
	RecordRejected RecordKind = "rejected"
	RecordDropped  RecordKind = "dropped"
)

type Record struct {
	Time           time.Time
	Kind           RecordKind
	Code           string
	Reason         string
	MessageID      string
	Topic          string
	SubscriptionID string
	RuleIDs        []string
	// RuleVersion is the rule-set version the message matched before it was
	// dropped or evicted; zero for records that are not tied to a match.
	RuleVersion int
}

type Option func(*Router) error

func WithBufferSize(size int) Option {
	return func(r *Router) error {
		if size < 0 {
			return fmt.Errorf("%w: buffer size cannot be negative", ErrInvalidSubscription)
		}
		r.bufferSize = size
		return nil
	}
}

func WithHistorySize(size int) Option {
	return func(r *Router) error {
		if size < 0 {
			return fmt.Errorf("%w: history size cannot be negative", ErrInvalidSubscription)
		}
		r.historySize = size
		return nil
	}
}

// WithStateFile makes subscription state recoverable across restarts. The
// router persists every subscription's rules, rule version, overflow
// policy, pause state, unreceived backlog, and pause-time gaps to the
// given file on each state change, and a new Router created with the same
// file restores them: replay continues from exactly the unreceived
// messages, cancelled subscriptions stay cancelled, and RecoveryReports
// describes what can still be replayed and what the retention policy had
// already evicted.
func WithStateFile(path string) Option {
	return func(r *Router) error {
		if path == "" {
			return fmt.Errorf("%w: state file path is required", ErrInvalidSubscription)
		}
		r.store = &stateStore{path: path}
		return nil
	}
}

type SubscribeOption func(*subscription) error

func WithSubscriptionBufferSize(size int) SubscribeOption {
	return func(sub *subscription) error {
		if size < 0 {
			return fmt.Errorf("%w: buffer size cannot be negative", ErrInvalidSubscription)
		}
		sub.bufferSize = size
		return nil
	}
}

// WithOverflowPolicy selects how a subscriber behaves while its buffer is
// full. KeepOldest drops the incoming (newest) message; KeepNewest evicts
// the oldest buffered message instead. The choice affects only that
// subscriber; other subscribers are never impacted by a slow consumer.
func WithOverflowPolicy(policy OverflowPolicy) SubscribeOption {
	return func(sub *subscription) error {
		switch policy {
		case KeepOldest, KeepNewest:
			sub.overflow = policy
		default:
			return fmt.Errorf("%w: unknown overflow policy %q", ErrInvalidSubscription, policy)
		}
		return nil
	}
}

type Subscription struct {
	ID         string
	Deliveries <-chan Delivery
}

// ResumeReport describes what a paused subscription can still replay and
// which messages were evicted while it was paused. Replayable lists the
// buffered message IDs in the exact order they will be delivered once the
// subscription resumes; Evicted lists the messages the retention policy
// dropped during the pause, so the subscriber can see the gaps explicitly.
type ResumeReport struct {
	SubscriptionID string
	Replayable     []string
	Evicted        []string
	// Gaps details every evicted message with the reason, the matched
	// rules, and the retention policy in effect when the gap was created.
	Gaps []Gap
}

type subscription struct {
	id         string
	bufferSize int
	deliveries chan Delivery

	mu       sync.Mutex
	rules    []Rule
	version  int
	overflow OverflowPolicy

	// queued is the bounded ring of accepted-but-not-yet-sent deliveries.
	queued []Delivery
	head   int
	count  int
	closed bool

	// pending holds the oldest queued delivery while the pump is waiting
	// for the consumer to accept it. It is still an unreceived message:
	// KeepNewest may evict it, in which case gen advances and the pump
	// reconciles the outcome of its in-flight send against the eviction.
	pending *Delivery
	// gen identifies the current pending assignment; it changes whenever
	// pending is set, cleared, or evicted.
	gen int
	// reconciled holds a pending delivery that offer evicted while the
	// pump might concurrently complete its send. The pump either records
	// the eviction (the send lost the race) or drops it (the consumer
	// actually received the message, so nothing was evicted).
	reconciled *Delivery

	// paused stops the pump from handing deliveries to the consumer.
	// Offers still enter the bounded buffer and the retention policy
	// keeps applying while paused.
	paused bool
	// gaps accumulates the messages dropped or evicted while paused,
	// with the reason, matched rules, and policy at gap time; Resume
	// reports them as the replay gaps.
	gaps []Gap

	notify   chan struct{}
	inFlight sync.WaitGroup
	// done is closed when the pump goroutine exits; Unsubscribe waits
	// for it so no delivery can be sent after cancellation returns.
	done chan struct{}
	// discarded is set by the pump as it exits after close: every
	// accepted-but-unreceived delivery, in original order. Unsubscribe
	// reads it after done closes and records the cancellations.
	discarded []Delivery
}

type Router struct {
	mu          sync.RWMutex
	bufferSize  int
	historySize int
	subscribers map[string]*subscription

	store     *stateStore
	recovered []RecoveryReport

	auditMu   sync.Mutex
	history   []Record
	nextAudit int
}

func NewRouter(options ...Option) (*Router, error) {
	r := &Router{
		bufferSize:  defaultBufferSize,
		historySize: defaultHistorySize,
		subscribers: make(map[string]*subscription),
	}
	for _, option := range options {
		if err := option(r); err != nil {
			return nil, err
		}
	}
	if r.store != nil {
		if err := r.recover(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// recover restores every subscription found in the state file. Recovered
// subscriptions keep their rules, rule version, overflow policy, and pause
// state; their unreceived backlog is queued again in the original order,
// and pause-time gaps are restored so Resume and RecoveryReports can still
// explain them. Cancelled subscriptions are absent from the state file and
// therefore never come back.
func (r *Router) recover() error {
	state, err := r.store.load()
	if err != nil {
		return err
	}
	for _, persisted := range state.Subscriptions {
		entry := &subscription{
			id:         persisted.ID,
			bufferSize: persisted.BufferSize,
			overflow:   persisted.Overflow,
			rules:      persisted.Rules,
			version:    persisted.Version,
			paused:     persisted.Paused,
		}
		entry.deliveries = make(chan Delivery)
		entry.queued = make([]Delivery, maxInt(entry.bufferSize, 1))
		entry.notify = make(chan struct{}, 1)
		entry.done = make(chan struct{})
		report := RecoveryReport{SubscriptionID: persisted.ID, Paused: persisted.Paused}
		for _, delivery := range persisted.Backlog {
			restored := restoreDelivery(persisted.ID, delivery)
			entry.queued[(entry.head+entry.count)%len(entry.queued)] = restored
			entry.count++
			report.Replayable = append(report.Replayable, restored.Message.id)
		}
		for _, gap := range persisted.Gaps {
			entry.gaps = append(entry.gaps, copyGap(gap))
			report.Gaps = append(report.Gaps, copyGap(gap))
		}
		r.subscribers[persisted.ID] = entry
		r.recovered = append(r.recovered, report)
		go entry.pump(r)
	}
	return nil
}

// RecoveryReports describes the subscriptions restored from the state
// file at startup, in the order they were persisted. Empty when the router
// was created without WithStateFile or the state file had no subscriptions.
func (r *Router) RecoveryReports() []RecoveryReport {
	reports := make([]RecoveryReport, 0, len(r.recovered))
	for _, report := range r.recovered {
		report.Replayable = append([]string(nil), report.Replayable...)
		gaps := make([]Gap, 0, len(report.Gaps))
		for _, gap := range report.Gaps {
			gaps = append(gaps, copyGap(gap))
		}
		report.Gaps = gaps
		reports = append(reports, report)
	}
	return reports
}

// Subscription returns the live handle for an active subscription,
// including one restored from the state file, so a consumer can attach to
// its delivery channel after a restart.
func (r *Router) Subscription(id string) (Subscription, bool) {
	r.mu.RLock()
	entry, ok := r.subscribers[id]
	r.mu.RUnlock()
	if !ok {
		return Subscription{}, false
	}
	return Subscription{ID: entry.id, Deliveries: entry.deliveries}, true
}

// persist writes the current recoverable state of every live subscription.
// Failures are recorded in the audit history; routing itself continues.
func (r *Router) persist() {
	if r.store == nil {
		return
	}
	r.mu.RLock()
	entries := make([]*subscription, 0, len(r.subscribers))
	for _, entry := range r.subscribers {
		entries = append(entries, entry)
	}
	r.mu.RUnlock()

	state := persistedState{Subscriptions: make([]persistedSubscription, 0, len(entries))}
	for _, entry := range entries {
		entry.mu.Lock()
		persisted := persistedSubscription{
			ID:         entry.id,
			Rules:      append([]Rule(nil), entry.rules...),
			Version:    entry.version,
			Overflow:   entry.overflow,
			BufferSize: entry.bufferSize,
			Paused:     entry.paused,
		}
		if entry.pending != nil {
			persisted.Backlog = append(persisted.Backlog, persistDelivery(*entry.pending))
		}
		for index := 0; index < entry.count; index++ {
			persisted.Backlog = append(persisted.Backlog,
				persistDelivery(entry.queued[(entry.head+index)%len(entry.queued)]))
		}
		for _, gap := range entry.gaps {
			persisted.Gaps = append(persisted.Gaps, copyGap(gap))
		}
		entry.mu.Unlock()
		state.Subscriptions = append(state.Subscriptions, persisted)
	}
	if err := r.store.save(state); err != nil {
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "persistence_error",
			Reason: err.Error(),
		})
	}
}

func (r *Router) Subscribe(id string, rules []Rule, options ...SubscribeOption) (Subscription, error) {
	if id == "" {
		return Subscription{}, fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
	}
	prepared, err := prepareRules(rules)
	if err != nil {
		return Subscription{}, err
	}

	entry := &subscription{
		id:         id,
		bufferSize: r.bufferSize,
		overflow:   KeepOldest,
	}
	for _, option := range options {
		if err := option(entry); err != nil {
			return Subscription{}, err
		}
	}

	ch := make(chan Delivery)
	entry.deliveries = ch
	entry.rules = prepared
	entry.version = 1
	entry.queued = make([]Delivery, maxInt(entry.bufferSize, 1))
	entry.notify = make(chan struct{}, 1)
	entry.done = make(chan struct{})

	r.mu.Lock()
	if _, exists := r.subscribers[id]; exists {
		r.mu.Unlock()
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionExists, id)
	}
	r.subscribers[id] = entry
	r.mu.Unlock()

	go entry.pump(r)
	r.persist()

	return Subscription{ID: id, Deliveries: ch}, nil
}

// snapshot returns the currently active rules and their version as one
// atomic read.
func (s *subscription) snapshot() ([]Rule, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rules, s.version
}

// pump forwards accepted deliveries to the consumer in order. A delivery is
// held in pending until the consumer accepts it, so a slow consumer cannot
// block Publish and never affects how newer messages enter the ring. The
// pending delivery still counts against the retention policy: if KeepNewest
// evicts it mid-handoff, the pump abandons that send and moves on. When the
// consumer send and an eviction race, the actual outcome decides the audit
// record: a message the consumer received is never reported as evicted.
func (s *subscription) pump(router *Router) {
	defer close(s.done)
	for {
		s.mu.Lock()
		if s.reconciled != nil {
			evicted := *s.reconciled
			s.reconciled = nil
			s.mu.Unlock()
			router.addRecord(Record{
				Time:           time.Now(),
				Kind:           RecordDropped,
				Code:           "subscriber_buffer_evicted",
				Reason:         "policy keep_newest evicted the oldest buffered message",
				MessageID:      evicted.Message.id,
				Topic:          evicted.Message.topic,
				SubscriptionID: evicted.SubscriptionID,
				RuleIDs:        append([]string(nil), evicted.RuleIDs...),
				RuleVersion:    evicted.RuleVersion,
			})
			continue
		}
		if s.closed {
			// Collect every accepted-but-unreceived delivery exactly
			// once. Because the pump itself does this right before
			// exiting, a handoff that already completed can never be
			// counted as discarded.
			if s.pending != nil {
				s.discarded = append(s.discarded, *s.pending)
				s.pending = nil
			}
			for index := 0; index < s.count; index++ {
				s.discarded = append(s.discarded, s.queued[(s.head+index)%len(s.queued)])
				s.queued[(s.head+index)%len(s.queued)] = Delivery{}
			}
			s.head = 0
			s.count = 0
			s.mu.Unlock()
			close(s.deliveries)
			return
		}
		if s.paused {
			notify := s.notify
			s.mu.Unlock()
			<-notify
			continue
		}
		if s.pending == nil {
			if s.count > 0 {
				delivery := s.queued[s.head]
				s.queued[s.head] = Delivery{}
				s.head = (s.head + 1) % len(s.queued)
				s.count--
				s.pending = &delivery
				s.gen++
			}
		}
		delivery := s.pending
		gen := s.gen
		notify := s.notify
		s.mu.Unlock()

		if delivery == nil {
			<-notify
			continue
		}

		select {
		case s.deliveries <- *delivery:
			s.mu.Lock()
			if s.gen == gen {
				s.pending = nil
				s.gen++
				s.mu.Unlock()
				// The message is now the consumer's; persist so a
				// restart never replays what was already received.
				router.persist()
			} else {
				// The consumer received a delivery that offer had
				// already evicted from pending. Received messages are
				// no longer subject to the retention policy, so the
				// eviction simply never happened.
				s.reconciled = nil
				s.mu.Unlock()
			}
		case <-notify:
			// State changed (arrival, eviction, or close); re-read
			// under the lock before retrying the handoff.
		}
	}
}

func (r *Router) UpdateRules(subscriptionID string, rules []Rule) error {
	if subscriptionID == "" {
		err := fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "invalid_subscription",
			Reason: err.Error(),
		})
		return err
	}
	prepared, err := prepareRules(rules)
	if err != nil {
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "invalid_rules",
			Reason:         err.Error(),
			SubscriptionID: subscriptionID,
		})
		return err
	}

	r.mu.Lock()
	entry, ok := r.subscribers[subscriptionID]
	if !ok {
		r.mu.Unlock()
		err := fmt.Errorf("%w: %s", ErrSubscriptionGone, subscriptionID)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_gone",
			Reason:         err.Error(),
			SubscriptionID: subscriptionID,
		})
		return err
	}
	entry.mu.Lock()
	entry.rules = prepared
	entry.version++
	entry.mu.Unlock()
	r.mu.Unlock()
	r.persist()
	return nil
}

// SetOverflowPolicy switches a running subscription's overflow policy. The
// new policy applies to every delivery offered after the call returns;
// offers already in flight may still observe the previous policy. The
// backlog is never rewritten by the switch itself: every message already
// accepted (buffered or in handoff) is retained and delivered in order.
// Under KeepNewest, later overflow evicts the oldest unreceived message,
// which may well be part of that pre-switch backlog; under KeepOldest,
// later arrivals are dropped while the buffer stays full. The choice only
// ever affects this subscription.
func (r *Router) SetOverflowPolicy(subscriptionID string, policy OverflowPolicy) error {
	if subscriptionID == "" {
		err := fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "invalid_subscription",
			Reason: err.Error(),
		})
		return err
	}
	if policy != KeepOldest && policy != KeepNewest {
		err := fmt.Errorf("%w: unknown overflow policy %q", ErrInvalidSubscription, policy)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "invalid_overflow_policy",
			Reason:         err.Error(),
			SubscriptionID: subscriptionID,
		})
		return err
	}

	r.mu.RLock()
	entry, ok := r.subscribers[subscriptionID]
	r.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("%w: %s", ErrSubscriptionGone, subscriptionID)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_gone",
			Reason:         err.Error(),
			SubscriptionID: subscriptionID,
		})
		return err
	}
	entry.mu.Lock()
	entry.overflow = policy
	entry.mu.Unlock()
	r.persist()
	return nil
}

// offer accepts a matched delivery into the bounded buffer without ever
// blocking on the consumer. It returns the delivery that had to be
// discarded together with the audit code for it; an empty code means the
// delivery was accepted with no loss.
//
// The retention policy covers every message the consumer has not received
// yet: the ring plus the pump's in-flight handoff (pending) together hold
// at most bufferSize messages. KeepOldest drops the incoming delivery when
// full; KeepNewest evicts the oldest unreceived delivery, which is the
// pending handoff when one exists.
func (s *subscription) offer(delivery Delivery) (Delivery, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return delivery, "subscriber_buffer_full"
	}

	occupied := s.count
	if s.pending != nil {
		occupied++
	}
	if occupied < s.bufferSize {
		capacity := len(s.queued)
		s.queued[(s.head+s.count)%capacity] = delivery
		s.count++
		s.signal()
		return Delivery{}, ""
	}

	if s.overflow == KeepNewest && s.bufferSize > 0 {
		if s.pending != nil {
			// The oldest unreceived message is the pump's in-flight
			// handoff. Evict it and wake the pump; the pump records
			// the eviction once it confirms its send did not complete.
			evicted := *s.pending
			s.pending = nil
			s.gen++
			s.reconciled = &evicted
			s.noteGap(evicted, "subscriber_buffer_evicted")
			capacity := len(s.queued)
			s.queued[(s.head+s.count)%capacity] = delivery
			s.count++
			s.signal()
			return Delivery{}, ""
		}
		capacity := len(s.queued)
		evicted := s.queued[s.head]
		s.queued[s.head] = Delivery{}
		s.head = (s.head + 1) % capacity
		s.queued[(s.head+s.count-1)%capacity] = delivery
		s.noteGap(evicted, "subscriber_buffer_evicted")
		s.signal()
		return evicted, "subscriber_buffer_evicted"
	}

	s.noteGap(delivery, "subscriber_buffer_full")
	return delivery, "subscriber_buffer_full"
}

// noteGap remembers a message discarded while the subscription is paused
// so Resume and recovery reports can explain the exact replay gap: the
// reason, the matched rules, and the retention policy in effect right
// now. Later policy switches never rewrite gaps recorded earlier.
// Callers hold s.mu.
func (s *subscription) noteGap(delivery Delivery, code string) {
	if s.paused {
		s.gaps = append(s.gaps, Gap{
			MessageID:   delivery.Message.id,
			Topic:       delivery.Message.topic,
			Code:        code,
			Reason:      gapReason(code),
			RuleIDs:     append([]string(nil), delivery.RuleIDs...),
			RuleVersion: delivery.RuleVersion,
			Policy:      s.overflow,
		})
	}
}

func gapReason(code string) string {
	switch code {
	case "subscriber_buffer_evicted":
		return "policy keep_newest evicted the oldest buffered message"
	default:
		return "subscriber did not accept delivery without blocking; buffered or handoff messages were retained"
	}
}

// close marks the subscription closed and wakes the pump. The pump then
// discards every accepted-but-unreceived delivery into s.discarded, closes
// the consumer channel, and exits; Unsubscribe waits for that exit before
// recording the discarded messages, so a message is never both delivered
// and reported as cancelled.
func (s *subscription) close() {
	s.mu.Lock()
	s.closed = true
	s.signal()
	s.mu.Unlock()
}

func (s *subscription) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Publish dispatches a message using one atomic subscription/rule snapshot.
// A full subscriber buffer causes only that delivery to be dropped; other
// subscribers are unaffected and Publish never waits for a slow consumer.
func (r *Router) Publish(message Message) error {
	if message.id == "" || message.topic == "" {
		err := fmt.Errorf("%w: use NewMessage to construct messages", ErrInvalidMessage)
		r.addRecord(Record{
			Kind:      RecordRejected,
			Code:      "invalid_message",
			Reason:    err.Error(),
			MessageID: message.id,
			Topic:     message.topic,
		})
		return err
	}

	r.mu.RLock()
	type resolved struct {
		entry   *subscription
		ruleIDs []string
		version int
	}
	targets := make([]resolved, 0, len(r.subscribers))
	for _, entry := range r.subscribers {
		rules, version := entry.snapshot()
		ruleIDs := matchRules(rules, message)
		if len(ruleIDs) == 0 {
			continue
		}
		entry.inFlight.Add(1)
		targets = append(targets, resolved{entry: entry, ruleIDs: ruleIDs, version: version})
	}
	r.mu.RUnlock()

	for _, target := range targets {
		delivery := Delivery{
			Message:        message,
			SubscriptionID: target.entry.id,
			RuleIDs:        target.ruleIDs,
			RuleVersion:    target.version,
		}
		discarded, code := target.entry.offer(delivery)
		switch code {
		case "":
		case "subscriber_buffer_full":
			r.addRecord(Record{
				Time:           time.Now(),
				Kind:           RecordDropped,
				Code:           "subscriber_buffer_full",
				Reason:         "subscriber did not accept delivery without blocking; buffered or handoff messages were retained",
				MessageID:      message.id,
				Topic:          message.topic,
				SubscriptionID: target.entry.id,
				RuleIDs:        append([]string(nil), target.ruleIDs...),
				RuleVersion:    target.version,
			})
		case "subscriber_buffer_evicted":
			r.addRecord(Record{
				Time:           time.Now(),
				Kind:           RecordDropped,
				Code:           "subscriber_buffer_evicted",
				Reason:         "policy keep_newest evicted the oldest buffered message",
				MessageID:      discarded.Message.id,
				Topic:          discarded.Message.topic,
				SubscriptionID: target.entry.id,
				RuleIDs:        append([]string(nil), discarded.RuleIDs...),
				RuleVersion:    discarded.RuleVersion,
			})
		}
		target.entry.inFlight.Done()
	}
	r.persist()
	return nil
}

// Unsubscribe prevents new deliveries and waits for in-flight offers to
// finish. It then cancels the subscription outright: every message the
// consumer has not received yet (buffered backlog plus any in-flight
// handoff) is discarded and recorded with code "subscription_cancelled",
// and the delivery channel is closed. Once Unsubscribe returns, no further
// delivery can reach the consumer — the pump goroutine has already exited.
// Messages the consumer did receive before cancellation remain theirs to
// keep; nothing is reclaimed. Other subscriptions are unaffected, and a
// later Subscribe with the same ID starts from a completely fresh state:
// no leftover messages ever migrate to the new subscription.
func (r *Router) Unsubscribe(id string) error {
	r.mu.Lock()
	entry, ok := r.subscribers[id]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrSubscriptionGone, id)
	}
	delete(r.subscribers, id)
	r.mu.Unlock()

	entry.inFlight.Wait()
	entry.close()
	<-entry.done
	for _, delivery := range entry.discarded {
		r.addRecord(Record{
			Time:           time.Now(),
			Kind:           RecordDropped,
			Code:           "subscription_cancelled",
			Reason:         "subscription cancelled; unreceived message discarded at the cancellation boundary",
			MessageID:      delivery.Message.id,
			Topic:          delivery.Message.topic,
			SubscriptionID: delivery.SubscriptionID,
			RuleIDs:        append([]string(nil), delivery.RuleIDs...),
			RuleVersion:    delivery.RuleVersion,
		})
	}
	// The cancelled subscription is gone from the router, so persisting
	// now drops it from the state file as well: a restart cannot revive
	// it, and a same-name resubscribe starts from a clean slate.
	r.persist()
	return nil
}

// Pause stops the pump from handing deliveries to the consumer. Publishing
// continues unaffected: matched messages still enter the subscription's
// bounded buffer and the subscription's own retention policy (KeepOldest or
// KeepNewest) decides what survives while it is paused. The publisher never
// blocks on a paused subscriber, and other subscriptions are unaffected.
func (r *Router) Pause(id string) error {
	if id == "" {
		err := fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "invalid_subscription",
			Reason: err.Error(),
		})
		return err
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
		return err
	}
	entry.mu.Lock()
	if entry.paused {
		entry.mu.Unlock()
		err := fmt.Errorf("%w: %s", ErrSubscriptionPaused, id)
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_already_paused",
			Reason:         err.Error(),
			SubscriptionID: id,
		})
		return err
	}
	entry.paused = true
	entry.signal()
	entry.mu.Unlock()
	r.persist()
	return nil
}

// Resume restarts delivery for a paused subscription and reports what can
// be replayed: the buffered messages (in delivery order) and the messages
// the retention policy evicted during the pause. The replay itself happens
// on the subscription's existing delivery channel right after Resume
// returns. Resuming a subscription that is not paused is rejected.
func (r *Router) Resume(id string) (ResumeReport, error) {
	if id == "" {
		err := fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
		r.addRecord(Record{
			Kind:   RecordRejected,
			Code:   "invalid_subscription",
			Reason: err.Error(),
		})
		return ResumeReport{}, err
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
		return ResumeReport{}, err
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
		return ResumeReport{}, err
	}
	report := ResumeReport{SubscriptionID: id}
	if entry.pending != nil {
		report.Replayable = append(report.Replayable, entry.pending.Message.id)
	}
	for index := 0; index < entry.count; index++ {
		report.Replayable = append(report.Replayable,
			entry.queued[(entry.head+index)%len(entry.queued)].Message.id)
	}
	for _, gap := range entry.gaps {
		report.Evicted = append(report.Evicted, gap.MessageID)
		report.Gaps = append(report.Gaps, copyGap(gap))
	}
	entry.gaps = nil
	entry.paused = false
	entry.signal()
	entry.mu.Unlock()
	r.persist()
	return report, nil
}

func (r *Router) Records() []Record {
	r.auditMu.Lock()
	defer r.auditMu.Unlock()

	records := make([]Record, 0, len(r.history))
	if len(r.history) < r.historySize {
		for _, record := range r.history {
			records = append(records, copyRecord(record))
		}
		return records
	}
	for index := r.nextAudit; index < len(r.history); index++ {
		records = append(records, copyRecord(r.history[index]))
	}
	for index := 0; index < r.nextAudit; index++ {
		records = append(records, copyRecord(r.history[index]))
	}
	return records
}

func (r *Router) addRecord(record Record) {
	if r.historySize == 0 {
		return
	}
	if record.Time.IsZero() {
		record.Time = time.Now()
	}

	r.auditMu.Lock()
	defer r.auditMu.Unlock()
	if len(r.history) < r.historySize {
		r.history = append(r.history, copyRecord(record))
		if len(r.history) == r.historySize {
			r.nextAudit = 0
		}
		return
	}
	r.history[r.nextAudit] = copyRecord(record)
	r.nextAudit = (r.nextAudit + 1) % r.historySize
}

func prepareRules(rules []Rule) ([]Rule, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("%w: at least one rule is required", ErrInvalidRule)
	}

	prepared := make([]Rule, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for index, rule := range rules {
		if rule.ID == "" {
			return nil, fmt.Errorf("%w: rule id is required at index %d", ErrInvalidRule, index)
		}
		if _, exists := seen[rule.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate rule id %s", ErrInvalidRule, rule.ID)
		}
		seen[rule.ID] = struct{}{}
		if rule.Topic == "" {
			return nil, fmt.Errorf("%w: rule %s requires a topic", ErrInvalidRule, rule.ID)
		}

		attrs := make(map[string]string, len(rule.Attrs))
		for key, value := range rule.Attrs {
			if key == "" {
				return nil, fmt.Errorf("%w: rule %s has an empty attribute key", ErrInvalidRule, rule.ID)
			}
			attrs[key] = value
		}
		prepared[index] = Rule{ID: rule.ID, Topic: rule.Topic, Attrs: attrs}
	}
	return prepared, nil
}

func matchRules(rules []Rule, message Message) []string {
	var matched []string
	for _, rule := range rules {
		if rule.Topic != message.topic {
			continue
		}
		matches := true
		for key, value := range rule.Attrs {
			if actual, ok := message.attrs[key]; !ok || actual != value {
				matches = false
				break
			}
		}
		if matches {
			matched = append(matched, rule.ID)
		}
	}
	sort.Strings(matched)
	return matched
}

func copyRecord(record Record) Record {
	record.RuleIDs = append([]string(nil), record.RuleIDs...)
	return record
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
