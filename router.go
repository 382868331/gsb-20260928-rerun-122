package router

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalidMessage      = errors.New("invalid message")
	ErrInvalidSubscription = errors.New("invalid subscription")
	ErrInvalidRule         = errors.New("invalid rule")
	ErrSubscriptionExists  = errors.New("subscription already exists")
	ErrSubscriptionGone    = errors.New("subscription not found")
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
	// PolicyVersion is the version of the overflow policy in effect when the
	// message was accepted (or discarded) for this subscriber. It starts at 1
	// and advances only on a successful UpdateOverflowPolicy call that actually
	// changes the policy. Every delivery, drop and eviction record is stamped
	// with it, so backlog observed after a switch is attributable to the exact
	// policy revision that retained or removed it.
	PolicyVersion int
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
	// PolicyVersion is the overflow-policy version that produced the drop or
	// eviction; zero for records that are not tied to subscriber buffering.
	PolicyVersion int
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

type subscription struct {
	id         string
	bufferSize int
	overflow   OverflowPolicy
	deliveries chan Delivery

	mu      sync.Mutex
	rules   []Rule
	version int

	// policyVersion starts at 1 and advances only when the overflow policy is
	// actually changed via UpdateOverflowPolicy.
	policyVersion int

	// queued is the ring of accepted-but-not-yet-consumed deliveries.
	// It transiently holds at most bufferSize+1 entries: bufferSize retained
	// deliveries plus, optionally, one in-flight handoff delivery that the
	// pump is currently offering to the consumer. Counting the handoff as a
	// real backlog entry means KeepNewest can evict it: a message the consumer
	// has not yet accepted is treated identically to any other buffered
	// message, so moving it into the handoff slot never pins it.
	queued []Delivery
	head   int
	count  int
	closed bool

	// pending is the delivery the pump is currently handing to the consumer.
	// It is physically removed from the ring but logically still the oldest
	// unconsumed message; when KeepNewest must evict it, reclaim is closed and
	// pendingHeld is cleared while the pump is blocked in the channel send.
	pending     *Delivery
	pendingHeld bool
	reclaim     chan struct{}
	// pendingEvicted is populated by offer when it has marked the in-flight
	// handoff for eviction. The pump consumes it once: when the reclaim wins
	// the race it is reported as the evicted delivery; when the send wins the
	// consumer genuinely accepted the handoff first, so it is discarded.
	pendingEvicted *Delivery

	notify   chan struct{}
	inFlight sync.WaitGroup
}

type Router struct {
	mu          sync.RWMutex
	bufferSize  int
	historySize int
	subscribers map[string]*subscription

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
	return r, nil
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

	// Every subscription uses the same unbuffered channel plus an internal
	// bounded ring driven by a pump goroutine. Bounds and overflow handling
	// are enforced entirely inside offer(); the consumer-facing channel only
	// transfers one delivery at a time. This keeps the policy switchable at
	// runtime and makes handoff deliveries reclaimable.
	entry.deliveries = make(chan Delivery)
	entry.rules = prepared
	entry.version = 1
	entry.policyVersion = 1
	entry.queued = make([]Delivery, entry.bufferSize+1)
	entry.notify = make(chan struct{}, 1)

	r.mu.Lock()
	if _, exists := r.subscribers[id]; exists {
		r.mu.Unlock()
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionExists, id)
	}
	r.subscribers[id] = entry
	r.mu.Unlock()

	go entry.pump(r)

	return Subscription{ID: id, Deliveries: entry.deliveries}, nil
}

// snapshot returns the currently active rules and rule version as one atomic
// read. Overflow policy is resolved later, when the offer acquires the queue
// lock, so a successful UpdateOverflowPolicy is linearized before every offer
// that starts after it returns.
func (s *subscription) snapshot() ([]Rule, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rules, s.version
}

// pump forwards accepted deliveries to the consumer in FIFO order. While a
// send is in flight the delivery remains the oldest unconsumed message: a
// concurrent KeepNewest offer may reclaim it by closing reclaim. The race
// between the send and the reclaim is resolved deterministically:
//
//   - if the send wins, the consumer accepted the delivery, so it is
//     permanently consumed and the pending eviction is discarded;
//   - if reclaim wins (or was already closed), the consumer has not yet
//     accepted anything, and the handoff is aborted without a delivery on
//     the wire; the eviction stands.
func (s *subscription) pump(router *Router) {
	for {
		s.mu.Lock()
		if s.pending != nil && !s.pendingHeld {
			// Previous handoff completed or was reclaimed; release it before
			// picking up the next queued delivery.
			s.pending = nil
		}
		if s.pending == nil {
			if s.count > 0 {
				delivery := s.queued[s.head]
				s.queued[s.head] = Delivery{}
				s.head = (s.head + 1) % len(s.queued)
				s.count--
				s.pending = &delivery
				s.pendingHeld = true
				s.reclaim = make(chan struct{})
			} else if s.closed {
				s.mu.Unlock()
				close(s.deliveries)
				return
			}
		}
		delivery := s.pending
		notify := s.notify
		reclaim := s.reclaim
		s.mu.Unlock()

		if delivery == nil {
			<-notify
			continue
		}

		select {
		case s.deliveries <- *delivery:
			s.mu.Lock()
			// A value received from an unbuffered channel is by definition
			// already in the consumer's hands; even a racing reclaim cannot
			// take it back. Discard any eviction intent for this handoff.
			s.pendingHeld = false
			s.pendingEvicted = nil
			s.mu.Unlock()
		case <-reclaim:
			s.mu.Lock()
			// Reclaim was selected over the send, so nothing reached the
			// consumer. offer() staged the evicted handoff; release the slot
			// and report it. (If the send happened to win first instead, this
			// branch is never reached for that handoff: the channel send
			// case cleared pendingEvicted and replaced reclaim with a fresh
			// channel for the next delivery.)
			evicted := s.pendingEvicted
			s.pendingEvicted = nil
			s.pendingHeld = false
			s.pending = nil
			s.mu.Unlock()
			if evicted != nil {
				router.recordEviction(*evicted)
			}
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
	return nil
}

// recordEviction records a delivery that KeepNewest removed from the backlog
// before the consumer accepted it. The delivery itself carries the rule and
// policy versions in effect when that message had been accepted.
func (r *Router) recordEviction(discarded Delivery) {
	r.addRecord(Record{
		Time:           time.Now(),
		Kind:           RecordDropped,
		Code:           "subscriber_buffer_evicted",
		Reason:         "policy keep_newest evicted the oldest unconsumed message, including an in-flight handoff reclaimed before the consumer accepted it",
		MessageID:      discarded.Message.id,
		Topic:          discarded.Message.topic,
		SubscriptionID: discarded.SubscriptionID,
		RuleIDs:        append([]string(nil), discarded.RuleIDs...),
		RuleVersion:    discarded.RuleVersion,
		PolicyVersion:  discarded.PolicyVersion,
	})
}

// UpdateOverflowPolicy switches a running subscriber's overflow policy.
//
// The switch is atomic and applies to offers that start after it returns;
// offers already resolved under the old policy are unaffected. Messages
// already in the backlog are never purged by the switch itself: they keep
// their FIFO positions and are simply judged by the new policy once more
// messages arrive at capacity.
//
// Concretely:
//   - switching to KeepOldest while full: the existing backlog (oldest
//     messages) is retained and subsequent incoming messages are dropped;
//   - switching to KeepNewest while full: the existing backlog is kept until
//     the next incoming message, at which point the oldest unconsumed
//     message is evicted.
//
// A successful switch to a different policy advances that subscriber's
// policy version (the initial policy is version 1). Switching to the policy
// already in effect is a successful no-op and does not advance the version.
// The choice affects only the named subscriber.
func (r *Router) UpdateOverflowPolicy(subscriptionID string, policy OverflowPolicy) error {
	switch policy {
	case KeepOldest, KeepNewest:
	default:
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
	defer entry.mu.Unlock()
	if entry.closed {
		return fmt.Errorf("%w: %s", ErrSubscriptionGone, subscriptionID)
	}
	if entry.overflow == policy {
		return nil
	}
	entry.overflow = policy
	entry.policyVersion++
	return nil
}

// offer accepts a matched delivery into the bounded buffer under the
// currently selected overflow policy. It never blocks on the consumer.
//
// Every unconsumed message counts toward the capacity, including the one the
// pump is currently handing off: a message the consumer has not yet accepted
// cannot escape retention bookkeeping merely by being in the handoff slot.
//
//   - KeepOldest: at capacity, or when capacity is zero, the incoming
//     message is dropped and reported as subscriber_buffer_full.
//   - KeepNewest: at capacity, the oldest unconsumed message is evicted so
//     the newest one is retained in FIFO order. If the oldest is the live
//     handoff, offer stages a reclaim intent; the pump decides whether the
//     reclaim or the consumer's receive wins and records the eviction only
//     if the message really never reached the consumer.
func (s *subscription) offer(delivery Delivery) (evicted Delivery, dropped bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return delivery, true
	}
	if s.bufferSize == 0 {
		delivery.PolicyVersion = s.policyVersion
		s.mu.Unlock()
		select {
		case s.deliveries <- delivery:
			return Delivery{}, false
		default:
			return delivery, true
		}
	}
	defer s.mu.Unlock()
	delivery.PolicyVersion = s.policyVersion

	inFlight := s.pending != nil && s.pendingHeld
	backlog := s.count
	if inFlight {
		backlog++
	}
	if backlog < s.bufferSize {
		s.enqueue(delivery)
		return Delivery{}, false
	}

	if s.overflow == KeepOldest {
		return delivery, true
	}

	// KeepNewest at capacity: evict the oldest unconsumed message.
	if inFlight && s.pendingEvicted == nil {
		// Oldest is the in-flight handoff. Stage the new delivery and ask the
		// pump to abandon the send. The ring transiently holds bufferSize+1
		// entries until the pump resolves the reclaim/send race.
		staged := *s.pending
		s.pendingEvicted = &staged
		s.enqueue(delivery)
		close(s.reclaim)
		return Delivery{}, false
	}

	capacity := len(s.queued)
	evicted = s.queued[s.head]
	s.queued[s.head] = Delivery{}
	s.head = (s.head + 1) % capacity
	s.count--
	s.enqueue(delivery)
	return evicted, false
}

// enqueue appends a delivery to the ring and wakes an idle pump. Callers
// hold s.mu.
func (s *subscription) enqueue(delivery Delivery) {
	capacity := len(s.queued)
	s.queued[(s.head+s.count)%capacity] = delivery
	s.count++
	if s.pending == nil && s.count == 1 {
		s.signal()
	}
}

// close marks the subscription closed; the pump finishes sending every
// queued message (including any live handoff) and then closes the consumer
// channel.
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
		discarded, dropped := target.entry.offer(delivery)
		if dropped {
			r.addRecord(Record{
				Time:           time.Now(),
				Kind:           RecordDropped,
				Code:           "subscriber_buffer_full",
				Reason:         "overflow policy retained the existing unconsumed backlog and dropped the incoming message",
				MessageID:      message.id,
				Topic:          message.topic,
				SubscriptionID: target.entry.id,
				RuleIDs:        append([]string(nil), target.ruleIDs...),
				RuleVersion:    target.version,
				PolicyVersion:  discarded.PolicyVersion,
			})
		} else if discarded.Message.id != "" {
			r.recordEviction(discarded)
		}
		target.entry.inFlight.Done()
	}
	return nil
}

// Unsubscribe prevents new deliveries and waits for in-flight offers to
// finish. It then marks the subscription closed: every message already
// accepted into the bounded buffer (the drain backlog) is still delivered
// to the consumer in order, followed by channel close.
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
	return nil
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
