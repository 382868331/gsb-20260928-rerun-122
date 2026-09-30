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

	// direct subscriptions (KeepOldest) publish straight into the buffered
	// channel; pump/ring fields stay unused.
	direct bool

	mu      sync.Mutex
	rules   []Rule
	version int

	// queued is the bounded ring of accepted-but-not-yet-sent deliveries.
	queued []Delivery
	head   int
	count  int
	closed bool

	// pending holds the oldest queued delivery while the pump is waiting
	// for the consumer to accept it. Keeping it out of the ring lets the
	// pump continue accepting newer messages (and apply KeepNewest).
	pending *Delivery

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

	var ch chan Delivery
	if entry.overflow == KeepNewest {
		ch = make(chan Delivery)
	} else {
		ch = make(chan Delivery, entry.bufferSize)
	}
	entry.deliveries = ch
	entry.rules = prepared
	entry.version = 1
	if entry.overflow == KeepNewest {
		entry.direct = false
		entry.queued = make([]Delivery, maxInt(entry.bufferSize, 1))
		entry.notify = make(chan struct{}, 1)
	} else {
		entry.direct = true
	}

	r.mu.Lock()
	if _, exists := r.subscribers[id]; exists {
		r.mu.Unlock()
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionExists, id)
	}
	r.subscribers[id] = entry
	r.mu.Unlock()

	if entry.overflow == KeepNewest {
		go entry.pump(r)
	}

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
// block Publish and never affects how newer messages enter the ring.
func (s *subscription) pump(router *Router) {
	for {
		s.mu.Lock()
		if s.pending == nil {
			if s.count > 0 {
				delivery := s.queued[s.head]
				s.queued[s.head] = Delivery{}
				s.head = (s.head + 1) % len(s.queued)
				s.count--
				s.pending = &delivery
			} else if s.closed {
				s.mu.Unlock()
				close(s.deliveries)
				return
			}
		}
		delivery := s.pending
		notify := s.notify
		s.mu.Unlock()

		if delivery != nil {
			s.deliveries <- *delivery
			s.mu.Lock()
			s.pending = nil
			s.mu.Unlock()
			continue
		}

		_ = notify
		<-notify
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

// offer accepts a matched delivery into the bounded buffer. The return value
// is the delivery that had to be discarded (if any) along with a stable
// copy; offers never block on the consumer.
func (s *subscription) deliver(router *Router, delivery Delivery) (Delivery, bool) {
	if s.direct {
		select {
		case s.deliveries <- delivery:
			return Delivery{}, false
		default:
			return delivery, true
		}
	}
	return s.offer(delivery)
}

func (s *subscription) offer(delivery Delivery) (Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return delivery, true
	}

	// The ring holds up to bufferSize buffered messages; the pump's
	// active handoff (pending) is one additional non-reclaimable slot.
	// KeepNewest always makes room for the newest message by evicting the
	// oldest buffered message when the ring is full. The pinned handoff
	// message, if any, is the oldest message still in the system and is
	// never reclaimed mid-send.
	if s.count < s.bufferSize {
		capacity := len(s.queued)
		s.queued[(s.head+s.count)%capacity] = delivery
		s.count++
		if s.pending == nil && s.count == 1 {
			s.signal()
		}
		return Delivery{}, false
	}

	if s.bufferSize == 0 {
		return delivery, true
	}
	capacity := len(s.queued)
	evicted := s.queued[s.head]
	s.queued[s.head] = Delivery{}
	s.head = (s.head + 1) % capacity
	s.queued[(s.head+s.count-1)%capacity] = delivery
	return evicted, false
}

// close marks the subscription closed; the pump finishes sending every
// queued message and then closes the consumer channel.
func (s *subscription) close() {
	s.mu.Lock()
	s.closed = true
	direct := s.direct
	ch := s.deliveries
	if !direct {
		s.signal()
	}
	s.mu.Unlock()
	if direct {
		close(ch)
	}
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
		discarded, dropped := target.entry.deliver(r, delivery)
		if dropped {
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
		} else if discarded.Message.id != "" {
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
