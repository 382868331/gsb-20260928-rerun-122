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
	// RuleVersion is the subscription rule-set version that matched this
	// message. The version starts at 1 when Subscribe succeeds and is
	// incremented by every successful UpdateRules call. It lets a consumer
	// tell which edition of the rules produced a delivery when publishes
	// race with rule updates.
	RuleVersion int
}

type RecordKind string

const (
	RecordRejected RecordKind = "rejected"
	RecordDropped  RecordKind = "dropped"
)

// Drop reason codes recorded on RecordDropped entries.
const (
	// DropReasonBufferFull is used with OverflowKeepOldest: the buffer was
	// full so the new message was discarded and the oldest buffered
	// messages were retained.
	DropReasonBufferFull = "subscriber_buffer_full"
	// DropReasonOldestEvicted is used with OverflowKeepNewest: the buffer
	// was full so the oldest buffered message was evicted to make room
	// for the new message.
	DropReasonOldestEvicted = "subscriber_oldest_evicted"
)

// OverflowPolicy decides what happens to a single subscription whose bounded
// buffer is full. A slow subscription never blocks Publish or affects any
// other subscription regardless of the chosen policy.
type OverflowPolicy string

const (
	// OverflowKeepOldest keeps the messages already in the buffer and
	// discards the new one (the default).
	OverflowKeepOldest OverflowPolicy = "keep_oldest"
	// OverflowKeepNewest evicts the oldest buffered message so the newest
	// one can always be delivered.
	OverflowKeepNewest OverflowPolicy = "keep_newest"
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
	// RuleVersion is the subscription rule-set version in effect for the
	// operation. For a dropped delivery it identifies the rule edition the
	// message was matched against; EvictedMessageID identifies which
	// buffered message was discarded under OverflowKeepNewest.
	RuleVersion      int
	EvictedMessageID string
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

// WithOverflowPolicy sets the default overflow policy for subscriptions that
// do not override it with WithSubscriptionOverflowPolicy.
func WithOverflowPolicy(policy OverflowPolicy) Option {
	return func(r *Router) error {
		if err := validateOverflowPolicy(policy); err != nil {
			return err
		}
		r.overflowPolicy = policy
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

// WithSubscriptionOverflowPolicy chooses between retaining the oldest or the
// newest messages when this subscription cannot keep up. It does not affect
// any other subscription.
func WithSubscriptionOverflowPolicy(policy OverflowPolicy) SubscribeOption {
	return func(sub *subscription) error {
		if err := validateOverflowPolicy(policy); err != nil {
			return err
		}
		sub.overflowPolicy = policy
		return nil
	}
}

func validateOverflowPolicy(policy OverflowPolicy) error {
	switch policy {
	case OverflowKeepOldest, OverflowKeepNewest:
		return nil
	default:
		return fmt.Errorf("%w: unknown overflow policy %q", ErrInvalidSubscription, policy)
	}
}

type Subscription struct {
	ID         string
	Deliveries <-chan Delivery
}

type subscription struct {
	id             string
	rules          []Rule
	ruleVersion    int
	bufferSize     int
	overflowPolicy OverflowPolicy
	deliveries     chan Delivery
	sendMu         sync.Mutex
	inFlight       sync.WaitGroup
}

type Router struct {
	mu             sync.RWMutex
	bufferSize     int
	historySize    int
	overflowPolicy OverflowPolicy
	subscribers    map[string]*subscription

	auditMu   sync.Mutex
	history   []Record
	nextAudit int
}

func NewRouter(options ...Option) (*Router, error) {
	r := &Router{
		bufferSize:     defaultBufferSize,
		historySize:    defaultHistorySize,
		overflowPolicy: OverflowKeepOldest,
		subscribers:    make(map[string]*subscription),
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
		id:             id,
		rules:          prepared,
		ruleVersion:    1,
		bufferSize:     r.bufferSize,
		overflowPolicy: r.overflowPolicy,
	}
	for _, option := range options {
		if err := option(entry); err != nil {
			return Subscription{}, err
		}
	}
	ch := make(chan Delivery, entry.bufferSize)
	entry.deliveries = ch

	r.mu.Lock()
	if _, exists := r.subscribers[id]; exists {
		r.mu.Unlock()
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionExists, id)
	}
	r.subscribers[id] = entry
	r.mu.Unlock()

	return Subscription{ID: id, Deliveries: ch}, nil
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
		err := fmt.Errorf("%w: %s", ErrSubscriptionGone, subscriptionID)
		r.mu.Unlock()
		r.addRecord(Record{
			Kind:           RecordRejected,
			Code:           "subscription_gone",
			Reason:         err.Error(),
			SubscriptionID: subscriptionID,
		})
		return err
	}
	entry.rules = prepared
	entry.ruleVersion++
	r.mu.Unlock()
	return nil
}

// Publish dispatches a message using one atomic subscription/rule snapshot.
// A full subscriber buffer causes only that delivery to be dropped; other
// subscribers are unaffected and Publish never waits for a slow consumer.
//
// Each matching subscription is evaluated against one immutable snapshot of
// its rule list, and every produced Delivery is stamped with that snapshot's
// RuleVersion. A message that races with UpdateRules therefore matches either
// the old edition or the new edition entirely, never a mixture of both.
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

	type target struct {
		entry   *subscription
		ruleIDs []string
		version int
	}

	r.mu.RLock()
	targets := make([]target, 0, len(r.subscribers))
	for _, entry := range r.subscribers {
		ruleIDs := matchRules(entry.rules, message)
		if len(ruleIDs) == 0 {
			continue
		}
		entry.inFlight.Add(1)
		targets = append(targets, target{entry: entry, ruleIDs: ruleIDs, version: entry.ruleVersion})
	}
	r.mu.RUnlock()

	for _, target := range targets {
		delivery := Delivery{
			Message:        message,
			SubscriptionID: target.entry.id,
			RuleIDs:        target.ruleIDs,
			RuleVersion:    target.version,
		}
		target.entry.dispatch(delivery, r)
		target.entry.inFlight.Done()
	}
	return nil
}

// Unsubscribe prevents new deliveries, waits for sends using the final snapshot
// to finish, then closes the delivery channel. Messages already accepted into
// the bounded channel remain available and are followed by channel close.
//
// dispatch serializes sends for one subscription so the receive-evict-resend
// sequence of OverflowKeepNewest cannot interleave with concurrent Publishes.
// It never waits for the subscriber: when the bounded buffer is full either
// the new message or the oldest buffered message is discarded and a
// RecordDropped audit entry explains which one and why.
func (sub *subscription) dispatch(delivery Delivery, r *Router) {
	sub.sendMu.Lock()
	defer sub.sendMu.Unlock()

	select {
	case sub.deliveries <- delivery:
		return
	default:
	}

	if sub.overflowPolicy == OverflowKeepNewest {
		select {
		case evicted, ok := <-sub.deliveries:
			if !ok {
				return
			}
			select {
			case sub.deliveries <- delivery:
				r.addRecord(Record{
					Kind:             RecordDropped,
					Code:             DropReasonOldestEvicted,
					Reason:           "subscriber buffer full; oldest buffered message evicted to retain the newest",
					MessageID:        delivery.Message.id,
					Topic:            delivery.Message.topic,
					SubscriptionID:   sub.id,
					RuleIDs:          append([]string(nil), delivery.RuleIDs...),
					RuleVersion:      delivery.RuleVersion,
					EvictedMessageID: evicted.Message.id,
				})
				return
			default:
				// Another goroutine cannot refill the buffer while sendMu is
				// held; fall through to dropping the new message anyway.
			}
		default:
			// Zero-capacity buffer with no waiting receiver: keep nothing new.
		}
	}

	r.addRecord(Record{
		Kind:           RecordDropped,
		Code:           DropReasonBufferFull,
		Reason:         "subscriber did not accept delivery without blocking; buffered messages retained",
		MessageID:      delivery.Message.id,
		Topic:          delivery.Message.topic,
		SubscriptionID: sub.id,
		RuleIDs:        append([]string(nil), delivery.RuleIDs...),
		RuleVersion:    delivery.RuleVersion,
	})
}

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
	close(entry.deliveries)
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
