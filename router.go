// Package messagerouter performs in-process topic and attribute routing.
//
// Subscriptions, rule changes, and delivery records are scoped to a Router
// instance. Replacing the local transport boundary later does not require
// global subscription state.
package messagerouter

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrClosed          = errors.New("message router is closed")
	ErrEmptyID         = errors.New("id must not be empty")
	ErrDuplicateID     = errors.New("duplicate id")
	ErrDuplicateRuleID = errors.New("duplicate rule id")
	ErrNotFound        = errors.New("subscription not found")
)

type RecordKind string

const (
	RecordRejected  RecordKind = "rejected"
	RecordDiscarded RecordKind = "discarded"
)

type Attributes map[string]string

type Message struct {
	ID         string
	Topic      string
	Attributes Attributes
}

type Rule struct {
	ID         string
	Topic      string
	Attributes Attributes
}

func (rule Rule) matches(message Message) bool {
	if rule.Topic != message.Topic {
		return false
	}
	for key, value := range rule.Attributes {
		if message.Attributes[key] != value {
			return false
		}
	}
	return true
}

type Delivery struct {
	SubscriptionID string
	Message        Message
	RuleIDs        []string
}

type Record struct {
	Kind           RecordKind
	Reason         string
	SubscriptionID string
	MessageID      string
	RuleIDs        []string
	Time           time.Time
}

type Subscription struct {
	ID     string
	C      <-chan Delivery
	router *Router
}

func (subscription *Subscription) ReplaceRules(rules ...Rule) error {
	return subscription.router.ReplaceRules(subscription.ID, rules)
}

func (subscription *Subscription) Unsubscribe() {
	subscription.router.Unsubscribe(subscription.ID)
}

type Router struct {
	mu              sync.RWMutex
	subscriptions   map[string]*subscriber
	closed          bool
	defaultCapacity int

	recordsMu sync.Mutex
	records   []Record
}

func NewRouter(capacity ...int) *Router {
	capValue := 16
	if len(capacity) > 0 && capacity[0] > 0 {
		capValue = capacity[0]
	}
	return &Router{
		subscriptions:   make(map[string]*subscriber),
		defaultCapacity: capValue,
	}
}

func (router *Router) Subscribe(id string, rules []Rule, capacity ...int) (*Subscription, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	compiledRules, err := cloneRules(rules)
	if err != nil {
		return nil, err
	}
	queueCapacity := router.defaultCapacity
	if len(capacity) > 0 && capacity[0] > 0 {
		queueCapacity = capacity[0]
	}

	delivery := make(chan Delivery, queueCapacity)
	sub := &subscriber{
		id:       id,
		rules:    compiledRules,
		delivery: delivery,
		router:   router,
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	if router.closed {
		return nil, ErrClosed
	}
	if _, exists := router.subscriptions[id]; exists {
		return nil, ErrDuplicateID
	}
	router.subscriptions[id] = sub
	return &Subscription{ID: id, C: delivery, router: router}, nil
}

func (router *Router) Publish(message Message) error {
	if message.ID == "" {
		return ErrEmptyID
	}
	message = cloneMessage(message)

	router.mu.RLock()
	if router.closed {
		router.mu.RUnlock()
		return ErrClosed
	}
	targets := make([]*subscriber, 0, len(router.subscriptions))
	for _, sub := range router.subscriptions {
		targets = append(targets, sub)
	}
	router.mu.RUnlock()

	for _, sub := range targets {
		delivery, matched := sub.match(message)
		if !matched {
			continue
		}
		accepted, reason := sub.offer(delivery)
		if !accepted {
			sub.recordRejection(delivery, reason)
		}
	}
	return nil
}

// ReplaceRules atomically replaces the rules visible to later matching calls.
// Deliveries already in a subscription channel retain the rules that matched
// them.
func (router *Router) ReplaceRules(subscriptionID string, rules []Rule) error {
	compiledRules, err := cloneRules(rules)
	if err != nil {
		return err
	}
	router.mu.RLock()
	sub := router.subscriptions[subscriptionID]
	router.mu.RUnlock()
	if sub == nil {
		return ErrNotFound
	}
	sub.replaceRules(compiledRules)
	return nil
}

func (router *Router) Unsubscribe(id string) {
	router.mu.Lock()
	sub := router.subscriptions[id]
	delete(router.subscriptions, id)
	router.mu.Unlock()
	if sub != nil {
		sub.cancel()
	}
}

func (router *Router) Close() {
	router.mu.Lock()
	if router.closed {
		router.mu.Unlock()
		return
	}
	router.closed = true
	targets := make([]*subscriber, 0, len(router.subscriptions))
	for _, sub := range router.subscriptions {
		targets = append(targets, sub)
	}
	router.subscriptions = make(map[string]*subscriber)
	router.mu.Unlock()

	for _, sub := range targets {
		sub.cancel()
	}
}

func (router *Router) Records() []Record {
	router.recordsMu.Lock()
	defer router.recordsMu.Unlock()
	records := make([]Record, len(router.records))
	copy(records, router.records)
	return records
}

func (router *Router) addRecord(record Record) {
	router.recordsMu.Lock()
	router.records = append(router.records, record)
	router.recordsMu.Unlock()
}

type subscriber struct {
	id       string
	mu       sync.RWMutex
	rules    map[string]Rule
	delivery chan Delivery
	stopped  bool
	router   *Router
}

func (sub *subscriber) match(message Message) (Delivery, bool) {
	sub.mu.RLock()
	ruleIDs := make([]string, 0)
	for _, rule := range sub.rules {
		if rule.matches(message) {
			ruleIDs = append(ruleIDs, rule.ID)
		}
	}
	sub.mu.RUnlock()
	if len(ruleIDs) == 0 {
		return Delivery{}, false
	}
	sort.Strings(ruleIDs)
	return Delivery{
		SubscriptionID: sub.id,
		Message:        message,
		RuleIDs:        ruleIDs,
	}, true
}

func (sub *subscriber) replaceRules(rules map[string]Rule) {
	sub.mu.Lock()
	sub.rules = rules
	sub.mu.Unlock()
}

func (sub *subscriber) offer(delivery Delivery) (bool, string) {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	if sub.stopped {
		return false, "subscription canceled"
	}
	select {
	case sub.delivery <- delivery:
		return true, ""
	default:
		return false, "subscription queue full"
	}
}

func (sub *subscriber) cancel() {
	sub.mu.Lock()
	if sub.stopped {
		sub.mu.Unlock()
		return
	}
	sub.stopped = true

	for {
		select {
		case delivery := <-sub.delivery:
			sub.recordDiscard(delivery, "canceled before delivery")
		default:
			sub.mu.Unlock()
			close(sub.delivery)
			return
		}
	}
}

func (sub *subscriber) recordRejection(delivery Delivery, reason string) {
	sub.router.addRecord(Record{
		Kind:           RecordRejected,
		Reason:         reason,
		SubscriptionID: sub.id,
		MessageID:      delivery.Message.ID,
		RuleIDs:        append([]string(nil), delivery.RuleIDs...),
		Time:           time.Now(),
	})
}

func (sub *subscriber) recordDiscard(delivery Delivery, reason string) {
	sub.router.addRecord(Record{
		Kind:           RecordDiscarded,
		Reason:         reason,
		SubscriptionID: sub.id,
		MessageID:      delivery.Message.ID,
		RuleIDs:        append([]string(nil), delivery.RuleIDs...),
		Time:           time.Now(),
	})
}

func cloneRules(rules []Rule) (map[string]Rule, error) {
	compiled := make(map[string]Rule, len(rules))
	for _, rule := range rules {
		if rule.ID == "" {
			return nil, ErrEmptyID
		}
		if _, exists := compiled[rule.ID]; exists {
			return nil, ErrDuplicateRuleID
		}
		rule.Attributes = cloneAttributes(rule.Attributes)
		compiled[rule.ID] = rule
	}
	return compiled, nil
}

func cloneMessage(message Message) Message {
	message.Attributes = cloneAttributes(message.Attributes)
	return message
}

func cloneAttributes(attributes Attributes) Attributes {
	if len(attributes) == 0 {
		return nil
	}
	result := make(Attributes, len(attributes))
	for key, value := range attributes {
		result[key] = value
	}
	return result
}
