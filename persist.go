package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Gap describes a message the retention policy discarded while the
// subscription was paused. It keeps the reason, the matched rules, and the
// overflow policy in effect at the moment the gap was created, so a later
// policy switch never rewrites the history of an existing gap.
type Gap struct {
	MessageID   string
	Topic       string
	Code        string
	Reason      string
	RuleIDs     []string
	RuleVersion int
	// Policy is the overflow policy that produced this gap. Switching the
	// policy afterwards does not alter gaps recorded earlier.
	Policy OverflowPolicy
}

func copyGap(gap Gap) Gap {
	gap.RuleIDs = append([]string(nil), gap.RuleIDs...)
	return gap
}

// RecoveryReport describes one subscription restored from the state file
// when the router starts: which messages can still be replayed (in
// delivery order) and which were evicted under the retention policy
// before the restart, with their original reasons and matched rules.
type RecoveryReport struct {
	SubscriptionID string
	// Paused reports whether the subscription was paused when the state
	// was persisted; a paused subscription stays paused after recovery
	// until Resume is called.
	Paused bool
	// Replayable lists the unreceived message IDs in the order they will
	// be delivered on the subscription's channel.
	Replayable []string
	// Gaps lists the messages the retention policy discarded while the
	// subscription was paused, with the reason and rules at gap time.
	Gaps []Gap
}

// persistedMessage is the serializable form of Message.
type persistedMessage struct {
	ID    string            `json:"id"`
	Topic string            `json:"topic"`
	Attrs map[string]string `json:"attrs,omitempty"`
}

type persistedDelivery struct {
	Message     persistedMessage `json:"message"`
	RuleIDs     []string         `json:"rule_ids,omitempty"`
	RuleVersion int              `json:"rule_version"`
}

type persistedSubscription struct {
	ID         string              `json:"id"`
	Rules      []Rule              `json:"rules"`
	Version    int                 `json:"version"`
	Overflow   OverflowPolicy      `json:"overflow"`
	BufferSize int                 `json:"buffer_size"`
	Paused     bool                `json:"paused"`
	Backlog    []persistedDelivery `json:"backlog,omitempty"`
	Gaps       []Gap               `json:"gaps,omitempty"`
}

type persistedState struct {
	Subscriptions []persistedSubscription `json:"subscriptions"`
}

func persistMessage(message Message) persistedMessage {
	attrs := make(map[string]string, len(message.attrs))
	for key, value := range message.attrs {
		attrs[key] = value
	}
	return persistedMessage{ID: message.id, Topic: message.topic, Attrs: attrs}
}

func restoreMessage(message persistedMessage) Message {
	attrs := make(map[string]string, len(message.Attrs))
	for key, value := range message.Attrs {
		attrs[key] = value
	}
	return Message{id: message.ID, topic: message.Topic, attrs: attrs}
}

func persistDelivery(delivery Delivery) persistedDelivery {
	return persistedDelivery{
		Message:     persistMessage(delivery.Message),
		RuleIDs:     append([]string(nil), delivery.RuleIDs...),
		RuleVersion: delivery.RuleVersion,
	}
}

func restoreDelivery(subscriptionID string, delivery persistedDelivery) Delivery {
	return Delivery{
		Message:        restoreMessage(delivery.Message),
		SubscriptionID: subscriptionID,
		RuleIDs:        append([]string(nil), delivery.RuleIDs...),
		RuleVersion:    delivery.RuleVersion,
	}
}

// stateStore persists the router's recoverable subscription state as one
// JSON document. Writes go to a temporary file that is atomically renamed
// over the state file, so a crash mid-write cannot corrupt the last good
// state.
type stateStore struct {
	path string
	mu   sync.Mutex
}

func (s *stateStore) save(state persistedState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *stateStore) load() (persistedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return persistedState{}, nil
	}
	if err != nil {
		return persistedState{}, err
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistedState{}, fmt.Errorf("state file %s: %w", s.path, err)
	}
	return state, nil
}
