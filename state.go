package router

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// WithStateFile persists recoverable subscription state as JSONL events.
// A router reopened with the same path can Restore paused or active
// subscriptions and their retained, unreceived messages.
func WithStateFile(path string) Option {
	return func(r *Router) error {
		if path == "" {
			return fmt.Errorf("%w: state file path is required", ErrInvalidSubscription)
		}
		r.statePath = path
		return nil
	}
}

type persistedMessage struct {
	ID    string            `json:"id"`
	Topic string            `json:"topic"`
	Attrs map[string]string `json:"attrs,omitempty"`
}

type persistedRule struct {
	ID    string            `json:"id"`
	Topic string            `json:"topic"`
	Attrs map[string]string `json:"attrs,omitempty"`
}

type persistedGap struct {
	Message persistedMessage `json:"message"`
	RuleIDs []string         `json:"rule_ids,omitempty"`
	Version int              `json:"rule_version"`
	Code    string           `json:"code"`
	Reason  string           `json:"reason"`
}

type persistedSubscription struct {
	ID         string          `json:"id"`
	InstanceID string          `json:"instance_id"`
	BufferSize int             `json:"buffer_size"`
	Overflow   OverflowPolicy  `json:"overflow"`
	Rules      []persistedRule `json:"rules"`
	Version    int             `json:"rule_version"`
	Paused     bool            `json:"paused"`
	Canceled   bool            `json:"canceled"`
	Queued     []persistedGap  `json:"queued,omitempty"`
	Gaps       []persistedGap  `json:"gaps,omitempty"`
	Seq        int64           `json:"seq"`
}

type stateEvent struct {
	GlobalSeq    int64                 `json:"global_seq"`
	Subscription persistedSubscription `json:"subscription"`
}

func (r *Router) loadState() error {
	file, err := os.Open(r.statePath)
	if os.IsNotExist(err) {
		r.restored = make(map[string]*subscription)
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	latest := make(map[string]persistedSubscription)
	latestGlobal := make(map[string]int64)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var event stateEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("corrupt router state: %w", err)
		}
		state := event.Subscription
		key := state.ID + "\x00" + state.InstanceID
		if current, ok := latest[key]; !ok || state.Seq > current.Seq {
			latest[key] = state
			latestGlobal[key] = event.GlobalSeq
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	byID := make(map[string]persistedSubscription)
	for _, state := range latest {
		globalSeq := latestGlobal[state.ID+"\x00"+state.InstanceID]
		current, ok := byID[state.ID]
		currentGlobal := int64(0)
		if ok {
			currentGlobal = latestGlobal[state.ID+"\x00"+current.InstanceID]
		}
		if !ok || globalSeq > currentGlobal {
			byID[state.ID] = state
		}
	}

	r.restored = make(map[string]*subscription, len(byID))
	r.storeSeq = make(map[string]int64, len(byID))
	for _, state := range byID {
		key := state.ID + "\x00" + state.InstanceID
		r.storeSeq[state.ID] = latestGlobal[key]
		if state.Canceled {
			entry := restoredSubscription(state)
			entry.canceled = true
			r.restored[state.ID] = entry
			continue
		}
		r.restored[state.ID] = restoredSubscription(state)
	}
	return nil
}

func restoredSubscription(state persistedSubscription) *subscription {
	rules := make([]Rule, 0, len(state.Rules))
	for _, rule := range state.Rules {
		rules = append(rules, Rule{ID: rule.ID, Topic: rule.Topic, Attrs: cloneAttrs(rule.Attrs)})
	}
	queued := make([]Delivery, 0, len(state.Queued))
	for _, gap := range state.Queued {
		queued = append(queued, persistedDelivery(state.ID, gap))
	}
	gaps := make([]ResumeGap, 0, len(state.Gaps))
	for _, gap := range state.Gaps {
		gaps = append(gaps, persistedResumeGap(state.ID, gap))
	}
	bufferSize := state.BufferSize
	if cap := maxInt(bufferSize, 1); len(queued) > cap {
		bufferSize = len(queued)
	}
	ring := make([]Delivery, maxInt(bufferSize, 1))
	for index, delivery := range queued {
		ring[index] = delivery
	}
	return &subscription{
		id:          state.ID,
		instance:    state.InstanceID,
		bufferSize:  bufferSize,
		overflow:    state.Overflow,
		rules:       rules,
		version:     state.Version,
		paused:      state.Paused,
		pausedDrops: gaps,
		queued:      ring,
		head:        0,
		count:       len(queued),
		seq:         state.Seq,
	}
}

func persistedDelivery(subscriptionID string, gap persistedGap) Delivery {
	return Delivery{
		Message:        messageFromPersisted(gap.Message),
		SubscriptionID: subscriptionID,
		RuleIDs:        append([]string(nil), gap.RuleIDs...),
		RuleVersion:    gap.Version,
	}
}

func persistedResumeGap(subscriptionID string, gap persistedGap) ResumeGap {
	return ResumeGap{
		Delivery: persistedDelivery(subscriptionID, gap),
		Code:     gap.Code,
		Reason:   gap.Reason,
	}
}

func messageFromPersisted(message persistedMessage) Message {
	return Message{id: message.ID, topic: message.Topic, attrs: cloneAttrs(message.Attrs)}
}

func cloneAttrs(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return map[string]string{}
	}
	copied := make(map[string]string, len(attrs))
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
}

func newInstanceID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", timeNowUnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func timeNowUnixNano() int64 { return time.Now().UnixNano() }

func (r *Router) snapshotSubscription(entry *subscription) persistedSubscription {
	entry.seq++
	state := persistedSubscription{
		ID:         entry.id,
		InstanceID: entry.instance,
		BufferSize: entry.bufferSize,
		Overflow:   entry.overflow,
		Version:    entry.version,
		Paused:     entry.paused,
		Canceled:   entry.closed,
		Seq:        entry.seq,
	}
	for _, rule := range entry.rules {
		state.Rules = append(state.Rules, persistedRule{
			ID:    rule.ID,
			Topic: rule.Topic,
			Attrs: cloneAttrs(rule.Attrs),
		})
	}
	appendDelivery := func(target *[]persistedGap, delivery Delivery) {
		*target = append(*target, persistedGap{
			Message: persistedMessage{ID: delivery.Message.id, Topic: delivery.Message.topic, Attrs: cloneAttrs(delivery.Message.attrs)},
			RuleIDs: append([]string(nil), delivery.RuleIDs...),
			Version: delivery.RuleVersion,
		})
	}
	if entry.pending != nil {
		appendDelivery(&state.Queued, *entry.pending)
	}
	for index := 0; index < entry.count; index++ {
		appendDelivery(&state.Queued, entry.queued[(entry.head+index)%len(entry.queued)])
	}
	for _, gap := range entry.pausedDrops {
		persisted := persistedGap{
			Message: persistedMessage{ID: gap.Delivery.Message.id, Topic: gap.Delivery.Message.topic, Attrs: cloneAttrs(gap.Delivery.Message.attrs)},
			RuleIDs: append([]string(nil), gap.Delivery.RuleIDs...),
			Version: gap.Delivery.RuleVersion,
			Code:    gap.Code,
			Reason:  gap.Reason,
		}
		state.Gaps = append(state.Gaps, persisted)
	}
	return state
}

func (r *Router) persist(entry *subscription) error {
	if r.statePath == "" {
		return nil
	}
	entry.mu.Lock()
	state := r.snapshotSubscription(entry)
	entry.mu.Unlock()
	return r.writeState(state)
}

func (r *Router) writeState(state persistedSubscription) error {
	if r.statePath == "" {
		return nil
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(r.statePath), 0o755); err != nil && !os.IsExist(err) {
		return err
	}
	file, err := os.OpenFile(r.statePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	globalSeq := r.nextGlobalSeqLocked()
	encoded, err := json.Marshal(stateEvent{GlobalSeq: globalSeq, Subscription: state})
	if err != nil {
		_ = file.Close()
		return err
	}
	_, writeErr := file.Write(append(encoded, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	directory, err := os.Open(filepath.Dir(r.statePath))
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	if err := directory.Close(); err != nil {
		return err
	}
	r.storeSeq[state.ID] = globalSeq
	return nil
}

func (r *Router) nextGlobalSeqLocked() int64 {
	if r.storeSeq == nil {
		r.storeSeq = make(map[string]int64)
	}
	var latest int64
	for _, seq := range r.storeSeq {
		if seq > latest {
			latest = seq
		}
	}
	latest++
	return latest
}
