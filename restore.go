package router

import (
	"fmt"
	"sort"
)

// Restore reattaches a channel to a subscription recovered from a previous
// process. Paused subscriptions stay paused and can be queried through
// Resume; active subscriptions continue delivering their unreceived queue.
func (r *Router) Restore(id string) (Subscription, error) {
	if id == "" {
		return Subscription{}, fmt.Errorf("%w: subscription id is required", ErrInvalidSubscription)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, active := r.subscribers[id]; active {
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionExists, id)
	}
	entry, ok := r.restored[id]
	if !ok {
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionGone, id)
	}
	if entry.canceled {
		return Subscription{}, fmt.Errorf("%w: %s; cancelled subscriptions cannot be restored", ErrSubscriptionGone, id)
	}
	if entry.deliveries != nil {
		return Subscription{}, fmt.Errorf("%w: %s", ErrSubscriptionExists, id)
	}
	ch := make(chan Delivery)
	entry.deliveries = ch
	entry.notify = make(chan struct{}, 1)
	entry.done = make(chan struct{})
	delete(r.restored, id)
	r.subscribers[id] = entry
	go entry.pump(r)
	return Subscription{ID: id, Deliveries: ch}, nil
}

// RestoreAll restores every non-cancelled recovered subscription. The map
// is keyed by subscription ID.
func (r *Router) RestoreAll() (map[string]Subscription, error) {
	r.mu.Lock()
	ids := make([]string, 0, len(r.restored))
	for id, entry := range r.restored {
		if !entry.canceled && entry.deliveries == nil {
			ids = append(ids, id)
		}
	}
	r.mu.Unlock()
	sort.Strings(ids)

	result := make(map[string]Subscription, len(ids))
	for _, id := range ids {
		sub, err := r.Restore(id)
		if err != nil {
			return nil, err
		}
		result[id] = sub
	}
	return result, nil
}

// Close waits for accepted state changes to reach the state file without
// cancelling subscriptions. It is the orderly process-restart boundary.
func (r *Router) Close() error {
	r.mu.RLock()
	entries := make([]*subscription, 0, len(r.subscribers))
	for _, entry := range r.subscribers {
		entries = append(entries, entry)
	}
	r.mu.RUnlock()
	for _, entry := range entries {
		entry.inFlight.Wait()
		entry.mu.Lock()
		entry.stopping = true
		entry.signal()
		entry.mu.Unlock()
		entry.flushed.Wait()
	}
	return nil
}
