// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import (
	"context"
	"sync"
	"time"
)

const (
	defaultMaxNotifs = 1000
	defaultMaxAge    = 24 * time.Hour
)

// notifStore is a concurrency-safe, append-only notification log.
// Subscribers poll it using monotonic IDs.
type notifStore struct {
	mu       sync.Mutex
	notifs   []Notification
	nextID   uint64
	maxCount int
	maxAge   time.Duration

	// broadcast is closed and replaced each time a notification is appended,
	// waking any long-pollers blocked in waitAfter.
	broadcast chan struct{}
}

func newNotifStore() *notifStore {
	return &notifStore{
		maxCount:  defaultMaxNotifs,
		maxAge:    defaultMaxAge,
		broadcast: make(chan struct{}),
		nextID:    1,
	}
}

// Append adds a notification to the store and wakes any long-pollers.
func (s *notifStore) Append(title, body, category string, meta map[string]string) Notification {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := Notification{
		ID:        s.nextID,
		Timestamp: time.Now(),
		Title:     title,
		Body:      body,
		Category:  category,
		Meta:      meta,
	}
	s.nextID++
	s.notifs = append(s.notifs, n)
	s.cleanupLocked()

	// Wake all waiters.
	close(s.broadcast)
	s.broadcast = make(chan struct{})
	return n
}

// After returns up to limit notifications with IDs strictly greater than afterID.
// A limit <= 0 means no limit.
func (s *notifStore) After(afterID uint64, limit int) []Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.afterLocked(afterID, limit)
}

func (s *notifStore) afterLocked(afterID uint64, limit int) []Notification {
	// Binary search for the first notification with ID > afterID.
	lo, hi := 0, len(s.notifs)
	for lo < hi {
		mid := (lo + hi) / 2
		if s.notifs[mid].ID <= afterID {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	result := s.notifs[lo:]
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	// Return a copy so callers can't mutate internal state.
	out := make([]Notification, len(result))
	copy(out, result)
	return out
}

// WaitAfter blocks until there are notifications after afterID or ctx is done.
func (s *notifStore) WaitAfter(ctx context.Context, afterID uint64, limit int) []Notification {
	for {
		s.mu.Lock()
		result := s.afterLocked(afterID, limit)
		ch := s.broadcast
		s.mu.Unlock()

		if len(result) > 0 {
			return result
		}
		select {
		case <-ch:
			// New notification arrived, check again.
		case <-ctx.Done():
			return nil
		}
	}
}

// cleanupLocked evicts old or excess notifications. Must be called with mu held.
func (s *notifStore) cleanupLocked() {
	cutoff := time.Now().Add(-s.maxAge)
	// Remove old entries from the front.
	i := 0
	for i < len(s.notifs) && s.notifs[i].Timestamp.Before(cutoff) {
		i++
	}
	if i > 0 {
		s.notifs = s.notifs[i:]
	}
	// Trim to maxCount.
	if len(s.notifs) > s.maxCount {
		s.notifs = s.notifs[len(s.notifs)-s.maxCount:]
	}
}
