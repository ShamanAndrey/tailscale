// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnext"
	"tailscale.com/tailcfg"
	"tailscale.com/types/logger"
)

const (
	pollWaitSec    = 60
	minBackoff     = 1 * time.Second
	maxBackoff     = 30 * time.Second
	maxPollResults = 100
)

// subscription tracks an active subscription to a peer's notifications.
type subscription struct {
	peerID     tailcfg.StableNodeID
	lastSeenID uint64
	cancel     context.CancelFunc // cancels the polling goroutine
}

// subscriptionMgr manages polling goroutines for notification subscriptions.
type subscriptionMgr struct {
	logf logger.Logf
	host ipnext.Host
	sb   ipnext.SafeBackend

	mu   sync.Mutex
	subs map[tailcfg.StableNodeID]*subscription

	// onNotification is called when new notifications arrive from a peer.
	// This is set to sendNotifications by default but can be overridden for tests.
	onNotification func(tailcfg.StableNodeID, []Notification)
}

func newSubscriptionMgr(logf logger.Logf, host ipnext.Host, sb ipnext.SafeBackend) *subscriptionMgr {
	m := &subscriptionMgr{
		logf: logf,
		host: host,
		sb:   sb,
		subs: make(map[tailcfg.StableNodeID]*subscription),
	}
	m.onNotification = m.sendNotifications
	return m
}

// subscribe starts polling a peer for notifications.
// If already subscribed, this is a no-op.
func (m *subscriptionMgr) subscribe(peerID tailcfg.StableNodeID, lastSeenID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.subs[peerID]; ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	sub := &subscription{
		peerID:     peerID,
		lastSeenID: lastSeenID,
		cancel:     cancel,
	}
	m.subs[peerID] = sub
	go m.pollLoop(ctx, sub)
}

// unsubscribe stops polling a peer.
func (m *subscriptionMgr) unsubscribe(peerID tailcfg.StableNodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribeLocked(peerID)
}

func (m *subscriptionMgr) unsubscribeLocked(peerID tailcfg.StableNodeID) {
	if sub, ok := m.subs[peerID]; ok {
		sub.cancel()
		delete(m.subs, peerID)
	}
}

// list returns the current subscriptions with their last-seen IDs.
func (m *subscriptionMgr) list() []persistedSubscription {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]persistedSubscription, 0, len(m.subs))
	for _, sub := range m.subs {
		out = append(out, persistedSubscription{
			PeerID:     sub.peerID,
			LastSeenID: sub.lastSeenID,
		})
	}
	return out
}

// startAll restarts polling for all subscriptions.
func (m *subscriptionMgr) startAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sub := range m.subs {
		sub.cancel()
		ctx, cancel := context.WithCancel(context.Background())
		sub.cancel = cancel
		go m.pollLoop(ctx, sub)
	}
}

// stopAll cancels all active polling goroutines without removing subscriptions.
func (m *subscriptionMgr) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sub := range m.subs {
		sub.cancel()
	}
}

// shutdown cancels all pollers and clears subscriptions.
func (m *subscriptionMgr) shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.subs {
		m.subs[id].cancel()
	}
	m.subs = make(map[tailcfg.StableNodeID]*subscription)
}

// pollLoop long-polls a peer's PeerAPI /v0/notify endpoint.
func (m *subscriptionMgr) pollLoop(ctx context.Context, sub *subscription) {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		nb := m.host.NodeBackend()
		peerAPIBase := ""
		peers := nb.AppendMatchingPeers(nil, func(p tailcfg.NodeView) bool {
			return p.StableID() == sub.peerID
		})
		if len(peers) > 0 {
			peerAPIBase = nb.PeerAPIBase(peers[0])
		}
		if peerAPIBase == "" {
			m.logf("no PeerAPI for %v, backing off", sub.peerID)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-ctx.Done():
				return
			}
			continue
		}

		url := fmt.Sprintf("%s/v0/notify?after=%d&waitsec=%d", peerAPIBase, sub.lastSeenID, pollWaitSec)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			m.logf("failed to create request for %v: %v", sub.peerID, err)
			return
		}

		client := &http.Client{
			Transport: m.sb.Sys().Dialer.Get().PeerAPITransport(),
		}
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logf("poll %v failed: %v", sub.peerID, err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-ctx.Done():
				return
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			m.logf("poll %v returned %d", sub.peerID, resp.StatusCode)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-ctx.Done():
				return
			}
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB max
		resp.Body.Close()
		if err != nil {
			m.logf("failed to read response from %v: %v", sub.peerID, err)
			continue
		}

		var notifs []Notification
		if err := json.Unmarshal(body, &notifs); err != nil {
			m.logf("failed to decode notifications from %v: %v", sub.peerID, err)
			continue
		}

		if len(notifs) > 0 {
			// Update the cursor.
			m.mu.Lock()
			sub.lastSeenID = notifs[len(notifs)-1].ID
			m.mu.Unlock()

			m.onNotification(sub.peerID, notifs)
			backoff = minBackoff // reset on success
		}
		// On empty response (timeout), just loop again immediately.
	}
}

// sendNotifications pushes received notifications to the IPN bus.
func (m *subscriptionMgr) sendNotifications(from tailcfg.StableNodeID, notifs []Notification) {
	pns := make([]ipn.PushNotification, len(notifs))
	for i, n := range notifs {
		pns[i] = ipn.PushNotification{
			FromPeerID: from,
			ID:         n.ID,
			Timestamp:  n.Timestamp,
			Title:      n.Title,
			Body:       n.Body,
			Category:   n.Category,
			Meta:       n.Meta,
		}
	}
	m.host.SendNotifyAsync(ipn.Notify{
		PushNotifications: pns,
	})
}
