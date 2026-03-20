// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import (
	"encoding/json"
	"sync"

	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnext"
	"tailscale.com/tailcfg"
	"tailscale.com/types/logger"
)

func init() {
	ipnext.RegisterExtension("pushnotify", newExtension)
}

// stateKeySubscriptions is the StateStore key for persisted subscriptions.
const stateKeySubscriptions = "pushnotify-subscriptions"

func newExtension(logf logger.Logf, b ipnext.SafeBackend) (ipnext.Extension, error) {
	return &Extension{
		logf:       logger.WithPrefix(logf, "pushnotify: "),
		sb:         b,
		stateStore: b.Sys().StateStore.Get(),
	}, nil
}

// Extension implements peer-to-peer push notifications over tailnet.
type Extension struct {
	logf       logger.Logf
	sb         ipnext.SafeBackend
	stateStore ipn.StateStore
	host       ipnext.Host

	mu     sync.Mutex
	store  *notifStore      // notification store for outgoing notifications
	subMgr *subscriptionMgr // manages subscriptions to other peers
}

func (e *Extension) Name() string { return "pushnotify" }

func (e *Extension) Init(h ipnext.Host) error {
	e.host = h
	h.Hooks().ProfileStateChange.Add(e.onProfileChange)
	h.Hooks().BackendStateChange.Add(e.onBackendStateChange)

	profile, prefs := h.Profiles().CurrentProfileState()
	e.onProfileChange(profile, prefs, false)
	return nil
}

func (e *Extension) Shutdown() error {
	e.mu.Lock()
	if e.subMgr != nil {
		e.subMgr.shutdown()
	}
	e.mu.Unlock()
	return nil
}

func (e *Extension) onBackendStateChange(st ipn.State) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if st == ipn.Running {
		if e.subMgr != nil {
			e.subMgr.startAll()
		}
	} else {
		if e.subMgr != nil {
			e.subMgr.stopAll()
		}
	}
}

func (e *Extension) onProfileChange(profile ipn.LoginProfileView, _ ipn.PrefsView, sameNode bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if sameNode && e.store != nil {
		return
	}

	// Shut down any existing subscription manager.
	if e.subMgr != nil {
		e.subMgr.shutdown()
	}

	uid := profile.UserProfile().ID()
	if uid == 0 {
		e.store = nil
		e.subMgr = nil
		return
	}

	e.store = newNotifStore()
	e.subMgr = newSubscriptionMgr(e.logf, e.host, e.sb)

	// Restore persisted subscriptions.
	e.loadSubscriptionsLocked()
}

// notifStore returns the active notification store, or nil.
func (e *Extension) getStore() *notifStore {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store
}

// getSubMgr returns the active subscription manager, or nil.
func (e *Extension) getSubMgr() *subscriptionMgr {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.subMgr
}

// persistedSubscription is the on-disk format for a subscription.
type persistedSubscription struct {
	PeerID     tailcfg.StableNodeID `json:"peerID"`
	LastSeenID uint64               `json:"lastSeenID"`
}

func (e *Extension) loadSubscriptionsLocked() {
	data, err := e.stateStore.ReadState(ipn.StateKey(stateKeySubscriptions))
	if err != nil {
		return // no saved state
	}
	var subs []persistedSubscription
	if err := json.Unmarshal(data, &subs); err != nil {
		e.logf("failed to unmarshal subscriptions: %v", err)
		return
	}
	for _, s := range subs {
		e.subMgr.subscribe(s.PeerID, s.LastSeenID)
	}
}

func (e *Extension) saveSubscriptions() {
	mgr := e.getSubMgr()
	if mgr == nil {
		return
	}
	subs := mgr.list()
	data, err := json.Marshal(subs)
	if err != nil {
		e.logf("failed to marshal subscriptions: %v", err)
		return
	}
	if err := e.stateStore.WriteState(ipn.StateKey(stateKeySubscriptions), data); err != nil {
		e.logf("failed to persist subscriptions: %v", err)
	}
}
