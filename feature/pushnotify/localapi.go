// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import (
	"encoding/json"
	"net/http"
	"strconv"

	"tailscale.com/ipn/ipnlocal"
	"tailscale.com/ipn/localapi"
	"tailscale.com/tailcfg"
)

func init() {
	localapi.Register("notify/send", serveNotifySend)
	localapi.Register("notify/subscribe", serveNotifySubscribe)
	localapi.Register("notify/subscriptions", serveNotifySubscriptions)
	localapi.Register("notify/inbox", serveNotifyInbox)
}

// serveNotifySend handles POST /localapi/v0/notify/send.
// It appends a notification to this node's outgoing notification store.
func serveNotifySend(h *localapi.Handler, w http.ResponseWriter, r *http.Request) {
	if !h.PermitWrite {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "expected POST", http.StatusMethodNotAllowed)
		return
	}

	ext, ok := ipnlocal.GetExt[*Extension](h.LocalBackend())
	if !ok {
		http.Error(w, "pushnotify extension not available", http.StatusInternalServerError)
		return
	}
	store := ext.getStore()
	if store == nil {
		http.Error(w, "notifications not initialized", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		Title    string            `json:"title"`
		Body     string            `json:"body"`
		Category string            `json:"category"`
		Meta     map[string]string `json:"meta"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Title == "" {
		http.Error(w, "title is required", http.StatusBadRequest)
		return
	}

	n := store.Append(req.Title, req.Body, req.Category, req.Meta)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(n)
}

// subscribeRequest is the JSON body for subscribe/unsubscribe.
type subscribeRequest struct {
	PeerID tailcfg.StableNodeID `json:"peerID"`
}

// serveNotifySubscribe handles POST/DELETE /localapi/v0/notify/subscribe.
// POST subscribes to a peer; DELETE unsubscribes.
func serveNotifySubscribe(h *localapi.Handler, w http.ResponseWriter, r *http.Request) {
	if !h.PermitWrite {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}

	ext, ok := ipnlocal.GetExt[*Extension](h.LocalBackend())
	if !ok {
		http.Error(w, "pushnotify extension not available", http.StatusInternalServerError)
		return
	}
	mgr := ext.getSubMgr()
	if mgr == nil {
		http.Error(w, "notifications not initialized", http.StatusServiceUnavailable)
		return
	}

	var req subscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.PeerID == "" {
		http.Error(w, "peerID is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "POST":
		mgr.subscribe(req.PeerID, 0)
		ext.saveSubscriptions()
		w.WriteHeader(http.StatusCreated)
	case "DELETE":
		mgr.unsubscribe(req.PeerID)
		ext.saveSubscriptions()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "expected POST or DELETE", http.StatusMethodNotAllowed)
	}
}

// serveNotifySubscriptions handles GET /localapi/v0/notify/subscriptions.
func serveNotifySubscriptions(h *localapi.Handler, w http.ResponseWriter, r *http.Request) {
	if !h.PermitRead {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "expected GET", http.StatusMethodNotAllowed)
		return
	}

	ext, ok := ipnlocal.GetExt[*Extension](h.LocalBackend())
	if !ok {
		http.Error(w, "pushnotify extension not available", http.StatusInternalServerError)
		return
	}
	mgr := ext.getSubMgr()
	if mgr == nil {
		http.Error(w, "notifications not initialized", http.StatusServiceUnavailable)
		return
	}

	subs := mgr.list()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subs)
}

// serveNotifyInbox handles GET /localapi/v0/notify/inbox?after=<id>&limit=<n>.
// It returns notifications stored in this node's local store (for source-side inspection).
func serveNotifyInbox(h *localapi.Handler, w http.ResponseWriter, r *http.Request) {
	if !h.PermitRead {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "expected GET", http.StatusMethodNotAllowed)
		return
	}

	ext, ok := ipnlocal.GetExt[*Extension](h.LocalBackend())
	if !ok {
		http.Error(w, "pushnotify extension not available", http.StatusInternalServerError)
		return
	}
	store := ext.getStore()
	if store == nil {
		http.Error(w, "notifications not initialized", http.StatusServiceUnavailable)
		return
	}

	afterID, _ := strconv.ParseUint(r.FormValue("after"), 10, 64)
	limit, _ := strconv.Atoi(r.FormValue("limit"))

	notifs := store.After(afterID, limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(notifs)
}
