// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"tailscale.com/ipn/ipnlocal"
	"tailscale.com/tailcfg"
)

func init() {
	ipnlocal.RegisterPeerAPIHandler("/v0/notify", handlePeerNotify)
}

const maxWaitSec = 120

// canNotify reports whether the peer is allowed to poll notifications from this node.
func canNotify(h ipnlocal.PeerAPIHandler) bool {
	if h.Peer().UnsignedPeerAPIOnly() {
		return false
	}
	return h.IsSelfUntagged() || h.PeerCaps().HasCapability(tailcfg.PeerCapabilityNotifySend)
}

// handlePeerNotify handles GET /v0/notify?after=<id>&waitsec=<seconds>.
// It returns notifications with IDs greater than "after". If none are available
// and waitsec > 0, it long-polls until new notifications arrive or the timeout.
func handlePeerNotify(h ipnlocal.PeerAPIHandler, w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "expected GET", http.StatusMethodNotAllowed)
		return
	}

	ext, ok := ipnlocal.GetExt[*Extension](h.LocalBackend())
	if !ok {
		http.Error(w, "pushnotify extension not available", http.StatusInternalServerError)
		return
	}

	if !canNotify(h) {
		http.Error(w, "not allowed to poll notifications", http.StatusForbidden)
		return
	}

	store := ext.getStore()
	if store == nil {
		http.Error(w, "notifications not initialized", http.StatusServiceUnavailable)
		return
	}

	afterID, _ := strconv.ParseUint(r.FormValue("after"), 10, 64)

	waitSec, _ := strconv.Atoi(r.FormValue("waitsec"))
	if waitSec < 0 {
		waitSec = 0
	}
	if waitSec > maxWaitSec {
		waitSec = maxWaitSec
	}

	var notifs []Notification
	if waitSec > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(waitSec)*time.Second)
		defer cancel()
		notifs = store.WaitAfter(ctx, afterID, 0)
	} else {
		notifs = store.After(afterID, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(notifs)
}
