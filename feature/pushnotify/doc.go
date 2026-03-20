// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package pushnotify implements peer-to-peer push notifications over tailnet.
//
// Devices can subscribe to notifications from other devices on the same
// tailnet. The source device stores notifications in an append-only log;
// subscriber devices long-poll the source's PeerAPI endpoint to retrieve them.
//
// The feature is structured as an ipnext.Extension and exposes:
//   - A PeerAPI endpoint (GET /v0/notify) for peers to poll notifications.
//   - LocalAPI endpoints for sending, subscribing, and reading notifications.
package pushnotify
