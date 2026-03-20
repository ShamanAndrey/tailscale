// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import "time"

// Notification is a single push notification stored on the source device.
type Notification struct {
	// ID is a monotonically increasing identifier assigned by the source.
	ID uint64 `json:"id"`

	// Timestamp is when the notification was created.
	Timestamp time.Time `json:"timestamp"`

	// Title is a short summary of the notification.
	Title string `json:"title"`

	// Body is the full notification text.
	Body string `json:"body,omitempty"`

	// Category is an optional classification (e.g. "alert", "info").
	Category string `json:"category,omitempty"`

	// Meta holds optional key-value metadata.
	Meta map[string]string `json:"meta,omitempty"`
}
