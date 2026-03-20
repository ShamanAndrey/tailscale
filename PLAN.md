# Push Notification Feature Implementation Plan

## Overview

Add a peer-to-peer push notification system over tailnet. Devices can subscribe to notifications from other devices on the same tailnet. The source device stores notifications locally; subscriber devices poll (with long-polling) to retrieve them.

## Architecture

```
Server (source)                          Client (subscriber)
┌──────────────────────┐                 ┌──────────────────────┐
│ LocalAPI:            │                 │ LocalAPI:            │
│  POST /notify/send   │                 │  POST /notify/sub    │
│   → store notif      │                 │   → start poller     │
│   → wake pollers     │                 │                      │
│                      │                 │  GET /notify/inbox   │
│ PeerAPI:             │                 │   → list received    │
│  GET /v0/notify      │◄── long-poll ──│                      │
│   ?after=<id>        │                 │  DELETE /notify/ack  │
│   &waitsec=60        │────────────────►│   → ack/dismiss      │
│   → return new       │                 │                      │
│     notifications    │                 │ Poller goroutine:    │
│                      │                 │  → GET peer's        │
│ Storage:             │                 │    /v0/notify        │
│  per-subscriber      │                 │  → store locally     │
│  append-only log     │                 │  → SendNotifyAsync   │
│  with monotonic IDs  │                 │    to IPN bus        │
└──────────────────────┘                 └──────────────────────┘
```

## Step-by-step Plan

### Step 1: Create the feature package skeleton

Create `feature/pushnotify/` with:

- **`doc.go`** — Package doc explaining the feature
- **`ext.go`** — Extension struct, `init()` registration, `Init()`, `Shutdown()`, `Name()`
  - Register hooks: `ProfileStateChange`, `BackendStateChange`
  - Create the notification manager on profile change
  - Store subscriptions in `ipn.StateStore` for persistence across restarts

Pattern to follow: `feature/taildrop/ext.go`

### Step 2: Define the notification data model

In **`pushnotify.go`** (core types and manager):

```go
// Notification is a single push notification stored on the source device.
type Notification struct {
    ID        uint64    `json:"id"`        // Monotonic, per-source
    Timestamp time.Time `json:"timestamp"`
    Title     string    `json:"title"`
    Body      string    `json:"body"`
    Category  string    `json:"category,omitempty"` // e.g. "alert", "info"
    Meta      map[string]string `json:"meta,omitempty"`
}

// Subscription tracks a client's intent to poll a specific peer.
type Subscription struct {
    PeerID    tailcfg.StableNodeID `json:"peerID"`
    LastSeenID uint64              `json:"lastSeenID"`
}
```

### Step 3: Implement the notification store (source side)

In **`store.go`**:

- `notifStore` — in-memory append-only log with monotonic IDs
- Per-subscriber read cursors tracked by `tailcfg.StableNodeID`
- Methods:
  - `Append(title, body, category string, meta map[string]string) Notification`
  - `After(id uint64, limit int) []Notification` — return notifications after given ID
  - `WaitAfter(ctx context.Context, id uint64) []Notification` — long-poll variant
  - `Cleanup(maxAge time.Duration)` — evict old notifications (TTL-based)
- Wake mechanism: channel-based broadcast (like taildrop's `fileWaiters` pattern) to unblock long-pollers when new notifications arrive
- Configurable max retention: default 1000 notifications or 24 hours

### Step 4: Implement the PeerAPI endpoint (source side)

In **`peerapi.go`**:

Register: `ipnlocal.RegisterPeerAPIHandler("/v0/notify", handleNotify)`

**`GET /v0/notify?after=<id>&waitsec=<seconds>`**
- Validate peer capability (new `PeerCapabilityNotify`)
- Look up notifications after the given ID
- If none and `waitsec > 0`, long-poll up to that duration
- Return JSON array of `Notification` objects
- Cap `waitsec` at 120 seconds

### Step 5: Implement the subscription manager & poller (client side)

In **`poller.go`**:

- `subscriptionManager` — manages active subscriptions and polling goroutines
- For each subscription:
  - Run a goroutine that long-polls the peer's `/v0/notify?after=<lastID>&waitsec=60`
  - On receiving notifications, store them locally and send `ipn.Notify` to the IPN bus
  - Track `lastSeenID` per peer, persist to `StateStore`
  - Exponential backoff on errors (1s → 30s), reset on success
  - Respect context cancellation for clean shutdown
- Methods:
  - `Subscribe(peerID tailcfg.StableNodeID)` — start polling a peer
  - `Unsubscribe(peerID tailcfg.StableNodeID)` — stop polling
  - `Subscriptions() []Subscription` — list active subs

### Step 6: Implement the LocalAPI endpoints (client-facing)

In **`localapi.go`**:

Register these endpoints:

1. **`POST /localapi/v0/notify/send`** — Send a notification (source side)
   - Body: `{"title": "...", "body": "...", "category": "...", "meta": {...}}`
   - Requires `PermitWrite`
   - Appends to local `notifStore`

2. **`POST /localapi/v0/notify/subscribe`** — Subscribe to a peer's notifications
   - Body: `{"peerID": "..."}`
   - Requires `PermitWrite`
   - Starts a poller goroutine for that peer

3. **`DELETE /localapi/v0/notify/subscribe`** — Unsubscribe from a peer
   - Body: `{"peerID": "..."}`
   - Requires `PermitWrite`
   - Stops the poller

4. **`GET /localapi/v0/notify/subscriptions`** — List subscriptions
   - Requires `PermitRead`
   - Returns `[]Subscription`

5. **`GET /localapi/v0/notify/inbox`** — List received notifications
   - Query: `?from=<peerID>&after=<id>&limit=<n>`
   - Requires `PermitRead`
   - Returns locally-stored notifications received from peers

6. **`POST /localapi/v0/notify/ack`** — Acknowledge/dismiss notifications
   - Body: `{"ids": [1,2,3]}` or `{"from": "<peerID>", "before": <id>}`
   - Requires `PermitWrite`

### Step 7: Add capability constants

In **`tailcfg/tailcfg.go`**:

```go
PeerCapabilityNotifySend   PeerCapability = "https://tailscale.com/cap/notify-send"
PeerCapabilityNotifyTarget PeerCapability = "https://tailscale.com/cap/notify-target"
```

In the PeerAPI handler, check:
- The requesting peer has `PeerCapabilityNotifySend`
- This node has `PeerCapabilityNotifyTarget` (or fall back to `IsSelfUntagged()`)

### Step 8: Extend `ipn.Notify` for push notifications

In **`ipn/backend.go`**, add:

```go
type Notify struct {
    // ... existing fields ...

    // PushNotifications, if non-nil, contains new push notifications
    // received from subscribed peers.
    PushNotifications []PushNotification `json:",omitzero"`
}

type PushNotification struct {
    FromPeerID tailcfg.StableNodeID `json:"fromPeerID"`
    Notification                     // Embed the notification data
}
```

### Step 9: Wire into the extension lifecycle

In **`ext.go`**, the `Init()` method:

```go
func (e *Extension) Init(h ipnext.Host) error {
    e.host = h
    h.Hooks().ProfileStateChange.Add(e.onProfileChange)
    h.Hooks().BackendStateChange.Add(e.onBackendStateChange)

    // Restore subscriptions from StateStore on profile load
    profile, prefs := h.Profiles().CurrentProfileState()
    e.onProfileChange(profile, prefs, false)
    return nil
}
```

On profile change:
- Load persisted subscriptions from `StateStore`
- Start pollers for each subscription
- Initialize the notification store

On shutdown:
- Cancel all poller contexts
- Persist subscription state (last-seen IDs)

### Step 10: Write tests

- **`store_test.go`** — Unit tests for notification store (append, after, TTL cleanup, concurrent access)
- **`poller_test.go`** — Unit tests for subscription manager (mock PeerAPI responses)
- **`ext_test.go`** — Extension lifecycle tests using `NewExtensionHostForTest`
- **`integration_test.go`** — Full end-to-end test:
  1. Start two nodes (n1 = source, n2 = subscriber)
  2. n2 subscribes to n1
  3. n1 sends a notification via LocalAPI
  4. n2 receives it via polling
  5. Verify notification content matches
  6. n2 unsubscribes, verify polling stops

## File Summary

```
feature/pushnotify/
├── doc.go              — Package documentation
├── ext.go              — Extension registration, Init, Shutdown, hooks
├── pushnotify.go       — Core types (Notification, Subscription)
├── store.go            — Notification store (append-only log, long-poll wake)
├── peerapi.go          — GET /v0/notify (peer-to-peer endpoint)
├── poller.go           — Subscription manager, polling goroutines
├── localapi.go         — Client-facing LocalAPI endpoints
├── store_test.go       — Store unit tests
├── poller_test.go      — Poller unit tests
├── ext_test.go         — Extension lifecycle tests
└── integration_test.go — End-to-end integration test
```

Plus modifications to:
- `tailcfg/tailcfg.go` — Add `PeerCapabilityNotifySend`, `PeerCapabilityNotifyTarget`
- `ipn/backend.go` — Add `PushNotifications` field and `PushNotification` type to `Notify`

## Implementation Order

1. Step 2 (types) + Step 7 (capabilities) — No dependencies
2. Step 3 (store) — Depends on types
3. Step 1 (extension skeleton) — Depends on types
4. Step 4 (PeerAPI) — Depends on store + extension
5. Step 5 (poller) — Depends on store + extension
6. Step 8 (Notify extension) — Depends on types
7. Step 6 (LocalAPI) — Depends on everything above
8. Step 9 (lifecycle wiring) — Final integration
9. Step 10 (tests) — Throughout, but integration test last
