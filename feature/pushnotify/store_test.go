// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package pushnotify

import (
	"context"
	"testing"
	"time"
)

func TestStoreAppendAndAfter(t *testing.T) {
	s := newNotifStore()

	n1 := s.Append("title1", "body1", "", nil)
	n2 := s.Append("title2", "body2", "alert", map[string]string{"k": "v"})
	n3 := s.Append("title3", "body3", "", nil)

	if n1.ID != 1 || n2.ID != 2 || n3.ID != 3 {
		t.Fatalf("unexpected IDs: %d, %d, %d", n1.ID, n2.ID, n3.ID)
	}

	// After(0) returns all.
	all := s.After(0, 0)
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}

	// After(1) returns 2 and 3.
	rest := s.After(1, 0)
	if len(rest) != 2 || rest[0].ID != 2 || rest[1].ID != 3 {
		t.Fatalf("unexpected After(1): %v", rest)
	}

	// After with limit.
	limited := s.After(0, 2)
	if len(limited) != 2 {
		t.Fatalf("expected 2, got %d", len(limited))
	}

	// After the last ID returns empty.
	empty := s.After(3, 0)
	if len(empty) != 0 {
		t.Fatalf("expected 0, got %d", len(empty))
	}
}

func TestStoreWaitAfter(t *testing.T) {
	s := newNotifStore()

	// Start a waiter in a goroutine.
	got := make(chan []Notification, 1)
	go func() {
		got <- s.WaitAfter(context.Background(), 0, 0)
	}()

	// Give the goroutine time to block.
	time.Sleep(50 * time.Millisecond)

	s.Append("hello", "world", "", nil)

	select {
	case notifs := <-got:
		if len(notifs) != 1 || notifs[0].Title != "hello" {
			t.Fatalf("unexpected: %v", notifs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notification")
	}
}

func TestStoreWaitAfterContextCancelled(t *testing.T) {
	s := newNotifStore()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := s.WaitAfter(ctx, 0, 0)
	if len(result) != 0 {
		t.Fatalf("expected empty result on cancelled context, got %d", len(result))
	}
}

func TestStoreCleanupMaxCount(t *testing.T) {
	s := newNotifStore()
	s.maxCount = 3

	for i := 0; i < 5; i++ {
		s.Append("title", "", "", nil)
	}

	all := s.After(0, 0)
	if len(all) != 3 {
		t.Fatalf("expected 3 after cleanup, got %d", len(all))
	}
	// Should keep the latest 3: IDs 3, 4, 5.
	if all[0].ID != 3 {
		t.Fatalf("expected first ID to be 3, got %d", all[0].ID)
	}
}
