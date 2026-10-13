package dynamolock

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestProtocolImplicitHeartbeatSchedule(t *testing.T) {
	for _, duration := range []time.Duration{4 * time.Nanosecond, 7 * time.Nanosecond, 10 * time.Second, 30 * time.Second} {
		t.Run(duration.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var mu sync.Mutex
				var renewals []time.Time
				recorded := func() []time.Time {
					mu.Lock()
					defer mu.Unlock()
					return slices.Clone(renewals)
				}
				client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
					if w.ReturnValues == "ALL_NEW" {
						if wireInt(w, ":expires")-time.Now().UnixNano() != int64(duration) {
							t.Error("acquisition did not establish the requested lifetime")
						}
						time.Sleep(duration / 8)
						return acquireReply(w, "")
					}
					if w.ExpressionAttributeValues[":next"] != nil {
						mu.Lock()
						renewals = append(renewals, time.Now())
						mu.Unlock()
						if wireInt(w, ":next")-time.Now().UnixNano() != int64(duration) {
							t.Error("renewal did not establish the original requested lifetime")
						}
						time.Sleep(duration / 16)
					}
					return 200, `{}`, nil
				})
				in := protocolInput()
				in.LeaseDuration = duration
				started := time.Now()
				l, _, err := Lock[keyedData](t.Context(), client, in)
				if err != nil {
					t.Fatal(err)
				}
				cleanupLease(t, l)
				// Mutating the caller's configuration cannot retime an existing lease.
				in.LeaseDuration = time.Hour
				for i := 1; i <= 3; i++ {
					want := started.Add(time.Duration(i) * (duration / 4))
					time.Sleep(time.Until(want) - time.Nanosecond)
					synctest.Wait()
					if starts := recorded(); len(starts) != i-1 {
						t.Fatalf("renewal before quarter-duration boundary: %v", starts)
					}
					time.Sleep(time.Nanosecond)
					synctest.Wait()
					if starts := recorded(); len(starts) != i || !starts[i-1].Equal(want) {
						t.Fatalf("renewal %d: %v, want start %v", i, starts, want)
					}
					// Confirmation latency must not shift the next renewal's start.
					time.Sleep(duration / 16)
					synctest.Wait()
					if err := context.Cause(l.Context()); err != nil {
						t.Fatal(err)
					}
				}
				if err := l.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
				time.Sleep(2 * duration)
				synctest.Wait()
				if starts := recorded(); len(starts) != 3 {
					t.Fatalf("renewal continued after release: %v", starts)
				}
			})
		})
	}
}
