package dynamolock

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func TestCreateWithoutRead(t *testing.T) {
	var requests []wireRequest
	client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
		requests = append(requests, w)
		return 200, `{}`, nil
	})
	in := protocolInput()
	if err := Create(t.Context(), client, in.Table, in.Key, &Data{Value: "created"}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("creation made %d requests, want one conditional write", len(requests))
	}
	w := requests[0]
	if w.Target != "DynamoDB_20120810.UpdateItem" || w.TableName != in.Table.Name || string(w.Key["id"]) != `{"S":"key"}` {
		t.Fatalf("unexpected creation request: %+v", w)
	}
	var generation struct{ S string }
	if err := json.Unmarshal(w.ExpressionAttributeValues[":generation"], &generation); err != nil {
		t.Fatal(err)
	}
	if generation.S == "" || wireInt(w, ":newVersion") != 1 || w.ExpressionAttributeValues[":owner"] != nil {
		t.Fatalf("creation did not initialize an unlocked revision: %+v", w.ExpressionAttributeValues)
	}
}

func TestCreateUnconfirmedOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		transport error
		want      error
	}{
		{name: "existing item", status: 400, body: conditionalJSON, want: ErrConflict},
		{name: "server error", status: 500, body: serverErrorJSON, want: ErrOutcomeUnknown},
		{name: "lost response", transport: io.ErrUnexpectedEOF, want: ErrOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := protocolClient(func(_ context.Context, w wireRequest) (int, any, error) {
				calls++
				if w.Target != "DynamoDB_20120810.UpdateItem" {
					t.Fatalf("creation unexpectedly read or reconciled: %s", w.Target)
				}
				return tc.status, tc.body, tc.transport
			})
			in := protocolInput()
			err := Create(t.Context(), client, in.Table, in.Key, &Data{Value: "created"})
			if !errors.Is(err, tc.want) || calls != 1 {
				t.Fatalf("creation returned %v after %d requests, want %v after one", err, calls, tc.want)
			}
		})
	}
}

func TestCreateValidationDoesNotCallAWS(t *testing.T) {
	client := protocolClient(func(context.Context, wireRequest) (int, any, error) {
		t.Error("invalid creation reached AWS")
		return 0, nil, errors.New("unexpected request")
	})
	for _, change := range []func(*LockInput){
		func(in *LockInput) { in.Table.Name = "" },
		func(in *LockInput) { in.Table.PartitionKey = "generation" },
		func(in *LockInput) { in.Table.Attributes = []string{"pending", "pending"} },
		func(in *LockInput) { in.Key.Partition = "" },
		func(in *LockInput) { in.Table.SortKey = "sk" },
	} {
		in := protocolInput()
		change(in)
		if err := Create(t.Context(), client, in.Table, in.Key, &Data{}); err == nil {
			t.Fatalf("invalid creation target accepted: %+v", in)
		}
	}
	in := protocolInput()
	if err := Create(t.Context(), nil, in.Table, in.Key, &Data{}); err == nil {
		t.Fatal("nil client accepted")
	}
	if err := Create[Data](t.Context(), client, in.Table, in.Key, nil); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("nil payload: %v", err)
	}
	var nilMap map[string]any
	if err := Create(t.Context(), client, in.Table, in.Key, &nilMap); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("nil map: %v", err)
	}
	value := 1
	if err := Create(t.Context(), client, in.Table, in.Key, &value); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("unsupported payload type: %v", err)
	}
	pointer := &Data{}
	if err := Create(t.Context(), client, in.Table, in.Key, &pointer); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("pointer type parameter: %v", err)
	}
	wrongKey := map[string]string{"id": "another key"}
	if err := Create(t.Context(), client, in.Table, in.Key, &wrongKey); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("conflicting payload key: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Create(ctx, client, in.Table, in.Key, &Data{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled creation: %v", err)
	}
}
