package dynamolock

import (
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestWriterBoundariesAWS(t *testing.T) {
	client, name := liveTable(t, func(in *dynamodb.CreateTableInput) {
		in.AttributeDefinitions = []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		}
		in.KeySchema = []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		}
	})
	table := Table{Name: name, PartitionKey: "pk", SortKey: "sk"}
	key := func(sort string) Key { return Key{Partition: "account", Sort: sort} }
	input := func(k Key) *LockInput {
		return &LockInput{Table: table, Key: k, LeaseDuration: 2 * time.Minute}
	}
	read := func(t *testing.T, k Key) (*CAS[Data], *Data) {
		t.Helper()
		c, data, err := ReadCAS[Data](t.Context(), client, table, k, ReadOptions{ConsistentRead: true})
		if err != nil {
			t.Fatal(err)
		}
		return c, data
	}
	stored := func(t *testing.T, k Key) map[string]types.AttributeValue {
		t.Helper()
		item, err := getItem(t.Context(), client, table, k)
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	seed := func(t *testing.T, k Key, version types.AttributeValue) map[string]types.AttributeValue {
		t.Helper()
		item, err := MarshalItem(table, k, Data{Value: "retained"})
		if err != nil {
			t.Fatal(err)
		}
		item["generation"], item["version"] = &types.AttributeValueMemberS{Value: "generation"}, version
		if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
			t.Fatal(err)
		}
		return item
	}

	t.Run("absence payloadless records and siblings", func(t *testing.T) {
		sibling := key("sibling")
		neighbor, _ := read(t, sibling)
		if err := neighbor.Update(t.Context(), &Data{Value: "untouched"}); err != nil {
			t.Fatal(err)
		}
		siblingBefore := stored(t, sibling)
		absentKey := key("absent")
		absent, data := read(t, absentKey)
		if data != nil || absent.Key() != absentKey {
			t.Fatal("absent compound key read returned its sibling")
		}
		if err := absent.Delete(t.Context()); err != nil || len(stored(t, absentKey)) != 0 {
			t.Fatalf("absent Delete created an item or failed: %v", err)
		}
		if err := absent.Update(t.Context(), &Data{}); !errors.Is(err, ErrReleased) {
			t.Fatalf("successful absent Delete did not consume handle: %v", err)
		}
		stale, _ := read(t, absentKey)
		creator, _ := read(t, absentKey)
		if err := creator.Update(t.Context(), &Data{Value: "created"}); err != nil {
			t.Fatal(err)
		}
		before := stored(t, absentKey)
		if err := stale.Delete(t.Context()); !errors.Is(err, ErrConflict) || !reflect.DeepEqual(before, stored(t, absentKey)) {
			t.Fatalf("absent snapshot deleted a concurrent creation: %v", err)
		}

		for _, operation := range []string{"update", "delete"} {
			t.Run("payloadless "+operation, func(t *testing.T) {
				k := key("payloadless " + operation)
				l, data, err := Lock[Data](t.Context(), client, input(k))
				cleanupLease(t, l)
				if err != nil || data != nil {
					t.Fatalf("create payloadless lease: %+v %v", data, err)
				}
				if err := l.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
				current, data := read(t, k)
				stale, _ := read(t, k)
				if data != nil {
					t.Fatal("payloadless item fabricated a value")
				}
				before := stored(t, k)
				if len(before) == 0 || before["data"] != nil {
					t.Fatal("fixture is not physically present without data")
				}
				if operation == "update" {
					if err := current.Update(t.Context(), &Data{Value: "published"}); err != nil {
						t.Fatalf("payloadless item was treated as absent: %v", err)
					}
					after := stored(t, k)
					if !reflect.DeepEqual(after["generation"], before["generation"]) || !reflect.DeepEqual(after["version"], versionNumber(2)) {
						t.Fatalf("payloadless Update lost revision: %+v", after)
					}
					if err := stale.Delete(t.Context()); !errors.Is(err, ErrConflict) {
						t.Fatalf("stale payloadless snapshot deleted new data: %v", err)
					}
				} else {
					if err := current.Delete(t.Context()); err != nil || len(stored(t, k)) != 0 {
						t.Fatalf("payloadless Delete failed: %v", err)
					}
					if err := stale.Update(t.Context(), &Data{}); !errors.Is(err, ErrConflict) {
						t.Fatalf("deleted payloadless snapshot recreated the item: %v", err)
					}
				}
			})
		}
		if !reflect.DeepEqual(stored(t, sibling), siblingBefore) {
			t.Fatal("compound-key sibling changed")
		}
	})

	t.Run("malformed revisions rejected without mutation", func(t *testing.T) {
		generation := &types.AttributeValueMemberS{Value: "generation"}
		for _, tc := range []struct {
			name                string
			generation, version types.AttributeValue
		}{
			{"missing generation", nil, versionNumber(1)},
			{"missing version", generation, nil},
			{"empty generation", &types.AttributeValueMemberS{}, versionNumber(1)},
			{"numeric generation", versionNumber(1), versionNumber(1)},
			{"boolean generation", &types.AttributeValueMemberBOOL{Value: true}, versionNumber(1)},
			{"null generation", &types.AttributeValueMemberNULL{Value: true}, versionNumber(1)},
			{"string version", generation, &types.AttributeValueMemberS{Value: "1"}},
			{"boolean version", generation, &types.AttributeValueMemberBOOL{Value: true}},
			{"null version", generation, &types.AttributeValueMemberNULL{Value: true}},
			{"zero version", generation, versionNumber(0)},
			{"negative version", generation, &types.AttributeValueMemberN{Value: "-1"}},
			{"oversized version", generation, &types.AttributeValueMemberN{Value: "18446744073709551616"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				k := key(tc.name)
				item, err := MarshalItem(table, k, Data{Value: "retained"})
				if err != nil {
					t.Fatal(err)
				}
				item["future"] = &types.AttributeValueMemberS{Value: "untouched"}
				if tc.generation != nil {
					item["generation"] = tc.generation
				}
				if tc.version != nil {
					item["version"] = tc.version
				}
				if _, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(name), Item: item}); err != nil {
					t.Fatal(err)
				}
				before := stored(t, k)
				l, data, err := Lock[Data](t.Context(), client, input(k))
				cleanupLease(t, l)
				if l != nil || data != nil || !errors.Is(err, ErrInvalidRecord) {
					t.Fatalf("malformed revision authorized a lease or returned the wrong error: %v %+v %v", l, data, err)
				}
				if !reflect.DeepEqual(stored(t, k), before) {
					t.Fatal("rejected acquisition changed the stored item")
				}
			})
		}
	})

	t.Run("fractional revision rejected with ownership cleanup", func(t *testing.T) {
		k := key("fractional")
		before := seed(t, k, &types.AttributeValueMemberN{Value: "1.5"})
		l, data, err := Lock[Data](t.Context(), client, input(k))
		cleanupLease(t, l)
		if l != nil || data != nil || !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("fractional revision authorized a lease: %v %+v %v", l, data, err)
		}
		after := stored(t, k)
		// DynamoDB can check number type/range but not integer-ness. Preserve
		// one-request acquisition; reject after decode without publishing data.
		if after["owner_token"] != nil || after["expires_at"] != nil || !reflect.DeepEqual(after["data"], before["data"]) || !reflect.DeepEqual(after["generation"], before["generation"]) {
			t.Fatalf("malformed acquisition retained authority or damaged payload: %+v", after)
		}
		if _, err := readRevision(after); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("fractional metadata was silently accepted: %v", err)
		}
		c, value, err := ReadCAS[Data](t.Context(), client, table, k, ReadOptions{ConsistentRead: true})
		if c != nil || value != nil || !errors.Is(err, ErrInvalidRecord) || !reflect.DeepEqual(after, stored(t, k)) {
			t.Fatalf("CAS accepted or mutated malformed metadata: %v", err)
		}
	})

	for _, transition := range []string{"acquire", "update", "commit", "CAS"} {
		for _, finish := range []string{"release", "delete"} {
			if transition == "commit" || transition == "CAS" {
				if finish == "release" {
					continue
				}
			}
			t.Run("exhaustion "+transition+" then "+finish, func(t *testing.T) {
				k := key("exhaustion " + transition + " " + finish)
				start := uint64(math.MaxUint64 - 1)
				if transition == "update" || transition == "commit" {
					start--
				}
				before := seed(t, k, versionNumber(start))
				var l *Lease[Data]
				var c *CAS[Data]
				if transition == "CAS" {
					c, _ = read(t, k)
					if err := c.Update(t.Context(), &Data{Value: "last"}); err != nil {
						t.Fatal(err)
					}
				} else {
					var err error
					l, _, err = Lock[Data](t.Context(), client, input(k))
					cleanupLease(t, l)
					if err != nil {
						t.Fatal(err)
					}
					if transition == "update" {
						err = l.Update(t.Context(), &Data{Value: "last"})
					} else if transition == "commit" {
						err = l.Commit(t.Context(), &Data{Value: "last"})
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				atLimit := stored(t, k)
				if !reflect.DeepEqual(atLimit["version"], versionNumber(math.MaxUint64)) || !reflect.DeepEqual(atLimit["generation"], before["generation"]) {
					t.Fatalf("wrong persisted exhaustion transition: %+v", atLimit)
				}
				if transition == "acquire" || transition == "update" {
					if !errors.Is(l.Update(t.Context(), &Data{Value: "overflow"}), ErrVersionExhausted) || !errors.Is(l.Commit(t.Context(), &Data{Value: "overflow"}), ErrVersionExhausted) {
						t.Fatal("exhausted lease allowed a payload mutation")
					}
					if !reflect.DeepEqual(atLimit, stored(t, k)) || l.Context().Err() != nil {
						t.Fatal("overflow rejection changed record or lost the live lease")
					}
					if finish == "release" {
						if err := l.Release(t.Context()); err != nil {
							t.Fatal(err)
						}
						want := maps.Clone(atLimit)
						delete(want, "owner_token")
						delete(want, "expires_at")
						if !reflect.DeepEqual(want, stored(t, k)) {
							t.Fatal("exhausted Release changed revision or data")
						}
					} else if err := l.Delete(t.Context()); err != nil || len(stored(t, k)) != 0 {
						t.Fatalf("exhausted lease Delete failed: %v", err)
					}
				} else {
					if transition == "commit" {
						if !errors.Is(l.Update(t.Context(), &Data{}), ErrReleased) {
							t.Fatal("successful final Commit left a usable lease")
						}
						c, _ = read(t, k)
					}
					if !errors.Is(c.Update(t.Context(), &Data{Value: "overflow"}), ErrVersionExhausted) || !reflect.DeepEqual(atLimit, stored(t, k)) {
						t.Fatal("exhausted CAS changed the record")
					}
					if err := c.Delete(t.Context()); err != nil || len(stored(t, k)) != 0 {
						t.Fatalf("exhausted CAS Delete failed: %v", err)
					}
				}
			})
		}
	}

	for _, operation := range []string{"update", "delete"} {
		t.Run("cancellation after applied "+operation, func(t *testing.T) {
			k := key("cancel " + operation)
			seed(t, k, versionNumber(7))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var requests atomic.Int32
			base := client.Options().HTTPClient
			faulty := dynamodb.New(client.Options(), func(o *dynamodb.Options) {
				o.HTTPClient = httpFunc(func(req *http.Request) (*http.Response, error) {
					mutation := strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".UpdateItem") || strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".DeleteItem")
					if mutation {
						requests.Add(1)
					}
					resp, err := base.Do(req)
					if err != nil || !mutation || resp.StatusCode != http.StatusOK {
						return resp, err
					}
					_, readErr := io.Copy(io.Discard, resp.Body)
					if err := errors.Join(readErr, resp.Body.Close()); err != nil {
						return nil, err
					}
					// DynamoDB has applied the mutation, but the caller receives
					// only cancellation, not the successful response.
					cancel()
					return nil, ctx.Err()
				})
			})
			c, data, err := ReadCAS[Data](t.Context(), faulty, table, k, ReadOptions{ConsistentRead: true})
			if err != nil {
				t.Fatal(err)
			}
			if operation == "update" {
				data.Value = "applied"
				err = c.Update(ctx, data)
			} else {
				err = c.Delete(ctx)
			}
			if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, context.Canceled) {
				t.Fatalf("applied cancellation: %v", err)
			}
			if !errors.Is(c.Update(t.Context(), data), ErrOutcomeUnknown) || !errors.Is(c.Delete(t.Context()), ErrOutcomeUnknown) || requests.Load() != 1 {
				t.Fatal("ambiguous cancellation allowed another mutation")
			}
			after := stored(t, k)
			if operation == "delete" {
				if len(after) != 0 {
					t.Fatal("canceled delete was not actually applied")
				}
			} else {
				value, err := UnmarshalItem[Data](table, after)
				if err != nil || value == nil || value.Value != "applied" || !reflect.DeepEqual(after["version"], versionNumber(8)) || c.revision.version != 7 {
					t.Fatalf("confirmed local state exceeded knowledge or remote write did not apply: %+v %v", value, err)
				}
			}
		})
	}
}
