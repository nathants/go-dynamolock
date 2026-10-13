package dynamolock

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrConflict means a CAS snapshot no longer authorizes writes. ReadCAS must be
// called again before recomputing and retrying application work.
var ErrConflict = errors.New("CAS snapshot conflicts with current record")

// ErrVersionExhausted means the generation's counter cannot be incremented.
// Counters never wrap. Hard deletion is still allowed with the current authority.
var ErrVersionExhausted = errors.New("record version exhausted")

// A generation survives all leases and updates, but not physical deletion.
// Missing metadata is accepted only for lazy initialization of existing records.
type revision struct {
	generation string
	version    uint64
}

func readRevision(item map[string]types.AttributeValue) (revision, error) {
	generation, hasGeneration := item["generation"]
	version, hasVersion := item["version"]
	if !hasGeneration && !hasVersion {
		return revision{}, nil
	}
	g, gok := generation.(*types.AttributeValueMemberS)
	v, vok := version.(*types.AttributeValueMemberN)
	if !hasGeneration || !hasVersion || !gok || !vok || g == nil || v == nil || g.Value == "" {
		return revision{}, fmt.Errorf("%w: generation and version must be a nonempty string and positive integer", ErrInvalidRecord)
	}
	n, err := strconv.ParseUint(v.Value, 10, 64)
	if err != nil || n == 0 {
		return revision{}, fmt.Errorf("%w: version must be a positive uint64", ErrInvalidRecord)
	}
	return revision{generation: g.Value, version: n}, nil
}

func versionNumber(n uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(n, 10)}
}

const availableCondition = "((attribute_not_exists(#owner) AND attribute_not_exists(#expires)) OR #expires < :now)"
const revisionCondition = "#generation = :generation AND #version = :version"

// Guard the pair before initializing or incrementing it. Partial metadata must
// never be silently repaired, and exhausted counters must never wrap. DynamoDB
// cannot test integrality in a condition: malformed fractions can be incremented
// before decode rejects them and releases ownership. Avoid an extra acquisition
// read for values that participating writers cannot produce.
func acquisitionRevision(names map[string]string, values map[string]types.AttributeValue, generation string) string {
	names["#generation"], names["#version"] = "generation", "version"
	values[":generation"] = &types.AttributeValueMemberS{Value: generation}
	values[":zero"], values[":one"], values[":maxVersion"] = versionNumber(0), versionNumber(1), versionNumber(math.MaxUint64)
	values[":stringType"], values[":numberType"] = &types.AttributeValueMemberS{Value: "S"}, &types.AttributeValueMemberS{Value: "N"}
	return "((attribute_not_exists(#generation) AND attribute_not_exists(#version)) OR " +
		"(attribute_type(#generation, :stringType) AND size(#generation) > :zero AND " +
		"attribute_type(#version, :numberType) AND #version >= :one AND #version < :maxVersion))"
}

func (r revision) fence(names map[string]string, values map[string]types.AttributeValue) {
	names["#generation"], names["#version"] = "generation", "version"
	values[":generation"] = &types.AttributeValueMemberS{Value: r.generation}
	values[":version"] = versionNumber(r.version)
}
