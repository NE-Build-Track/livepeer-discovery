// Package directory reads the Livepeer orchestrator pool and keeps those
// registered with a service URI. Probing, validation of what an orchestrator
// answers, caching and eviction are livepeer/node's discovery collector's.
package directory

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	shared "github.com/livepeer/node/discovery/shared"
)

// AIServiceRegistry is the Arbitrum AI Service Registry. The Controller does not
// know it, so it is the default override of the protocol ServiceRegistry.
const AIServiceRegistry = "0x04C0b249740175999E5BF5c9ac1dA92431EF34C5"

// ArbitrumController is the protocol Controller on Arbitrum mainnet.
const ArbitrumController = "0xD8E8328501E9645d16Cf49539efC04f734606ee4"

const defaultMaxPool = 1000

// Orchestrator is an eligible orchestrator's base URL and Ethereum address.
type Orchestrator struct {
	URL     *url.URL
	Address common.Address
}

// Error is a chain read this package produced. Reason is stable and safe to
// show: it never includes an RPC URL or a provider's error text.
type Error struct {
	Reason    string
	Transient bool
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Reason
}

// Caller is the chain read surface of github.com/livepeer/node/eth.Contracts.
type Caller interface {
	Resolve(ctx context.Context, name string) (common.Address, error)
	Call(ctx context.Context, name string, address common.Address, method string, args ...any) ([]any, error)
}

// Directory reads the bonded pool and keeps orchestrators registered for a service.
//
// ServiceRegistry empty (or the AI registry address) reads the AI Service Registry.
// The value "controller" reads the Controller's protocol ServiceRegistry instead.
// Any other Ethereum address is that registry.
//
// RetryDelays nil uses 0.5s and 2s. A non-nil empty slice does not retry.
// MaxPool zero means 1000.
type Directory struct {
	Chain           Caller
	ServiceRegistry string
	MaxPool         int
	RetryDelays     []time.Duration
	Logger          *slog.Logger
}

// Load returns the eligible orchestrators in pool order. An error means the
// directory is unknown; the caller keeps the previous one.
func (d *Directory) Load(ctx context.Context) ([]Orchestrator, error) {
	bonding, err := d.resolve(ctx, "bondingManager")
	if err != nil {
		return nil, err
	}
	registry, err := d.registry(ctx)
	if err != nil {
		return nil, err
	}
	pool, err := d.pool(ctx, bonding)
	if err != nil {
		return nil, err
	}
	found := make([]Orchestrator, 0, len(pool))
	for _, address := range pool {
		orch, err := d.eligible(ctx, bonding, registry, address)
		if err != nil {
			return nil, err
		}
		if orch != nil {
			found = append(found, *orch)
		}
	}
	return found, nil
}

func (d *Directory) registry(ctx context.Context) (common.Address, error) {
	value := strings.TrimSpace(d.ServiceRegistry)
	switch strings.ToLower(value) {
	case "", strings.ToLower(AIServiceRegistry):
		return common.HexToAddress(AIServiceRegistry), nil
	case "controller":
		return d.resolve(ctx, "serviceRegistry")
	default:
		if !common.IsHexAddress(value) {
			return common.Address{}, &Error{Reason: "invalid_service_registry"}
		}
		return common.HexToAddress(value), nil
	}
}

func (d *Directory) pool(ctx context.Context, bonding common.Address) ([]common.Address, error) {
	values, err := d.call(ctx, "bondingManager", bonding, "getTranscoderPoolSize")
	if err != nil {
		return nil, err
	}
	size, ok := result[*big.Int](values)
	if !ok || size == nil || size.Sign() < 0 {
		return nil, malformed()
	}
	limit := d.maxPool()
	if size.Cmp(big.NewInt(int64(limit))) > 0 {
		d.logger().Warn("the transcoder pool reports more orchestrators than the walk reads",
			"event", "discovery.pool_truncated", "reported", size.String(), "read", limit)
	} else {
		limit = int(size.Int64())
	}
	values, err = d.call(ctx, "bondingManager", bonding, "getFirstTranscoderInPool")
	if err != nil {
		return nil, err
	}
	address, ok := result[common.Address](values)
	if !ok {
		return nil, malformed()
	}
	pool := make([]common.Address, 0, limit)
	seen := map[common.Address]bool{}
	for address != (common.Address{}) && len(pool) < limit {
		if seen[address] {
			break
		}
		seen[address] = true
		pool = append(pool, address)
		values, err = d.call(ctx, "bondingManager", bonding, "getNextTranscoderInPool", address)
		if err != nil {
			return nil, err
		}
		address, ok = result[common.Address](values)
		if !ok {
			return nil, malformed()
		}
	}
	return pool, nil
}

func (d *Directory) eligible(ctx context.Context, bonding, registry, address common.Address) (*Orchestrator, error) {
	values, err := d.call(ctx, "bondingManager", bonding, "isActiveTranscoder", address)
	if err != nil {
		return nil, d.orchestratorRead(address, err)
	}
	active, ok := result[bool](values)
	if !ok {
		return nil, malformed()
	}
	if !active {
		return nil, nil
	}
	values, err = d.call(ctx, "serviceRegistry", registry, "getServiceURI", address)
	if err != nil {
		return nil, d.orchestratorRead(address, err)
	}
	uri, ok := result[string](values)
	if !ok {
		return nil, malformed()
	}
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil, nil
	}
	base, err := url.Parse(uri)
	if err != nil || shared.ValidateURL(base) != nil {
		d.logger().Warn("orchestrator registered an unusable service URI",
			"event", "discovery.service_uri_invalid", "orchestrator", address.Hex())
		return nil, nil
	}
	base.Path, base.RawPath = strings.TrimRight(base.Path, "/"), ""
	return &Orchestrator{URL: base, Address: address}, nil
}

// orchestratorRead skips one orchestrator whose own record reverted, and fails
// the load when the RPC itself failed.
func (d *Directory) orchestratorRead(address common.Address, err error) error {
	var chain *Error
	if errors.As(err, &chain) && chain.Transient {
		return chain
	}
	reason := "rpc_rejected"
	if chain != nil {
		reason = chain.Reason
	}
	d.logger().Warn("orchestrator left out: its on-chain record could not be read",
		"event", "discovery.orchestrator_unreadable", "orchestrator", address.Hex(), "error", reason)
	return nil
}

func (d *Directory) resolve(ctx context.Context, name string) (common.Address, error) {
	var last error
	delays := d.delays()
	for attempt := 0; attempt <= len(delays); attempt++ {
		if err := wait(ctx, delays, attempt); err != nil {
			return common.Address{}, err
		}
		address, err := d.Chain.Resolve(ctx, name)
		if err == nil {
			if address == (common.Address{}) {
				return common.Address{}, &Error{Reason: "contract_unresolved"}
			}
			return address, nil
		}
		classified := classify(err)
		if strings.Contains(err.Error(), "is not registered") {
			classified = &Error{Reason: "contract_unresolved"}
		}
		if !classified.Transient {
			return common.Address{}, classified
		}
		last = classified
	}
	return common.Address{}, last
}

func (d *Directory) call(ctx context.Context, name string, address common.Address, method string, args ...any) ([]any, error) {
	var last error
	delays := d.delays()
	for attempt := 0; attempt <= len(delays); attempt++ {
		if err := wait(ctx, delays, attempt); err != nil {
			return nil, err
		}
		values, err := d.Chain.Call(ctx, name, address, method, args...)
		if err == nil {
			return values, nil
		}
		classified := classify(err)
		if !classified.Transient {
			return nil, classified
		}
		last = classified
	}
	return nil, last
}

func (d *Directory) delays() []time.Duration {
	if d.RetryDelays != nil {
		return d.RetryDelays
	}
	return []time.Duration{500 * time.Millisecond, 2 * time.Second}
}

func (d *Directory) maxPool() int {
	if d.MaxPool > 0 {
		return d.MaxPool
	}
	return defaultMaxPool
}

func (d *Directory) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func wait(ctx context.Context, delays []time.Duration, attempt int) error {
	if attempt == 0 {
		return nil
	}
	timer := time.NewTimer(delays[attempt-1])
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func classify(err error) *Error {
	var already *Error
	if errors.As(err, &already) {
		return already
	}
	msg := err.Error()
	if code, ok := leadingInt(msg, "ethereum RPC returned HTTP "); ok {
		return &Error{Reason: "rpc_http_" + strconv.Itoa(code), Transient: code == 429 || code >= 500}
	}
	if code, ok := leadingInt(msg, "ethereum RPC error "); ok {
		return &Error{Reason: "rpc_error_" + strconv.Itoa(code), Transient: code == -32005 || code == -32603}
	}
	if strings.Contains(msg, "ethereum RPC request failed") || strings.Contains(msg, "exceeds 1 MiB") || errors.Is(err, context.DeadlineExceeded) {
		return &Error{Reason: "rpc_request_failed", Transient: true}
	}
	return &Error{Reason: "rpc_rejected"}
}

func leadingInt(msg, marker string) (int, bool) {
	i := strings.Index(msg, marker)
	if i < 0 {
		return 0, false
	}
	rest := msg[i+len(marker):]
	end := 0
	if end < len(rest) && rest[end] == '-' {
		end++
	}
	start := end
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == start {
		return 0, false
	}
	code, err := strconv.Atoi(rest[:end])
	if err != nil {
		return 0, false
	}
	return code, true
}

func malformed() *Error { return &Error{Reason: "malformed_result", Transient: true} }

// result is a call's single return value. node's ABI unpack types it; a fake or a broken
// RPC that returns something else fails the load as malformed_result.
func result[T any](values []any) (T, bool) {
	var zero T
	if len(values) != 1 {
		return zero, false
	}
	value, ok := values[0].(T)
	return value, ok
}
