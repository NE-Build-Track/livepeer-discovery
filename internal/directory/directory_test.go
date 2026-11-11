package directory

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestActiveOrchestratorIsKeptFromTheAIRegistry(t *testing.T) {
	orch := common.HexToAddress("0x2222222222222222222222222222222222222222")
	chain := &fakeChain{
		bonding: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		size:    big.NewInt(1),
		next:    map[common.Address]common.Address{orch: {}},
		active:  map[common.Address]bool{orch: true},
		uris:    map[common.Address]string{orch: "https://orch.example/"},
	}
	got, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("orchestrators = %d, want 1", len(got))
	}
	if got[0].URL.String() != "https://orch.example" || got[0].Address != orch {
		t.Fatalf("got %+v", got[0])
	}
	if chain.uriRegistry != common.HexToAddress(AIServiceRegistry) {
		t.Fatalf("getServiceURI registry = %s, want the AI Service Registry", chain.uriRegistry.Hex())
	}
	if chain.resolvedServiceRegistry {
		t.Fatal("the default directory resolved the Controller ServiceRegistry")
	}
}

// The directory reads the chain only. A URI node's ValidateURL accepts is a source: an http URI or a
// manifest URI is probed like any other and evicted by node's collector if it does not answer.
func TestInactiveEmptyAndInvalidRegistrationsAreLeftOut(t *testing.T) {
	kept := common.HexToAddress("0x0000000000000000000000000000000000000001")
	inactive := common.HexToAddress("0x0000000000000000000000000000000000000002")
	empty := common.HexToAddress("0x0000000000000000000000000000000000000003")
	plain := common.HexToAddress("0x0000000000000000000000000000000000000004")
	manifest := common.HexToAddress("0x0000000000000000000000000000000000000005")
	credentials := common.HexToAddress("0x0000000000000000000000000000000000000006")
	query := common.HexToAddress("0x0000000000000000000000000000000000000007")
	chain := &fakeChain{
		bonding: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		size:    big.NewInt(7),
		first:   kept,
		next: map[common.Address]common.Address{
			kept: inactive, inactive: empty, empty: plain, plain: manifest, manifest: credentials, credentials: query,
		},
		active: map[common.Address]bool{kept: true, inactive: false, empty: true, plain: true, manifest: true, credentials: true, query: true},
		uris: map[common.Address]string{
			kept:        "https://kept.example/",
			inactive:    "https://inactive.example",
			empty:       "",
			plain:       "http://plain.example",
			manifest:    "https://manifest.example/.well-known/livepeer-registry.json",
			credentials: "https://user:pass@credentials.example",
			query:       "https://query.example/?x=1",
		},
	}
	got, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var urls []string
	for _, orch := range got {
		urls = append(urls, orch.URL.String())
	}
	want := []string{"https://kept.example", "http://plain.example", "https://manifest.example/.well-known/livepeer-registry.json"}
	if fmt.Sprint(urls) != fmt.Sprint(want) {
		t.Fatalf("sources = %v, want %v", urls, want)
	}
}

func TestControllerSettingReadsTheProtocolRegistry(t *testing.T) {
	orch := common.HexToAddress("0x2222222222222222222222222222222222222222")
	protocol := common.HexToAddress("0x3333333333333333333333333333333333333333")
	ai := common.HexToAddress(AIServiceRegistry)
	chain := &fakeChain{
		bonding:          common.HexToAddress("0x1111111111111111111111111111111111111111"),
		protocolRegistry: protocol,
		size:             big.NewInt(1),
		first:            orch,
		next:             map[common.Address]common.Address{orch: {}},
		active:           map[common.Address]bool{orch: true},
		uriByRegistry: map[common.Address]map[common.Address]string{
			protocol: {orch: "https://protocol.example"},
			ai:       {orch: "https://ai.example"},
		},
	}
	got, err := (&Directory{Chain: chain, ServiceRegistry: "controller", RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 || got[0].URL.String() != "https://protocol.example" {
		t.Fatalf("got %+v, want the protocol registry URI", got)
	}
	if chain.uriRegistry != protocol || !chain.resolvedServiceRegistry {
		t.Fatalf("registry %s resolved=%t, want %s", chain.uriRegistry.Hex(), chain.resolvedServiceRegistry, protocol.Hex())
	}
}

func TestPoolWalkStopsAtTheReportedSize(t *testing.T) {
	first := nAddress(1)
	second := nAddress(2)
	chain := linkedPool(2)
	chain.size = big.NewInt(1)
	chain.first = first
	chain.next = map[common.Address]common.Address{first: second}
	got, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 || got[0].Address != first {
		t.Fatalf("got %+v, want only the first pool member", got)
	}
	if chain.activeCalls != 1 {
		t.Fatalf("active reads = %d, want 1", chain.activeCalls)
	}
}

func TestPoolWalkStopsAtOneThousand(t *testing.T) {
	chain := linkedPool(1001)
	got, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1000 || chain.activeCalls != 1000 {
		t.Fatalf("kept %d, active reads %d, want 1000", len(got), chain.activeCalls)
	}
}

func TestPoolWalkDoesNotFollowACycle(t *testing.T) {
	only := nAddress(1)
	chain := &fakeChain{
		bonding: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		size:    big.NewInt(50),
		first:   only,
		next:    map[common.Address]common.Address{only: only},
		active:  map[common.Address]bool{only: true},
		uris:    map[common.Address]string{only: "https://cycle.example"},
	}
	got, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 || chain.nextCalls != 1 || chain.activeCalls != 1 {
		t.Fatalf("kept %d next %d active %d, a cycle must stop after the repeated address", len(got), chain.nextCalls, chain.activeCalls)
	}
}

func TestARevertingOrchestratorIsSkipped(t *testing.T) {
	bad := nAddress(1)
	good := nAddress(2)
	chain := &fakeChain{
		bonding:    common.HexToAddress("0x1111111111111111111111111111111111111111"),
		size:       big.NewInt(2),
		first:      bad,
		next:       map[common.Address]common.Address{bad: good},
		active:     map[common.Address]bool{good: true},
		failActive: map[common.Address]error{bad: fmt.Errorf("bondingManager.isActiveTranscoder: ethereum RPC error -32000")},
		uris:       map[common.Address]string{good: "https://good.example"},
	}
	got, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{}}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 || got[0].Address != good || got[0].URL.String() != "https://good.example" {
		t.Fatalf("got %+v, want the readable orchestrator", got)
	}
}

func TestATransientRPCErrorIsRetriedThenFailsTheLoad(t *testing.T) {
	only := nAddress(1)
	chain := &fakeChain{
		bonding:    common.HexToAddress("0x1111111111111111111111111111111111111111"),
		size:       big.NewInt(1),
		first:      only,
		active:     map[common.Address]bool{only: true},
		failActive: map[common.Address]error{only: fmt.Errorf("bondingManager.isActiveTranscoder: ethereum RPC returned HTTP 429")},
		uris:       map[common.Address]string{only: "https://only.example"},
	}
	_, err := (&Directory{Chain: chain, RetryDelays: []time.Duration{0}}).Load(context.Background())
	var chainErr *Error
	if !errors.As(err, &chainErr) || chainErr.Reason != "rpc_http_429" || !chainErr.Transient {
		t.Fatalf("Load error = %v, want transient rpc_http_429", err)
	}
	if chain.activeCalls != 2 {
		t.Fatalf("active reads = %d, want the attempt and one retry", chain.activeCalls)
	}
}

func nAddress(n int) common.Address {
	var address common.Address
	address[18] = byte(n >> 8)
	address[19] = byte(n)
	return address
}

func linkedPool(n int) *fakeChain {
	chain := &fakeChain{
		bonding: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		size:    big.NewInt(int64(n)),
		next:    map[common.Address]common.Address{},
		active:  map[common.Address]bool{},
		uris:    map[common.Address]string{},
	}
	var prev common.Address
	for i := 1; i <= n; i++ {
		address := nAddress(i)
		if i == 1 {
			chain.first = address
		} else {
			chain.next[prev] = address
		}
		chain.active[address] = true
		chain.uris[address] = "https://orch.example/" + strconv.Itoa(i)
		prev = address
	}
	return chain
}

// fakeChain answers the directory's Caller. It records which registry getServiceURI used.
type fakeChain struct {
	bonding                 common.Address
	protocolRegistry        common.Address
	size                    *big.Int
	first                   common.Address
	next                    map[common.Address]common.Address
	active                  map[common.Address]bool
	uris                    map[common.Address]string
	uriByRegistry           map[common.Address]map[common.Address]string
	failActive              map[common.Address]error
	failURI                 map[common.Address]error
	failPool                error
	uriRegistry             common.Address
	resolvedServiceRegistry bool
	activeCalls             int
	nextCalls               int
	poolCalls               int
}

func (f *fakeChain) Resolve(_ context.Context, name string) (common.Address, error) {
	switch name {
	case "bondingManager":
		return f.bonding, nil
	case "serviceRegistry":
		f.resolvedServiceRegistry = true
		return f.protocolRegistry, nil
	default:
		return common.Address{}, errFake("unexpected resolve " + name)
	}
}

func (f *fakeChain) Call(_ context.Context, name string, address common.Address, method string, args ...any) ([]any, error) {
	switch method {
	case "getTranscoderPoolSize":
		f.poolCalls++
		if f.failPool != nil {
			return nil, f.failPool
		}
		return []any{f.size}, nil
	case "getFirstTranscoderInPool":
		if f.failPool != nil {
			return nil, f.failPool
		}
		first := f.first
		if first == (common.Address{}) {
			for addr := range f.next {
				first = addr
				break
			}
		}
		return []any{first}, nil
	case "getNextTranscoderInPool":
		f.nextCalls++
		if f.failPool != nil {
			return nil, f.failPool
		}
		current := args[0].(common.Address)
		return []any{f.next[current]}, nil
	case "isActiveTranscoder":
		f.activeCalls++
		who := args[0].(common.Address)
		if err := f.failActive[who]; err != nil {
			return nil, err
		}
		return []any{f.active[who]}, nil
	case "getServiceURI":
		if name != "serviceRegistry" {
			return nil, errFake("getServiceURI used ABI " + name)
		}
		f.uriRegistry = address
		who := args[0].(common.Address)
		if err := f.failURI[who]; err != nil {
			return nil, err
		}
		if byRegistry, ok := f.uriByRegistry[address]; ok {
			return []any{byRegistry[who]}, nil
		}
		return []any{f.uris[who]}, nil
	default:
		return nil, errFake("unexpected method " + method)
	}
}

type errFake string

func (e errFake) Error() string { return string(e) }
