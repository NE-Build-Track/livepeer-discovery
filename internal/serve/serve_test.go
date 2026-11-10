package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NE-Build-Track/livepeer-onchain-discovery/internal/directory"
	"github.com/ethereum/go-ethereum/common"
)

func TestAFailedReloadKeepsThePreviousSources(t *testing.T) {
	chain := &toggleChain{uri: "https://first.example"}
	svc, err := New(Config{
		Directory:          &directory.Directory{Chain: chain, RetryDelays: []time.Duration{}},
		RefreshInterval:    time.Hour,
		DirectoryInterval:  time.Hour,
		MaxFailedRefreshes: 6,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.Reload(context.Background()); err != nil {
		t.Fatalf("first reload: %v", err)
	}
	if got := svc.sources(); len(got) != 1 || got[0] != "https://first.example/discovery" {
		t.Fatalf("sources = %v", got)
	}
	chain.fail = true
	chain.uri = "https://second.example"
	if err := svc.Reload(context.Background()); err == nil {
		t.Fatal("second reload succeeded, want the chain error")
	}
	if got := svc.sources(); len(got) != 1 || got[0] != "https://first.example/discovery" {
		t.Fatalf("sources after the failed reload = %v, want the previous list", got)
	}
	ready := get(svc, "/readyz")
	if ready.Code != http.StatusServiceUnavailable || !strings.Contains(ready.Body.String(), `"directory_error":"rpc_http_429"`) || strings.Contains(ready.Body.String(), "second.example") {
		t.Fatalf("readyz = %d %s", ready.Code, ready.Body.String())
	}
}

type toggleChain struct {
	fail bool
	uri  string
}

func TestAFailedFirstLoadPublishesNothing(t *testing.T) {
	chain := &toggleChain{fail: true, uri: "https://chain.example"}
	svc, err := New(Config{
		Directory:          &directory.Directory{Chain: chain, RetryDelays: []time.Duration{}},
		RefreshInterval:    time.Hour,
		DirectoryInterval:  time.Hour,
		MaxFailedRefreshes: 6,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.Reload(context.Background()); err == nil {
		t.Fatal("reload succeeded, want the chain error")
	}
	if got := svc.sources(); len(got) != 0 {
		t.Fatalf("sources = %v, want nothing until the chain loads", got)
	}
}

func TestDiscoveryStaysUnreadyUntilAProbeThenServesTheInventory(t *testing.T) {
	const inventory = `[{"address":"https://child.example","runners":[{"url":"https://child.example/app","app":"test","observed_at":"1999-01-01T00:00:00Z","price_usd":"1.25"}]}]`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/discovery" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, inventory)
	}))
	t.Cleanup(upstream.Close)
	svc, err := New(Config{
		Directory:          fixedDirectory{urls: []string{upstream.URL}},
		Grants:             []string{"127.0.0.1"},
		RefreshInterval:    time.Hour,
		DirectoryInterval:  time.Hour,
		MaxFailedRefreshes: 6,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if code := get(svc, "/readyz").Code; code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before the probe = %d, want 503", code)
	}
	if code := get(svc, "/healthz").Code; code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	deadline := time.Now().Add(2 * time.Second)
	var ready *httptest.ResponseRecorder
	for time.Now().Before(deadline) {
		ready = get(svc, "/readyz")
		if ready.Code == http.StatusOK {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ready.Code != http.StatusOK {
		t.Fatalf("readyz after the probe = %d body %s", ready.Code, ready.Body.String())
	}
	discovery := get(svc, "/discovery")
	if discovery.Code != http.StatusOK {
		t.Fatalf("discovery = %d", discovery.Code)
	}
	var body []map[string]json.RawMessage
	if err := json.Unmarshal(discovery.Body.Bytes(), &body); err != nil {
		t.Fatalf("discovery body: %v %s", err, discovery.Body.String())
	}
	if len(body) != 2 {
		t.Fatalf("discovery entries = %d, want the nodes entry and one orchestrator: %s", len(body), discovery.Body.String())
	}
	if _, ok := body[0]["nodes"]; !ok {
		t.Fatalf("first entry has no nodes metadata: %s", body[0])
	}
	if string(body[1]["address"]) != `"https://child.example"` || !strings.Contains(string(body[1]["runners"]), `"price_usd":"1.25"`) {
		t.Fatalf("runner record was not passed through: %s", body[1])
	}
}

func get(svc *Service, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	svc.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

type fixedDirectory struct{ urls []string }

func (d fixedDirectory) Load(context.Context) ([]directory.Orchestrator, error) {
	out := make([]directory.Orchestrator, len(d.urls))
	for i, raw := range d.urls {
		out[i] = directory.Orchestrator{URL: raw}
	}
	return out, nil
}

func (c *toggleChain) Resolve(context.Context, string) (common.Address, error) {
	if c.fail {
		return common.Address{}, fmt.Errorf("resolve BondingManager: ethereum RPC returned HTTP 429")
	}
	return common.HexToAddress("0x1111111111111111111111111111111111111111"), nil
}

func (c *toggleChain) Call(_ context.Context, _ string, _ common.Address, method string, _ ...any) ([]any, error) {
	if c.fail {
		return nil, fmt.Errorf("%s: ethereum RPC returned HTTP 429", method)
	}
	switch method {
	case "getTranscoderPoolSize":
		return []any{big.NewInt(1)}, nil
	case "getFirstTranscoderInPool":
		return []any{common.HexToAddress("0x2222222222222222222222222222222222222222")}, nil
	case "getNextTranscoderInPool":
		return []any{common.Address{}}, nil
	case "isActiveTranscoder":
		return []any{true}, nil
	case "getServiceURI":
		return []any{c.uri}, nil
	default:
		return nil, errors.New("unexpected method " + method)
	}
}
