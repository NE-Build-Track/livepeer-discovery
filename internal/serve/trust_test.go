package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NE-Build-Track/livepeer-onchain-discovery/internal/directory"
	"github.com/ethereum/go-ethereum/common"
)

// Each registered orchestrator speaks only for itself (issue #1, item 4).

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type registeredDirectory []directory.Orchestrator

func (d registeredDirectory) Load(context.Context) ([]directory.Orchestrator, error) { return d, nil }

var (
	alice = common.HexToAddress("0xA11CE00000000000000000000000000000000001")
	bob   = common.HexToAddress("0xB0B0000000000000000000000000000000000002")
)

func runnerJSON(url, app string) string {
	return `{"url":"` + url + `","app":"` + app + `","observed_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`
}

func probedWithLog(t *testing.T, dir Directory) (*Service, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	svc, err := New(Config{
		Directory: dir, Grants: []string{"127.0.0.1"}, RefreshInterval: time.Hour, DirectoryInterval: time.Hour,
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(svc.collector.Close)
	if err := svc.Reload(context.Background()); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	svc.probe(context.Background())
	return svc, logs
}

func addresses(t *testing.T, svc *Service) []string {
	t.Helper()
	var entries []struct{ Address string }
	if err := json.Unmarshal(get(svc, "/discovery").Body.Bytes(), &entries); err != nil {
		t.Fatalf("discovery: %v", err)
	}
	var out []string
	for _, e := range entries {
		if e.Address != "" {
			out = append(out, e.Address)
		}
	}
	return out
}

func TestAnEntryForAnotherAddressIsDroppedAndLogged(t *testing.T) {
	a := orchestrator(t, nil, func(base string) string {
		// Its own entry, written with a trailing slash, which is still its own base URL.
		return `[{"address":"` + base + `/","runners":[` + runnerJSON(base+"/apps/own/app", "own") + `]},` +
			`{"address":"https://victim.example","runners":[` + runnerJSON("https://victim.example/apps/x/app", "theirs") + `]}]`
	})
	svc, logs := probedWithLog(t, registeredDirectory{{URL: a.URL, Address: alice}})
	if got := addresses(t, svc); len(got) != 1 || got[0] != a.URL+"/" {
		t.Fatalf("served addresses = %v, want only the orchestrator's own", got)
	}
	if body := get(svc, "/readyz").Body.String(); !strings.Contains(body, `"dropped_foreign_entries":1`) {
		t.Fatalf("readyz = %s", body)
	}
	if line := logs.String(); !strings.Contains(line, "event=discovery.foreign_entry_dropped") ||
		!strings.Contains(line, "orchestrator="+alice.Hex()) || !strings.Contains(line, "address=https://victim.example") {
		t.Fatalf("log = %s", line)
	}
}

func TestARunnerURLTwoOrchestratorsAdvertiseIsKeptAndLoggedOnce(t *testing.T) {
	const shared = "https://shared.example/apps/s/app"
	serve := func(own string) func(string) string {
		return func(base string) string {
			return `[{"address":"` + base + `","runners":[` + runnerJSON(shared, "shared") + `,` + runnerJSON(base+own, "own") + `]}]`
		}
	}
	a := orchestrator(t, nil, serve("/apps/a/app"))
	b := orchestrator(t, nil, serve("/apps/b/app"))
	svc, logs := probedWithLog(t, registeredDirectory{{URL: a.URL, Address: alice}, {URL: b.URL, Address: bob}})
	body := get(svc, "/discovery").Body.String()
	if strings.Count(body, shared) != 1 || !strings.Contains(body, a.URL+"/apps/a/app") || !strings.Contains(body, b.URL+"/apps/b/app") {
		t.Fatalf("discovery = %s, want both orchestrators' own runners and the shared one once", body)
	}
	if ready := get(svc, "/readyz").Body.String(); !strings.Contains(ready, `"contested_runner_urls":1`) {
		t.Fatalf("readyz = %s", ready)
	}
	svc.probe(context.Background()) // the next round still sees it: no second log line
	if n := strings.Count(logs.String(), "event=discovery.runner_url_contested"); n != 1 {
		t.Fatalf("contested runner logged %d times:\n%s", n, logs.String())
	}
	if line := logs.String(); !strings.Contains(line, "runner="+shared) || !strings.Contains(line, alice.Hex()) || !strings.Contains(line, bob.Hex()) {
		t.Fatalf("log = %s", line)
	}
	if code := get(svc, "/discovery").Code; code != http.StatusOK {
		t.Fatalf("discovery = %d", code)
	}
}
