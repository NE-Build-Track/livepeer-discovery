package serve

// The served /discovery is livepeer/node's contract. These tests validate it the way node's
// discovery/schema_test.go does at the pinned commit: node's own docs/discovery.schema.json, read
// from the livepeer/node module go.mod pins, compiled by santhosh-tekuri/jsonschema/v6 with
// AssertFormat, and numbers decoded as tokens.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// discoverySchema compiles the schema file of the livepeer/node module this build uses, so the
// go.mod pin is the schema pin.
func discoverySchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	dir, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/livepeer/node").Output()
	if err != nil {
		t.Fatalf("locate the livepeer/node module: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join(strings.TrimSpace(string(dir)), "docs", "discovery.schema.json"))
	if err != nil {
		t.Fatalf("compile node's discovery schema: %v", err)
	}
	return schema
}

func validateDiscoveryJSON(t *testing.T, schema *jsonschema.Schema, data []byte) error {
	t.Helper()
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("discovery body is not JSON: %v\n%s", err, data)
	}
	if err := decoder.Decode(&value); err != io.EOF {
		t.Fatalf("discovery body has trailing data: %s", data)
	}
	return schema.Validate(value)
}

// orchestrator serves a /discovery inventory that names its own base URL as the address.
func orchestrator(t *testing.T, hits *atomic.Int32, inventory func(base string) string) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		if r.URL.Path != "/discovery" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, inventory(server.URL))
	}))
	t.Cleanup(server.Close)
	return server
}

// probed loads the directory and runs one complete probe round, then serves the service over HTTP.
func probed(t *testing.T, cfg Config) (*Service, *httptest.Server) {
	t.Helper()
	cfg.RefreshInterval, cfg.DirectoryInterval, cfg.MaxFailedRefreshes = time.Hour, time.Hour, 6
	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(svc.collector.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := svc.Reload(ctx); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	svc.probe(ctx)
	front := httptest.NewServer(svc)
	t.Cleanup(front.Close)
	return svc, front
}

func fetch(t *testing.T, front *httptest.Server, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(front.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, body
}

func TestTheServedInventoryMatchesNodesSchema(t *testing.T) {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	// Extension fields and numbers past float64 must come through as the orchestrator wrote them.
	kept := `{"url":"%s/apps/a/app","app":"example","observed_at":"` + stamp + `","mode":"future",` +
		`"extension":{"count":9007199254740993},"price_info":{"price":123456789012345678901234567890,` +
		`"price_usd":0.123456789012345678901234567890123456,"currency":"wei","unit":"fixed","extension":true}}`
	good := orchestrator(t, nil, func(base string) string {
		return `[{"address":"` + base + `","runners":[` + fmt.Sprintf(kept, base) + `,` +
			`{"url":"` + base + `/apps/b/app","app":"unstamped"},` +
			`{"url":"ftp://elsewhere.example/app","app":"not-http","observed_at":"` + stamp + `"}]}]`
	})
	broken := orchestrator(t, nil, func(base string) string {
		return `[{"address":"` + base + `","runners":[{"url":"` + base + `/apps/c/app","app":""}]}]`
	})
	_, front := probed(t, Config{
		Directory: fixedDirectory{urls: []string{good.URL, broken.URL}},
		Grants:    []string{"127.0.0.1"},
	})
	code, body := fetch(t, front, "/discovery")
	if code != http.StatusOK {
		t.Fatalf("discovery = %d %s", code, body)
	}
	if err := validateDiscoveryJSON(t, discoverySchema(t), body); err != nil {
		t.Fatalf("served inventory fails node's schema: %v\n%s", err, body)
	}
	var entries []struct {
		Nodes   []struct{ URL string } `json:"nodes"`
		Address string                 `json:"address"`
		Runners []json.RawMessage      `json:"runners"`
	}
	if err := json.Unmarshal(body, &entries); err != nil || len(entries) != 2 {
		t.Fatalf("want the nodes entry and one orchestrator, got %s", body)
	}
	if len(entries[0].Nodes) != 1 || entries[0].Nodes[0].URL != good.URL {
		t.Fatalf("nodes entry = %+v, want only the source that answered with a valid runner", entries[0].Nodes)
	}
	if entries[1].Address != good.URL || len(entries[1].Runners) != 1 {
		t.Fatalf("orchestrator entry = %s, want only the runner the schema accepts", body)
	}
	if got, want := string(entries[1].Runners[0]), fmt.Sprintf(kept, good.URL); got != want {
		t.Fatalf("runner was not passed through unchanged:\n got %s\nwant %s", got, want)
	}
}

func TestAChainWithNoOrchestratorsServesAnEmptyInventory(t *testing.T) {
	_, front := probed(t, Config{Directory: fixedDirectory{}})
	code, body := fetch(t, front, "/discovery")
	if code != http.StatusOK || strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("discovery = %d %s, want 200 []", code, body)
	}
	if err := validateDiscoveryJSON(t, discoverySchema(t), body); err != nil {
		t.Fatalf("empty inventory fails node's schema: %v", err)
	}
}

func TestARegistrationAtAPrivateAddressIsNeverProbed(t *testing.T) {
	var hits atomic.Int32
	private := orchestrator(t, &hits, func(base string) string { return `[]` })
	port := strings.TrimPrefix(private.URL, "http://127.0.0.1")
	_, front := probed(t, Config{
		// Anyone registered on chain sets their own URI. Without a grant, loopback, private and
		// link-local destinations (169.254.169.254 among them) are refused before a connection.
		Directory: fixedDirectory{urls: []string{private.URL, "http://localhost" + port, "http://169.254.169.254"}},
	})
	if n := hits.Load(); n != 0 {
		t.Fatalf("the private orchestrator received %d requests", n)
	}
	if code, body := fetch(t, front, "/discovery"); code != http.StatusOK || strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("discovery = %d %s, want no sources", code, body)
	}
}

// A registered URI can be a discovery service in front of several orchestrators. Its entries name
// those orchestrators and are published as written, as livepeer/node's discovery does (issue #3).
func TestAnAggregateURIIsPublishedAsItsSourceWroteIt(t *testing.T) {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	entry := func(address string) string {
		return `{"address":"` + address + `","runners":[{"url":"` + address + `/apps/a/app","app":"example","observed_at":"` + stamp + `"}]}`
	}
	front := orchestrator(t, nil, func(string) string {
		return `[{"nodes":[{"url":"https://ai1.example:8936","queried_at":"` + stamp + `"}]},` +
			entry("https://ai1.example:8936") + `,` + entry("https://ai2.example:8936") + `]`
	})
	_, server := probed(t, Config{Directory: fixedDirectory{urls: []string{front.URL}}, Grants: []string{"127.0.0.1"}})
	code, body := fetch(t, server, "/discovery")
	if code != http.StatusOK {
		t.Fatalf("discovery = %d %s", code, body)
	}
	if err := validateDiscoveryJSON(t, discoverySchema(t), body); err != nil {
		t.Fatalf("served inventory fails node's schema: %v\n%s", err, body)
	}
	var entries []struct {
		Address string            `json:"address"`
		Runners []json.RawMessage `json:"runners"`
	}
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var got []string
	for _, e := range entries {
		if e.Address != "" && len(e.Runners) == 1 {
			got = append(got, e.Address)
		}
	}
	if fmt.Sprint(got) != "[https://ai1.example:8936 https://ai2.example:8936]" {
		t.Fatalf("published addresses = %v, want both orchestrators the aggregate names\n%s", got, body)
	}
}
