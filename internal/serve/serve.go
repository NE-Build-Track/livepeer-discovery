// Package serve collects runner inventories for the on-chain directory and
// serves them in livepeer/node's discovery format.
package serve

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/NE-Build-Track/livepeer-onchain-discovery/internal/directory"
	"github.com/livepeer/node/destination"
	shared "github.com/livepeer/node/discovery/shared"
)

// Directory is the chain read. The service probes whatever Load returns.
type Directory interface {
	Load(ctx context.Context) ([]directory.Orchestrator, error)
}

// Config selects who is probed and how often.
type Config struct {
	Directory          Directory
	Grants             []string
	RefreshInterval    time.Duration
	DirectoryInterval  time.Duration
	MaxFailedRefreshes int
	Logger             *slog.Logger
}

// Service is the discovery HTTP handler. Run refreshes the directory and the
// collector until the context ends.
type Service struct {
	directory      Directory
	refresh        time.Duration
	directoryEvery time.Duration
	logger         *slog.Logger
	collector      *shared.Collector[runner]
	mux            *http.ServeMux

	mu            sync.Mutex
	current       []string
	orchestrators int
	loaded        bool
	loadedAt      time.Time
	dirErr        string
	running       bool
}

// New builds a service that has not loaded a directory yet.
func New(cfg Config) (*Service, error) {
	if cfg.RefreshInterval <= 0 || cfg.DirectoryInterval <= 0 {
		return nil, errors.New("refresh and directory intervals must be positive")
	}
	policy, err := destination.New("discovery", cfg.Grants)
	if err != nil {
		return nil, err
	}
	client := policy.Client()
	client.Timeout = 3 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	failures := cfg.MaxFailedRefreshes
	if failures < 1 {
		failures = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Service{
		directory: cfg.Directory,
		refresh:   cfg.RefreshInterval, directoryEvery: cfg.DirectoryInterval,
		logger: logger,
		collector: &shared.Collector[runner]{
			Client: client, MaxFailures: failures, Logger: logger,
		},
		mux: http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /discovery", s.discovery)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /readyz", s.readyz)
	return s, nil
}

// Reload reads the directory once. A failed read leaves the previous sources
// in place.
func (s *Service) Reload(ctx context.Context) error {
	bases, err := s.bases(ctx)
	if err != nil {
		s.noteError(err)
		return err
	}
	s.noteLoaded()
	s.setSources(bases)
	if len(bases) == 0 {
		s.collector.AcceptEmpty()
	}
	return nil
}

// Run reloads the directory on its interval and probes until ctx ends.
func (s *Service) Run(ctx context.Context) {
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		s.collector.Run(ctx, s.refresh)
	}()
	go func() {
		defer workers.Done()
		s.Reload(ctx)
		ticker := time.NewTicker(s.directoryEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Reload(ctx)
			}
		}
	}()
	workers.Wait()
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Service) bases(ctx context.Context) ([]*url.URL, error) {
	var chain []directory.Orchestrator
	if s.directory != nil {
		var err error
		chain, err = s.directory.Load(ctx)
		if err != nil {
			return nil, err
		}
	}
	bases := make([]*url.URL, 0, len(chain))
	seen := map[string]bool{}
	for _, orch := range chain {
		parsed, err := url.Parse(orch.URL)
		if err != nil || shared.ValidateURL(parsed) != nil {
			continue
		}
		parsed.Path = strings.TrimRight(parsed.Path, "/")
		key := parsed.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		bases = append(bases, parsed)
	}
	return bases, nil
}

func (s *Service) setSources(bases []*url.URL) {
	endpoints := make([]*url.URL, 0, len(bases))
	stored := make([]string, 0, len(bases))
	for _, base := range bases {
		endpoint := discoveryEndpoint(base)
		endpoints = append(endpoints, endpoint)
		stored = append(stored, endpoint.String())
	}
	s.mu.Lock()
	s.current = stored
	s.orchestrators = len(stored)
	s.mu.Unlock()
	s.collector.SetSources(endpoints)
}

func (s *Service) sources() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.current...)
}

func (s *Service) noteLoaded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loaded = true
	s.loadedAt = time.Now().UTC()
	s.dirErr = ""
}

func (s *Service) noteError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirErr = reason(err)
}

func (s *Service) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (s *Service) readyz(w http.ResponseWriter, _ *http.Request) {
	body, code := s.readyResponse()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Service) readyResponse() (readyBody, int) {
	probed := s.collector.Initialized()
	var latest time.Time
	serving := 0
	for _, snapshot := range s.collector.Snapshots() {
		serving++
		if snapshot.QueriedAt.After(latest) {
			latest = snapshot.QueriedAt
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ready := s.running && s.loaded && probed
	code := http.StatusServiceUnavailable
	if ready {
		code = http.StatusOK
	}
	body := readyBody{
		Ready: ready, Refreshing: s.running, Orchestrators: s.orchestrators, Serving: serving,
	}
	if s.loaded {
		loaded := s.loadedAt
		body.DirectoryLoadedAt = &loaded
	}
	if !latest.IsZero() {
		probedAt := latest
		body.ProbedAt = &probedAt
	}
	if s.dirErr != "" {
		reason := s.dirErr
		body.DirectoryError = &reason
	}
	return body, code
}

type readyBody struct {
	Ready             bool       `json:"ready"`
	Refreshing        bool       `json:"refreshing"`
	Orchestrators     int        `json:"orchestrators"`
	Serving           int        `json:"serving"`
	DirectoryLoadedAt *time.Time `json:"directory_loaded_at"`
	ProbedAt          *time.Time `json:"probed_at"`
	DirectoryError    *string    `json:"directory_error"`
}

func (s *Service) discovery(w http.ResponseWriter, r *http.Request) {
	body, err := json.Marshal(s.discoveryResponse())
	if err != nil {
		http.Error(w, "cannot encode discovery inventory", http.StatusInternalServerError)
		return
	}
	body = append(body, '\n')
	if len(body) > shared.MaxBody {
		s.logger.WarnContext(r.Context(), "discovery response exceeds signer size limit", "response_bytes", len(body), "max_bytes", shared.MaxBody)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (s *Service) discoveryResponse() []map[string]any {
	type node struct {
		URL       string    `json:"url"`
		QueriedAt time.Time `json:"queried_at"`
	}
	var nodes []node
	var entries []shared.Entry[runner]
	for _, snapshot := range s.collector.Snapshots() {
		nodes = append(nodes, node{URL: strings.TrimSuffix(snapshot.URL, "/discovery"), QueriedAt: snapshot.QueriedAt})
		entries = append(entries, snapshot.Entries...)
	}
	result := []map[string]any{}
	if len(nodes) > 0 {
		result = append(result, map[string]any{"nodes": nodes})
	}
	for _, entry := range shared.Merge(entries) {
		if len(entry.Runners) == 0 {
			continue
		}
		result = append(result, map[string]any{"address": entry.Address, "runners": entry.Runners})
	}
	return result
}

func reason(err error) string {
	var chain *directory.Error
	if errors.As(err, &chain) && chain.Reason != "" {
		return chain.Reason
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "interrupted"
	}
	return "directory_unavailable"
}

func discoveryEndpoint(base *url.URL) *url.URL {
	endpoint := base.Clone()
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/discovery"
	endpoint.RawPath = ""
	return endpoint
}

// runner keeps the upstream JSON, including fields this service does not interpret.
type runner struct {
	URL        string `json:"url"`
	App        string `json:"app"`
	ObservedAt string `json:"observed_at"`
	data       json.RawMessage
}

func (r *runner) UnmarshalJSON(data []byte) error {
	type identity runner
	var value identity
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*r = runner(value)
	r.data = append(json.RawMessage(nil), data...)
	return nil
}

func (r runner) MarshalJSON() ([]byte, error) { return r.data, nil }

func (r runner) DiscoveryIdentity() (string, string, string) {
	return r.URL, r.App, r.ObservedAt
}
