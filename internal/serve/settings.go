package serve

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NE-Build-Track/livepeer-onchain-discovery/internal/directory"
)

// Settings is the process configuration. RPCURL is never written to a log by
// this package; callers must not include it in an error they show.
type Settings struct {
	Config          Config
	Listen          string
	RPCURL          string
	Controller      string
	ServiceRegistry string
}

// LoadSettings reads flags and, for any flag left unset, the environment.
// The process reads the chain only. A static orchestrator list is not a setting.
func LoadSettings(args []string, getenv func(string) string) (Settings, error) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	fs := flag.NewFlagSet("livepeer-onchain-discovery", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	// Defaults shown in help are the public ones. Environment values are applied
	// only when the flag was not passed, so help cannot print an RPC URL.
	rpcFlag := fs.String("rpc-url", "", "Arbitrum JSON-RPC URL")
	controllerFlag := fs.String("controller", directory.ArbitrumController, "Controller contract address")
	registryFlag := fs.String("service-registry", "", "Service registry address, or controller")
	directoryFlag := fs.String("directory-interval", "1h", "How often to reload the chain directory")
	refreshFlag := fs.String("refresh-interval", "5m", "How often to probe each source")
	maxFailedFlag := fs.String("max-failed-refreshes", "6", "Consecutive failed probes before a source is evicted")
	listenFlag := fs.String("listen", "127.0.0.1:8939", "HTTP listen address")
	grantsFlag := fs.String("grants", "", "Comma-separated IP, IP:port, or CIDR grants for private sources")
	if err := fs.Parse(args); err != nil {
		return Settings{}, errorsWithoutValues(err)
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	rpc := chosen(set, "rpc-url", *rpcFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_RPC_URL"))
	controllerValue := chosen(set, "controller", *controllerFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_CONTROLLER"))
	registry := chosen(set, "service-registry", *registryFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_SERVICE_REGISTRY"))
	directoryInterval := chosen(set, "directory-interval", *directoryFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_DIRECTORY_INTERVAL"))
	refresh := chosen(set, "refresh-interval", *refreshFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_REFRESH_INTERVAL"))
	maxFailed := chosen(set, "max-failed-refreshes", *maxFailedFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_MAX_FAILED_REFRESHES"))
	listen := chosen(set, "listen", *listenFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_LISTEN"))
	grants := chosen(set, "grants", *grantsFlag, getenv("LIVEPEER_ONCHAIN_DISCOVERY_GRANTS"))
	directoryEvery, err := time.ParseDuration(directoryInterval)
	if err != nil || directoryEvery <= 0 {
		return Settings{}, fmt.Errorf("directory interval must be a positive duration")
	}
	refreshEvery, err := time.ParseDuration(refresh)
	if err != nil || refreshEvery <= 0 {
		return Settings{}, fmt.Errorf("refresh interval must be a positive duration")
	}
	failed, err := strconv.Atoi(strings.TrimSpace(maxFailed))
	if err != nil || failed < 0 {
		return Settings{}, fmt.Errorf("max failed refreshes must be a non-negative integer")
	}
	if _, _, err := net.SplitHostPort(strings.TrimSpace(listen)); err != nil {
		return Settings{}, fmt.Errorf("listen address must be host:port")
	}
	if strings.TrimSpace(rpc) == "" {
		return Settings{}, fmt.Errorf("RPC URL is required")
	}
	controllerAddr := strings.TrimSpace(controllerValue)
	if controllerAddr == "" {
		controllerAddr = directory.ArbitrumController
	}
	return Settings{
		Config: Config{
			Grants:          splitList(grants),
			RefreshInterval: refreshEvery, DirectoryInterval: directoryEvery, MaxFailedRefreshes: failed,
		},
		Listen: strings.TrimSpace(listen), RPCURL: strings.TrimSpace(rpc),
		Controller: controllerAddr, ServiceRegistry: strings.TrimSpace(registry),
	}, nil
}

// chosen uses the flag value when the flag was present and otherwise the
// environment. An empty environment value keeps the flag's public default.
func chosen(set map[string]bool, name, flagValue, envValue string) string {
	if set[name] {
		return flagValue
	}
	if strings.TrimSpace(envValue) != "" {
		return envValue
	}
	return flagValue
}

func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func errorsWithoutValues(err error) error {
	if err == nil || err == flag.ErrHelp {
		return err
	}
	return fmt.Errorf("invalid arguments")
}
