package serve

import (
	"strings"
	"testing"
	"time"
)

func TestSettingsUseTheChainDefaults(t *testing.T) {
	env := map[string]string{
		"LIVEPEER_ONCHAIN_DISCOVERY_RPC_URL":          "https://rpc.example/v2/secret",
		"LIVEPEER_ONCHAIN_DISCOVERY_SERVICE_REGISTRY": "controller",
	}
	settings, err := LoadSettings(nil, func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if settings.RPCURL != "https://rpc.example/v2/secret" || settings.Controller != "0xD8E8328501E9645d16Cf49539efC04f734606ee4" || settings.ServiceRegistry != "controller" {
		t.Fatalf("chain settings = %+v", settings)
	}
	if settings.Listen != "127.0.0.1:8939" || settings.Config.DirectoryInterval != time.Hour || settings.Config.RefreshInterval != 5*time.Minute || settings.Config.MaxFailedRefreshes != 6 {
		t.Fatalf("intervals = %+v listen %s", settings.Config, settings.Listen)
	}
}

func TestSettingsRequireAnRPCURL(t *testing.T) {
	_, err := LoadSettings(nil, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "RPC URL is required") {
		t.Fatalf("LoadSettings error = %v, want a missing RPC URL", err)
	}
}
