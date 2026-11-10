# livepeer-onchain-discovery

Turns the Livepeer chain into a discovery source. It reads the bonded pool on
Arbitrum, keeps the orchestrators registered with an `https` URI in the AI
Service Registry, probes each one, and serves their runners at `GET /discovery`
in livepeer/node's contract ([schema](https://github.com/livepeer/node/blob/f45d83fd68f73d195dffe8b45f6cac296d023c49/docs/discovery.schema.json)).
That is all it does: no static list and no filtering beyond what
[What each orchestrator may advertise](#what-each-orchestrator-may-advertise)
describes.

## Combining it with other sources

livepeer/node's `livepeer-discovery` combines sources. It takes a static `Nodes`
list, and each entry may be an orchestrator or another discovery service. To
serve the chain together with a curated list, run node's discovery in front and
list this service among its `Nodes`; consumers read node's discovery. This
service's runners pass through unchanged, and when two sources list the same
runner, the newer stamp wins.

That arrangement needs two settings:

- **`Grants` in node's discovery.** Node refuses loopback, private and
  link-local addresses unless they are granted, so when this service sits on a
  private network, its address or subnet goes in node's `Grants`.
- **Short intervals on both.** A runner's stamp ages in both caches. At the
  defaults (5 minutes here and 5 minutes in node) it can pass 10 minutes before a
  consumer reads it, and livepeer-builder-core marks a rate stale after 600s.
  Use `--refresh-interval 30s` here and `RefreshInterval = "30s"` in node.

## Run

The module requires the Go toolchain livepeer/node declares at the pin below
(Go 1.27.3). `GOTOOLCHAIN=auto` downloads it.

```sh
go test ./...
go build -o livepeer-onchain-discovery ./cmd/livepeer-onchain-discovery

export LIVEPEER_ONCHAIN_DISCOVERY_RPC_URL='https://arb-mainnet.example/rpc'
./livepeer-onchain-discovery
```

The RPC URL usually carries a provider key. It is not written to the log, to
`/readyz`, or to `--help`.

CI runs `go mod tidy -diff`, `go vet ./...` and `go test -race ./...` on every
pull request and every push to `main`. The schema tests serve real inventories
through the service and validate `/discovery` against `docs/discovery.schema.json`
from the pinned livepeer/node module, with `santhosh-tekuri/jsonschema/v6` v6.0.3
and formats asserted, as node's own `discovery/schema_test.go` does.

`GET /healthz`, `GET /readyz`, and `GET /discovery` share `127.0.0.1:8939`,
which is the port the current stacks health-check.

## livepeer/node pin

`go.mod` pins livepeer/node at `f45d83fd68f73d195dffe8b45f6cac296d023c49`
(`v1.0.7-0.20261105031836-f45d83fd68f7`), the commit that publishes
`docs/discovery.schema.json` ([livepeer/node#49](https://github.com/livepeer/node/pull/49)).
`/discovery` serves that schema's contract, and the tests validate it against the
schema file in that module. The pin moves together with livepeer-builder-core's
`docs/references/upstream-baseline.md`, which watches node for schema changes;
this repository has no watcher of its own.

## Who is eligible

On each directory load the process looks up `BondingManager` in the Controller
(`0xD8E8328501E9645d16Cf49539efC04f734606ee4`) and walks
`getFirstTranscoderInPool` / `getNextTranscoderInPool`. The walk stops at the
reported pool size, at 1000, and at a repeated address.

An orchestrator is kept when `isActiveTranscoder` is true and `getServiceURI`
is a non-empty `https` base URL. The default registry is the AI Service
Registry `0x04C0b249740175999E5BF5c9ac1dA92431EF34C5`. The Controller does not
register that contract, so this is an override of the protocol
`ServiceRegistry`. `--service-registry controller` reads the protocol registry
instead. Any other address is used as that registry, with the service-registry
ABI.

These registrations are not kept:

- an inactive orchestrator, or an empty URI
- a URI that is not `https`, or that has user info, a query, or a fragment
- a network-modules manifest, a URI ending in `/.well-known/livepeer-registry.json`

One orchestrator whose own call reverts is skipped. A failure of the RPC
itself (HTTP 429 or 5xx, JSON-RPC `-32005` or `-32603`, or a transport
failure) is retried after 0.5s and 2s, then the load fails and the previous
directory is kept. A failed first load is retried after 30s, doubling up to the
directory interval, and nothing is published until a load succeeds. A malformed
chain result fails the load rather than being read as "not registered".

Each registered source is probed at `<base>/discovery`. There is no static
list and no orchestrator file.

## HTTP

`GET /discovery` is livepeer/node's inventory. A `nodes` metadata entry lists
each source that has answered and when it was last queried. Runner objects are
the JSON the orchestrator sent. There is no app filter.

Until the first directory load and the first complete probe round have finished,
`/discovery` answers 503, not an empty inventory. A consumer, livepeer/node's
`livepeer-discovery` or an engine, keeps what it had after a non-200, while `[]`
would tell it the chain has no runners. From then on it answers 200, including a
genuine `[]`.

`GET /healthz` is 200 while the process is serving.

`GET /readyz` is 503 until a directory load has succeeded and one complete probe
round over every source has finished, then 200 while the refresh loops are
running. There is no quorum: `orchestrators` and `serving` report how many
sources are registered and how many answered, for alerting. `directory_error`
is a stable reason this process wrote (`rpc_http_429`, `rpc_error_-32000`,
`rpc_request_failed`, `malformed_result`, `contract_unresolved`), or null. It
is not the provider's error text.

Probing, TLS, the 1 MiB limit, eviction, and duplicate-runner merge are the
node collector's: 5 at a time, a 3s timeout, no redirects, and the system's
trust store. Private addresses are refused unless granted. The cache is memory
only and is dropped on exit.

## What each orchestrator may advertise

Each registered orchestrator speaks only for itself. An entry whose `address` is
not the probed orchestrator's registered base URL is dropped and logged as
`discovery.foreign_entry_dropped`, with the orchestrator's on-chain address.
Scheme, host, port and path are compared; a default port or a trailing slash
does not matter. A stock livepeer/node orchestrator always reports its own
service address.

A runner URL that more than one orchestrator advertises is kept from each of
them, and the node collector's merge serves the newest record. It is logged as
`discovery.runner_url_contested`, with every orchestrator's on-chain address.
The contract cannot say who owns a runner URL, which may sit on a proxy host.

Each finding is logged once, when it first appears. `/readyz` reports the last
probe round's counts as `dropped_foreign_entries` and `contested_runner_urls`.

There is no allowlist, and no rule on a runner's `metadata`: the chain's
admission bar above is the only one. A curated list belongs in livepeer/node's
static `Nodes` configuration.

## Settings

Flags override the environment when the flag is present. Names use the prefix
`LIVEPEER_ONCHAIN_DISCOVERY_`.

| Flag | Environment suffix | Default |
| --- | --- | --- |
| `--rpc-url` | `RPC_URL` | unset. Required for the chain directory |
| `--controller` | `CONTROLLER` | `0xD8E8328501E9645d16Cf49539efC04f734606ee4` |
| `--service-registry` | `SERVICE_REGISTRY` | the AI Service Registry. `controller` reads the protocol registry |
| `--directory-interval` | `DIRECTORY_INTERVAL` | `1h` |
| `--refresh-interval` | `REFRESH_INTERVAL` | `5m` |
| `--max-failed-refreshes` | `MAX_FAILED_REFRESHES` | `6`. `0` evicts on the first failed probe |
| `--listen` | `LISTEN` | `127.0.0.1:8939` |
| `--grants` | `GRANTS` | unset. Comma-separated IP, IP:port, or CIDR for private sources. Leave unset in production: anyone registered on chain chooses their own URI |

## Known limit

The collector drops a runner with no `observed_at`, or one more than 30 seconds
in the future. Orchestrators on livepeer/node v1.0.2 do not stamp `observed_at`,
so their runners do not appear until they run v1.0.4 or later; they then appear
with no change here. That is the same limit as node's `livepeer-discovery`.

## Decisions

- One process, and livepeer/node's `GET /discovery` contract. A consumer
  cannot tell this service from node's discovery.
- The node's collector does the probing. This repository decides the source
  list from the chain and serves the collector's cache. `discovery.Server`
  keeps its list unexported, so the list is applied with `Collector.SetSources`.
  A static `Nodes` list stays in livepeer/node's TOML config, and node's
  discovery combines sources.
- The AI Service Registry is the default because that is where AI orchestrators
  register, and the Controller does not know that contract.
- Health and discovery share one listener so a check of port 8939 sees
  readiness. The node's own discovery puts `/readyz` on a second port.
