# livepeer-onchain-discovery

A discovery process for Livepeer AI orchestrators. It reads the bonded pool on
Arbitrum, keeps the orchestrators registered with an https service URI, and
serves their runner inventories at `GET /discovery`.

`livepeer discovery` in [livepeer/node](https://github.com/livepeer/node) polls
a static `Nodes` list from a TOML config
([config.example.toml](https://github.com/livepeer/node/blob/main/configs/discovery/config.example.toml)).
Its chain inspection reads the Controller's protocol `ServiceRegistry`, which
is the transcoding registry. This process uses the node's chain client and
discovery collector, and it reads the AI Service Registry instead. It does not
take a static list: a fixed orchestrator file is the node's job, and that
config is TOML, not a text file of URLs.

The inventory format is the one `livepeer/node` serves. A builder engine that
today uses `lpb discovery` can point `LPB_DISCOVERY_URL` here once this process
is the catalog that engine should pay. Removing `lpb discovery` is that later
cutover, not this repository.

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
directory is kept. The first failed load publishes nothing. A malformed chain
result fails the load rather than being read as "not registered".

Each registered source is probed at `<base>/discovery`. There is no static
list and no orchestrator file.

## HTTP

`GET /discovery` is livepeer/node's inventory. A `nodes` metadata entry lists
each source that has answered and when it was last queried. Runner objects are
the JSON the orchestrator sent. There is no app filter.

`GET /healthz` is 200 while the process is serving.

`GET /readyz` is 503 until a directory load has succeeded and one probe pass
has finished, then 200 while the refresh loops are running. `directory_error`
is a stable reason this process wrote (`rpc_http_429`, `rpc_error_-32000`,
`rpc_request_failed`, `malformed_result`, `contract_unresolved`), or null. It
is not the provider's error text.

Probing, TLS, the 1 MiB limit, eviction, and duplicate-runner merge are the
node collector's: 5 at a time, a 3s timeout, no redirects, and the system's
trust store. Private addresses are refused unless granted. The cache is memory
only and is dropped on exit.

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
| `--grants` | `GRANTS` | unset. Comma-separated IP, IP:port, or CIDR for private sources |

## Known limit

The collector drops a runner with no `observed_at`, or one more than 30
seconds in the future. Orchestrators that still speak the v1.0.2 inventory,
which does not stamp `observed_at`, do not appear. That is the same limit as
`livepeer discovery`. This process does not reimplement probing to keep those
runners. Current stacks that depend on v1.0.2 inventories should stay on
`lpb discovery` until the orchestrators they pay stamp `observed_at`.

## Decisions

- One process, and the existing `GET /discovery` contract. The engine is
  unchanged.
- The node's collector does the probing. This repository decides the source
  list from the chain and serves the collector's cache. `discovery.Server`
  keeps its list unexported, so the list is applied with `Collector.SetSources`.
  A static `Nodes` list stays in livepeer/node's TOML config.
- The AI Service Registry is the default because that is where AI orchestrators
  register, and the Controller does not know that contract.
- Health and discovery share one listener so a check of port 8939 sees
  readiness. The node's own discovery puts `/readyz` on a second port.
