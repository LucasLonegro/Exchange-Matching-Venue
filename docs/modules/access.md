# access

Who may connect to the venue: teams (firms), their members (one order entry session each), API keys and
throttle tiers. A separate service with its own database tables; it is not part of the matcher, the cluster
or the gateways. Main class `io.lmb.access.AccessMain`, printing `LMB_READY access <instance>` once both
listeners are up.

The exchange never asks this service anything on the order path. Gateways (and the post-trade drop copy)
poll a read-only snapshot of the whole directory, check a key once at logon against their local copy, and
keep what the key allows in that session's state (docs/modules/gateway.md, "The grant"). If the service is
down, gateways keep trading on their last snapshot (also cached on disk, so a restarted gateway does not
need the service either); only changes wait. The risk limits of a firm (credit, position) stay with the
control plane and the matcher: `scripts/lmb-team register` writes both.

## Model

| Thing | What |
|---|---|
| firm | `firmId` (the venue's firm id, 1..2^32-2), name, tier, enabled |
| session | `sessionId` (venue-wide unique), the firm it belongs to, label (the member), own tier or the firm's, enabled |
| key | `lmb_<keyId>_<secret>`: keyId 12 hex characters (public: listings, logs, audit), secret 32 random bytes URL-safe base64. Bound to one session or firm-wide (`sessionId` 0), scopes, optional expiry, revocation time |
| tier | order bucket (rate/s, burst: new orders and replaces) and message bucket (rate/s, burst: every request), per session |
| audit | every change: time, operator, action, detail (never a key) |

Only the SHA-256 of a key is stored. With a 256-bit random secret a slow password hash adds nothing: the
hash only has to make a leaked table useless. A key is shown once, in the response that created it.

Scopes: `TRADE` (new orders, replaces), `CANCEL` (cancels, mass cancels; implied by TRADE), `QUERY`
(`OrderQuery` with `x-lmb-api-key`), `DROP_COPY` (post-trade drop copy logons: any key of the firm with this scope). A key with
`CANCEL` alone is a panic key: it can pull the team's orders but not add any.

Default tiers (created at bootstrap, changed with `PUT /api/tiers/{name}`):

| Tier | Orders /s, burst | Messages /s, burst |
|---|---|---|
| `restricted` | 10, 10 | 50, 50 |
| `standard` (default) | 500, 100 | 2000, 400 |
| `bot` | 5000, 1000 | 20000, 4000 |

**Rotation.** `POST /api/keys/{keyId}/rotate` issues a new key with the same binding and scopes and gives the
old one an expiry `graceMs` from now (default `rotationGraceMs`, 24 h; 0 = at once). Both work during the
grace period; at its end gateways drop connections still using the old key. **Revocation** (`DELETE
/api/keys/{keyId}`) takes effect at the next poll: about `pollMs + revalidateMs` on each gateway.

**Directory version.** One counter, bumped in the same transaction as every change. It is the ETag of the
snapshot, so an idle poll is a 304.

## Interfaces

**Operators' API** (`adminPort`, 9750+instance, bound to `127.0.0.1` by default: reach it over ssh or
`kubectl port-forward`). `Authorization: Bearer <token>`, checked against `operatorsFile` (one
`<name> <sha256 hex of token>` line per operator; the name is the audit actor) or, without it, the single
`adminToken`. JSON in and out; errors are `{"error": ...}` with 400 (bad input), 401, 404, 409 (conflict).

| Call | Body / query | |
|---|---|---|
| `GET /api/firms`, `GET /api/firms/{id}` | | firms with sessions and keys (no secrets) |
| `POST /api/firms` | `firmId`, `name`, `tier`?, `scopes`? (default TRADE, QUERY), `members`?: [{`sessionId`?, `label`, `tier`?}] | 201: the firm, each member with its session id (allocated from 10001 up if omitted) and its first key |
| `PUT /api/firms/{id}` | `name`?, `tier`?, `enabled`? | disabling ends every session of the firm |
| `POST /api/firms/{id}/sessions` | `sessionId`?, `label`, `tier`?, `scopes`?, `key`? (false: no key) | 201: a new member |
| `PUT /api/sessions/{id}` | `label`?, `tier`? (null: the firm's), `enabled`? | |
| `POST /api/firms/{id}/keys` | `sessionId`? (omitted: firm-wide), `scopes`, `label`?, `expiresInMs`? | 201: `apiKey`, shown once |
| `GET /api/keys` | `?firmId=` | key ids, bindings, scopes, state |
| `POST /api/keys/{keyId}/rotate` | `graceMs`? | 201: the new key |
| `DELETE /api/keys/{keyId}` | | revoke |
| `GET /api/tiers`, `PUT /api/tiers/{name}` | `orderRatePerSec`, `orderBurst`, `messageRatePerSec`, `messageBurst` | |
| `GET /api/audit` | `?limit=` (100) | newest first |

`scripts/lmb-team` wraps these (and the control plane's `POST /api/firms` for the risk limits); see
docs/operations.md, "Teams and keys".

**Directory** (`internalPort`, 9760+instance, all interfaces): `GET /internal/v1/directory` with
`Authorization: Bearer <serviceToken>` and optionally `If-None-Match`. 200 with the snapshot as JSON and an
`ETag`, or 304. The snapshot holds firms, sessions, key hashes with scopes and validity, and tiers: no
secret, but still a credential store's index, so the endpoint is closed to anything but gateways and
post-trade (k8s NetworkPolicy `lmb-access-ingress`).

Clients use `io.lmb.common.access.AccessDirectorySource` (common, a `PolledSnapshot`): a daemon poller that keeps the newest
snapshot in a volatile reference, writes it to `<dataDir>/access-directory.json`, starts from that file
when the service cannot be reached, and keeps the last good snapshot on any error.

## Bootstrap

On a database that was never bootstrapped: the default tiers, once (`access_meta.bootstrapped_ms`). No teams:
every team, session and key is registered through the operators' API (`scripts/lmb-team`). Local runs are
seeded by `scripts/local-seed`, which registers the local teams and writes the keys it was given to
`data/local/credentials.json` for the tools.

## Configuration

Topology keys, read through `Topology` (system property, environment variable, topology file, default).
Instance id `lmb.instance.id`; data under `<lmb.data.dir>/access/<instance>`.

| Key | Default | Meaning |
|---|---|---|
| `lmb.access.instances` | 1 | for monitoring targets |
| `lmb.access.<i>.adminPort` / `.internalPort` / `.metricsPort` | `lmb.access.adminPort` (9750) + i, 9760 + i, 9770 + i | |
| `lmb.access.adminBindHost` | `127.0.0.1` | the operators' API |
| `lmb.access.internalBindHost` / `.metricsBindHost` | `0.0.0.0` | |
| `lmb.access.db.url` / `.user` / `.password` | H2 file under the data dir / `sa` / empty | PostgreSQL in production; tables `access_*` |
| `lmb.access.operatorsFile` | unset | operator token hashes; unset: `adminToken` only |
| `lmb.access.adminToken` | `lmb-local-admin` | single operator token (local) |
| `lmb.access.serviceToken` | `lmb-local-access` | bearer token of the directory; gateways `lmb.gateway.access.token`, post-trade `lmb.posttrade.access.token` |
| `lmb.access.allowDefaultTokens` | true | false (production): refuse to start with a built-in token |
| `lmb.access.rotationGraceMs` | 86400000 | default grace of a rotation |

Several instances may run over the same database (they keep no state of their own besides a cached
snapshot); any of them serves the directory.

## Metrics (`/metrics` on `metricsPort`)

`lmb_access_up`, `lmb_access_directory_version`, `lmb_access_requests_total{action}`,
`lmb_access_directory_fetches_total{result=full|unchanged}`, `lmb_access_auth_failures_total{listener=operators|internal}`.
Alerts: `LmbAccessDown`, `LmbAccessServiceTokenRejected`, and on the gateways' side `LmbAccessDirectoryStale`,
`LmbGatewayAuthFailuresHigh`.

## Running and tests

`deploy/bin/lmb-service access <instance>`; `scripts/local-up.sh` starts `access-0` after the control plane and
then seeds it (`scripts/local-seed`). Gateways and post-trade do not start without `access.url`. Tests: `./mvnw -q -pl access test`
(`AccessServiceTest`: store, rotation, revocation, the HTTP API, ETag/304, the polling client and its disk
cache, refusal of default tokens).

## Known gaps

- Changes reach gateways by polling (2 s default): no push.
- Drop copy connections are checked at logon only; a revoked key's drop copy session stays up until it
  reconnects.
- The firm-wide rate is the sum of its sessions' buckets on every gateway; there is no firm bucket.
- The tools read their keys from the credentials file `scripts/local-seed` writes (`lmb.tools.credentials`).
- No self-service: teams get their keys from an operator.
