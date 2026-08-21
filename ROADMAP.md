# FluxMQ Roadmap to 1.0

**Current:** `v0.51.0`
**Last updated:** 2026-08-21
**State:** five pull requests merged (#576–#580); nothing outstanding on a branch
**Companion document:** [`V1-READINESS.md`](./V1-READINESS.md) — findings, evidence, file:line references.

This roadmap exists because the repository had no committed plan. `plan.md` was
deleted in `5101c90f2` and the three design documents that followed it were
deleted over the next three weeks; release scope has since lived only in
untracked working-tree files. This document is the tracked replacement.

**This is the only release plan.** `v1.md` — a second, independently written
v1 plan — was folded into this document on 2026-08-21 and deleted. See [Reconciliation with `v1.md`](#reconciliation-with-v1md)
for what was imported, what it had already delivered, and why the two
estimates differed.

---

## The 1.0 contract

Tagging 1.0 freezes three things. Everything in Milestone 1 exists because it
is expensive or impossible to change afterward:

1. **`proto/cluster/v1` and `proto/auth/v1`** — wire contracts shared with
   `atom` (vendored) and `magistrala` (go.mod). Breaking these post-1.0 means a
   coordinated three-repository migration.
2. **The `broker.Authorizer` / `broker.Authenticator` interfaces** — adding
   `context.Context` is a breaking signature change.
3. **The YAML configuration schema** — key names, and the absent-vs-zero
   semantics of every limit.
4. **The queue delivery address** — what a consumer receives and how it
   recovers the source topic. `9cceb7335` settled it *for protocols that carry
   message properties*: the address identifies the queue, and the origin travels
   in the broker-owned `types.PropSourceTopic` property
   (`queue/delivery_engine.go:624`). **MQTT 3.1.1 has no property field**
   (`mqtt/session/encode.go:57-68`), so a 3.1.1 consumer of a captured message
   receives an address that is deliberately not injective and no way to recover
   the origin. Decide before the tag whether that is the contract or a gap.

   **Resolved 2026-08-21, merged to `main` as `4046c0b69` (#580).** It went in
   ahead of this branch on purpose: the code depended on nothing here, and this
   branch is gated behind pinning magistrala off the `latest` image tag. the broker settles a
   3.1.1 consumer's queue message on PUBACK/PUBCOMP, using identifiers it
   stamped itself, so 3.1.1 consumes classic queues at QoS 1 or 2. QoS 0
   subscriptions to a classic queue are refused. Origin recovery for a captured
   message still needs MQTT 5.0 or AMQP, and that is now stated in the support
   matrix rather than implied. What follows is why the address was not changed
   instead.

   **Making the address injective does not work.** Delivering every capture as
   `$queue/<queue>/<source>` unconditionally leaves the collision intact: a
   capture of `acme/temp` into queue `m` still renders as `$queue/m/acme/temp`,
   which is exactly what an explicit publish to that address renders as.
   Separating them needs an escape or a marker level imposed on every protocol
   to serve the one that cannot read properties. **Recommendation: qualify the
   claim in the support matrix — a 3.1.1 consumer of a *captured* message gets
   the queue identity only — rather than complicate the wire.** Explicit queue
   publishes are unaffected everywhere; their address is already canonical.
5. **The admin API surface and its authentication model** — route paths, role
   names, and where the token comes from. Adding auth after 1.0 breaks every
   deployment that was relying on its absence.

Anything that is *only* a bug fix or a performance improvement can land after
1.0. Anything that changes one of the five above cannot.

### What 1.0 supports

The tag makes a support claim, not just a version bump. Ship the matrix below
in the release notes and in `README.md`; today's README markets features this
list qualifies.

| Area                                     | 1.0 status                                                  |
| ---------------------------------------- | ----------------------------------------------------------- |
| MQTT 5.0                                 | Production-supported                                        |
| MQTT 3.1.1                               | Production-supported. Consumes classic queues at QoS 1/2, settling on PUBACK — no explicit nack/reject, and no origin recovery for captured messages |
| AMQP 0.9.1 / 1.0                         | Production-supported, **documented subset** — not every operation |
| Durable queues, single node              | Production-supported                                        |
| Broker clustering (etcd coordination)    | Production-supported, secure by default                     |
| Consumer groups                          | Supported; **beta** unless Milestone 3's `queue/consumer` suite lands first |
| Queue Raft replication                   | Experimental, off by default, outside the compatibility contract |
| HTTP and CoAP bridges                    | Experimental, off by default                                |
| Performance numbers                      | Only figures reproduced on the Milestone 5 reference profiles |

No FIPS certification, no multi-user dashboard login, no external identity
provider: operators provision file-backed admin tokens. Existing queue storage
must upgrade without data loss; pre-1.0 *configuration* is replaced outright,
with no aliases and no migration tool — strict decode (1.4) is what makes that
safe.

---

## Progress log

### 2026-08-21 — what merged

| PR | Roadmap items | What it changed |
| --- | --- | --- |
| #576 | 1.4, 1.4b | Strict config decode, missing file is an error, MQTT listeners under `server.mqtt`, key-set and shipped/documented-config guards |
| #577 | 1.2 | `context.Context` through both authorization interfaces, sourced from the connection |
| #578 | 1.5, 1.11 | Queue acknowledgement durability per queue, AMQP 1.0 handshake bound, sync failures made sticky |
| #579 | 1.5b | Durability barriers coalesced into one fsync |
| #580 | — | MQTT 3.1.1 settles queue messages on PUBACK |

Milestone 1 is five items lighter. What remains is 1.1, 1.3, 1.6, 1.7, 1.8, 1.9
and 1.10 — see [Next](#next).

**Three defects were found by building the fixes, not by the audit** that
produced `V1-READINESS.md`: MQTT 3.1.1 silently losing queue work, a rotated
segment nothing ever fsynced, and the AMQP 1.0 TLS handshake blocking the accept
loop. None was visible to a reading pass. That is the case for starting 1.8
early rather than saving it for the end.

**Two review lessons worth keeping**, both from tests that passed when they
should not have:

- A cancellation test satisfied by any fast failure, not just cancellation, went
  green against deliberately unfixed code. A test for "gives up early" needs a
  peer that never answers on its own.
- A durability benchmark that measured nothing, because `b.TempDir()` follows
  `TMPDIR` and `/tmp` is tmpfs, where fsync is free. Any benchmark claiming to
  measure durability states its filesystem.

### 2026-08-20 / 21 — #576, strict configuration

`make test` green across 63 packages, `make lint` at 0 issues.

```
a334ad17f  Give zero one meaning across the whole document
e3b4c3a25  Name the storage fsync key for the engine it configures
b3706a4bc  Validate listen addresses instead of deferring to bind
a8d2eb5c7  refactor(config): keep the filesystem out of configuration validation
9cceb7335  feat(queue): carry the source topic in queue deliveries
70c4b41b5  Make config loading strict and move MQTT listeners under server.mqtt
```

Companion commit in magistrala, branch `fluxmq-server-mqtt`:
`711da6912  NOISSUE - Move FluxMQ MQTT listeners under server.mqtt`.

> **Blocking precondition for merging this branch, raised in review.** Every
> push to FluxMQ `main` publishes `ghcr.io/absmach/fluxmq:latest`
> (`.github/workflows/build.yml:4-7`), magistrala's `docker/.env` consumes that
> tag, and its three node configs still write the removed `server.tcp` /
> `server.websocket` keys. Merging first means all three brokers fail strict
> decoding and restart-loop. `711da6912` exists only locally — no remote branch,
> no PR.
>
> Order that works: pin magistrala to the last released tag → merge and publish
> FluxMQ → update magistrala's configs → unpin. The load path now names the
> replacement (`server.tcp is no longer supported; use server.mqtt.tcp`) instead
> of reporting an unknown field, which makes the failure legible — **it does not
> make it survivable.** Sequencing is the fix.

**Decision taken:** the listener schema is `server.mqtt.tcp.v3`, not the
unmerged `config` branch's `listeners.mqtt[]` document model. The `config`
branch's v1 model is therefore **not** being adopted; its design-independent
improvements were ported onto main instead (commits 2–6 above).

**Ported from the `config` branch:** queue source topic; filesystem kept out of
config validation; listen-address shape validation; the `badger_sync_writes`
rename; absent-vs-zero semantics; `v1.md` — which has since been folded into
this document and deleted.

**Deliberately not ported:**

- `622b8ab97` and the commits layered on it — the `config/v1.go` document model,
  superseded by the decision above.
- `6d055c8d9` cluster membership docs — its text documents v1-only keys
  (`cluster.members`, `cluster.ports.etcd_peer`, `cluster.allow_insecure`,
  `experimental.queue_raft`) that do not exist on main. The *fact* it states may
  still hold; restating it against main's cluster schema is unported work.
- The `config validate` subcommand and its `make validate-configs` target, which
  depend on the v1 CLI and v1 example filenames.

**Open, found along the way:**

- Three `/usr/local/bin/fluxmq -config /etc/fluxmq/config.yaml` processes are
  running on the development host against a path that does not exist. Under the
  pre-fix behaviour that means they are on built-in defaults: no auth, no TLS,
  plaintext `:1883`/`:1884`/`:8083`/`:8084`, admin API `:8082`. Not touched.
  Worth confirming whether that is a throwaway instance.
- `./cluster` took 294s standalone and failed once at 149s before passing on
  rerun. `make test` allows 3m, so this package is close enough to the limit to
  produce intermittent CI failures. Candidate for milestone 1.8.

### 2026-08-21 — #576, documentation fixes found in review

Strict decoding (1.4) turned every stale documentation example into a broker
that refuses to start. Sweeping all 29 fenced yaml blocks under `docs/` and
`README.md` through `config.Load` found five that no longer load:

| Page | Problem |
| --- | --- |
| `configuration/server.md` | `server.mqtt.websocket.plain` — the page documenting the listener rename kept the removed slot |
| `messaging/consumer-groups.md` | five `queue_manager` keys that have never existed in the schema |
| `configuration/security.md` | exact publish target without `cluster.enabled: false`, which the same page's prose calls a startup error |
| `deployment/internal-amqp-local-principals.md` | the deliberate "removed form" example, now marked as such |
| `reference/configuration-reference.md` | one section quoted in isolation, now marked as such |

`TestDocumentedConfigsLoad` (`config/schema_test.go`) closes the gap that let
these through: `TestShippedConfigsDecodeStrictly` covers `examples/` and
`deployments/` but never looked at documentation. It guards 27 blocks, treats a
block as broker configuration when any top-level key is a config key, and takes
an explicit `<!-- fluxmq:config-skip: reason -->` marker for blocks that must
not load.

### 2026-08-21 — #577, authorization context

`context.Context` now reaches both authorization interfaces from the connection
that triggered the decision. `make test` green, `make lint` at 0 issues. Details
and the tests that pin it are under [1.2](#12-plumb-contextcontext-through-the-authorization-interfaces--done-2026-08-21).

Two things worth carrying forward from doing it:

- **`contextcheck` earns its place.** Adding a context in scope made the linter
  point at every downstream call still inventing its own, which is how the
  `NotifyConnect` and offline-delivery sites were found rather than guessed at.
- **A cancellation test can pass for the wrong reason.** The first version
  asserted only that the call returned quickly, which a fast unrelated failure
  also satisfies; it passed against deliberately unfixed code. The rule that
  came out of it: a test for "gives up early" needs a peer that never answers on
  its own, so early return has exactly one explanation.

### 2026-08-21 — plan reconciliation, no code changes

`v1.md` folded into this document and deleted; the `config` branch parked. Both
of the previous session's open planning items are closed. Every finding
imported from `v1.md` was re-verified against the tree before being
written down — the table in the reconciliation section records which ones
survived.

**`config` branch: parked, not deleted.** 15 ahead / 18 behind main. Its
listener model (`config/v1.go`, `listeners.mqtt[]`) is superseded by the
`server.mqtt` decision above, so it will never merge as-is; but three of its
commits carry design-independent ideas worth re-implementing against main's
schema, and the branch is the only record of them:

| Commit      | Idea                                                | Verdict                                                        |
| ----------- | --------------------------------------------------- | -------------------------------------------------------------- |
| `eb98910a9` | Generate the configuration reference from the schema | **Salvage** → Milestone 4. Kills reference-vs-code drift permanently. |
| `44757534d` | Derive telemetry export from the signals it carries  | **Salvage** → Milestone 2, S. Applies to main's telemetry block unchanged. |
| `6d055c8d9` | Membership is fixed once a node holds data           | **Salvage as documentation** → Milestone 1.6, restated against main's cluster keys. |
| `622b8ab97` + 9 layered commits | The `config/v1.go` document model  | Dead. Superseded.                                              |

Do not delete the branch until those three land on main; the tombstone is this
table. It is pushed to `origin`, so nothing is at risk in the meantime.

### Next

Seven Milestone 1 items remain. Only one pair is serial.

1. **1.9 — authenticate the admin API.** `[L · 5–7d]` The worst finding in
   `V1-READINESS.md` and the only one an outsider can reach without
   credentials: anyone who can open `:8082` can call `DeleteQueue` and
   `reload`, and the default Compose stack publishes that port. Route paths and
   role names freeze at the tag, so it cannot be deferred past 1.0 the way a bug
   fix can.
2. **1.3 — cache authorization decisions.** `[L · 4–5d]` What is left of the
   critical path now that 1.2 has merged. Until it lands, a callout-configured
   broker still performs one synchronous round-trip per published message; the
   context added in #577 makes that cancellable, not cheap.
3. **1.8 — second audit pass.** `[L · 5–8d]` Run it in parallel from day one.
   Three of the defects fixed this week were found by building, not by reading,
   which is exactly what this pass is for and exactly why its findings arrive
   late.
4. **1.10 — DLQ and replication failure-safety.** `[M · 4–6d]` Completes the
   durability work: an acknowledged publish now reaches disk, but the consumer
   path still drops the pending entry whether or not the dead-letter append
   succeeded.

1.1, 1.6 and 1.7 are genuinely parallel and get no cheaper or dearer by
waiting.

**Also open, cheap, unclaimed:** `badger_sync_writes: false` in the three
cluster reference deployments — the shipped cluster example is non-durable —
and an additive `proto/cluster/v1` field so inflight message properties survive
a takeover, without which a settled MQTT 3.1.1 delivery redelivers after a node
move (#580).

**Working agreements** that held up across the five merges, worth keeping:

- One branch per item, off the base it needs, never committed onto a branch
  already under review.
- A test that passes against deliberately broken code is rewritten or deleted,
  not kept as reassurance. Four were caught that way.
- Benchmarks that claim to measure durability state their filesystem.

---

## Reconciliation with `v1.md`

`v1.md` was a second, independently written v1 plan, committed on this branch
at 209 lines. It estimated 54–81 engineer-days against this roadmap's 31–44,
and the gap was scope, not disagreement: it costed admin RBAC, fuzzing,
interoperability testing, reference performance profiles, release-pipeline
work, and a 72-hour soak, none of which this document had. Those are now
Milestones 1.9–1.11, 3, 4 and 5 here.

Where the two overlapped, they agreed. `v1.md`'s Phase 0 (strict versioned
config) is this roadmap's 1.4, delivered 2026-08-20 — against `server.mqtt`
rather than its `listeners:` model, which is the one place the two documents
genuinely differed and where the decision has already been taken and shipped.

**Delivered before the merge, per `v1.md` and confirmed in the tree:**

| Item                            | Evidence                                                       |
| ------------------------------- | -------------------------------------------------------------- |
| Capture off the publish hot path | `queue/capture.go`, bounded per-queue lanes, `queues.capture_dropped` |
| Topic matching by trie           | `queue/storage/patterntrie.go` — flat 37–325 ns from 8 to 8192 patterns |
| Queue delivery address settled — for protocols with properties | `types.PropSourceTopic`, `queue/delivery_engine.go:624`. MQTT 3.1.1 cannot carry it |
| AMQP 0.9.1 handshake deadline    | `server/amqp/server.go:155` — AMQP 1.0 still unbounded, now 1.11 |

Two follow-ups ride along with those: bound the capture backlog by bytes rather
than job count (Milestone 2), and revalidate the trie numbers on the Milestone 5
reference profiles rather than a review workstation.

**Re-verified on 2026-08-21 before import** — every row below was read in place
against the tree, not taken from `v1.md`:

| `v1.md` finding                                | Verdict    | Where it went |
| ---------------------------------------------- | ---------- | ------------- |
| Admin/Connect API has no authentication         | Holds      | 1.9           |
| Ordinary durable publish is buffered            | Holds      | 1.5           |
| DLQ removes the PEL entry regardless of outcome | Holds      | 1.10          |
| Replication fails open                          | Holds, narrowed — a `WritePolicy` switch now exists (`queue/manager.go:1025-1049`); the default `WritePolicyLocal` (`:204`) is the open path | 1.10 |
| etcd peer/client traffic is plaintext           | Holds      | 1.1 (Track A, widened) |
| AMQP 1.0 handshake is unbounded                 | Holds      | 1.11          |
| Images are pushed before they are scanned       | Holds      | 4             |
| `AsyncEventHook.Close` races enqueue            | Holds      | 2             |
| Readiness is liveness-only                      | **Stale** — `/ready` checks broker, storage `Ping`, and peer reachability (`server/health/server.go:178-254`). Rewritten as *extend* readiness, not add it | 2 |

`v1.md`'s release-acceptance list was merged into the Definition of done, and
its assumptions into the support matrix under "The 1.0 contract".

---

## Weights

| Class  | Range     |                                            |
| ------ | --------- | ------------------------------------------ |
| **S**  | ≤ 1 day   | one person, one sitting                    |
| **M**  | 2–4 days  | one person, under a week                   |
| **L**  | 1–2 weeks | needs a test harness or touches many files |
| **XL** | 3 weeks+  | its own project                            |

Person-days for one engineer already familiar with the codebase, including
tests and review. They do **not** include remediation for anything 1.8 finds.

| Milestone                     | Weight      | Serial          | 3 engineers   |
| ----------------------------- | ----------- | --------------- | ------------- |
| 1 — Contract freeze (7 items) | 29–42 d     | 6–9 weeks       | ~2.5 weeks    |
| 2 — Correctness & honesty     | 9–13 d      | ~2–3 weeks      | ~4 days       |
| 3A — Verification             | 11–15 d     | 2–3 weeks       | ~1 week       |
| 3B — Coverage (may slip)      | 15–25 d     | 3–5 weeks       | ~2 weeks      |
| 4 — Governance                | 3.5–6 d     | ~1 week         | ~2 days       |
| 5 — Baselines & operations    | 9–14 d      | 2–3 weeks       | ~1.5 weeks    |
| **To the tag (1+2+3A+4+5)**   | **62–90 d**  | **12–18 weeks** | **~5–6 weeks** |

Plus **7–11 days of reserve** for interoperability defects, performance
regressions, and whatever the soak turns up. Milestone 1.8 carries its own
unbounded remediation tail on top of that.

**This is larger than either source document claimed** — this roadmap said
31–44 days, `v1.md` said 54–81. Neither was wrong about its own scope; the
union of the two is simply bigger, and the merged number is the one to plan
against. The growth is concentrated in three places: the admin API has no
authentication at all (1.9), CI runs only the short race suite so verification
had been costed at zero (3A), and no performance claim in the README is
currently reproducible (5).

3B is listed separately because `queue/consumer` at XL is the one item that can
reasonably slip past 1.0 — see the note at the end of that section.

---

## Critical path

Only 1.2 → 1.3 are genuinely serial: they edit the same interface and the same
ten call sites, and doing them as one change avoids touching 113 test
occurrences twice. Everything else in Milestone 1 parallelizes.

```
1.2 Authorizer ctx  ✅ #577
        └──────────▶  1.3 authz cache            4–5 d   ◀── critical path
1.5 durability ✅ #578 ─▶ 1.10 DLQ + replication ─▶ 3A crash drills   6–9 d
1.5b group commit   ✅ #579
1.11 AMQP 1.0 handshake ✅ #578
1.4 strict config   ✅ #576
1.9 admin API auth          (parallel)            5–7 d
1.6 split-brain             (parallel)            4–6 d
1.7 CRL/OCSP                (parallel)            4–5 d
1.1 cluster transports      (parallel)          3.5–5 d
1.8 audit pass              (parallel, feeds back)  5–8 d
                                    │
                   everything above ▼
                          5 soak + baselines     ◀── final gate, 3 days elapsed
```

The second chain is new and is the only other genuine serial run: the
durability default (1.5) decides what "acknowledged" means, 1.10 makes the
failure paths honour it, and 3A's crash drills are what prove both. Doing them
out of order means writing the drills twice.

Milestone 5's soak gates the tag by construction — it runs on a release
candidate, so nothing else can be in flight during it.

With three engineers, Milestone 1 lands in about two weeks — but **1.8 is the
schedule risk**, not 1.2/1.3. It reviews nine unaudited areas including the
MQTT codec's DoS surface, and anything it finds arrives late and unscoped.
Start 1.8 on day one, in parallel, rather than treating it as a final gate.

### 1.2 — Authorizer context, measured

The raw counts look alarming and are misleading: 54 non-test and 113 test
occurrences of `CanPublish` / `CanSubscribe` / `Authenticate` across 14
non-test and 10 test files. The actual surface is much smaller.

**Implementations of the two-method `broker.Authorizer` — 9 total:**

|      | Type                     | File                                    |
| ---- | ------------------------ | --------------------------------------- |
| prod | `*AuthEngine`            | `broker/auth.go`                        |
| prod | `*Broker` (pass-through) | `mqtt/broker/broker.go:270,279`         |
| prod | `*HTTPClient`            | `broker/authcallout/http.go`            |
| prod | `*GRPCClient`            | `broker/authcallout/grpc.go`            |
| test | `*captureAuthorizer`     | `mqtt/broker/external_identity_test.go` |
| test | `*stubAuthorizer`        | `broker/auth_test.go`                   |
| test | `*localAuthorizerStub`   | `amqp/broker/policy_test.go`            |
| test | `*mockAuthz`             | `amqp1/broker/integration_test.go`      |
| test | `*reloadRaceLocalPolicy` | `server/amqp/server_test.go`            |

**Not affected:** `broker/localauth/store.go` and `cmd/main.go:140,155`
implement a *different* interface — `LocalPolicy` with `CanPublishLocal` /
`CanSubscribeLocal` (`amqp/broker/policy.go:127-128`), a different shape on the
AMQP local-auth path. Worth converting later for consistency; not on this
critical path.

**The ten call sites, by how much context work each needs:**

| Call site                           | `ctx` in scope?                              | Cost         |
| ----------------------------------- | -------------------------------------------- | ------------ |
| `server/http/server.go:364`         | `r.Context()`, already used at :346          | trivial      |
| `server/coap/server.go:277`         | `r.Context()`, already used at :259, :297    | trivial      |
| `amqp/broker/channel.go:641,1435`   | `Connection.ctx` exists (`connection.go:53`) | field access |
| `amqp1/broker/link.go:164,310`      | `Connection.ctx` exists (`connection.go:34`) | field access |
| `mqtt/broker/v3_handler.go:291,506` | **none** — `connCtx` has no ctx field        | see below    |
| `mqtt/broker/v5_handler.go:362,600` | **none** — same                              | see below    |

`connCtx` (`mqtt/broker/conn_context.go:26-30`) embeds `*session.Session` plus
`conn` and `epoch` — no context. Two ways to fix it:

- **Add a `ctx` field to `connCtx`.** It is constructed in exactly **one**
  production site (`mqtt/broker/lifecycle.go:36`) and 5 test sites. This
  matches what `amqp/broker` and `amqp1/broker` already do with
  `Connection.ctx`, so it is the codebase's own idiom. **Chosen.**
- Thread `ctx` as a parameter through `HandlePublish` / `HandleSubscribe` —
  changes the handler interface and every handler test. Rejected.

**So the work is:** 2 interface methods, 4 production implementations, 5 test
mocks, 1 struct field, 1 production construction site, 10 call sites of which 6
are one-liners. The bulk of the elapsed time is mechanical churn across the 113
test occurrences, which is why this is M and not S.

### 1.3 — Authorization cache, measured

`broker/identitycache.go` is 125 LOC with 120 LOC of tests, and is the shape to
copy. A decision cache keyed `(identity, topic, action)` instead of a single
string lands around 150–200 LOC plus ~180 LOC of tests.

| Piece                                                                                    | Days  |
| ---------------------------------------------------------------------------------------- | ----- |
| Cache implementation + unit tests                                                        | 1.5–2 |
| Config surface (size, TTL, negative TTL) + defaults + validation + `schema_test.go` line | 0.5   |
| Invalidation on reload and on `auth.local_principals` change                             | 1     |
| Benchmark: publish throughput with authz on, before/after, `b.ReportAllocs()`            | 1     |
| Drop the per-authorize `slog.Info` to `Debug`, verify no per-message logging remains     | 0.5   |

Negative caching needs its own TTL, shorter than the positive one, so that a
newly granted permission takes effect promptly while a denial still absorbs a
flood. That interaction is where the correctness risk sits, not in the LRU.

**Dependency note:** 1.2 shipped alone on 2026-08-21, which is the cheap
direction to split: all the churn is in 1.2's call sites, and the cache is
additive on top of an interface that already carries a context, so it needs no
second pass over them. The reverse split would not have been: caching against a
context-free interface leaves every cold key stalling uncancellably.

**What that leaves.** P0-1 is open until 1.3 lands. A cache miss is now
cancellable, so a disconnecting client releases its callout — but a
callout-configured broker still performs one synchronous round-trip per
published message.

---

## Milestone 1 — Contract freeze (blocks the tag)

Ordered by "cannot be changed after 1.0" first, then by risk.

### 1.1 Secure the cluster transports; delete the stub RPCs from the proto

**Weight: M · 3.5–5 days** — 1.5–2 for Raft and the proto, 2–3 for etcd.

**This is two tracks, not one.** Separating them is what makes the work small:

- **Track A — `queue/raft/`, queue log replication.** hashicorp/raft v1.7.3,
  2,771 non-test LOC against 1,506 test LOC, started from `cmd/main.go:974`
  and configured through `config.RaftConfig`. Wired and working. Its only
  blocker is the cleartext transport at `queue/raft/manager.go:166`.
- **Track B — replacing etcd as the coordination layer.** The stub RPCs at
  `cluster/transport.go:678,687,696` are scaffolding for a deleted 20-week
  design document. Post-1.0 regardless. (For the record: "custom Raft" in that
  document meant a custom *coordination layer built on* hashicorp/raft, never
  hand-written consensus — the question of implementing Raft ourselves was
  never open.)

Options, given that split:

|            | A. TLS the existing transport                                                                                   | B. gRPC transport now                                                                                               | C. Cut Track A                                                             |
| ---------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Work       | `raft.NewNetworkTransport` with a TLS `StreamLayer` (the Consul pattern); delete the 3 stub RPCs from the proto | Fix the proto (`stream InstallSnapshot`, add `TimeoutNow`), implement `raft.Transport` over the mTLS broker channel | Remove the RPCs, drop `queue/raft` from the build, delete the config block |
| Cost       | ~1 day                                                                                                          | ~1–2 weeks                                                                                                          | ~1 day                                                                     |
| Closes     | P0-2, P0-3                                                                                                      | P0-2, P0-3, and drops a port from the deployment surface                                                            | P0-2, P0-3                                                                 |
| Cost after | B stays available as a non-breaking 1.1 item                                                                    | —                                                                                                                   | Discards wired, tested code                                                |

**Recommendation: A.** B is the right end state, but it is proto surgery on the
critical path to a tag. A ships in a day and leaves B as a clean 1.1 item. C
throws away working code.

Either way, **the three stub RPCs come out of `proto/cluster/v1`.** Nothing
calls them; the shipped versions could not carry a real `raft.Transport` anyway
(unary `InstallSnapshot`, no `TimeoutNow`); and re-adding RPCs later is
additive and non-breaking, while removing them after 1.0 is not.

Route the `os.Stderr` writer at `manager.go:166` into `slog` at the same time.

**Track A′ — etcd peer and client traffic.** *Imported from `v1.md`; verified
2026-08-21.* Securing Raft alone leaves the coordination layer in the clear:
`cluster/etcd.go:39` hardcodes `urlPrefix = "http://"` and every peer URL is
built from it (`:174`), so session ownership, subscription routing, and queue
consumer state cross the network unauthenticated and unencrypted on a cluster
that has broker mTLS configured. That is a worse exposure than the Raft
transport, because clustering is a supported 1.0 feature and queue Raft is not.

- Feed the existing cluster TLS material to embedded etcd's peer and client
  transports; keep the etcd client listener on loopback as an implementation
  detail.
- One identity for both etcd peer traffic and broker transport traffic. Two
  certificate configurations for one trust domain is a footgun.
- **Non-TLS clustering fails to start** unless an explicit
  development-only insecure opt-in is set. Default-secure is the whole point;
  an opt-out that nobody sets in production is the only acceptable escape.
- Schema change, so it lands before the tag with the key named in
  `config/schema_test.go`.

### 1.2 Plumb `context.Context` through the authorization interfaces — ✅ DONE 2026-08-21

**Weight: M · 2–3 days — landed in one sitting**, against the estimate. The
measurement in the critical-path breakdown held: 2 interfaces, 4 production
implementations, 6 test mocks, 1 struct field, 10 production call sites, and 50
test call sites, all mechanical.

**What shipped**

| Change | Where |
| --- | --- |
| `Authenticator` and `Authorizer` take a context | `broker/auth.go` |
| Callouts derive their per-attempt deadline from the caller's context | `broker/authcallout/{http,grpc}.go` |
| `connCtx` carries a context canceled with that connection | `mqtt/broker/connection.go`, `conn_context.go`, `lifecycle.go` |
| `HandleConnect` and `runSession` take a context | `mqtt/broker/handler.go`, `v3_handler.go`, `v5_handler.go` |
| Two `//nolint:contextcheck` suppressions deleted | `mqtt/broker/connection.go` |
| `NotifyConnect` and `deliverOfflineMessages` take a context | `mqtt/broker/broker.go`, both handlers |
| Superseded-connection drain inherits the connection's values | `context.WithoutCancel(ctx)` in both handlers |

The AMQP call sites already had `Connection.ctx` in scope. AMQP 1.0 already
derived it per connection; AMQP 0.9.1 now does the same, so closing one client
does not cancel another client's work.

**Tests.** Nine cover the context and cancellation contract:
`TestPublishCarriesConnectionContextToAuthorizer` and its subscribe twin assert
the authorizer receives the *connection's* context. Generation-scoping tests
assert that closing one MQTT or AMQP 0.9.1 connection cancels only its own
context. The MQTT lifecycle test closes the session's real stored connection
while authorization is blocked and proves both the call and connection handler
exit. Three callout cancellation tests assert that HTTP and Connect clients
abandon a request when the caller walks away instead of waiting out a 30s
timeout and a 10s backoff. A first version of those passed against the unfixed
code because a fast unrelated failure looks like a fast cancellation; they now
run against a server that answers only when the caller gives up. The final test
proves repeated caller cancellations do not open the shared circuit breaker,
while a real service failure still does.

**`contextcheck` is the regression guard.** With a context in scope, the linter
flags any downstream call that invents its own — which is how the
`NotifyConnect` and offline-delivery sites were found. It is enabled in
`.golangci.yaml` and runs on every PR.

**Left open, deliberately:** `Broker.CreateSession` still takes no context and
carries a suppression at the two handler call sites. It has 73 call sites and
its own conversion; `restoreSubscriptionsFromTakeover` is the path that wants
it. Not on the tag's critical path.

**This does not close P0-1.** Authorization is still one synchronous callout
per PUBLISH; the context makes it cancellable, not cheap. 1.3 is what closes
it.

### 1.3 Cache authorization decisions

**Weight: L · 4–5 days** — see the critical-path breakdown.

The headline scalability blocker. `CanPublish` runs per PUBLISH packet
(`mqtt/broker/v3_handler.go:291`, `v5_handler.go:362`) with no decision cache,
so a callout-configured broker performs one synchronous HTTP round-trip per
published message — up to 4.1s of blocking at defaults.

- Bounded TTL+LRU keyed `(identity, topic, action)`. `broker/identitycache.go`
  is the pattern to copy; it is 125 lines.
- Invalidation on reload and on `auth.local_principals` change.
- Negative caching with a shorter TTL than positive.
- Drop the per-authorize `slog.Info` (`http.go:146`) to `Debug`.

Ships with a benchmark: publish throughput with authz enabled, before and
after, `b.ReportAllocs()`.

### 1.4 Make configuration strict and fail-fast — ✅ DONE 2026-08-20

**Weight: M · ~2 days — landed in one sitting.** *Sized by spike first: strict
decode broke 4 of 10 shipped config files on exactly one key (`plain`, 8
occurrences), so the 2–4 day range collapsed. Delivered: strict decode,
fail-fast load, duplicate-bind validation, `config/schema_test.go`, and five
corrected example files. `make test` green across 63 packages, `make lint` at 0
issues.*

**What shipped**

| Change | Where |
| --- | --- |
| `KnownFields(true)` strict decode | `config/config.go` — `parse()` |
| Missing file is an error (`ErrConfigNotFound`) | `config.Load` |
| Opt-in fallback | `config.LoadOptional` + `--config-optional` flag |
| Duplicate-bind rejection, TCP and UDP kept separate | `Config.validateNoDuplicateBinds` |
| Key-set pinning + shipped-config regression guard | `config/schema_test.go` |
| Corrected listener declarations | 5 files under `examples/` |

**Behaviour change worth calling out in the release notes:** reload uses
`Load`, so a config file that goes missing under a running broker now fails the
reload and keeps the live configuration. It previously reset the running broker
to defaults — a worse outcome than the startup case, since it silently
discarded a working deployment's auth and TLS settings.

### 1.4b MQTT listener sections renamed — ✅ DONE 2026-08-20

**Weight: S · same sitting.** Taken while the schema was already being broken,
because after the tag it is frozen.

`server.tcp` and `server.websocket` said nothing about MQTT while sitting
directly beside `server.amqp` and `server.amqp091`. Both moved under a
`server.mqtt` parent:

```yaml
server:
  mqtt:
    tcp:
      v3: { addr: ":1883" }
      v5: { addr: ":1884" }
    websocket:
      v3: { addr: ":8083" }
  amqp091:
    plain: { addr: ":5682" }
```

Go types renamed to match: `TCPConfig` → `MQTTTCPConfig`,
`TCPListenerConfig` → `MQTTTCPListenerConfig`, `WebSocketConfig` →
`MQTTWebSocketConfig`, `WSListenerConfig` → `MQTTWebSocketListenerConfig`, and a
new `MQTTConfig` groups the two transports. `ServerConfig.TCP` /
`ServerConfig.WebSocket` became `ServerConfig.MQTT.TCP` /
`ServerConfig.MQTT.WebSocket`.

Updated with it: all 10 shipped config files, 7 documentation pages, the
validation error paths (`server.mqtt.tcp.v3.protocol` and friends), and the
schema test — which now also asserts the flat `server.tcp` key is gone.

**Breaking for every deployed configuration.** It belongs in the 1.0 release
notes with a before/after snippet, and it is the reason strict decoding
(1.4) matters: a config still on the old shape now fails loudly at startup
instead of silently losing its listener settings.

Two changes, both in `config/config.go`, both small, both closing entire
incident classes:

- **Strict decode.** `config.go:1295` uses plain `yaml.Unmarshal`;
  `KnownFields` appears nowhere in the repo. A misspelled key under `auth:` or
  `cluster.transport.tls_*` is silently ignored and the broker starts with that
  protection absent.
- **Missing file is an error.** `config.go:1283-1287` returns `Default()` on
  `os.IsNotExist`. Because `broker/auth.go:88,109` treat a nil authenticator
  and nil authorizer as allow-all, a typo'd `--config` path starts a fully
  open broker that reports healthy. Add `--config-optional` as the explicit
  opt-in for the current behavior.

Add `config/schema_test.go` pinning the accepted key set, so that renaming a
key requires a deliberate test edit.

**Spike result (2026-08-20).** Decoding all 10 shipped config files with
`KnownFields(true)`:

```
files broken by strict decode: 4 / 10
distinct unknown keys:         1
  plain                        x8
```

`examples/config.yaml`, `no-cluster.yaml`, `single-node-cluster.yaml`, and
`tls-server.yaml` still write `tcp.plain` / `websocket.plain`; the schema slots
are `v3` / `v5` / `tls` / `mtls` (`config/config.go:629-634`, `:649-654`).
`production.yaml`, the three cluster node configs, and both docker configs pass
clean.

**This was a live bug, not drift — see P0-5a.** The discarded blocks meant four
examples silently opened two plaintext TCP listeners (`:1883` *and* `:1884`)
and two plaintext WebSocket listeners (`:8083` *and* `:8084`) where each
declared one of each.

Fixing those four then unblocked validation of `examples/production.yaml`,
which failed on a *second* `:8084` collision. That file — the one an operator
copies for a real deployment — asserted three times that it served no
plaintext, while opening four plaintext ports, one of them racing its own WSS
listener. **The spike paid for itself before the work started, and the
duplicate-bind check paid for itself within an hour of being written.**

### 1.5 Make queue durability configurable and documented — ✅ DONE 2026-08-21

**Weight: S · 1–2 days.** The keys were an hour. Benchmarking the default was
the rest, and it changed the answer — see below.

`logstorage/types.go:168` sets `DefaultSyncInterval = time.Second` and
`cmd/main.go:834` never overrides it. `sync_interval` does not exist in the
config schema, so **no operator can choose fsync-per-append** — while the
README markets durable queues and QoS 1/2 at-least-once delivery.

The gap is wider than the missing key. *Imported from `v1.md`; verified
2026-08-21:* the protected internal stream appends with `AppendAndSync`
(`queue/manager.go:1158`) while **every ordinary durable publish uses buffered
`Append`** (`:1306`). So the one path an operator never sees is the one that is
crash-safe, and an acknowledged ordinary publish can lose up to the background
sync window on a process or host crash.

- Expose `storage.queue.sync_interval` (`logstorage/options.go:112`
  `WithSyncForEveryWrite` already exists, unwired).
- Add an explicit acknowledgement-durability policy — fsync-before-success
  versus buffered — and **default durable queues to fsync**. Buffered stays
  available for throughput-first deployments that choose it deliberately, with
  `sync_interval` naming their loss window.
- Document the guarantee against each value, explicitly naming the
  acknowledged-message loss window. The README's durability claims get
  corrected in the same change.
- Revisit `sync_writes: false` in `deployments/cluster/config/node{1,2,3}.yaml:66`.

**What shipped**

| Change | Where |
| --- | --- |
| `storage.queue_ack_durability` (`buffered` \| `fsync`) | `config/config.go` |
| `storage.queue_sync_interval`, previously hardcoded at 1s and unreachable | `config/config.go`, wired into the adapter in `cmd/main.go` |
| Per-queue `queues[].ack_durability` override | `config/config.go`, `queue/types/config.go` |
| Ordinary durable publishes take `AppendAndSync` under fsync | `queue/manager.go` — `appendWithAckDurability` |
| Startup fails if anything asks for fsync and the log cannot sync one append | `cmd/main.go` — `wantsDurableSync` |

**The default stayed `buffered`, against the plan, because of a measurement.**
Durable publish, 256-byte payload, ext4 on consumer NVMe:

| | ns/op | msg/s |
| --- | ---: | ---: |
| `buffered` | ~7,700 | ~130,000 |
| `fsync`, serial | ~4,930,000 | ~203 |
| `fsync`, 16 goroutines | ~4,916,000 | ~203 |

Concurrency buys nothing: `appendWithBarrier` (`logstorage/manager.go:198`)
holds the segment manager's exclusive lock across `segment.Sync()`, so
publishers to one queue serialize into one fsync each. **There is no group
commit anywhere in `logstorage`.** A durable queue is therefore capped at the
reciprocal of the device's fsync latency — around 200 messages a second —
however many publishers it has.

Making that the default would have cost every existing deployment ~640x
throughput on upgrade, silently, with no way to get it back except changing the
default they never set. So the policy is opt-in, and it is opt-in **per queue**
rather than per broker: durability is a property of what a queue carries, and a
global switch forces the strictest queue's cost onto every queue beside it. An
audit stream asks for `fsync`; the telemetry queues next to it do not pay for
it.

Benchmarks live in `queue/durability_bench_test.go` and must run on a real
filesystem — `/tmp` is tmpfs on most Linux workstations, where fsync is free and
the comparison silently measures nothing. That mistake was made and caught here:
the first run reported fsync and buffered within 3% of each other.

**Follow-up: 1.5b, group commit.** Until it exists, `fsync` is only usable on
low-volume queues, which is a real limit on what the 1.0 durability claim can
say.

### 1.5b Group commit for the queue log — ✅ DONE 2026-08-21

**Weight: M · 1–2 days.**

Barriers on one segment coalesce: the first caller fsyncs, everyone who arrived
before it captured its coverage rides that barrier, and a caller whose append
landed later takes the next one rather than trusting someone else's. The fsync
runs with no segment lock held, which is what lets appends continue while it is
in flight.

Durable publish, 256-byte messages, ext4 on consumer NVMe: ~185 msg/s at one
publisher, ~1,260 at sixteen, ~3,100 at sixty-four, against a flat ~200 at every
concurrency before. A single publisher is unchanged and always will be — it has
nobody to share a barrier with.

**1.5's default stays `buffered`.** Sharing the barrier makes `fsync` usable on
a busy queue rather than unusable, but two orders of magnitude still separate
them, so it remains something a queue asks for.

**Built on the sync-failure handling from #578 rather than replacing it.** A
failed barrier still sticks, and the next append retries it under the lock. The
one behaviour change: because the barrier now runs outside the lock, an append
can be accepted between a barrier failing and that failure being recorded. It is
not acknowledged on a broken device — in fsync mode it takes its own barrier and
fails the same way — and the stickiness still stops the one after it.

The original description follows.

Coalesce appends waiting on the same segment into one fsync, the way every
write-ahead log does: the first writer syncs, everyone who arrived while it was
syncing rides that barrier, and each caller learns whether *its* record was
covered. Throughput then scales with concurrent publishers instead of pinning to
device latency, and `fsync` becomes defensible as a default rather than an
expert setting.

Blocked behind nothing. Revisit 1.5's default the day it lands, and say in the
1.0 notes which of the two shipped.

### 1.6 Close the session-ownership split-brain

**Weight: L · 4–6 days** — the CAS is an hour; the partition test harness is the work.

`cluster/etcd.go:933` acquires ownership with an unconditional leased Put while
`ReleaseSession` twelve lines below uses a proper CAS. Separately,
`cluster/etcd.go:2176-2210` **restores** an ownership key that etcd deleted on
lease expiry — inverting the one signal that says this node is no longer
trusted to own the session.

- CAS on acquire: `Compare(CreateRevision(key), "=", 0)`, or against the
  ModRevision observed during takeover.
- Delete the resurrection path. On lease loss a node drops the session.
- Test: partition, lease expiry, takeover on the peer, heal, assert exactly one
  owner.
- Document that membership is fixed once a node holds data, stated against
  main's cluster keys — salvaged from the parked `config` branch (`6d055c8d9`),
  whose text documents v1-only keys and cannot be cherry-picked as written. A
  changed member map against existing cluster data must fail startup, and the
  partition harness built here is what proves it.

### 1.7 Test the TLS revocation stack

**Weight: L · 4–5 days**, plus 2–3 more if the fail-open check comes back positive.

547 untested lines (`pkg/tls/verifier/crl`, `.../ocsp`, `.../verifier`) on the
certificate-validation path for every mTLS listener
(`pkg/tls/tls.go:253,287`).

First test to write: **OCSP responder unreachable — does verification fail open
or closed?** If open, that is a security finding and joins Milestone 1 on its
own merits.

### 1.8 Second audit pass over the unreviewed surface

**Weight: L · 5–8 days** of review, plus unbounded remediation for whatever it finds.

The areas this audit could not reach — listed under "Coverage and confidence"
in `V1-READINESS.md`. Highest value, in order: MQTT codec DoS surface
(attacker-controlled length prefixes), AMQP 1.0 partial-delivery reassembly,
`ratelimit/` per-IP map growth, CoAP UDP amplification, `reload/` atomicity.

### 1.9 Authenticate the admin API

**Weight: L · 5–7 days** *(imported from `v1.md` Phase 1; verified 2026-08-21)*

`server/api/server.go:45-72` builds its mux with **no authentication
middleware of any kind**. What that leaves open, in one place:

- REST: `/api/v1/reload`, `/api/v1/sessions/…`, `/api/v1/subscriptions`,
  `/api/v1/stats`, `/api/v1/cluster`, `/api/v1/overview`.
- The whole Connect `QueueService` (`server/queue/handler.go`) —
  `CreateQueue`, `DeleteQueue`, `UpdateQueue`, `Append`, `AppendBatch`,
  `AppendStream`, `Read`, `Tail`, `Ack`, `Nack`, `Claim`, `JoinGroup`,
  `DeleteConsumerGroup`, and the seek operations.

`deployments/docker/compose.yaml:12` publishes that surface to the host on
`8082`, and the shipped `fluxmq-dashboard` image (`:45`) is a client of it. An
unauthenticated `DeleteQueue` plus an unauthenticated `reload` is a full
compromise of the broker's data and its configuration, reachable by anyone who
can open a TCP connection.

- `admin.auth` with `mode: token|disabled`; **`disabled` is accepted only on a
  loopback bind**, and a non-loopback bind without auth is a startup error.
- File-backed tokens with three roles: `viewer` (read-only stats, sessions,
  subscriptions, overview, queue inspection), `operator` (session disconnect,
  append/ack/nack/claim), `admin` (queue lifecycle, purge, truncate, reload).
- Default the admin bind to loopback.
- Structured audit events on every mutation, with the acting token identity.
- The dashboard authenticates server-side via `FLUXMQ_ADMIN_TOKEN_FILE`. The
  token must never reach browser JavaScript.

Both the config schema and the role names freeze at the tag — see contract
item 5. Tests: 401 without credentials, 403 for insufficient role, on every
route, table-driven over the route set so a new route without a role assignment
fails the suite.

### 1.10 Make DLQ movement and replication failure-safe

**Weight: M · 4–6 days** *(imported from `v1.md` Phase 2; verified 2026-08-21)*

Two independent ways a message disappears without anyone being told.

**DLQ is lossy.** `queue/consumer/manager.go:441-446`: on exceeding
`MaxDeliveryCount`, the handler calls `OnDLQ` best-effort — it is skipped
entirely when the source read fails — and then removes the pending entry with
`_ = RemovePendingEntry(...)` regardless of whether the DLQ append happened.
The one error return that would say "this message was not saved" is discarded
by an `_`. Explicit `Reject` does not route through DLQ at all.

- Remove the PEL entry **only after** a successful durable DLQ append.
- At-least-once with a stable transfer ID, so a crash between append and PEL
  removal produces a detectable duplicate rather than a silent loss.
- Route `Reject` through the same path.
- Propagate sync/append failures to the protocol layer instead of logging them
  — a publisher that got an ack must not have lost its message.
- Tests inject append, fsync, and DLQ failures and assert the source PEL entry
  survives every one of them.

**Replication fails open.** `queue/manager.go:204` defaults to
`WritePolicyLocal`, and the switch at `:1025-1049` appends locally when the
Raft coordinator is absent or disabled; `:339` logs
`distribution_mode=replicate requires raft to be enabled; falling back to
forward`. A queue configured with `replication_factor: 3` can therefore accept
writes with a replication factor of one and report success.

- A replication-enabled queue without the experimental gate and a healthy Raft
  manager is a **startup error**, not a warning.
- Unknown or unavailable write paths return errors. No silent local fallback.
- `replication_factor` and `min_in_sync_replicas` are enforced or they are not
  accepted.

### 1.11 Bound the AMQP 1.0 handshake — ✅ DONE 2026-08-21

**Weight: S · 0.5–1 day.** *(imported from `v1.md`; verified 2026-08-21)*

**What shipped:** per-listener `handshake_timeout` on both AMQP families
(default 10s, `"0s"` disables), covering transport, TLS, SASL, and OPEN, cleared
in `amqp1/broker/connection.go` once OPEN succeeds. AMQP 0.9.1 previously
hardcoded the same 10s in `cmd/main.go`; it now reads the key too.

**Found while fixing it:** the TLS handshake ran *inline in the accept loop*
(`server/amqp1/server.go`), so one unresponsive peer stalled every pending
connection — worse than the unbounded-slot problem the item was written for. It
now runs on the connection's own goroutine, under the deadline, via
`HandshakeContext`.

The original description follows.

`server/amqp1/server.go:120-123` performs the TLS handshake with no deadline
set, and nothing bounds SASL or AMQP `Open` afterwards, so a client that
connects and stalls holds a connection slot indefinitely. AMQP 0.9.1 already
has exactly the model to copy — `HandshakeTimeout` covering transport through
`Connection.Open`, cleared on success (`server/amqp/server.go:24-26,155`).

Per-listener `handshake_timeout`, default 10 seconds, covering TCP/TLS, SASL,
and Open. Schema change, so it lands before the tag.

---

## Milestone 2 — Correctness and honesty (should precede the tag)

- **Move event hooks off the publish hot path.** `[S · 1–2d]` `mqtt/broker/publish.go:117`
  calls `OnPublish` inline before `distribute`. `broker/asynchook.go` exists.
- **Give AMQP shutdown a deadline.** `[M · 2–3d]` `amqp/broker/broker.go:426` and
  `amqp1/broker/broker.go:329` are contextless, so `shutdown_timeout` cannot be
  honored. Also `amqp/broker/channel.go:735` — `qm.Publish(context.Background(), …)`.
- **Fix or delete `benchmarks/e2e_bench_test.go`.** `[S · 0.5d to delete; L · 1–2w to implement — delete]` It returns a mock server,
  `Stop()` is empty, and line 395 is `time.Sleep(time.Duration(b.N) * time.Second)`.
  `make bench` runs it. No 1.0 should ship a benchmark suite that fabricates
  numbers.
- **`errors.Is` sweep.** `[S · 1d]` ~20 sentinel `==`/`!=` comparisons, concentrated in
  `logstorage/adapter.go` and `queue/consumer/manager.go`.
- **Reconcile the documentation with the code.** `[S · 1–2d]` `CLAUDE.md`'s Configuration
  section describes the `config` branch's schema, not main's: `config/v1.go`,
  a `version: 1` document key, a `fluxmq config validate` subcommand, and
  `listeners`/`admin`/`telemetry`/`experimental` top-level keys — none of which
  exist here (re-checked 2026-08-21; `config/schema_test.go` is the one item on
  that list that 1.4 has since made real). Its Known Issues list is 60% stale.
  `README.md:29` marks Raft `✅` and `(WIP)` on one line; `README.md:96` admits
  DLQ delivery wiring is pending while the Features list advertises DLQ
  unqualified. The support matrix under "The 1.0 contract" is what README
  should say.
- **Fix the `AsyncEventHook` close race.** `[S · 0.5d]` `broker/asynchook.go`
  guards enqueue with `closed.Load()` (`:127`, `:139`) and then sends on
  `h.queue`, while `Close` does `close(h.queue)` at `:191`. A send that passes
  the check before the close panics on a closed channel — a hook configured
  broker crashing on shutdown under load. Stop closing the queue channel, or
  gate the send behind the same `closeCh` select.
- **Bound remote HTTP response bodies.** `[S · 0.5d]` Neither
  `broker/authcallout/http.go` nor the webhook client limits the response it
  reads — no `io.LimitReader`, no `http.MaxBytesReader` anywhere on either path
  (checked 2026-08-21). A hostile or broken auth service can drive broker
  memory.
- **Extend readiness.** `[S · 1–2d]` `server/health/server.go:178-254` already
  checks the broker, `store.Ping()`, and peer reachability — the `v1.md`
  finding that readiness did not exist is stale. What it does not check:
  listener state, queue-log errors, actual cluster *quorum* as opposed to peer
  count, and required auth dependencies. Extend it additively, and point the
  Docker and Kubernetes examples at `/ready`; `/health` stays liveness-only.
- **Derive telemetry export from the signals it carries.** `[S · 0.5d]`
  Salvaged from the parked `config` branch (`44757534d`); the change is
  independent of that branch's listener model.
- **Bound the capture backlog by bytes, not job count.** `[S · 1d]` The
  follow-up `queue/capture.go` left open: a job-count bound admits an unbounded
  byte backlog when messages are large.

---

## Milestone 3 — Verification and coverage

Split, because half of this gates the tag and half does not.

### 3A — Verification the tag depends on

*Imported from `v1.md` Phase 3. CI today runs `make test` — the short race
suite — and nothing else (`.github/workflows/ci.yml:57`). Integration, stress,
interoperability, and cluster-failure scenarios never run.*

- **Stabilize and shard the integration suite.** `[M · 3–4d]` Remove fixed-port
  assumptions and teardown noise; move the unconditionally skipped stable-core
  cluster scenarios — leader failover, subscription propagation, retained
  cross-node behaviour, takeover — into gated jobs that actually run. Either
  they pass or they come out of the 1.0 support claim. Folds in the `./cluster`
  timing problem noted in the progress log: 294s standalone against a 3m
  `make test` budget, with one failure at 149s.
- **Interoperability tests.** `[M · 3–4d]` MQTT 3.1.1 and 5.0 against Mosquitto
  and Paho on one auto-detect listener; AMQP 0.9.1 against `amqp091-go`; AMQP
  1.0 against an independent client. This is the evidence behind "documented
  subset" in the support matrix — without it the matrix is an assertion.
- **Fuzz the parsers.** `[M · 3–4d]` No fuzz targets exist. Cover MQTT 3/5
  packets, AMQP 0.9.1 and 1.0 frames, and configuration decoding. Bounded
  smoke runs on PRs, longer scheduled runs nightly. Directly feeds 1.8's MQTT
  codec DoS review.
- **Crash, backup, and restore drills.** `[M · 2–3d]` Acknowledged fsync-mode
  messages survive `SIGKILL` and restart; injected append/fsync/DLQ failures
  never drop the source PEL entry; backup/restore and upgrade/rollback are
  exercised, not assumed. Pairs with 1.5 and 1.10 — they are the changes these
  drills verify.

### 3B — Coverage where the next bug will be (may slip past the tag)

- **`queue/consumer`** `[XL · 2–3w]` — 1,789 non-test LOC against 260 test LOC (0.15), owning
  consumer-group membership, heartbeats, work-stealing, and the PEL. Compare
  `mqtt/` at 0.94. Needs a table-driven rebalance suite and a partition
  simulation.
- **Suppressed errors** `[M · 3–4d]` — 70 `//nolint:errcheck` in `mqtt/broker`, 23 `_ =` in
  `amqp/broker`. Sweep those two; the remaining 400-odd can wait.
- **`storage/` root** `[M · 4–5d]` (423 non-test / 60 test) and `server/queue` (1,118 / 643).
- **Badger vs memory backend parity** `[L · 5–7d]` — not verified in this audit. Both
  implementations are required to stay semantically identical; nothing enforces
  that. A shared conformance suite run against both backends is the fix.

**On slipping Milestone 3:** `queue/consumer` is the only XL item, and it is
coverage rather than a known defect — 1,789 non-test LOC at a 0.15 test ratio
owning consumer-group membership, heartbeats, work-stealing, and the PEL. If
1.0 must ship before that suite exists, say so explicitly in the release notes
and mark consumer groups as beta. Do not ship it silently at 0.15 while the
README markets consumer groups as a headline feature.

---

## Milestone 4 — Release governance

- `CHANGELOG.md` `[S]` — keep-a-changelog, starting from the 1.0 entry.
- `SECURITY.md` `[S]` — supported versions, disclosure address, response window.
- `CODEOWNERS` `[S]`.
- An ADR directory `[S]`, so the next three design documents are not deleted.
- Replace `docs/content/docs/roadmap.md` (26 lines of emoji bullets) with a
  pointer to this file.
- Move `CLAUDE.md` into version control `[S, folded into 2.5]` once Milestone 2's documentation
  reconciliation makes it accurate.
- **Fix the release pipeline order.** `[S · 1–2d]` *(imported from `v1.md`;
  verified 2026-08-21)* `.github/workflows/build.yml` pushes the image at `:60`,
  signs it, and only then scans it at `:81` — so a Critical finding arrives
  after the artifact is public. It also triggers on tags in its own right, with
  no dependency on `ci.yml` passing, so a tag whose tests fail still publishes.
  Build → scan → publish, gated on CI, publishing immutable version tags by
  digest. Scan the dashboard image too. Pin actions by digest rather than by
  tag (`aquasecurity/trivy-action@v0.36.0`, `sigstore/cosign-installer@v3` are
  mutable references) and keep workflow permissions least-privilege.
- **Generate the configuration reference from the schema.** `[S · 1–2d]`
  Salvaged from the parked `config` branch (`eb98910a9`), rebuilt against
  main's schema. `docs/content/docs/reference/configuration-reference.md` is
  hand-maintained and drifts on every schema change — 1.4 and 1.4b each had to
  edit it by hand. Generating it from the same key set
  `config/schema_test.go` pins makes drift impossible, and makes docs builds a
  mandatory CI gate rather than a courtesy.

---

## Milestone 5 — Performance baselines and operations (gates the tag)

*Imported from `v1.md` Phase 4. This is the milestone that turns "fast" from a
claim into a number somebody else can reproduce.*

**Weight: L · 9–14 days**, of which the soak is elapsed time rather than
attention.

- **Reference profiles.** A dedicated 8-vCPU / 16-GiB / NVMe single-node
  profile and a three-node profile. Everything below is measured there, not on
  a workstation.
- **Workloads.** MQTT and AMQP throughput, queue fsync, fan-out, churn,
  reconnect, failover. Record p50/p95/p99 latency, throughput, CPU, RSS, GC,
  allocations, descriptors, loss, and duplicates.
- **Revalidate what has already landed.** The topic-trie and capture-isolation
  numbers in the progress log come from a review workstation. They become the
  baseline only after being rerun here.
- **Regression budgets.** No unexplained throughput regression above 10%, no
  p99-latency or RSS regression above 15%, no unexpected message loss. Commit
  the result artifacts — there are none in the repository today.
- **72-hour secure three-node soak** on a release candidate, with repeated
  disconnects and node termination, followed by the backup/restore and
  upgrade/rollback drills from 3A.
- **Runbooks:** capacity, backup, upgrade, rollback, incident response.

Optimization work — queue configuration lookups, the delivery scheduler,
remote-consumer indexing — happens only where these measurements justify it.
Not before.

---

## Explicitly post-1.0

Deferring these is the point of having a roadmap:

- **Promoting queue Raft to production support.** ~20–35 engineer-days plus at
  least two further weeks of partition, restart, snapshot, rolling-upgrade, and
  quorum-loss soak. 1.0 ships it behind an experimental gate, off by default,
  outside the compatibility contract — which is exactly why 1.1 (Track A) only
  has to secure its transport rather than harden it.
- Elastic cluster membership. 1.0 membership is static: a changed member map
  against existing cluster data fails startup. Join and remove at runtime are
  their own project.
- Refactoring the six files over 2,000 lines.
- The gRPC Raft transport (option B in 1.1) — additive and non-breaking, so it
  ships in 1.1.x.
- Track B, replacing etcd as the coordination layer — the 20-week project.
  P0-7's split-brain fix is far cheaper than replacing the coordination layer.
- AMQP 0.9.1 field-table support (`amqp/codec/types.go:145`).
- Zero-copy will messages (`mqtt/broker/v3_handler.go:113`,
  `v5_handler.go:114`).
- Broad `errcheck` cleanup beyond `mqtt/broker` and `amqp/broker`.

---

## Definition of done for 1.0

**Contract and security**

- [ ] Raft transport secured (TLS `StreamLayer`); 3 stub RPCs removed from `proto/cluster/v1`
- [ ] etcd peer and client traffic uses the cluster mTLS identity; clustering without TLS fails to start unless the explicit development-only opt-in is set
- [x] `Authorizer` carries `context.Context` — *#577*
- [ ] Authorization decisions cached; publish-throughput benchmark recorded
- [ ] Admin API returns 401 without credentials and 403 for an insufficient role, on every route; destructive operations emit structured audit events; non-loopback binds without auth refuse to start
- [x] Strict config decode; missing config file is a startup error — *#576*
- [x] MQTT listeners moved under `server.mqtt`; types renamed — *#576*
- [x] Queue delivery address settled for MQTT 5.0 and both AMQP versions; origin in `types.PropSourceTopic` — *#576*
- [x] MQTT 3.1.1 queue consumption decided: settle on PUBACK, no origin recovery — *#580*
- [ ] Inflight properties survive a cluster takeover, so a settled MQTT 3.1.1 delivery is not redelivered after a node move (additive `proto/cluster/v1` field)
- [x] AMQP 1.0 `handshake_timeout` bounds transport, SASL, and Open — *#578*

**Durability and correctness**

- [x] Queue acknowledgement durability configurable per queue and broker-wide; loss window and cost documented per setting — *#578*
- [x] Durability barriers coalesce, so `fsync` scales with concurrent publishers — *#579*. The default stays `buffered`: sharing the barrier makes `fsync` usable on a busy queue, not free
- [ ] Replicated queues can use `fsync` — today the combination is refused, because Raft apply never reaches the queue log's per-append barrier
- [ ] Acknowledged fsync-mode messages survive `SIGKILL` and restart
- [ ] Injected append/fsync/DLQ failures never remove the source PEL entry; crash-window DLQ duplicates share a stable transfer ID; `Reject` routes through DLQ
- [ ] Replication-enabled queues refuse to start without the experimental gate and a healthy Raft manager; no silent local fallback remains
- [ ] Session acquire is a CAS; lease-expiry resurrection removed
- [ ] A secure three-node cluster passes formation, leader and follower loss, partition, lease expiry, reconnect, takeover, retained delivery, and graceful shutdown

**Verification**

- [ ] CRL/OCSP tested, fail-open/fail-closed behavior known and intentional
- [ ] Second audit pass over the unreviewed surface complete
- [ ] Previously unconditional stable-core skips are implemented or removed from the support claim
- [ ] Stable-core suites pass under `-race` on three consecutive clean runs
- [ ] MQTT exercised against Mosquitto/Paho, AMQP 0.9.1 against `amqp091-go`, AMQP 1.0 against an independent client
- [ ] Fuzz targets cover MQTT 3/5, AMQP 0.9.1/1.0 frames, and configuration decoding
- [ ] `benchmarks/` produces real numbers or does not exist
- [ ] Reference-profile results committed; no unexplained regression above 10% throughput or 15% p99/RSS; no unexpected loss
- [ ] 72-hour secure three-node soak passed, followed by backup/restore and upgrade/rollback drills

**Release governance**

- [ ] `CLAUDE.md` and `README.md` agree with the code; the support matrix ships in both
- [ ] Configuration reference generated from the schema; docs build is a required gate
- [ ] Images built and scanned *before* publication, gated on CI, signed by immutable digest — dashboard included
- [ ] `CHANGELOG.md`, `SECURITY.md`, `CODEOWNERS` present
- [ ] `make test` and `make lint` green; `go vet` and `govulncheck` clean; no Critical/High called vulnerability
