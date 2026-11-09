---
type: Architecture
title: provider-runtime — credential-use finalizer, implementation plan
description: Plan, nothing built, for implementing the credential-use finalizer design as a stack of eleven PRs that ships as v1.13.0, with each PR's scope, design sections, tests, review focus, dependencies and size, plus the design's ambiguities and the premises to verify.
resource: github.com/krateo-platformops/provider-runtime
tags: [plan, design, finalizer, deletion, credentials, reconciler]
timestamp: 2026-11-09T00:00:00Z
---

# Credential-use finalizer: implementation plan

**Status:** approved by the user on 2026-11-09, with the decisions in §8.
The design doc was amended to match, as its revision 16. The spec is
[design-credential-use-finalizer](./design-credential-use-finalizer.md) as
merged at `cfd9b0f` (PR #47, 15 revisions). Section numbers (§) are the
design's. The work ships as **v1.13.0**, and nothing is tagged until the whole
stack has merged.

`pkg/reconciler/reconciler.go` hasn't changed since `77cce67`, so the design's
line references (`:806`, `:821`, `:870`, `:879-885`, `:1089`, `:1125`, `:1152`)
are still correct at `cfd9b0f`.

## 1. Summary

The suggested six PRs would make PRs 1 and 2 about 5,000 lines each, too
large for a concurrency review. So the stack has **eleven**. The main changes
from the suggested shape:

- **The protocol and its in-memory model come before any informer** (PRs 1
  and 2). The model runs the production A1, A2 and R0 to R3 code, so the
  safety argument is reviewed on its own, before any client-go plumbing.
- **The LabelScoped runtime is split from chains, consent and Disown** (PRs 3
  to 6). Each of those has its own review focus in §14.
- **The wiring PR comes before PerName and PerNamePoll** (PR 7). git-provider
  needs only LabelScoped, and the krateo-057 teardown envtest then runs
  against the real reconciler as early as possible.
- **`scaletozero` comes after the index** (PR 10), so that `WithHeld` and
  `WithHeldIndex` land in one PR.

| # | PR | Main design sections | Prod / test lines | Depends on | Concurrency review |
|---|---|---|---|---|---|
| 1 | `credentialuse`: vocabulary, classification, patch builders, premise envtest | §3, §4, §6.2 (table and R3 patch), §2 | 700 / 1,400 | — | light (patch semantics) |
| 2 | Protocol core, verified sets, in-memory API model with schedules | §5, §6.1–§6.8, §14 model | 1,300 / 2,900 | 1 | **yes** |
| 3 | `Shared` and `Tracker` on client-go: LabelScoped Secrets, release loop, prober | §7.1–§7.3, §10.1–§10.3, §10.5, §12 (core part) | 2,300 / 2,500 | 2 | **yes** |
| 4 | Chains: `Next`, `Resolved`, `Guaranteed`, content caches, enqueue on change | §9.1–§9.3, §10.4, §6.2 A2 for custom resources | 800 / 1,000 | 3 | **yes** |
| 5 | The cross-namespace consent gate and the `unheld` annotation | §11.3, §9.4, §9.2 (L1), §10.2 (L2) | 900 / 1,200 | 4 | **yes** |
| 6 | Disown and `DisownPrefix` | §7.4, §11.5, §10.6.8 (3) | 700 / 900 | 3 (stacked on 5) | **yes** |
| 7 | The provider-runtime adapter and the reconciler wiring | §7.5, §8, §12, §11.7–§11.10 | 900 / 1,600 | 6 | **yes** |
| 8 | PerName mode: the index ConfigMap | §10.6, §7.3, §13 (shape 1) | 1,300 / 1,600 | 7 | **yes** |
| 9 | PerNamePoll mode and the provisioner | §10.7, §13 (shape 2), §11.3 (L2) | 1,200 / 1,400 | 8 | **yes** |
| 10 | `scaletozero.WithHeld` and `WithHeldIndex` | §11.6, §10.6.5 | 250 / 450 | 8 (stacked on 9) | light |
| 11 | Docs and the `log.md` entry | §11.5, §13, §16 doc text | ~900 doc lines | 10 | — |

That is about **10,350 lines of production Go and 14,950 of tests**,
including the model (about 1,400) and the envtest harness (about 300). §7
explains the estimates.

The design's ambiguities, and what I'd choose for each, are in §5. Five of them
change an API or a rule the design states, so they need the user (§8). The
premises are in §6. Twelve are verified in the module cache already, with their
consequences, and the rest are pinned by tests in the PR that needs them.

## 2. Rules for every PR

- **Each PR builds and passes `go test -race ./...` on its own,** with CI's
  envtest (1.37.0). Under CI an envtest test fails rather than skips, as in
  `pkg/scaletozero/envtest_test.go`.
- **No behaviour change for consumers that don't implement
  `CredentialRefsProvider`.** PRs 1 to 6 add only `pkg/credentialuse`, which
  nothing imports. PR 7 is the first to touch `pkg/reconciler` and
  `pkg/resource`, and it carries a test that a connecter without the
  interface produces exactly today's sequence of client calls, builds no
  `Shared`, and gets a no-op `CredentialUseSource` (§11.10). PRs 8 to 10 add
  opt-in modes and options only.
- **The core depends only on client-go** (§7.1). PR 1 adds a test that
  `go list -deps ./pkg/credentialuse` contains no `sigs.k8s.io/controller-runtime`
  package. It runs in every later PR.
- **One protocol, two backends.** The protocol code (A1, A2, BeforeEstablish,
  R0 to R3, Disown's strips, the index writes) calls the API server only
  through a narrow internal interface (`server`: get, patch and list of
  metadata, plus the Namespace GET) and reads caches only through another
  (`views`). The in-memory model implements both, so the schedules of §14 run
  the production code, not a copy of it. PR 3 adds the client-go `server`.
- **No lock is held across an API call.** The model's scheduler parks every
  actor at each API call, so a lock held there deadlocks the model. That makes
  the rule testable.
- **Clocks.** Everything takes a `k8s.io/utils/clock.PassiveClock`. The skip
  times (`t_issue`, `observed`, `recordSince`, the relist time) are
  monotonic `time.Now()` values and are never passed through `UTC()`, `In()`,
  `Local()` or `Round(0)`, which strip the monotonic reading. The grace mixes
  the server's `deletionTimestamp` (wall clock) with local first sight, as §5
  says, and is computed in one function with its own tests.
- **Independent review for concurrency.** Every PR marked "yes" above gets a
  fresh reviewer before merge. The reviewer gets the diff, the design
  sections the PR implements and that PR's attack list (§3). It doesn't get
  the builder's test results or conclusions. Every finding comes back with a
  failing test or a model schedule, and the PR merges only when they are
  resolved. Each of those PRs also carries stress and drain tests.
- **Revert checks.** Each PR from 1 to 10 adds a
  `hack/revert-check-credential-use-<n>.py` in the style of
  `hack/revert-check-scale-to-zero.py`. It breaks each safety rule the PR
  adds, one at a time (for example: let a deletion pass skip A2, drop R3's
  epoch test, read the Namespace from the cache, or forget the relist
  reset), and requires a test to fail by assertion. Its output is committed
  next to it.
- **Test timing.** The envtests shorten `DeletionGrace`, `ProbeInterval` and
  `DisownInterval` (for example to 2s, 200ms and 500ms), and the stress tests
  are bounded to about 45s under `-race`. The aim is under 3 minutes per
  package in CI.
- **No cluster is touched.** Everything runs in envtest or in the model.
- **Releases.** The stack merges to `main` in order, and no tag is cut until
  PR 11. If a v1.12.x fix is needed meanwhile, it is cut from `v1.12.0` on a
  release branch.

## 3. The PRs

### PR 1: `credentialuse` vocabulary, classification, patch builders, premise envtest

**Scope.** New `pkg/credentialuse`:

- `ref.go`: `Ref`, `SecretRef`, identity (group, kind, namespace, name),
  sorting and deduplication by identity (§4);
- `record.go`: the `refs.v1` codec. An unparseable record decodes to a
  sentinel that "counts as a user of everything in its namespace" (§4);
- `names.go`: `FinalizerName` with the `IsQualifiedName` and length checks
  (§3), the `cluster-scoped` name, the marker key `k-<first 16 hex digits of
  SHA-256>` (§10.1), the epoch token (128 bits from `crypto/rand`), and the
  constants: `AnnotationRefs`, `AnnotationUnheld`, `LabelHeld`, and the
  disown, config, index, held-count, disown-settled and allow-holders-from
  keys;
- `classify.go`: the §6.2 table, plus the classes the table leaves out (§5,
  A1 to A3): NotFound, Conflict, the R3 "state changed" 422, other admission
  403s;
- `patch.go`: the A2 strategic merge patch (built-in kinds), the A2 guarded
  merge for custom resources (the full finalizer list plus resourceVersion),
  R3's JSON patch with RFC 6901 escaping (`~0`, `~1`) and its two optional
  parts (the full-array test with the marker label, and with the held label),
  and the label janitor's patch (§10.1);
- `decide.go`: §5's table and §6.1's user and deleting predicates, as pure
  functions.

Also `pkg/credentialuse/internal/cutest`, the envtest harness: `TestMain`
helper, `requireAPIServer`, CRD builder, impersonated clients, and a
namespace-controller stand-in that deletes a namespace's content in a given
order and finalizes it through the `finalize` subresource.

**Design sections:** §2 (premises), §3, §4, §6.1, §6.2 (the table, A2's
patches, R3's patch), §10.1 (labels).

**Tests.**

- Unit tests for every function above. They cover escaping, sorting and
  dedup, the unparseable record, the length limits, and the
  marker-label-removal conditions (K's last finalizer; the last
  `credential-use.` one).
- **Premise envtest** (§14 "Error classification", and the premises §2 and
  §14 say to pin):
  - (a) an A2 on a Secret, and on a custom resource, being deleted without
    the finalizer: 422 Invalid, with a `FieldValueForbidden` cause on
    `metadata.finalizers`;
  - (b) a write denied by a minimal test ValidatingAdmissionPolicy with
    `reason: Forbidden`: 403. The same policy with no `reason`: 422;
  - (c) a binding whose `paramRef` is missing, with
    `parameterNotFoundAction: Deny`: 422, with `failed to configure binding`
    and no `metadata.finalizers` cause;
  - (d) an A2 by an impersonated ServiceAccount without `patch`: 403, with
    the authorizer's exact message pinned, for a namespaced and a
    cluster-scoped object. CI runs it on 1.37.0, and it is run locally on
    1.34, 1.35 and 1.36 (L3 of #52's review);
  - (e) NamespaceLifecycle's 403 on a create in a terminating namespace;
  - the classifier gives each real error its class, and Deleting comes only
    from (a), never from a 403;
  - R3's failed `test` op: the exact status (§14 "R3's failed test op"; see
    A3), and the same shape for a `remove` at a missing index;
  - a strategic merge on a Secret merges finalizers as a set and keeps a
    third party's finalizer. A merge patch on a custom resource replaces the
    list. A merge patch carrying a stale resourceVersion gets a 409;
  - an A2 on an object that is being deleted *with* the finalizer succeeds
    and moves the epoch. Labels and annotations stay writable while it is
    deleted;
  - NamespaceLifecycle forbids a create and allows a patch in a terminating
    namespace (envtest step 2 of §14's teardown);
  - a label-selected metadata watch, with WatchListClient on, delivers losing
    the label as a deletion and gaining it as an addition (§2, §14).
- The dependency test (§2).

**Reviewer attack.** Can any error other than class (a) be read as Deleting?
Can R3's patch remove a finalizer that an A2 re-added, for example after
another finalizer before it was removed and shifted the index? Does R3 test
every value it removes, labels included (L1 of #52's review)? Can the
strategic merge or the guarded merge drop another writer's finalizer? Is the
escaping right, and is the marker removed only with K's last finalizer?

**Depends on:** nothing. **Size:** about 700 lines of production code and
1,400 of tests, harness included.

### PR 2: Protocol core, verified sets, in-memory API model

**Scope.**

- `acquire.go`: A1 (diff the record; a guarded merge patch, A18; a conflict
  requeues; the `observed` drops and resets in the same pass), A2 per ref with
  the skip predicate, the outcomes (held, Deleting, Missing, Degraded), new
  against established (`MustNotConnect` against Degraded),
  `BeforeEstablish`, and `Forget` (in memory only);
- `release.go`: R0 through `views`, R1, R2 (the Namespace GETs after every
  R1, then one consistent paginated metadata list of K in N, with no
  selector), §5's decision, R3, a failed test going back to R1 (A3), and the
  `written` drop when R3 succeeds;
- `verified.go`: the `written`, `recordSince` and `observed` maps, the skip
  predicate, and the relist time (A20);
- `handlers.go`: the holder and protected event logic as pure functions of
  (old, new, delivery time), so that PR 3's informers and the model call the
  same code;
- `pkg/credentialuse/internal/apimodel`: a linearizable store with
  resourceVersions. It validates through apimachinery's
  `ValidateNoNewFinalizers` and patches with the libraries the API server
  uses (`gopkg.in/evanphx/json-patch.v4` and `strategicpatch`). It has a
  namespace phase with NamespaceLifecycle's create rule, a lagging cache per
  actor that delivers events to `handlers.go`, a scheduler that parks each
  actor at every API call and cache read, fault injection (error, crash, lost
  response), a bounded depth-first enumerator of schedules plus seeded random
  walks for larger cases, and a recorder of reads and release decisions.

Nothing here talks to an API server. The client-go `server` comes in PR 3.

**Design sections:** §4, §5 (the table, the grace and the order of the
re-reads), §6.1 to §6.8, and §14's unit tests for the races.

**Tests** (§14 "Unit tests for the races", in full):

- every schedule of A1, A2 and Connect, each succeeding, failing, crashing or
  losing its response; one or two releasers, R0 to R3, each read fresh or
  stale; and P's, H's and the namespace's deletion at every point;
- HIGH-1, an error or crash after A1 with a concurrent releaser, and a
  restart that clears the verified set;
- M1: a ref A1 adds while a stale `written` entry from an earlier run of it
  exists, including away and back without the middle version ever being
  delivered. It must write;
- holder and namespace deletions during the grace, just after it, and between
  R2 and R3; a deletion pass that holds a `written` entry (it must write);
- a recreate behind a cached UID (L6), asserted to be the only window and to
  close when the deletion is delivered;
- a relist inside a watch gap (L2), with the relist reset;
- three processes, each with its own verified set (§6.8);
- **the safety invariant of §14 checked on every read.** A violation is
  allowed only if it is one of the listed exceptions, or the L3 window
  (§6.4), and the test asserts that the L3 window appears only where §6.4
  says it does. Convergence is checked separately: with no users left, the
  finalizer goes within a bounded number of steps;
- a **differential test** that runs PR 1's premise scenarios against the
  model and against envtest, and requires the same outcomes and classes.
  This is the check that the model is faithful;
- truth tables for the skip predicate and for the grace (the later of the
  `deletionTimestamp` and first sight; a server clock behind the local one; a
  local clock stepping back).

The model's self-write filter schedule (§14 "the epoch-only and label-only
self-write filter") lands with the filter in PR 4.

**Reviewer attack** (§14): the `written`/`observed` skip, and any way
`t_obs < t_issue` can hold for a record that wasn't visible when the write was
issued (clock source, leader change, away and back); R3's tests (the epoch,
the index, the full array); the §6.1 exclusion; the order of the re-reads
and the grace; whether anything widens the L3 window; whether
`recordSince` and `observed` can be made earlier than the record's arrival.
Also A1's copy of the returned `ObjectMeta`: a mutating webhook or
defaulting that changes the generation or labels must force a re-read before
the next write (L2 of #52's review). And the model itself: does every API
call yield to the scheduler, does any production path bypass `server` or
`views`, and do the enumerator's bounds cover every fault.

**Depends on:** PR 1. **Size:** about 1,300 lines of production code and
2,900 of tests, model included.

### PR 3: `Shared` and `Tracker` on client-go: LabelScoped Secrets, release loop, prober

**Scope.**

- `shared.go`, `track.go`, `setup.go`: `NewShared`, `Run`, and `Track`.
  `Track` is idempotent per GroupKind, can be called after start (it starts
  and syncs informers before returning), and validates everything in §7.3
  except the PerName items. It publishes `credential-use.krateo.io/config` on
  the holder CRD once (A4), with bounded retries (L4 of #52's review), and a
  disagreeing process refuses to start. It validates
  `DisownPasses × DisownInterval ≥ 2 × TimeoutBound` (A6). `NewShared`
  creates the (K, N) batch queue before any informer starts, so it buffers
  and deduplicates from the first event, grace timers included (M1 of #52's
  review);
- `informers.go`: the holder metadata informer with its transform and its
  index of records; the label-scoped, metadata-only protected informers with
  their transform; the Namespace metadata informer and its handler (the
  `deletionTimestamp` part, L5). The Namespace informer's sync gates `Run`
  (§7.5, L3);
- `listwatch.go`: the ListerWatcher wrapper that records when each list or
  relist completes, for both a List and a watch-list stream (A20);
- `server.go`: the client-go `server`, built on the metadata client, the
  dynamic client and the RESTMapper;
- `releaseloop.go`: a workqueue of (K, N) batches; enqueue from holder
  events, from protected events (at once, and at the end of the grace through
  `AddAfter`), and from the initial list (§10.2); R0's fast path from the
  index;
- `prober.go`: per-ref backoff from 15s to 5 minutes. Per (kind, namespace),
  three or fewer due names are GETs, more is one LIST, and a forbidden list
  falls back to GETs (§10.3);
- `janitor.go`: removing labels left without finalizers (§10.1);
- `events.go` and `metrics.go`: the core's event sink and metrics interface
  (A7), with nop defaults and throttling; `Users` for diagnostics;
- namespace-restricted caches with the live fallback (§10.5);
- `pkg/credentialuse/internal/driver`, test-only: a minimal reconcile loop
  that follows §7.2 to the letter (Acquire; a Connect that reads the Secret;
  a recorded external Delete; `BeforeEstablish`; `AddFinalizer`; `Forget`
  with `RemoveFinalizer`).

Cross-namespace refs are not held in this PR. That is the design's behaviour
for a target that hasn't consented (§11.3), and PR 5 adds consent. A ref with
`Next` or `Guaranteed` is an Acquire error until PR 4. The package is
unreleased and nothing imports it, so neither is visible to anyone.

**Design sections:** §7.1 to §7.3, §10.1 to §10.3, §10.5, §6.5, §6.6, §12
(the gauges and the protected-object events).

**Tests** (envtest with the driver, unless noted):

- **the krateo-057 teardown** (§14's five steps), at the core level, with the
  control (no tracker) wedging;
- U5 and the grace: held for the grace, AlreadyExists on a recreate during
  it, released after it, the new UID acquired through the prober; a Secret
  with no users released at once;
- a namespace deletion just after the grace, and a holder deletion between
  R2 and R3, with a test hook delaying R3;
- a user deleting past the grace keeps the Secret; a ref changed on update
  releases the old Secret;
- a Secret already being deleted gives a new holder `MustNotConnect` (the
  core part; deleting freely is checked in PR 7);
- `kubectl replace`-style stripping enqueues the holders, and they write
  again; a label loss is delivered as a deletion through the real informer;
- the prober: a GET with few names, one LIST with many, per-ref backoff;
- **a forced relist of the holder informer** (the wrapped watch returns a 410)
  resets `recordSince`, and the next passes write (L2). It runs on the
  watch-list path, and again with watch-list refused so that the reflector
  falls back to List (#52's review);
- the write loops' `Start` rescans the protected stores and enqueues every
  (K, N) batch; events seen before `Run` are buffered, deduplicated, and
  drained once it runs (M1 of #52's review);
- an R3 that fails against an unchanged resourceVersion is reported with
  `CredentialReleaseRejected` and `credentialuse_release_rejected_total`, and
  isn't retried as contention; a retry rebuilds the patch from R1's new read
  (L1 of #52's review);
- class (e) is counted in `credentialuse_admission_denied_total` (L3 of #52's
  review);
- the published settings: a second process with a different `deletionGrace`
  refuses to start; two `RefsFunc` versions stay safe, with the churn
  measured; both owner finalizer names accepted;
- start fails without `list` or `watch` on namespaces (an impersonated
  ServiceAccount);
- R0 stops for a deleting P with a deleting user in the cache; the Namespace
  handler ignores changes to anything but `credential-use.*` and the
  `deletionTimestamp`;
- `Track` after start, and twice with equal and with different Options;
- a namespace-restricted cache reads live outside its namespaces; an RBAC
  denial there fails closed for a new holder and open for an established one;
- **stress:** goroutines create, re-point and delete holders over a small
  pool of Secrets in several namespaces, and delete and recreate Secrets and
  namespaces. Two holder kinds, two `Shared` instances, and a variant with
  three and no leader election (§6.8). The invariant is checked on every
  read the driver makes, and at the end no object keeps a `credential-use.`
  finalizer with no users;
- **drain:** cancel between A1 and A2, between R1 and R3, and in the middle
  of a batch. Restart, and assert convergence with no violation.

**Reviewer attack** (§14): the label-scoped informer's handling of objects
that lose the label; the prober's cap and rotation; two processes for one
GroupKind with different version selectors (R2's list must have no
selector); relist detection with watch-list; handlers that never block the
informer; no lock across a call; goroutine and workqueue shutdown on context
cancel; `Track` after start racing `Run`; whether any event between informer
start and the write loops' `Start` can be lost, and whether the rescan
covers what the queue didn't (M1 of #52's review).

**Depends on:** PR 2. **Size:** about 2,300 lines of production code and
2,500 of tests, driver included.

### PR 4: Chains

**Scope.** Following a chain level by level, with each hop held before it is
read, A1 again with the union, and `MaxDepth` (§9.1); `WithResolved` and
`Resolved` (§9.2); `Guaranteed` refs, through `SharedOptions.Listers` or a
live GET (§9.3); content informers for kinds with `Content`, which are
dynamic and label-scoped, with a transform that drops `managedFields` and
`status` (§10.4); ConfigMaps and custom resources as protected kinds, with
A2's guarded merge, 3 retries, and Degraded for an established holder (M5);
enqueueing holders on a change to a held object with a `Next`, through the
self-write filter (§9.2, L2), and the narrower rule for kinds without a
`Next` (L7).

**Design sections:** §9.1 to §9.3, §10.4, §6.2 (A2 for custom resources).

**Tests.** A chain Widget → Target → Secret with the Target deleted first in
the teardown; a re-pointed Target enqueues its holders; the model's
self-write filter schedules (epoch-only, label-only, and a foreign change
equal in every other way); a custom-resource A2 conflict that retries, then
fails closed for a new holder and is Degraded for an established one; a
`Next` error, and a chain deeper than `MaxDepth`; `Guaranteed` refs: not
recorded, `Next` followed, read through a lister or live; without `Content`,
a `Next` on a Secret gets a metadata-only object; `Resolved` returns exactly
the object held in this pass, not a newer cached one; the stress test gains
Targets.

**Reviewer attack** (§14): the self-write filter. Can a foreign change be
taken for the tracker's own, so an enqueue is missed, or the reverse, so a
storm starts? Also hold-before-read on every hop; whether `Resolved` can hand
out an object that wasn't held; and the custom-resource guarded merge.

**Depends on:** PR 3. **Size:** about 800 lines of production code and 1,000
of tests.

### PR 5: The cross-namespace consent gate and the `unheld` annotation

**Scope.**

- `consent.go`: `allow-holders-from` as the union of the object's list and its
  Namespace's; `*` accepted on objects, and ignored on a Namespace with a
  Warning event. The gate runs on every pass: cached on a pass that skips A2,
  live before every A2 and every new hold. A drop needs a positive, live
  withdrawal; a failed read keeps the hold and requeues (A5); a NotFound
  means Missing;
- `unheld.go`: the annotation codec. Reasons `NotConsented` and
  `ConsentWithdrawn`, and `ReadDenied` (which PR 9 writes); `since`; at most
  16 entries, trimmed with the released holds first; the `More` entry;
- A1 writes the record and `unheld` in one patch;
- the shared live read of an unheld hop (per batch, 10s TTL), and `Resolved`
  carrying unheld hops (§9.2 L1, §9.4);
- the acquirer enqueues its own release batch after every cross-namespace A2
  (§10.2 L2);
- the Namespace handler's `credential-use.*` annotation branch;
- the `CredentialRefNotHeld` and `CredentialConsentWithdrawn` events, with
  the holder message §11.3 gives, and their metrics.

**Design sections:** §11.3, §9.4, §9.2 (L1), §10.2 (L2), §12 (the unheld
row).

**Tests** (§14 "Cross-namespace consent" and "The gate on every pass", except
their PerNamePoll and provisioner parts, which are in PR 9):

- no consent gives no record entry, no A2, `CredentialRefNotHeld`, and
  Connect as today;
- consent by object, by Namespace, and `*` on an object; `*` on a Namespace
  ignored; the union, and an object's list can't narrow its Namespace's;
- withdrawal releases the hold through the normal protocol;
- a concurrent A2 landing after the release is caught by the batch its own
  write enqueued (also a model schedule);
- both partly consenting chain shapes of §9.4;
- the Secret's own namespace deleted while an unheld holder lives, and its
  recovery; a cross-namespace user keeping a Secret whose namespace is
  terminating;
- the gate on every pass: a cached withdrawal confirmed live; one the live
  read contradicts; a failed live read; a restart with Namespace-level
  consent during a teardown, with the Namespace informer not yet synced; a
  skipping pass just after a new hold; a deleting holder always reading live;
- NotFound on the live confirmation gives Missing with no requeue loop;
- **a persistent consent-read failure under API-server overload** (an
  interceptor fails every consent GET): each holder keeps its hold, and its
  `RetryAfter` backs off from 1s, doubling to the poll interval, then
  returns to 1s after one read succeeds (M2 of #52's review);
- `Resolved` with an unheld hop: up to 10s on the old chain, never an unheld
  same-namespace Secret; one read per 10s for a shared hop;
- `unheld` written in the same patch as the record and never alone, capped at
  16 with `More`, trimmed with `ConsentWithdrawn` first; the holder event
  names the target, the namespace and the release;
- the stress test gains cross-namespace refs whose consent toggles.

**Reviewer attack** (§11.3, §14): can a drop race an A2 into a stranded or
unindexed finalizer? Can a stale or unsynced cache drop a hold? Can a tenant
get a finalizer onto an object that hasn't consented by any path (a `Next`
that returns a cross-namespace ref, `Guaranteed`, a hop's namespace)? Does the
10s shared read ever feed a release decision? Under a persistent consent-read
failure, does the backoff really bound the load, per (holder, ref), and does
it reset only on a successful read (M2 of #52's review)?

**Depends on:** PR 4. **Size:** about 900 lines of production code and 1,200
of tests.

### PR 6: Disown and `DisownPrefix`

**Scope.** `disown.go`: a field-selected watch on the holder CRD; parsing
`all` and lists of GroupKinds; Acquire returning every ref Unheld while the
switch is on (A13), re-read before each A2 (§10.6.8, interleaving 3); the
strip loop (records with a guarded merge, finalizers and epochs with R3's
patch without the user check, the labels); the live count through
label-selected, paginated metadata lists by marker, plus the holders that
still carry a record; the count fields, written only on change and guarded
(A4); settling after `DisownPasses` consecutive zero passes, a reset on
another process's non-zero count, and `"unknown"` when a count can't be made.
`disownprefix.go`: `DisownPrefix` (A8), which refuses while the CRD exists,
even while it is being deleted, and supports `DryRun`.

**Design sections:** §7.4, §11.5, §10.6.8 (interleaving 3, LabelScoped
part).

**Tests** (§14): Disown through the CRD annotation drains two `Shared`
instances to a live `held-count` of 0, including a namespace outside
`SharedOptions.Namespaces`; `DisownPrefix` after the CRD is deleted, its
refusal while the CRD exists, and `DryRun`; settling only after
`DisownPasses` consecutive zero passes, with a re-added finalizer resetting
the count; three `Shared` instances with no leader election, including one
that reads another's non-zero count; A1 before the switch with the A2 landing
after the strip (a hook holds the pass); a short `DisownPasses ×
DisownInterval` rejected at setup.

**Reviewer attack** (§14): Disown racing a still-running acquirer in a
rollout, and whether `DisownPasses × DisownInterval` really exceeds every
in-flight pass; races on the count fields; settling over a newer non-zero
count.

**Depends on:** PR 3, stacked on PR 5 to keep `acquire.go` from churning
between branches. **Size:** about 700 lines of production code and 900 of
tests.

### PR 7: The provider-runtime adapter and the reconciler wiring

**Scope.**

- `pkg/resource`: the `NamedFinalizer` interface, and
  `APIFinalizer.FinalizerName()` (§7.5, A17);
- `pkg/reconciler/credentialuse.go`:
  - `CredentialRefsProvider`, discovered in `WithExternalConnecter` and
    `WithExternalConnectDisconnecter` as `keyerOf` is;
  - the per-manager `Shared` registry and its runnables (A11, A12);
  - `WithCredentialUse` and `WithCredentialUseShared` (A11);
  - `CredentialUseSource`, a `TypedSyncingSource` whose `WaitForSync` waits on
    the handler registrations' `HasSynced`, and a no-op source when
    credential-use is off;
  - `IgnoreCredentialUseRecord` (A9);
  - the source start check (M4) and its 2-minute timer from election;
  - the nil-config fake-manager path, which logs once;
  - the `Enqueue` forwarding (A22);
- `pkg/reconciler/reconciler.go`, §8's nine steps:
  - the source check before the Get;
  - `Forget` on the orphan path, standalone when the owner finalizer is
    already gone;
  - Acquire after the orphan check and before the Connect gate, with a
    conflict returning `rateLimitedRequeue()`, new holders failing closed and
    established ones Degraded;
  - `pristine` taken after Acquire;
  - `WithResolved` on `externalCtx`;
  - `Forget` on the delete path, standalone when needed;
  - `BeforeEstablish` before `AddFinalizer`;
  - `RetryAfter` (A5);
  - the `MustNotConnect` message, and the events `CredentialUseDegraded` and
    `CredentialDeleting`;
  - the Synced suffix pointing at `unheld` (§12);
- `pkg/telemetry`: the otel implementation of the core's metrics (A7);
- `pkg/reconciler/reconcilertest`: `AssertCredentialRefsCover` and its
  recording reader (§11.9);
- `pkg/reconciler`: `CredentialUseCheckingReader` (A10);
- `pkg/credentialuse/e2e`, a test-only package with its own `TestMain`
  (A21): a manager running the real reconciler for a `Widget` CRD.

**Design sections:** §7.5, §8, §11.4, §11.7 to §11.10, §12.

**Tests.**

- **§14's namespace teardown with the real reconciler**, steps 1 to 5. That
  includes I1, a watch that checks every Widget status it delivers, and the
  control that must wedge;
- interleavings A and B (§5.1) with the real reconciler;
- a new holder on a Secret that is already being deleted deletes freely;
- start fails when `CredentialUseSource` isn't wired, both at the first
  Reconcile and on the 2-minute timer (shortened in the test);
- `IgnoreCredentialUseRecord` drops a record-only update this process wrote,
  and passes every other change, including a record change it didn't write
  (M6, L8);
- the Synced suffix is exact and under 160 characters; nothing is added on a
  successful pass or with an empty list;
- a foreground-deleted holder that owns its Secret completes. envtest has no
  garbage collector, so the test plays it;
- **no change for non-implementers:** a unit test with fakes asserts the same
  client calls as today, no `Shared`, and a no-op source (§11.10);
- with a fake manager's nil config, credential-use is off and the source is a
  no-op;
- a custom `WithFinalizer` without `NamedFinalizer` is a setup error at
  `Start`;
- the standalone `Forget` Updates on the orphan and delete paths;
- a governor deferral right after A1 compares against the `pristine` taken
  after Acquire; with saturation on, Acquire runs inside the slot and nothing
  new is gated;
- `AssertCredentialRefsCover` fails on an unnamed read, and on a path that
  resolves to nothing;
- a stress run with the real reconciler (smaller than PR 3's), and a drain
  that stops the manager in the middle of a pass;
- **failover** (M1 of #52's review): two managers under leader election. The
  leader removes a holder's owner finalizer and record, and stops before R3.
  The standby's informers see the holder go while it is still standby, and
  once elected it releases P. Again with a grace timer pending at the
  failover;
- after an A1 that a mutating webhook changed (labels or generation), the
  pass re-reads before its next write (L2 of #52's review).

**Reviewer attack** (§14): every path to `AddFinalizer`, and whether one can
be reached without an A2 or a live miss after the pass's last A2 (§6.7);
`Forget` in the same write on both paths, and the standalone write; fail-open
on deletion passes, and whether it ever turns a safe state into a wedge; the
source start check, including whether any controller-runtime path starts
workers before a source (`EnableWarmup`, A12); the per-manager registry
under concurrent `NewReconciler` calls; the runnables' leader-election
order; a failover between `Forget` and R3, or with a grace timer pending,
where a standby's informers saw the holder go before its write loops ran
(M1 of #52's review); A1 against mutating webhooks and defaulting (L2).

**Depends on:** PR 6. **Size:** about 900 lines of production code and 1,600
of tests.

### PR 8: PerName mode, the index ConfigMap

**Scope.** `index.go`:

- the line codec and sorting; the line cap and the 1 MiB byte check; the 80%
  Warning event and `credentialuse_index_fill_ratio`;
- the Dekker writes: add the line before A2 and live-GET the index after it;
  prune once per batch after R3 and live-GET each pruned P; merge on
  conflict; lines for Missing refs (A14);
- the pointer on the CRD, written once (A4), with start refused on
  disagreement;
- the `IndexNamespace` checks (it exists, and holds no holders) at `Track`
  and at acquire time; creating the index when absent;
- the startup recheck from the index, and the write loops' rescan of it at
  `Start` (M1 of #52's review);
- tamper detection, skipping lines whose R3 was seen;
- the rebuild by merging, after a deletion;
- field-selected per-name watches in place of the label-scoped informer;
- Disown's count and strip from the index and the records (A15);
- the prober doing GETs only.

**Design sections:** §10.6 (all of it), §7.3 (PerName errors), §7.4 (the
PerName count), §13 (shape 1).

**Tests** (§14):

- the pointer written once; a second process with a different index refuses
  to start;
- lines added before A2 and pruned after R3; a hand-deleted index rebuilt,
  with the residual found by its marker label;
- the reviewer's three interleavings (a restart between `Forget` and R3; a
  never-established holder deleted while the process is down; A1 before the
  switch, counted through the index);
- **the superset interleaving**, with interceptors holding each step, in
  both orders of H2's index GET and the prune, plus the startup prune
  against an A2. The same schedules also run in the model;
- the missing index created at start; a dropped line restored, with the
  event and the tampering metric; tamper checks skipping lines with a
  visible R3; a rebuild merging with a concurrent add; a missing or
  unreadable index keeping the count `"unknown"`;
- the 80% warning; a full index failing closed for a new holder and Degraded
  for an established one; setup rejecting a cap above the ceiling (A19) and
  an index namespace that holds holders;
- **the dedicated namespace and its admission policy:** start fails clearly
  when the namespace is absent. With the policy absent, the start check
  refuses a holder there. With the policy, its binding and its parameter
  ConfigMap applied:
  - Kubernetes' writers are allowed through impersonation (both service
    accounts, and `system:kube-controller-manager`), and the same names from
    other identities are denied;
  - deleting the namespace completes;
  - a dynamic `widgets-v1-2-0` ServiceAccount writes its `cu-index-*`;
  - a composition object is refused, as are a ConfigMap not named
    `cu-index-*` and a write from outside the management namespace;
  - installer's update of the Namespace object is allowed;
  - events are allowed;
- classes (b) and (c) with the real policy, including the missing-params
  message matched by text;
- `Watch: PerName` under `resourceNames` RBAC, through an impersonated
  ServiceAccount (envtest already runs `--authorization-mode=RBAC`, V8);
- drain: crash between A2 and its index post-check, and between the prune
  and its post-check.

**Reviewer attack** (§14): add-before-A2 and prune-after-R3 under conflicts
and crashes; the CRD pointer race between two first processes; the rebuild's
residual; tamper checks that could re-add a line another process rightly
pruned.

**Depends on:** PR 7, so the adapter exposes the mode. In the core it needs
only PR 6. **Size:** about 1,300 lines of production code and 1,600 of tests.

### PR 9: PerNamePoll mode and the provisioner

**Scope.**

- `poll.go`: no watch on a protected kind; held objects polled with GETs on
  the prober's backoff, back to 15s when one changes; the index polled the
  same way; a skip only after a live GET confirms the UID and the finalizer
  (§10.7.3); the self-enqueued release batch after every A2 (§10.2 L2);
- the class (d) retry (A2): `RetryAfter` of 0.5s, 1s, 2s and 4s per holder and
  ref, then, after about 8s, an Acquire error (§10.7.2, A5);
- `ReadDenied`: 5 consecutive 403s spanning at least 2 minutes drop the ref,
  with its event and metric (§11.3 L2);
- `pkg/credentialuse/provisioner`, a new package:
  - its inputs are every served kind's index, watched by name, the
    ClusterRole, cached through a watch by name, and a `ProvisionResync`
    fallback poll;
  - the desired grants are the union of the index lines;
  - grants are added in one guarded update per cycle;
  - a name is revoked only after live GETs find no finalizer with any served
    prefix, and a later re-read of every index finds no line;
  - it holds and grants only `get` and `patch`;
- the adapter returns `RequeueAfter` for `RetryAfter` without holding a
  worker.

**Design sections:** §10.7, §13 (shape 2), §11.3 (L2).

**Tests** (§14 "PerNamePoll", "The provisioner", the PerNamePoll parts of the
consent and gate lists, and L2):

- no watch is opened on a protected kind;
- holds under RBAC provisioned in order;
- an A2 that runs before its grant is a 403, reported as an Acquire error and
  never as Deleting;
- failed provisioning fails closed for a new holder and is Degraded for an
  established one;
- the 403 retry of about 8s, which holds no worker;
- the `written` skip happens only after the live GET;
- grants follow the index, coalesced to one update per cycle;
- the revoke interleaving ends with the name granted again within one cycle,
  and the finalizer never lost (envtest and model);
- no revoke while the object carries any served prefix;
- two kinds served from one ClusterRole;
- the provisioner is watch-driven, with its fallback poll;
- Disown and Degraded leave Connect working under the split ClusterRoles;
- withdrawal seen by the poll; the provisioner never grants an unheld ref;
- the gate's skip-pass live GET;
- five 403s over 2 minutes drop the ref (fake clock);
- an A2 enqueues its own batch.

**Reviewer attack:** the revoke proof, including two provisioner processes
racing on one ClusterRole; whether (d) can be confused with an admission 403
(A2); the written skip with its live GET; whether the coalescing ever delays
a grant past the acquirer's retry budget.

**Depends on:** PR 8. **Size:** about 1,200 lines of production code and
1,400 of tests.

### PR 10: `scaletozero.WithHeld` and `WithHeldIndex`

**Scope.** `pkg/scaletozero`, additively (A16, decided): new
`ReconcileWithOptions` and `ExistsWithOptions` take `...Option`. `Reconcile`
and `Exists` keep their signatures, and `Reconcile` calls
`ReconcileWithOptions` with no options. `ReconcileWithOptions` takes the
RESTMapper from `c.RESTMapper()`; `ExistsWithOptions` needs
`WithRESTMapper(m)` alongside `WithHeld`. `WithHeld` lists by K's marker label with
`Limit: 1`, stops at the first object with K's prefix, and pages past a hash
collision. A Disown switch that hasn't settled counts as an instance.
`WithHeldIndex` GETs index lines until one still carries K's prefix, and a
missing or unreadable index is an error, never false. The package comment
gains the new RBAC (`get` on the CRD, and `list` on the protected kinds or
`get` on the index).

**Design sections:** §11.6, §10.6.5.

**Tests** (§14): `Exists` with `WithHeld` stops at the first marked object
and pages past a forced collision (a foreign marker on an object without the
prefix); a Kind at zero replicas wakes for the switch; a missing or
unreadable index keeps `WithHeld` true through the error; the revert check
gains mutants for each rule.

**Reviewer attack:** any path that returns false on an error; paging past a
collision; reading the switch. Light review: there is no concurrency.

**Depends on:** PR 6 for the switch, and PR 8 for the index format. It is
stacked on PR 9. **Size:** about 250 lines of production code and 450 of
tests.

### PR 11: Docs and the log

**Scope.**

- `docs/configuration.md`: the options, annotations, labels and defaults;
- `docs/api.md`: the exported API of `credentialuse` and `provisioner`, the
  reconciler and `scaletozero` additions, and the metric instruments;
- `docs/usage.md`:
  - adopting the feature: `CredentialRefs`, the builder line, the predicate;
  - §13's RBAC text, and the consent text;
  - §5.2's remedy, and uninstalling (§11.5);
  - the Argo CD notes, the `DisownPrefix` Job recipe, and the runbook entry
    for the missing-params message;
  - the outside-runtime guide (§7.2);
- the admission policy sketch, as a reference installer copies;
- `docs/overview.md`: the new reconcile steps;
- `docs/index.md` and `docs/llms.txt`: entries for the design and this plan;
- the design doc: its status line, and a "what the build found" section like
  `design-local-slot-before-pop.md` §10, which records the choices in §5 of
  this plan;
- `docs/log.md`: the v1.13.0 entry.

**Depends on:** PR 10. **Size:** about 900 lines of docs.

## 4. Where each §14 test lands

| §14 item | PR |
|---|---|
| Namespace teardown, steps 1–5 and the control | 3 (core, driver); 7 (real reconciler) |
| U5 and the grace; recreate during and after; prober and new UID; no users released at once | 3 |
| Interleaving A, and its residual | 7 (3 at core level) |
| Interleaving B | 7 (3 at core level) |
| Namespace deletion just after the grace; holder deletion between R2 and R3 | 3 |
| A Secret deleted in a live namespace while a user is deleting | 3 |
| A ref changed on update | 3 |
| A cross-namespace user keeps a Secret whose namespace is terminating | 5 |
| A Secret already being deleted: `MustNotConnect`, deletes freely | 3 (core), 7 |
| Chain Widget → Target → Secret; re-pointed Target | 4 |
| `kubectl replace` stripping | 3 |
| A label-scoped watch delivers label loss as a deletion | 1 (premise), 3 |
| R3's failed `test`: the exact code | 1 |
| Disown through the CRD; `DisownPrefix` after deletion and its refusal | 6 |
| Start fails without `CredentialUseSource` | 7 |
| `IgnoreCredentialUseRecord` (M6, L8) | 7 |
| The prober: GET, LIST, backoff | 3 |
| PerName and the index; the three interleavings | 8 |
| Disown settling; reset; zero replicas; `DryRun` | 6; 10 (zero replicas) |
| Relist resets `recordSince` | 2 (model), 3 |
| The index superset, both orders, startup prune | 8 (envtest and model) |
| Missing index created; tamper; conflicting rebuild; missing index keeps `WithHeld` true and count `"unknown"` | 8; 10 (`WithHeld`) |
| 80% and full index; setup rejections | 8; 3 and 6 (`DisownPasses`) |
| N processes without leader election | 3 (stress), 6 (Disown) |
| PerNamePoll | 9 |
| Error classification (a)–(d), and a policy with no `reason` | 1; 8 (the real policy) |
| Deleting only from 422 with the finalizers cause | 1 |
| The provisioner | 9 |
| The 403 retry; the PerNamePoll skip | 9 |
| Published settings; two `RefsFunc` versions; both owner finalizer names | 3 |
| Tamper checks skip lines with a seen R3 | 8 |
| Cross-namespace consent | 5; 9 (provisioner, poll) |
| Provisioner with two kinds; watch-driven; 403 requeue without a worker | 9 |
| Disown and Degraded under split ClusterRoles | 9 |
| The gate on every pass | 5; 9 (PerNamePoll) |
| `Resolved` carries unheld hops | 5 |
| The dedicated index namespace and the admission policy | 8 |
| NotFound on live confirmation; five 403s; start without namespace list/watch | 5; 9; 3 |
| R0 with a deleting user; the PerNamePoll self-enqueue; union; trimming; Namespace handler; 10s read | 3; 9; 5; 5; 3 and 5; 5 |
| The unheld record and its pointer | 5 (record); 7 (suffix) |
| A foreground-deleted holder that owns its Secret | 7 |
| Setup after start; `Track` twice | 3 |
| `scaletozero.Exists` with `WithHeld` | 10 |
| `Watch: PerName` under `resourceNames` RBAC | 8 |
| Model: all schedules, HIGH-1, M1, grace timings, deletion pass with `written`, recreate behind a cached UID | 2 |
| Model: the self-write filter | 4 |
| Stress | 3 (Secrets); 4 (Targets); 5 (cross-namespace); 7 (real reconciler); 8 (PerName) |
| Drain | 3; 7 (manager stop); 8 (index post-checks) |

## 5. Ambiguities and contradictions, and what I'd choose

Each item names the section, what doesn't fit once coded, and my choice.
Items marked **(user)** change an API or a rule the design states. They are
repeated in §8.

**A1. §6.2, class (b) "message prefix `ValidatingAdmissionPolicy '`".** The
status message isn't prefixed that way. The dispatcher wraps the denial in
`admission.NewForbidden`, so the message reads `configmaps "cu-index-x" is
forbidden: ValidatingAdmissionPolicy '…' with binding '…' denied request: …`.
The bare text is in a cause the dispatcher appends, with no type and no
field (`policy/validating/dispatcher.go:300-317`; apimachinery
`errors.go:209-229`). *Choice:* recognise (b) and (c) by a cause whose
message starts with `ValidatingAdmissionPolicy '`, falling back to the
substring `is forbidden: ValidatingAdmissionPolicy '` in the message. PR 1
pins both.

**A2. §6.2, class (d) "403 with no `ValidatingAdmissionPolicy` prefix"
(user).** That also catches webhook denials, and NamespaceLifecycle's 403 on
a create in a terminating namespace (`lifecycle/admission.go:179`), for
example the index create. PerNamePoll would then retry those as RBAC
propagation. *Choice:* recognise (d) positively, by the authorizer's message
`User "…" cannot <verb> resource "…"`
(`endpoints/handlers/responsewriters/errors.go:60-80`). Every other 403 is an
admission denial, handled as (b): an Acquire error, never retried as RBAC.

**A3. §6.2, R3's failed `test` isn't in the table, and it is a 422 Invalid
too.** The patch handler returns `NewGenericServerResponse(422, …)`
(`endpoints/handlers/patch.go:423-426`), which discards json-patch's text,
leaving reason Invalid, the generic message "the server rejected our request
due to an error in our request", and no causes. A `remove` at an index that
has moved looks the same. *Choice:* errors from R3, from Disown's strips and
from the janitor are never classified with the A2 table. There, a 422 with
no causes and no policy text means "state changed": go back to R1, at most 5
times per batch, then requeue the batch with backoff. PR 1 pins the shape. It
can't be confused with (a), which needs the `metadata.finalizers` cause.

**A4. §7.3 and §10.6.1 "a patch that tests the annotation is absent", and §7.4
"a JSON patch that tests the field's previous value".** RFC 6902 has no test
for absence. json-patch v4 passes a `test` with `null` on a missing key only
when the parent map exists (`json-patch.v4@v4.13.0/patch.go:677-693`), which
is a library quirk, not the RFC. The first count write also has no previous
value to test. *Choice:* every CRD write (the config, the index pointer, the
count fields, `disown-settled`) is a GET, then a merge patch guarded by
resourceVersion. A conflict re-reads and decides again. That keeps every
guarantee (write-once, no older count over a newer one, `disown-settled` only
over `"0"`). It also conflicts with unrelated CRD writes, which only cost a
retry.

**A5. §7.1, `Acquired` can't carry a short requeue (user).** §10.7.2 (the 403
retry, 0.5s to 4s) and §11.3 (a failed live consent read keeps the hold "and
requeues") both need one. *Choice:* add `Acquired.RetryAfter`.

- During the 403 window, the pass stops before Connect for new and
  established holders alike, because §10.7.2 says "the pass returns", and
  returns `RequeueAfter` with no backoff. After about 8s an established
  holder is Degraded and proceeds. This overrides §8 step 5's
  "return the error so backoff applies" for that window only.
- After a failed consent read, the hold is kept, so the pass proceeds to
  Connect, and the adapter requeues at the smaller of `RetryAfter` and the
  poll interval.

**A6. §7.1 vs. §7.3, the timeout bound.** §7.3 validates
`DisownPasses × DisownInterval` against "the kind's largest reconcile or
request timeout" and publishes `timeoutBound`, but `Options` has no such
field. *Choice:* `Options.TimeoutBound`, which `Track` requires. The adapter
passes `timeout + reconcileGracePeriod` (90s by default).

**A7. §7.1 vs. §12, §10.6.4 and §11.3: events and metrics.** The core must
record events on holders, protected objects and the index, but `Clients` has
no way to, and it can't use provider-runtime's `event.Recorder`.
Two counters are named nowhere: the Degraded count (§6.2, §8: "counts the
metric") and `CheckingReader`'s count (§11.9). *Choice:* `Clients.Events`, a
client-go `tools/events` recorder (nil means no events), throttled in the
core. A `MetricsRecorder` interface in the core uses the design's names plus
`credentialuse_degraded_total{holder_kind,reason}` and
`credentialuse_unrecorded_reads_total{holder_kind}`. The otel implementation
lives in `pkg/telemetry`.

**A8. §7.4, `DisownPrefix`'s signature** lacks the `DryRun`, the explicit
index and the name list its text describes. *Choice:*
`DisownPrefix(ctx, c Clients, o DisownPrefixOptions) (DisownReport, error)`,
with `Holder`, `Protected`, `Index`, `Names` and `DryRun`.

**A9. §7.5, `reconciler.IgnoreCredentialUseRecord()` is package-level (user).**
It must compare with "what this process last wrote for that holder", which is
tracker state. A package-level predicate could only do that through hidden
process-global state. *Choice:* a method, `r.IgnoreCredentialUseRecord()`,
next to `r.CredentialUseSource()`. The builder line becomes
`For(&v1alpha1.MyResource{}, builder.WithPredicates(r.IgnoreCredentialUseRecord()))`.
The core exposes `t.IsOwnRecordWrite(old, new)` for outside runtimes.

**A10. §11.9, `credentialuse.CheckingReader(reader, tracker)`** sits in the
core, but the reader it wraps is a controller-runtime client, which the core
can't import (§7.1). *Choice:* the core exposes `t.CheckRead(holder, gk, ns,
name)`, and provider-runtime's wrapper is
`reconciler.CredentialUseCheckingReader`. Outside runtimes wrap their own
clients around `CheckRead`.

**A11. §7.5, how a consumer configures it.** "Implicit" wiring says nothing
about where per-kind settings (protected kinds, watch mode, grace) or
`SharedOptions` (namespaces, index namespace, listers) come from. *Choice:*

- `reconciler.WithCredentialUse(credentialuse.KindOptions)` sets per-kind
  settings. The default protects Secrets, LabelScoped;
- the implicit `Shared` uses the default `SharedOptions`. A provider that
  needs other values builds one and passes
  `reconciler.WithCredentialUseShared(s)`. §7.5 names that option for tests,
  and it is documented for production too;
- the per-manager `Shared` lives in a registry keyed by the manager, so a
  second `NewReconciler` on the same manager reuses it.

**A12. §7.5, the runnable that "needs leader election", and controller-runtime's
`EnableWarmup` (user).** With warmup on
(`pkg/config/controller.go:70-78`), sources start before election. The
source's `WaitForSync` would then wait on informers a leader-elected
runnable hasn't started, and time out. *Choice:* two runnables. The informers
are read-only, need no election (like the manager's cache), and start at the
first of the source's `Start` or `Run`. The release loop, the prober, Disown
and the janitor are leader-elected. Safety is unchanged (§6.8). The M4
2-minute timer counts from election.

**A13. §7.4, Disown's "returns every ref Unheld"** names no reason. *Choice:*
reason `Disowned`. It is never written to the `unheld` annotation, which
Disown strips with the record.

**A14. §10.6.2, index lines for Missing refs.** The line is added before A2.
When A2 then finds NotFound, nothing but R3 or the startup recheck prunes, so
lines for refs that never appear stay until a restart. *Choice:* keep the line
while some record names the ref (PerNamePoll needs its grant for the prober's
GET). Prune it in the normal batch once no record names it and the object
carries no finalizer of K, with the post-prune live GET unchanged.

**A15. §7.4, Disown under `resourceNames` RBAC.** The text says it "works from
the holders' records", and also that in PerName mode it reads the index.
*Choice:* strip and count from the union of the index lines and the records.
The index is the superset of the holds, and the records have to be stripped
anyway.

**A16. §11.6, `scaletozero.WithHeld(protected ...)` (user; decided
otherwise, see §8).**

- It must read the holder CRD's Disown switch. `Exists` has only a
  `client.Reader` and a GVK, and a CRD's name needs the plural.
- §11.6 names only `Exists`, but `Reconcile` is what keeps the controller up.

*Choice:* variadic options on both `Exists` and `Reconcile`, backward
compatible. The signature becomes
`WithHeld(mapper meta.RESTMapper, protected ...schema.GroupVersionKind)`,
with `Reconcile` able to take `c.RESTMapper()`. The alternative,
type-asserting a RESTMapper out of the reader, is a hidden requirement. The
owner also needs `get` on the CRD.

**A17. §8 steps 4 and 7, `Forget` "in the same Update".** That holds for
`APIFinalizer`, which writes the whole object. A custom `resource.Finalizer`
implementing `NamedFinalizer` might write only the finalizer list. *Choice:*
`NamedFinalizer`'s contract says `RemoveFinalizer` must write the object's
annotations along with the finalizer change. Where one doesn't, §6.1's
exclusion still keeps the stale record harmless, so this is the belt and
§6.1 the braces.

**A18. §6.2 A1, `client.MergeFromWithOptimisticLock`.** That is
controller-runtime, and the core is client-go only. *Choice:* the same patch,
built in the core: a JSON merge patch carrying `metadata.resourceVersion`,
sent through the metadata client. The returned resourceVersion and
annotations are copied onto the caller's object, which is otherwise
unchanged, because the patch touches only annotations.

**A19. §10.6.4, "about 17,000 lines".** *Choice:* the ceiling is the constant
17,000, and every write also checks the 1 MiB byte size, as §10.6.4 says.

**A20. §6.5 L2, "a relist resets `recordSince` … for every holder".** A
`ResourceEventHandler` can't see a relist. `processDeltas` turns `Replaced` and
`ReplacedAll` into ordinary `OnAdd`, `OnUpdate` and `OnDelete` calls
(`tools/cache/controller.go:823-870`, `:972-1002`). And with `WatchListClient`
on by default since client-go 1.35 (`features/known_features.go:141-144`), a
relist is a watch, not a List. *Choice:* the core builds its informers on its
own ListerWatcher wrapper. The wrapper records when each list completes:
when List returns, or when the `k8s.io/initial-events-end` bookmark passes
(`reflector.go:1061`). Then `t_obs = max(observed, recordSince,
lastListCompleted)`. That is the reset of every entry, in O(1), and it also
gives §6.3's restart rule (`t_sync`).

**A21. §14, where the reconciler envtests live.** §14 puts them in
`pkg/credentialuse` with a manager and the real reconciler. *Choice:* the
core's envtests stay in `pkg/credentialuse`, with the §7.2 driver. The ones
with the real reconciler go in a test-only package, `pkg/credentialuse/e2e`,
with its own `TestMain`, so that the core's tests don't depend on
`pkg/reconciler`.

**A22. §7.2 item 5, `Enqueue` is required by `Track`,** but in the adapter the
queue exists only once the controller starts `CredentialUseSource`, after
`NewReconciler` has called `Track`. *Choice:* the adapter's `Enqueue`
forwards to the queue the source gets in `Start`, and does nothing before
that. That loses nothing: no Reconcile runs before the source has started and
synced, and the controller's own `For` source delivers every holder once
anyway.

**A23. §10.2, two watches on the holder kind.** The controller-runtime cache
already watches the holder kind (typed), and the core adds a metadata
informer, so the holder memory is about twice §10.2's figure. *Choice:*
accept it, because the core can't use controller-runtime's cache, and state
it in the docs.

## 6. Premises

### Verified now in the module cache

| # | Premise | Where | Consequence |
|---|---|---|---|
| V1 | controller-runtime starts each source, then calls `WaitForSync` on syncing sources before starting workers | `controller-runtime@v0.25.2/pkg/internal/controller/controller.go:362-400`; `pkg/source/source.go:68-73` | M4's start check holds as §7.5 says. `EnableWarmup` is the exception (A12) |
| V2 | `ResourceEventHandlerRegistration.HasSynced` exists | `client-go@v0.37.2/tools/cache/shared_informer.go:268-276` | `WaitForSync` can wait on the registration |
| V3 | A relist can't be seen by a handler; watch-list is on by default | `tools/cache/controller.go:823-870`, `:972-1002`; `features/known_features.go:141-144`; `reflector.go:362`, `:1061` | A20's wrapper must cover both List and the watch-list stream |
| V4 | A failed JSON-patch `test` is a 422 Invalid with the generic message and no causes | `apiserver@v0.37.2/pkg/endpoints/handlers/patch.go:423-426`; `apimachinery@v0.37.2/pkg/api/errors/errors.go:476-478` | A3 |
| V5 | `ValidateNoNewFinalizers` is a `field.Forbidden` at `metadata.finalizers`, through `ValidateObjectMetaUpdate` | `apimachinery@v0.37.2/pkg/api/validation/objectmeta.go:126-134`, `:342` | class (a) as §6.2 says |
| V6 | A policy denial is wrapped by `admission.NewForbidden`, gets a bare-message cause, and defaults to reason Invalid and 422; a config error reads `failed to configure binding:` | `apiserver@v0.37.2/pkg/admission/plugin/policy/validating/dispatcher.go:75-100`, `:300-317`; `pkg/admission/errors.go:53-63` | A1 |
| V7 | The RBAC message is `User "…" cannot <verb> resource …`; NamespaceLifecycle's create denial is a 403 too | `apiserver@v0.37.2/pkg/endpoints/handlers/responsewriters/errors.go:60-80`; `pkg/admission/plugin/namespace/lifecycle/admission.go:179` | A2 |
| V8 | envtest's kube-apiserver already runs `--authorization-mode=RBAC` | `controller-runtime@v0.25.2/pkg/internal/testing/controlplane/apiserver.go:303`, `:339` | §14's RBAC envtests need no special start, only impersonation |
| V9 | json-patch v4's `test` with `null` passes on a missing key only when the parent exists | `gopkg.in/evanphx/json-patch.v4@v4.13.0/patch.go:677-693` | A4: not relied on |
| V10 | `GetConfig` disables client-side rate limiting when QPS is 0 | `controller-runtime@v0.25.2/pkg/client/config/config.go:101-104` | §6.6's backfill figures stand |
| V11 | kube-controller-manager's clients are `NewClient("root-ca-cert-publisher")` and `NewClient("service-account-controller")` | `kubernetes@v1.36.2/cmd/kube-controller-manager/app/certificates.go:269`, `core.go:618` | §10.6.1's identities stand; PR 8 impersonates them |
| V12 | The authorizer's message format is the same in `k8s.io/apiserver` v0.31.0, v0.33.0, v0.35.0, v0.36.5 and v0.37.2 | `pkg/endpoints/handlers/responsewriters/errors.go`, `forbiddenMessage`, in each version in the module cache | class (d)'s pattern holds from Kubernetes 1.31 to 1.37. The oldest version Krateo supports isn't stated anywhere found (installer's chart has no `kubeVersion`), so it is open for the user |

### To verify, in the PR that needs them

| # | Premise | PR | How |
|---|---|---|---|
| P1 | A strategic merge on a Secret through the metadata client merges finalizers as a set, and labels and annotations as maps, and the response decodes as `PartialObjectMetadata` | 1 | envtest premise test |
| P2 | A merge patch replaces a custom resource's finalizer list; a resourceVersion in the body gives a 409 | 1 | envtest |
| P3 | A label-selected metadata watch, with watch-list on, delivers label loss and gain as delete and add | 1, 3 | envtest |
| P4 | An A2 on an object being deleted with the finalizer passes, and labels and annotations stay writable | 1 | envtest |
| P5 | NamespaceLifecycle is enabled in envtest; `finalize` works; a namespace with no controller stays Terminating | 1 | envtest (§14 step 2 fails the test otherwise) |
| P6 | client-go returns a `*StatusError` with `Details.Causes` intact for a 422 through the metadata client | 1 | envtest |
| P7 | The metadata client supports watch-list semantics, or the reflector falls back to List (`reflector.go:362`), and A20's wrapper sees both | 3 | envtest with both paths forced |
| P8 | `SetTransform` runs before the store and the handlers, including for `ReplacedAll` (`the_real_fifo.go:162-168` says it does) | 3 | unit and envtest |
| P9 | A custom resource on apiextensions-apiserver gets the same class (a) error as a Secret | 1 | envtest |
| P10 | The dynamic informer for `Content` kinds takes the label selector and the transform | 4 | envtest |
| P11 | `WatchesRawSource` accepts a `TypedSyncingSource` and calls `WaitForSync`; what `EnableWarmup` does to it | 7 | envtest with a manager; A12 |
| P12 | controller-runtime v0.25.2's recommended event API for `mgr` (`GetEventRecorderFor` against the events/v1 recorder) | 7 | read the source; A7 |
| P13 | envtest has no garbage collector, so the foreground test plays it | 7 | envtest |
| P14 | A `metadata.name` field-selected watch is authorized by `resourceNames` (`requestinfo.go:245-250`) | 8 | envtest under RBAC |
| P15 | A policy's `namespaceSelector` matches the Namespace's own CREATE and UPDATE (`matcher.go:68-85`); `resources: ["*"]` doesn't match subresources (`rules.go:106-117`) | 8 | envtest with the policy |
| P16 | The policy allows exactly V11's identities, and `system:kube-controller-manager`, for those two names | 8 | envtest, impersonating each identity |
| P17 | Escalation prevention lets the provisioner grant only the `get` and `patch` it holds | 9 | envtest under RBAC |
| P18 | Monotonic times survive the paths they take (no `UTC()`, `In()` or `Round(0)`) | 2 | unit tests and a lint check in the revert check |

## 7. Size estimates

Lines are Go as `wc -l` counts them, comments included, estimated from the
scope above and from the sizes of comparable code here (`pkg/ratelimit/queue.go` is 472 lines
with 1,600 of tests; `pkg/scaletozero` is 347 with 1,040).

| PR | Production | Tests | Notes |
|---|---|---|---|
| 1 | 700 | 1,400 | includes the envtest harness (about 300) |
| 2 | 1,300 | 2,900 | the model is about 1,400 of that |
| 3 | 2,300 | 2,500 | the largest; the driver is about 300 of the tests |
| 4 | 800 | 1,000 | |
| 5 | 900 | 1,200 | |
| 6 | 700 | 900 | |
| 7 | 900 | 1,600 | reconciler changes about 350, adapter 450, telemetry 100 |
| 8 | 1,300 | 1,600 | includes the policy envtest |
| 9 | 1,200 | 1,400 | the provisioner is about 600 |
| 10 | 250 | 450 | |
| 11 | — | — | about 900 lines of docs |
| **Total** | **about 10,350** | **about 14,950** | |

PR 3 is the one most likely to need splitting during review. If so, the
prober and the janitor move to a PR 3b of about 500 lines.

## 8. The user's decisions

The user approved the plan on 2026-11-09, as eleven stacked PRs. Each PR
merges only with the user's approval, once it is green and independently
reviewed. No tag is cut until PR 11. The five choices, recorded in the design
as IP1 to IP5:

1. **A2 (IP1), accepted:** RBAC 403s are recognised positively, by the
   authorizer's own message, whose exact text an envtest pins. Every other
   403 is an admission denial: an Acquire error at once.
2. **A5 (IP2), accepted:** `Acquired.RetryAfter`. During the 403 window,
   established holders also stop before Connect, for up to about 8s. Building
   it into the design text showed it needs a second field,
   `Acquired.Pending`, so that the caller can tell "don't connect" from
   "proceed and requeue".
3. **A9 (IP3), accepted:** the predicate is a method,
   `r.IgnoreCredentialUseRecord()`, which changes the builder line in §7.5.
4. **A12 (IP4), accepted:** two runnables, the informers without leader
   election and the write loops with it.
5. **A16 (IP5), decided otherwise:** scale-to-zero is additive. `Reconcile`
   stays unchanged, and `ReconcileWithOptions(ctx, …, opts ...Option)` takes
   `WithHeld` and `WithHeldIndex`; `Reconcile` calls it with no options.
   `ExistsWithOptions` is added the same way, with `WithRESTMapper`, for
   owners that check outside a reconcile.

Every other item in §5 is an implementation choice within the design. All of
them are recorded in the design doc's §17, "Revision 16".
