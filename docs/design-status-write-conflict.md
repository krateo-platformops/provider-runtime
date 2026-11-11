---
type: Architecture
title: provider-runtime — status writes that conflict with a handler's own write
description: Design note for issue #80, options only. When an ExternalClient method persists the managed object mid-reconcile, the runtime's next status write fails with a conflict. Three options (document the contract, retry with re-read and merge, detect who wrote) are laid out from the code for the user to choose between.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, reconciler, status, optimistic-concurrency, conflicts]
timestamp: 2026-11-11T00:00:00Z
---

# Status writes that conflict with a handler's own write

**Status:** design note, options only, for
[#80](https://github.com/krateo-platformops/provider-runtime/issues/80).
On 2026-11-11 the user decided that a design note comes first, and that
they then choose an option. Nothing here is built. §11 is a recommendation
for the user to decide, not a decision.

**References.** Code is provider-runtime at `bf8ccf8` (main). The
credential-use protocol is the unmerged branch
`feat/credential-use-2-protocol` at `19e986c` (PR #59), read with
`git show` and not modified; `CU` below is its
`docs/design-credential-use-finalizer.md`. `CR/` is
`sigs.k8s.io/controller-runtime@v0.25.2`, `CG/` is `k8s.io/client-go@v0.37.2`,
`AS/` is `k8s.io/apiserver@v0.37.2` and `AX/` is
`k8s.io/apiextensions-apiserver@v0.37.2`, all read in the module cache at the
versions `go.mod` pins. `XP/` is `crossplane/crossplane-runtime` at tag
`v2.4.3` (its latest release, 2026-10-22), file
`pkg/reconciler/managed/reconciler.go`, read from GitHub. The consumers were
read on `main` with `gh api`:

| Consumer | Commit | provider-runtime pin |
|---|---|---|
| oasgen-provider (`go/oasgen-provider`) | `2c5d902` | v1.8.2 |
| incident-provider | `1becee0` | v1.8.2 |
| git-provider | `8bc9e3c` | v1.11.0 |
| alert-provider | `15417cd` | v1.14.1 |
| core-provider (`go/core-provider`) | `0e8dc06` | v1.14.3 |

§13 lists what was checked and what wasn't.

## 1. The problem

`Reconcile` reads the managed object once (`reconciler.go:801-802`) and
hands that pointer to every step of the pass. Every status write goes through
`statusUpdate` (`reconciler.go:850-855`), which calls the write it's given
once and records a metric. Each write is
`r.client.Status().Update(ctx, managed)`, with no conflict handling: a 409 is
wrapped as `cannot update managed resource status` and returned.

When an `ExternalClient` method persists the object itself, through a copy it
read fresh, the stored resourceVersion moves and `managed` still carries the
old one. The runtime's next status write is refused by the API server's
version check (`AS/pkg/registry/generic/registry/store.go:799-808`), on a pass
whose external work succeeded.

oasgen-provider measured it (oasgen-provider#579): one conflict per
RestDefinition whose `Update` wrote status, 25 of 25 kinds for one consumer
and 4 of 4 for another. Three things go wrong at once:
- the pass's conditions don't land;
- the pass returns an error, so controller-runtime logs `Reconciler error`
  and requeues with backoff (`CR/pkg/internal/controller/controller.go:484-497`);
- the message names optimistic concurrency, not the handler, so the consumer
  investigates its cluster.

oasgen-provider fixed its side in 0.59.0 by mirroring: after its own write it
copies what it wrote, and the new resourceVersion, onto the pointer the
runtime holds (`restdefinition.go:2158-2163`).

The issue names three options: document the contract (1), retry with re-read
and merge in `statusUpdate` (2), and detect "my handler wrote" against
"someone else wrote" (3). Its comment adds three constraints, which this note
keeps:
- **C1.** A resourceVersion-based detector is inverted by handlers that follow
  the contract. A mirroring handler makes the object look current.
- **C2.** oasgen-provider's mirror is the reference implementation of
  option 1.
- **C3.** Option 2 must not overwrite a real concurrent writer's changes.

## 2. What one pass writes

### 2.1 The invariant every locked write relies on

The pass's writes rest on one invariant: **`managed` is the object as
stored at the resourceVersion it carries, plus this pass's own changes.**
Every write that carries that resourceVersion is version-checked, so it
either lands on exactly that object or fails. The runtime keeps the invariant
for its own mid-pass writes:
- **The trace marker** is a merge patch locked on the read version, sent on a
  copy. On success only the new resourceVersion and the marker are copied
  back onto `managed` (`traces.go:377-390`). design-tracing §6.2 ("The write,
  with an optimistic lock") states why that's safe: the lock guarantees the
  stored object is the one the pass read plus the marker.
- **The create-pending annotation** is a plain `Update` that is meant to fail
  on a stale version: "we _want_ the update to fail if we get a 409 due to a
  stale version" (`reconciler.go:1275-1283`).
- **The finalizer add and remove** are `Update`s of `managed` itself
  (`pkg/resource/api.go:40-55`). The client decodes the response into
  `managed`, so the pointer stays current.
- **The credential-use record** (CU §6.2, A1) is a merge patch guarded by the
  holder's resourceVersion, and the whole returned `ObjectMeta` is copied
  onto the caller's object. If the returned generation or labels differ, the
  object is re-read before the pass's next write.

So the runtime already follows option 1's rule itself: whoever writes the
object mid-pass leaves `managed` current. A handler that writes a fresh copy
and doesn't mirror breaks the invariant for **every** later locked write of
the pass, not only the status write (§2.4).

### 2.2 Every status write

There are 20 call sites: 19 direct ones and the deferral's, which serves five
gates (`reconciler.go:945-950`, called at `:953`, `:997`, `:1166`, `:1264`,
`:1448`). Each is the last write of its path: every one is followed by a
`return`. "Preceded by" lists the provider calls that run earlier in the same
pass, so a handler write there would have moved the version.

| # | Line | Path | Conditions the site sets | Preceded by |
|---|---|---|---|---|
| 1 | `:876` | paused | ReconcilePaused | nothing |
| 2 | `:902` | orphan, RemoveFinalizer failed (not a conflict) | Deleting, ReconcileError | nothing |
| 3 | `:948` | governor deferral (`ratelimit.go:180-199`) | ReconcileDeferred, skipped when the object is unchanged from `pristine` (`ratelimit.go:191-194`) | Connect, Observe (by gate) |
| 4 | `:978` | Connect failed | NotObserved or Deleting (`ready.go:60-69`), ReconcileError | Connect |
| 5 | `:1033` | Observe failed | NotObserved or Deleting, ReconcileError | Connect, Observe |
| 6 | `:1089` | unresolved create, inside the recovery window | Creating, ReconcileError | Connect, Observe |
| 7 | `:1142` | observe-only, external resource absent | ReconcileError | Connect, Observe |
| 8 | `:1189` | Delete failed | Deleting, ReconcileError | Observe, Delete |
| 9 | `:1205` | Delete requested | Deleting, ReconcileSuccess | Observe, Delete |
| 10 | `:1225` | RemoveFinalizer failed (not a conflict) | Deleting, ReconcileError | Observe |
| 11 | `:1253` | AddFinalizer failed (not a conflict) | ReconcileError | Observe |
| 12 | `:1291` | create-pending Update failed (not a conflict) | Creating, ReconcileError | Observe |
| 13 | `:1344` | Create failed | Creating, ReconcileError | Create, then the re-read at `:1324` |
| 14 | `:1371` | critical annotations failed after Create | Creating, ReconcileError | Create, then the updater's failed re-reads |
| 15 | `:1384` | Create requested | Creating, ReconcileSuccess | Create, then the re-read at `:1367` |
| 16 | `:1409` | late-init Update failed (not a conflict) | ReconcileError | Observe |
| 17 | `:1426` | up to date | ReconcileSuccess | Observe, and the late-init Update if it ran |
| 18 | `:1441` | updates skipped by management policy | ReconcileSuccess | Observe |
| 19 | `:1470` | Update failed | ReconcileError | Observe, Update |
| 20 | `:1485` | Update requested | ReconcileSuccess | Observe, Update |

oasgen-provider's case is row 20.

Rows 10, 11, 12 and 16 follow an `Update` of `managed` that would itself have
conflicted if a handler had moved the version. A conflict there takes the
`IsConflict` branch and requeues before the status write (§2.4), so these rows
are reached only on other errors. Rows 13 and 15 follow
`UpdateCriticalAnnotations`, which re-reads the object into `managed`
(`api.go:63-76`). A write by `Create` is absorbed by that re-read, so these
rows don't conflict because of it.

### 2.3 What a status write carries

**On the wire, every status write carries the whole `.status`.** The
custom-resource status strategy copies the stored object and replaces only
its `status` with the one sent (`AX/pkg/registry/customresource/status_strategy.go:67-91`).
A status write can't change metadata or spec, and it can't merge.

**What the runtime itself changes is conditions only.** Every site sets
Synced, and 11 of the 20 also set Ready. No status write carries a
runtime-owned field other than conditions:
- **Rate-limit state** reaches the object only as the Synced condition
  `ReconcileDeferred` (row 3). The governor's windows and slots live in
  memory, in `pkg/ratelimit`.
- **Credential-use state** is an annotation on the holder, never status:
  "provider-runtime owns no status field in providers' CRDs" (CU §4). It's
  written by A1 and by the finalizer `Update`s (CU §8, steps 4, 5 and 7).
- `resource.Managed` exposes conditions only (`pkg/resource/interfaces.go:20-38`).
  The runtime can't name any other status field of a provider's kind
  without converting the object.

**What rides along is the provider's in-memory status.** The `Observe`
contract says "Status modifications are automatically persisted unless
ResourceLateInitialized is true" (`reconciler.go:252-257`). Consumers depend
on it:
- alert-provider's `Update` records the HyperDX alert and dashboard ids in
  status in memory (`alert.go:697`), and its later `Observe` reads them back
  (`alert.go:435`, `:526`). The runtime's write at row 20 is the one that
  persists them: "this pass's status write is the one that says the Alert is
  Synced" (`alert.go:699-700`).
- core-provider's `Observe` seeds Ready and relies on the runtime to persist
  it after an Observe error. The comment there reads "the reconciler persists
  conditions even when Observe returns an error"
  (`compositiondefinitions.go:818-819`, `:907-908`). That's row 5.

There are two exceptions. `Create`'s in-memory status is discarded by the
critical-annotation re-read (`reconciler.go:1360-1366`, `api.go:57-62`). And a
successful late-init `Update` resets `managed`'s status to the server's
(`reconciler.go:1390-1397`).

**So what a conflict loses is more than conditions.** It loses the site's
conditions, plus whatever status the provider computed in memory during the
pass, at every row after Observe except 13 to 15 (Create's re-read already
dropped it) and 17 after a late-init. On the error paths (rows 2, 4 to 8, 10
to 14, 16 and 19), the reconcile also returns the status error in place of
the pass's own. For example, at `:1033-1035` the Observe error is logged and
recorded as an event (`:1014`, `:1019`), but `Reconcile` returns `cannot
update managed resource status`, and that is what the queue and
controller-runtime's `Reconciler error` log see.

### 2.4 The other locked writes a moved version breaks

A handler write that moves the version breaks these too. Each one requeues on
a conflict through `rateLimitedRequeue()` (`requeue.go:13-15`), writes no
status, and records `k8s.conflict` with its step when tracing is on
(`pkg/tracing/names.go:44-62`):

| Write | Line | Runs after | When a writing handler makes it conflict |
|---|---|---|---|
| marker, `BeforeFirstAction` | `:1170`, `:1271`, `:1452` | Observe | Requeues before acting (D8) |
| handshake (pending marker removed) | `:1124` | Observe | Requeues; the stale handshake stays |
| RemoveFinalizer, delete path | `:1212` | Observe | Requeues; the finalizer stays |
| AddFinalizer, new objects only | `:1241` | Observe | Requeues; no finalizer is added, and nothing is created |
| create-pending | `:1283` | Observe | Requeues; nothing is created |
| late-init | `:1398` | Observe | Requeues; the spec isn't persisted |

All of these run after `Observe`, and none of them after `Update` or `Delete`.
That's why oasgen-provider's `Update`-time write hit only row 20. An `Observe`
that wrote unmirrored on every pass would be worse than losing conditions.
The object would never get its finalizer or be created, and a deleted object
would never release its finalizer. Each pass requeues with a conflict and
backs off, and no error is returned. The only trace is the `Error` logs at
`:1247`, `:1284` and `:1219`, and `k8s.conflict` with tracing on.

**Option 2, as the issue frames it, covers none of these.** They're locked on
purpose: the create-pending comment says so, and design-tracing §6.2 says why
for the marker. A rebase there would reopen exactly what the lock prevents
(design-tracing §6.2, "Why the lock").

### 2.5 The read is cached

`r.client` is the manager's client (`reconciler.go:737`), which reads from the
informer cache. So does `RetryingCriticalAnnotationUpdater`
(`reconciler.go:458`). It's the one existing re-read-and-retry in the
reconciler: it captures the runtime's own annotation keys, re-reads, writes
them back, and retries on any API error under `retry.DefaultRetry`
(`api.go:63-76`). That's five attempts about 10 ms apart (`CG/util/retry/util.go:28-33`).
A re-read through the cache can return the same stale version, so that retry
works when the cache catches up within about 50 ms. A retry in `statusUpdate`
(option 2) would need a live reader, `mgr.GetAPIReader()`. `pkg/scaletozero`
already takes one, for its own reasons (`scaletozero.go:54`, `:144`).

## 3. Interactions

### 3.1 The trace marker (design-tracing D8)

- **A mirroring handler is invisible to the marker.** The marker patch is
  locked on whatever version `managed` carries (`traces.go:384`). After a
  correct mirror, that version is the stored one, so the patch lands.
- **A non-mirroring `Observe` writer blocks acting** for the trace window.
  The `BeforeFirstAction` marker conflicts, and the pass requeues before it
  acts (`traces.go:356-359`). The memo entry stays `memoSeen`, so each pass
  until the window closes is a Parent pass that conflicts again. The default
  window is 10 minutes (`traces.go:24-29`). After it, `Decide`'s rule 5 writes
  the marker at `Now` (`reconciler.go:929`), before Observe, so that write
  lands. The pass then acts, and its status write conflicts (row 17 or 20).
- **Option 2 must not extend to the marker.** design-tracing §6.2 "Why the
  lock": an unlocked marker would let a pass that read a stale cache act a
  second time, with its act in R's trace.
- **Option 3's ledger (§9) sees the marker as a runtime write,** not a handler
  write. The marker already mirrors, so it never causes the conflict this note
  is about.

### 3.2 The credential-use protocol (#59, unmerged)

What the protocol assumes, from CU:
- **A1 runs before Connect** on every pass after the orphan check, and writes
  the record with a locked merge patch. It copies the returned `ObjectMeta`
  onto `managed` (CU §6.2, §8 step 5). Because it runs before any provider
  call, a handler write can't make A1 conflict in the same pass.
- **A2's skip** requires that "the pass works from H at resourceVersion *rv*,
  `delivered[H.uid]` is exactly *rv*" (CU §6.3). That's decided at Acquire,
  before any handler or status write. A status write later in the pass,
  rebased or not, doesn't change the version A2 decided on.
- **`Forget` lands in the `RemoveFinalizer` Update** of the delete path (CU §8
  step 7, and §7.5's `NamedFinalizer` contract). That write is row 10's
  predecessor, `:1212`, which is locked. A non-mirroring `Observe` writer
  makes it conflict on every pass, so the record is never forgotten, and the
  protected Secret's finalizer is never released while the holder lives on in
  deletion. Option 1 or 3 addresses that; option 2 doesn't (§2.4).
- **Status writes can't touch the record.** The status strategy keeps the
  stored metadata (`AX/.../status_strategy.go:77-81`), so no option here can
  revert or rewrite a record.
- **`IgnoreCredentialUseRecord`** compares only the record and the `unheld`
  annotation with what this process last wrote, setting aside
  resourceVersion and managedFields (CU §7.5). No option changes those.

None of the options needs a change to the protocol. Under option 2, the
rebase must read live, as in §2.5. A rebase that wrote into `managed` would
replace its annotations with the live ones, so it should write a fresh
object instead. Every status write is terminal, so nothing reads `managed`
after it in the pass.

### 3.3 The finalizer add and remove paths

`APIFinalizer` writes `managed` itself, so it mirrors by construction
(`pkg/resource/api.go:40-55`). Its conflicts already requeue (`:897-900`,
`:1220-1223`, `:1248-1251`). Under every option they stay as they are.

### 3.4 The governor's deferral

Row 3 runs after Observe when the Delete, Create or Update gate defers. So a
non-mirroring `Observe` writer makes the deferral's write conflict too, and
the pass returns an error instead of a clean deferral, which consumes failure
backoff. That works against the governor's rule that a deferral is not a
failure (`ratelimit.go:164-170`). `pristine` is taken before Connect
(`reconciler.go:940-943`), so after a mirrored handler write, `pristine` and
`managed` differ in resourceVersion and the write isn't skipped. That's
harmless: it costs one write.

## 4. What consumers do today

| Consumer | Writes the managed object from a handler? | How | Mirrors? |
|---|---|---|---|
| oasgen-provider | Yes: `Create`'s two phases and `Update` (`restdefinition.go:1023`, `:1535`, `:2044`) | `RetryOnConflict` over a fresh read, then the snapshot is applied onto `cr` and the written version is copied back (`:2122-2165`) | Yes, since 0.59.0: status fields and resourceVersion |
| incident-provider | Yes: `Update` (`incident.go:314`, `:324`), and `IncidentApply.Update` (`apply.go:419`) | `Status().Update(ctx, inc)`, `Update(ctx, inc)` and `Status().Patch(ctx, ia, MergeFrom(base))` on the pointer it was handed | By construction: the client decodes each response into `mg` |
| git-provider | Yes: `Create` and `Update`, via `SyncRepos` (`repo.go:525`, `:549`; `localresource.go:640-655`) | `Status().Update(ctx, cr)` on the pointer. On a conflict it re-reads into `cr`, keeps the pass's whole status, and retries (`repo.go:570-594`) | By construction |
| alert-provider | No. `Update` sets status in memory (`alert.go:697`) | It relies on row 20 | Not applicable |
| core-provider | No, from what was read. The re-reading helpers in `retry.go:28-106` have no call site in the files read (§13). A **separate in-process reconciler** patches `status.scaleToZero` on the same CompositionDefinitions (`scaletozero_reconciler.go:270-283`), with an unlocked merge patch, through `mgr.GetClient()` (`:339`) | Not a handler | Not applicable |

Three things follow:
- **No consumer needs a runtime change today.** The only one that wrote a
  fresh copy now mirrors.
- **Writing through `mg` is the simplest compliant form.** incident-provider
  and git-provider never hit this, because the client updates the pointer
  they write.
- **core-provider has a real concurrent status writer,** in the same process
  and through the same client. Today the runtime's whole-status write at rows
  17 to 20 conflicts after a `scaleToZero` patch, and the runtime's version
  check is what stops it from reverting the hold-down state. The patch itself
  carries no resourceVersion: `persist` (`scaletozero_reconciler.go:270-283`)
  sends a JSON merge patch, and `:242-247` documents its explicit nulls (a
  merge patch that omits a key leaves a stale `lastSeen`), not a version
  check. That's C3's case in production code. core-provider's session
  confirmed both claims at core-provider `0e8dc06` (2026-11-11).

**Two cautions for option 1's wording,** both found in this read:
- **Mirroring only the resourceVersion is wrong.** The issue says "(at least
  its resourceVersion)". A handler that writes field X on a copy and copies
  back only the version makes the runtime's next write, which is
  version-checked and carries `mg`'s whole status, succeed and silently revert
  X. That's worse than today's conflict. oasgen-provider mirrors both, and its
  own comment says the fields alone were "not enough"
  (`restdefinition.go:2161-2162`). The converse holds too: the version alone
  isn't enough.
- **Mirroring after a re-read can launder another writer's change.** oasgen
  re-reads (`cur`), writes `mutate(cur)`, and copies the new version onto `cr`,
  whose status is the pass's read plus `mutate`. If another writer changed
  status between the runtime's Get and oasgen's re-read, `cur` carries that
  change and `cr` doesn't. The runtime's next write is then version-current
  and reverts it. That's harmless while nothing else writes RestDefinition
  status (I found no other writer, but didn't search the cluster). It's the
  case design-tracing §6.2's lock exists for: a version may be copied back only
  when the write was locked on the version `mg` carries. git-provider's
  conflict branch has the same hazard in another form: it keeps the pass's
  whole status over a fresh read (`repo.go:584-592`). That's the subject of
  §8's variant 2a, inside a consumer. These are notes for those repos'
  sessions; nothing here changes them.

## 5. What controller-runtime and Crossplane do

- **controller-runtime** has no conflict handling in its client. A 409
  returned from `Reconcile` is logged as `Reconciler error` and requeued
  rate-limited (`CR/pkg/internal/controller/controller.go:484-497`). client-go
  offers `retry.RetryOnConflict`, whose documented pattern is to re-fetch,
  re-apply your change and retry, returning the conflict unchanged
  (`CG/util/retry/util.go:60-105`). It's a helper for the writer that owns the
  change; nothing calls it for you.
- **crossplane-runtime v2.4.3**, whose managed reconciler provider-runtime's
  derives from, behaves as provider-runtime does today:
  - `updateStatus` is a single `r.client.Status().Update(ctx, managed)`, with
    no retry (`XP/.../reconciler.go:995-1005`);
  - a conflict from any other write, or one returned by Observe, returns
    `Requeue: true` with a nil error (for example `:1019-1021`, `:1188-1190`);
  - a failed status write is wrapped as `errUpdateManagedStatus` and returned
    (for example `:1026`, `:1195`);
  - its `ExternalClient` contract is the one provider-runtime inherited.
    Observe "may update the supplied Managed resource ... Status modifications
    are automatically persisted", and Create's "other updates will be
    discarded" (`:387-400`). It also notes that "Create implementations are
    advised not to alter status" (`:1428-1437`).
  - Crossplane's model is that handlers change the object in memory and the
    runtime persists it. It doesn't expect handlers to write the managed
    object themselves, and has no mechanism for the case.

Neither retries the status write, and neither detects who wrote. Option 1
keeps provider-runtime where both are. Options 2 and 3 go beyond them.

## 6. Signals: what can tell "my handler wrote" from "someone else wrote"

Option 3 needs a signal, and option 2 can use one as a guard. Each candidate,
checked against the API server's behaviour:

- **resourceVersion on `mg`.** Inverted (C1). A mirroring handler makes `mg`
  current, so there's no conflict to classify, and a non-mirroring one is
  indistinguishable from any other writer.
- **The live object's resourceVersion against a ledger.** See "an explicit
  flag", below.
- **generation.** With a status subresource, a status write never changes
  `metadata.generation`; only spec writes do. So generation can't see a status
  write at all, whoever made it. It does see spec writes, but those come from
  handlers too: incident-provider's `Update` writes `spec.applied`
  (`incident.go:323-326`). Generation can't attribute a write. It can say that
  **the pass's input changed**, which is what a rebase must refuse (§8).
- **managedFields manager.** When a client sends no field manager, the API
  server derives one from the user agent's prefix
  (`AS/pkg/endpoints/handlers/create.go:259-264`). The runtime, every handler
  using the manager's client, and every other controller in the same binary
  share it. core-provider's scale-to-zero reconciler and its
  CompositionDefinition handlers both use `mgr.GetClient()`. So the manager
  can tell "another program wrote" (kubectl, another provider), but not "my
  handler" from "my process's other controller". It works only if every
  handler sets a distinct `client.FieldOwner`, which is a handler change, so
  an explicit flag in another form.
- **managedFields time.** There's one entry per manager, operation and
  subresource, with one timestamp (`metav1.ManagedFieldsEntry.Time`) at
  one-second resolution. It can't order two writes inside the same second, or
  two writers that share a manager.
- **An explicit flag (a write ledger).** The runtime gives each handler call a
  recorder in its `ctx`, or a client wrapper built on controller-runtime's
  `client/interceptor`, which the repo already uses in tests
  (`tracing_envtest_test.go:16`). It records, for each write to the managed
  object's key, the version the write was sent with and the version it
  produced. On a conflict the runtime reads live and checks the chain: the
  first recorded write was sent with the version the pass read, each later one
  with the previous one's result, and the last result is the live version. If
  so, only this pass wrote. If not, someone else wrote, at the gap.

  **A handler can't erase this by accident.** Mirroring changes `mg`, not the
  ledger, which avoids C1. Its limit: it covers only writes made through it. A
  handler that writes with its own client is "unknown". It also proves only
  that nobody else wrote; it says nothing about the content of the handler's
  write.

## 7. Option 1: document the contract

### 7.1 Exact behaviour change

**Documentation.** The `ExternalClient` comment (`reconciler.go:241-295`)
gains a rule that applies to all four methods:

> A method that writes the managed object to the API server must leave `mg`
> describing what is stored. Write `mg` itself, so the client decodes the
> response into it. Or write a copy that carries `mg`'s resourceVersion, then
> copy the new resourceVersion and every field you wrote onto `mg`. Never
> copy only the resourceVersion: the runtime's next write would succeed and
> revert your write. A method that re-reads before writing must copy the
> whole written object onto `mg`, not only its version, or it can revert
> another writer's change. A write that leaves `mg` stale makes the runtime's
> next write of the object fail with a conflict. After Observe, that includes
> the finalizer, create-pending and marker writes as well as status.

This replaces "(at least its resourceVersion)" in the issue's option 1, for
the reasons in §4.

**The status-conflict error.** At every status write site, a conflict
returns a message that names the likely cause and keeps the pass's own error:

```
cannot update managed resource status: the object changed during this pass
(read at resourceVersion V): if an ExternalClient method wrote it, the method
must leave mg current (see ExternalClient); pass error: <the error the pass was
returning, if any>
```

`errUpdateManagedStatus` stays the prefix, so existing searches still match.
The pass also records `k8s.conflict` with a new step `status`
(`pkg/tracing/names.go:44-62`), and `tracing_logs_test.go:175` counts one more
`IsConflict` site.

**It stays an error, by recommendation (§11, Q2).** The alternative is
`rateLimitedRequeue()` with a nil error, as the other conflict sites do
(§2.4). That would silence a deterministic contract breach, and oasgen
found this one through the error.

**Optional diagnostic read** (borrowed from option 3, §9, and Q3). On the
conflict, one live `Get` through an APIReader adds to the message:
- the live resourceVersion;
- which top-level status keys differ from the read;
- the manager and time of the newest status entry in managedFields, labelled
  as a hint.

Nothing is decided from it.

**A provider test helper.** It sits next to `reconcilertest.AssertObserveSetsReady`
(`reconcilertest/reconcilertest.go:32`): `AssertLeavesManagedCurrent` runs a
method against an envtest or fake object, then writes `mg`'s status, and
fails if that write conflicts or reverts a field the method wrote.

### 7.2 Failure scenario

- **A new consumer writes a fresh copy and forgets to mirror.** It gets
  today's symptom: the pass's conditions and in-memory status are lost and the
  pass errors. The message now names the cause.
- **A consumer mirrors only the resourceVersion.** The runtime's write
  succeeds and reverts the consumer's own write, silently. The doc and the
  test helper are the only defences.
- **An `Observe` that writes unmirrored on every pass** stalls the object at
  the locked writes of §2.4, with no error returned. That's unchanged by
  option 1, beyond the doc. The conflict events and `Error` logs there could
  carry the same hint; that's a small addition.

### 7.3 Compatibility

- **No behaviour change for a compliant consumer.** oasgen (v1.8.2) mirrors.
  incident-provider (v1.8.2) and git-provider (v1.11.0) write through `mg`.
  alert-provider (v1.14.1) and core-provider (v1.14.3) don't write `mg` from
  handlers.
- **The error text changes** after the existing prefix. oasgen's issue lists
  `cannot update managed resource status` among its searchable symptoms, and
  that still matches.
- **Each consumer gets it when it bumps its pin.** The doc applies to all of
  them now.
- **Tell oasgen-provider before it lands (C2).** Under option 1 its mirror is
  required behaviour, not redundant, and the re-read caution of §4 is worth
  relaying to it.

### 7.4 Tests that would prove it

1. **envtest:** a handler that writes `mg` through the client, in Update, then
   the runtime's row 20 write lands, with the handler's field and the
   runtime's ReconcileSuccess both stored.
2. **envtest:** a handler that writes a copy locked on `mg`'s version and
   mirrors the version and fields, then the same as test 1.
3. **envtest, the negative that pins the doc:** a handler that mirrors only
   the version. The runtime's write succeeds and the handler's field is
   reverted. The test asserts the revert, so the documented rule has a test
   behind it.
4. **unit (`pkg/test` MockClient, `MockStatusUpdate`):** a status conflict on
   an error path returns a message with the step's own error (the Observe
   error at row 5), and the `errUpdateManagedStatus` prefix.
5. **unit:** a status conflict records `k8s.conflict` with step `status`. The
   AST test's count goes from 9 to 10.
6. **unit, if the diagnostic read is taken:** the message names the live
   version and the changed keys, and no decision depends on it.
7. **`reconcilertest.AssertLeavesManagedCurrent`'s own tests:** it passes for
   tests 1 and 2, and fails for test 3 and for a non-mirroring handler.

### 7.5 Signal

None is needed. Option 1 decides nothing, and the diagnostic read only
describes.

## 8. Option 2: retry with re-read and merge in `statusUpdate`

### 8.1 Exact behaviour change

On `IsConflict` from a status write, the runtime reads the object live
(APIReader, §2.5) into a fresh object, rebases the pass's status changes onto
it, and writes again, locked on the live version, a bounded number of times.
What "the pass's status changes" means decides everything. There are three
variants:

- **2a. The whole in-memory status** goes onto the live object. It's rejected:
  it reverts core-provider's `status.scaleToZero` (§4), and in general any
  concurrent status write. That violates C3. git-provider's conflict branch
  does this inside a handler (§4).
- **2b. Only the conditions the site sets.** The site passes them
  (`r.writeStatus(ctx, managed, conds...)` at all 20 sites), and the rebase
  calls `SetConditions` on the live object. A concurrent writer's other fields
  and other condition types survive. But the provider's in-memory status is
  dropped on the rebase, while Synced=ReconcileSuccess lands. So the
  condition claims success for a pass whose data didn't land. alert-provider's
  ids (§2.3) are the case. Today neither lands, and the error retries.
- **2c. A three-way rebase.** A deep copy of `managed` is taken right after
  the Get, as the read. On a conflict:
  1. **Refuse to rebase** if the live object's UID, generation, deletion
     timestamp or pause annotation differ from the read. The pass's decision
     was made on inputs that changed. A refused rebase falls back to today's
     behaviour, with option 1's message.
  2. **Compute two deltas** over the status, as JSON leaf paths: the pass's
     (read to `managed`) and the other writer's (read to live). Conditions
     count by type, and other lists are atomic.
  3. **Refuse** if any path is in both deltas with different values. A path
     in both with equal values is compatible: a mirroring handler wrote the
     same thing (C2: the mirror becomes redundant, but harmless).
  4. **Apply the pass's delta onto the live object,** conditions through
     `SetConditions`, and write it locked on the live version.
  5. **On another conflict, repeat from the live read,** at most a few times
     (for example 3), then fall back.

Only status writes change. The locked writes of §2.4 stay locked: the marker
is excluded explicitly (§3.1). The reconciler needs a live reader, so
`NewReconciler` would take `m.GetAPIReader()`, or a new
`WithAPIReader` option.

### 8.2 Failure scenario

- **2c, semantic coupling.** The pass computes status field B from field A as
  it read it, and a concurrent writer changes A. The deltas are disjoint, so
  2c writes B, and the stored object has a new A beside a B derived from the
  old A. A generic merge over an opaque status can't know that B depends on A.
  That's exactly what the version check prevents today. core-provider is
  such a case: its Observe reads `status.scaleToZero`. `Observe`
  (`compositiondefinitions.go:854`) calls `controllerConditions` (`:1290`), and
  so does `observeFollower` (`controllerowner.go:634`). That reaches
  `controllerReadiness` (`scaletozero.go:67`), which reads `LastSeen`, `HeldBy`
  and `Reason` to compose the Ready and ControllerActive messages. Observe
  writes nothing to `status.scaleToZero`, but `cr.Status.ScaleToZero` is part
  of the status the runtime writes back whole. So a retry that re-applied the
  pass's view without a fresh read would write a stale `scaleToZero` over the
  reconciler's patch. A rebase that kept the fresh `scaleToZero` could still
  store conditions composed from the stale one: coupling, one pass long.
  (Confirmed by core-provider's session, 2026-11-11.)
- **2c, a handler that wrote a different value.** The handler sets Ready to
  Creating on a copy without mirroring, while Observe had set Ready to
  Available in `managed`. The deltas collide, so the rebase is refused and the
  result is today's. 2c helps only handlers whose write is disjoint from the
  pass's other changes, or equal to them.
- **2b:** Synced=ReconcileSuccess stored next to the previous pass's data
  (above).
- **All variants:** a writing `Observe` still stalls at §2.4's locked writes,
  so option 2 can't replace option 1's contract for `Observe`. Any variant
  also adds one live read per conflict, and a write that lands causes one
  more watch event and pass.

### 8.3 Compatibility

- **oasgen's mirror becomes redundant but harmless** under 2c, and harmful
  under no variant. Tell oasgen-provider before it lands (C2).
- **incident-provider, git-provider and alert-provider** are unaffected unless
  a real concurrent writer appears, and then 2c lands writes that conflict
  today.
- **core-provider:** 2c lets the CompositionDefinition reconciler's
  status write land after a `scaleToZero` patch, keeping `scaleToZero`. That
  changes behaviour, from conflict-and-retry to merge. That's the coupling
  question above.
- **Tests that build the reconciler on `pkg/resource/fake.Manager` would panic**
  if `NewReconciler` called `m.GetAPIReader()`. The fake embeds a nil
  `manager.Manager` (`fake/mocks.go:115-121`). The fake would need a
  `GetAPIReader`, and a consumer with its own fake manager would need one too.
  A `WithAPIReader` option that defaults to `r.client` avoids the panic, but
  then the default rebase reads the cache, which works only when the cache
  has caught up.
- **The metric `IncStatusUpdateFailure`** (`reconciler.go:566-574`) should
  count a rebased write once. Otherwise dashboards that alert on it change
  meaning.

### 8.4 Tests that would prove it (2c)

1. **envtest:** a handler writes a disjoint status field on a fresh copy
   without mirroring. One rebase lands, and both the handler's field and the
   runtime's conditions are stored.
2. **envtest, C3:** a concurrent writer patches a disjoint field (a
   `scaleToZero` analogue) between the Get and the write. The rebase lands and
   the concurrent value survives.
3. **envtest, C3:** a concurrent writer changes the same field, or the same
   condition type, to another value. The rebase is refused, and the concurrent
   value survives.
4. **envtest:** a spec change (generation bump), a deletion timestamp, or a
   pause annotation between the Get and the write. The rebase is refused.
5. **unit:** the live read goes through the APIReader. A cached reader that
   returns the stale version fails the test.
6. **unit:** a writer that writes on every attempt. The rebase gives up after
   the bound and falls back.
7. **stress:** N writers on disjoint and overlapping fields, against the
   reconciler, many rounds. No write is lost on a disjoint field, and no write
   is overwritten on an overlapping one. That's the independent
   concurrency-review bar this repo has set before.
8. **The marker is never rebased:** the marker's envtest tests
   (design-tracing §10, tests 12 and 13) stay green unchanged.

### 8.5 Signal

As a guard only: **generation, deletion timestamp and the pause annotation**
for "the pass's input changed", and **the two deltas** for "the changes
collide". None of these identifies the writer, and 2c doesn't need to know
who wrote.

## 9. Option 3: detect "my handler wrote" against "someone else wrote"

### 9.1 Exact behaviour change

- **A ledger.** The runtime puts a write ledger in each handler call's `ctx`,
  and offers a wrapper,
  `reconciler.TrackWrites(ctx, c client.Client) client.Client`, built with
  `client/interceptor`. It records the version each write to the managed
  object's key was sent with and the one it produced (§6). A handler that
  writes through its own client can record by hand with
  `reconciler.RecordWrite(ctx, obj, sentRV)`.
- **On a conflict at any runtime write after a handler call,** status or a
  locked write of §2.4, the runtime reads live once and classifies:
  - **handler-only:** the ledger chain runs from the read version to the live
    one. The message names the method and the rule: "Update wrote this object
    and didn't leave mg current".
  - **another writer:** the chain is broken. The message says so, with the
    managedFields manager and time as hints.
  - **unknown:** there's no ledger entry. The message is option 1's.
- **3a, report only.** Behaviour otherwise stays as today.
- **3b, report and rebase.** On handler-only, for status writes only, it
  rebases as 2c's steps 2 to 4 do. The chain already rules out another
  writer, so step 1's guard is needed only against the handler's own spec
  writes. The collision rule still applies, because the handler may have
  written a different value from `managed`'s.

### 9.2 Failure scenario

- **A handler that doesn't use the wrapper is "unknown",** so 3 degrades to
  option 1, for exactly the handlers most likely to be careless.
- **The chain proves the order of writes, not their content.** A handler
  that re-reads (oasgen's pattern) sends its write with the re-read version.
  If another writer wrote in between, the chain shows the gap and classifies
  correctly as "another writer". But a handler that mirrors after such a
  re-read never conflicts, so the laundering of §4 goes undetected under every
  option.
- **The wrapper must be a no-op outside the pass** (ctx lifetime), and must
  pass every other call through unchanged, including writes to other objects.
  A wrapper kept past its pass, as `Connect`'s ctx was warned against in
  design-tracing §4.4, would record into a finished ledger.

### 9.3 Compatibility

- **Additive.** A new function in `pkg/reconciler`. Nothing changes for a
  consumer that doesn't adopt it.
- **oasgen, incident-provider and git-provider never reach the classifier:**
  they don't conflict.
- **3b inherits 2c's compatibility notes** (the APIReader, fake.Manager and
  the metric).

### 9.4 Tests that would prove it

1. **unit:** a handler writes through `TrackWrites` without mirroring. The
   classification is handler-only, and the message names the method.
2. **envtest:** a handler writes through `TrackWrites` and another writer
   writes before it. The classification is "another writer". Repeat with the
   other writer after it.
3. **unit:** a mirroring handler through `TrackWrites`. No conflict, so
   nothing is classified.
4. **unit:** a handler writes with a plain client. The classification is
   "unknown".
5. **property test over interleavings** of the runtime's writes, the
   handler's writes and an outside writer's. "handler-only" is never reported
   when an outside write happened.
6. **unit:** a `TrackWrites` client used after its pass ends records nothing
   and passes calls through.
7. **For 3b:** 2c's tests 1, 3 and 4, plus test 7 restricted to the
   handler-only classification.

### 9.5 Signal

**The ledger: sent and produced versions per write, recorded by the wrapper
at write time.** A handler can't erase it by mirroring, which avoids C1.
managedFields manager and time are hints for the message only, for the
reasons in §6. Generation is used only by 3b's guard.

## 10. The options side by side

| | 1. Contract | 2c. Three-way rebase | 3a. Ledger, report | 3b. Ledger, rebase |
|---|---|---|---|---|
| Conditions land after a non-mirroring Update writer | No (correct error) | Yes, if the changes don't collide | No (precise error) | Yes, if the handler used the wrapper and the changes don't collide |
| Stalls at the locked writes after Observe (§2.4) | Documented | Unchanged | Diagnosed | Diagnosed |
| Can revert a real concurrent writer (C3) | No | Only through semantic coupling (§8.2) | No | No |
| Signal | None | Generation and deltas as a guard | Ledger | Ledger, plus the guard |
| Code touched | Doc, 20 error paths' message, one Step | 20 sites, APIReader, deep copy per pass, merge | New wrapper, classifier, message | 3a plus 2c's merge |
| Consumer change needed | Only for non-compliant handlers (none today) | None | To benefit: use the wrapper | To benefit: use the wrapper |
| Precedent | controller-runtime, Crossplane | None found | None found | None found |

## 11. Recommendation, for the user to decide

**I recommend option 1, with option 3's diagnostic read in its message, and
not option 2, for now.** Reasons, from the sections above:

1. **The contract is needed whatever is chosen.** A handler write that leaves
   `mg` stale breaks every locked write after it, not only the status write
   (§2.4). Those writes are locked on purpose (§2.1, design-tracing §6.2), so
   no option may rebase them, and option 2 can't fix them. Handlers have to
   follow the rule anyway. Option 2 would only make a breach invisible at the
   one site where it's cheapest to see, and §3.2 shows a case where the
   unseen breach stalls credential release.
2. **The fleet already complies.** oasgen mirrors, incident-provider and
   git-provider write through `mg`, and alert-provider and core-provider
   don't write `mg` from handlers (§4). No consumer needs option 2's
   behaviour today.
3. **Option 2's safe form is a generic merge over status the runtime doesn't
   understand.** It's sound for disjoint, independent fields, and wrong for
   coupled ones (§8.2). The one real concurrent status writer in the fleet,
   core-provider's scale-to-zero, is the kind of state where a wrong merge
   does damage. 2a violates C3, and 2b stores a success condition without its
   data.
4. **controller-runtime and Crossplane don't retry the status write** (§5).
   Option 1 keeps provider-runtime's semantics where they are, and changes
   only what the user sees.
5. **What option 1 costs is small:** the doc, the message on 20 error paths,
   which also stops a status conflict from hiding the pass's real error
   (§2.3), one new Step, and a test helper. It needs no new dependency on the
   manager.

**What option 1 doesn't give.** A future handler that forgets to mirror still
loses one pass's conditions and in-memory status per write, as oasgen did,
though now with a message that names the cause. If the user wants that case
to self-heal, the version to build is 2c, for status writes only, with §8.4's
tests including the stress test. A fresh reviewer would need to attack the
merge.

**Whichever option is chosen,** tell oasgen-provider before it lands (C2),
and relay §4's two cautions to the oasgen-provider and git-provider sessions.

## 12. Open questions for the user

- **Q1.** Which option: 1, 2c, 3a or 3b?
- **Q2.** Should a status conflict stay an error, as recommended (§7.1), or
  become a rate-limited requeue like the other conflict sites (§2.4)?
- **Q3.** Is one live `Get` per status conflict acceptable for the diagnostic
  message? It needs an APIReader, with the fake.Manager note of §8.3.
- **Q4.** Should the `ExternalClient` doc also cover the locked writes after
  Observe (§2.4)? Should their conflict logs carry the same hint?
- **Q5.** Should `reconcilertest.AssertLeavesManagedCurrent` ship with
  option 1?

## 13. Checked, and not

**Checked in source:**
- every `statusUpdate` site, and every `IsConflict` site, in `reconciler.go`
  and `traces.go`;
- `api.go`, `ratelimit.go`, `ready.go`, `requeue.go`, `pkg/resource/api.go` and
  `pkg/tracing/names.go`;
- the status strategy (`AX/.../status_strategy.go:67-91`), and that custom
  resources refuse unconditional updates (`AX/.../strategy.go:268-271`);
- the store's version check (`AS/.../store.go:799-808`), and the default field
  manager (`AS/.../create.go:259-264`);
- controller-runtime's error path, and client-go's `RetryOnConflict` and
  `DefaultRetry`;
- crossplane-runtime v2.4.3's managed reconciler;
- CU §4, §6.2, §6.3, §6.6, §7.5 and §8 on the credential-use branch;
- in oasgen-provider, `restdefinition.go`;
- in incident-provider, `incident.go` and `apply.go`;
- in git-provider, `repo.go` and `localresource.go`;
- in alert-provider, `alert.go` and `reanalysis.go`;
- in core-provider, every non-test file in
  `internal/controllers/compositiondefinitions` and its `helpers/status`.

**Checked after the first draft, by core-provider's session** (at
core-provider `0e8dc06`, 2026-11-11):
- No CompositionDefinition handler writes the managed object on the reconcile
  path. `retry.go`'s two helpers (`:28`, `:66`) have no non-test call site. The
  handler's `Status().Update` calls (`compositiondefinitions.go:716`, `:1393`)
  write CRDs. The one CD write outside the scale-to-zero reconciler,
  `kube.Update(ctx, cr)` at `:315` in `cleanupObsoleteFinalizerLabels`, runs
  once from `Setup` (`:148`), not per reconcile.
- Observe reads `status.scaleToZero` (§8.2).

**Not checked:**
- **Other packages in these repos.** I didn't read them, so a writer of the
  managed object elsewhere (for example in core-provider's
  `composition-dynamic-controller`, or oasgen's `rest-dynamic-controller`) may
  exist.
- **Whether any other writer touches RestDefinition status** (§4).
- **Clusters.** Nothing was run against a cluster, and no tests were run;
  this note is reading only.
- **Crossplane's issue tracker.** A search for this symptom in
  crossplane/crossplane-runtime found nothing, so §5 rests on the source alone.
- **Noticed, out of scope:** `:905` and `:1228` wrap a finalizer error as
  `errUpdateManagedStatus`, and `:1256` wraps an AddFinalizer error as
  `errCreateIncomplete`. Both labels mislead.
