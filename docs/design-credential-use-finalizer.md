---
type: Architecture
title: provider-runtime — credential-use finalizer
description: Design, not built, for holding the Secrets (and the objects that lead to them) that a managed resource reads, for as long as losing them would be unrecoverable, so a namespace deletion can't wedge on a managed resource whose credentials were deleted first.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, finalizer, deletion, credentials, reconciler]
timestamp: 2026-11-08T00:00:00Z
---

# Credential-use finalizer

**Status:** design, final revision. Nothing is built. The
user chose the approach: a finalizer on each Secret an object uses, the
analog of Crossplane's ProviderConfigUsage. The user's decisions are
binding here: U1 to U7 on the first review, and NQ1 to NQ4 on the second.
§17 maps every review item to the section that resolves it, and §18 holds
what is still open.

Code references are to provider-runtime at `77cce67`, controller-runtime
v0.25.2, client-go v0.37.2 and apimachinery v0.37.2 (`AM/` is
`k8s.io/apimachinery@v0.37.2`).

## 1. The problem

The deadlock was found on krateo-057 on 2026-11-08. The evidence is in
`~/.krateo-baselines/rlgov-probe-evidence/`: `README.md`, `repo.json`,
`namespace.json`, `git-provider-rlgov.log` and `git-provider-full-20m.log`.
`events.json` is empty because the events had been swept.

- A Repo, `rlgov-probe/rlgov-probe-repo` (git-provider 0.17.0), named the
  Secret `rlgov-probe/probe-dummy-creds` in both `fromRepo.secretRef` and
  `toRepo.secretRef` (`repo.json`).
- The namespace was deleted at 16:26:41Z. The Repo got its
  `deletionTimestamp` at 16:27:34Z, and its first pass after that already
  failed in Connect:
  `connect failed: retrieving .fromRepo credentials: cannot get probe-dummy-creds secret: Secret "probe-dummy-creds" not found`
  (`repo.json` status, Synced=False/ReconcileError with Ready=Deleting;
  `git-provider-rlgov.log` line 6). The namespace controller had deleted the
  Secret first.
- It never recovered. `git-provider-full-20m.log` has 38 lines naming the
  missing Secret, from line 477 (16:27:34Z) to line 1014 (16:38:01Z), with
  the backoff growing to its 60s cap.
- The Secret couldn't be put back. The README records the attempt:
  `secrets "probe-dummy-creds" is forbidden: unable to create new content in namespace rlgov-probe because it is being terminated`.
  The provider's own events were rejected the same way
  (`git-provider-rlgov.log` line 7).
- The namespace stayed Terminating. Its conditions name the blocker
  (`namespace.json`):
  `NamespaceContentRemaining: repoes.git.krateo.io has 1 resource instances`
  and
  `NamespaceFinalizersRemaining: finalizer.managedresource.krateo.io in 1 resource instances`.
  A human removed the finalizer.

The probe's rate-limit hold only widened the window. git-provider's Repo
Delete is a no-op, but the runtime needs Connect before it can remove the
finalizer. **The defect is generic: any provider and any kind whose Connect
reads a Secret that a namespace deletion can delete first.**

**It exists outside this runtime too.** oasgen-provider's
rest-dynamic-controller is built on unstructured-runtime, which core-provider
owns, and its delete path reads Secrets: `handler.Delete` →
`ensureSecretRefRBACAndResolve` → `secretref.GetSecretValue`. In
core-provider, the composition controller's Delete reads the chart's
`passwordRef` Secret (core-provider#402), a second known instance. **This is
flagged for core-provider.** It is why the bookkeeping is a standalone
package that doesn't need a manager (§7).

Related prior art in core-provider:

- #313: a namespace deletion deadlocks because the controller dies with the
  namespace (§11.5).
- #314: Delete couldn't proceed with an unreadable chart, and was fixed by
  answering from status (§16.4).

## 2. What we have to work with

- **Connect is unconditional on deletion.** On a deleted object with the
  default deletion policy, Reconcile gates and calls `Connect`
  (`pkg/reconciler/reconciler.go:879-885`) before Observe, Delete and
  `RemoveFinalizer` (`:1089`, `:1125`). Only `krateo.io/deletion-policy:
  orphan`, or a management policy that forbids delete, skips it
  (`:821-848`, `meta.ShouldDelete`).
- **The runtime's own finalizer comes after the first Connect.**
  `AddFinalizer` runs at `:1152`, after Connect and Observe have succeeded.
  An object whose first pass fails in Connect has no finalizer, so it is
  deleted at once.
- **Finalizers are written with a plain Update.** `APIFinalizer`
  (`pkg/resource/api.go:40-55`) mutates the object and calls `Update`, which
  is guarded by resourceVersion. It writes nothing when there is nothing to
  change. `resource.Finalizer` exposes no finalizer name, and `WithFinalizer`
  can replace it with any implementation.
- **Optional provider capabilities are discovered by type.** `keyerOf`
  (`pkg/reconciler/ratelimit.go:112`) is called from `WithExternalConnecter`
  and `WithExternalConnectDisconnecter` (`reconciler.go:638`, `:647`), before
  `NewNopDisconnecter` hides the connecter's other interfaces.
- **Credential refs can cross namespaces.** `commonv1.SecretKeySelector`
  carries an explicit namespace (`apis/common/v1/resource.go:31-37`).
- **Finalizer names are qualified names.** `ValidateFinalizerName` applies
  `IsQualifiedName` (`AM/pkg/api/validation/objectmeta.go:117-124`). That
  means an optional DNS-subdomain prefix of at most 253 characters, a `/`,
  and a name of at most 63 characters that starts and ends with a letter or
  digit, with `-`, `_` and `.` also allowed in between. Annotation and label keys
  follow the same rules. The API server warns on a finalizer with no `/`.
  The probe drew that warning for the existing finalizer
  (`git-provider-rlgov.log` line 2).
- **No finalizer can be added to an object that is being deleted.**
  `ValidateNoNewFinalizers` (`objectmeta.go:126-134`) rejects that with
  `Forbidden`. Removing finalizers is still allowed, and so is changing
  labels and annotations. NamespaceLifecycle forbids *creating* objects in
  a terminating namespace, but allows updates and patches.
- **Strategic merge patch merges finalizers as a set** on built-in kinds:
  `Finalizers` is tagged `patchStrategy:"merge"`
  (`AM/pkg/apis/meta/v1/types.go:289`). Custom resources don't accept
  strategic merge patches. For them, a merge patch replaces the whole list.
- **A watch with a label selector** delivers an object that stops matching
  as a deletion, and an object that starts matching as an addition. A test
  pins this (§14).
- **A Secret with a finalizer stays readable while it is being deleted.**
  That is the whole fix: Connect's `Get` keeps working until the finalizer
  is removed.
- **envtest is kube-apiserver and etcd only.** It has no namespace
  controller and no garbage collector. `pkg/scaletozero/envtest_test.go`
  starts it in `TestMain` when `KUBEBUILDER_ASSETS` is set, and
  `requireAPIServer` (`:94`) fails rather than skips under CI. CI installs
  the 1.37.0 binaries (`.github/workflows/run-tests.yaml`).

## 3. Finalizer naming and scope

A Secret can be used by several kinds, from several providers and
processes, at once. Each holder kind must track only its own users, and
must never remove or depend on another holder kind's entries.

**One finalizer per holder kind and holder namespace** (U6):

```
credential-use.<lowercase kind>.<group>/<holder namespace>
```

For example `credential-use.repo.git.krateo.io/rlgov-probe`. A
cluster-scoped holder kind uses the name `cluster-scoped`. A GroupKind is
either namespaced or cluster-scoped, so this can't collide with a namespace
of that name. Three more keys sit next to it on the protected object:

- **The epoch**, an annotation whose key is the finalizer name and whose
  value is a random 128-bit token. Every acquiring write sets a fresh token
  (§6).
- **The held label**, `credential-use.krateo.io/held: "true"`. It is set in
  the same write as the finalizer. The protected-object caches select on it
  (§10).
- **K's marker label**, `credential-use.krateo.io/k-<hash>: "true"`, also
  set in that write, so that one kind's holds can be selected (§10.1).

**The record** lives on the holder, not here (§4).

**Validity.** The name part is a namespace name, a DNS label of at most 63
characters. The prefix is a DNS subdomain: a CRD's lowercased kind must be a
DNS-1035 label, and its group is a DNS subdomain. Only the total length,
`15 + len(kind) + 1 + len(group) <= 253`, can fail, and only for a
pathological group. Setup checks it and refuses to start (§7.3). The name
contains a `/`, so the API server doesn't warn on it. The runtime's existing
finalizer, which lacks a `/` and draws the warning, is out of scope here. It
is filed as provider-runtime#46.

**Why shared, and not one finalizer per holder object.** Per-object
finalizers (`.../<uid>`) would need no release check at all. They are
rejected because of how many there would be (§15.1): a
configuration-level Secret is used by every object of a kind. With one
finalizer per user, its metadata grows by about 90 bytes per user. Every
acquire and release re-sends the whole object to the API server and to
every Secret watcher. That puts the practical ceiling at roughly 1,000 to
2,000 users. With the shared form, the count is bounded by
kinds × namespaces that use the Secret, whatever the number of objects.
(The first revision gave "they conflict" as the reason. That was wrong: a
strategic merge patch adds or removes one entry without conflicting.)

**Why the namespace is in the name.** It confines the release check to a
list of one namespace (§6.2). It also tells an operator who holds a stuck
Secret: "Repos in rlgov-probe".

**Several processes for one kind.** Two processes can run trackers for the
same GroupKind, as core-provider does with one controller per composition
version. They then share the finalizer, and that is safe: every decision in
§6 comes from consistent reads, guarded writes, and records persisted on the
holders. **The release list must cover the whole GroupKind in that
namespace, never only one controller's version selector.**

## 4. What is recorded: the refs a holder uses, on the holder

The release side never recomputes refs. Recomputing fails exactly when it
matters:

- path resolution fails, or the object's shape changed;
- a ref changed between acquire and delete;
- a ref is indirect and needs reads of objects that may already be gone;
- the refs function needs provider configuration (oasgen's RestDefinition)
  that may not be readable at release time.

That last case would bring the deadlock back one level out (§16.2). So
**`RefsFunc` is called only to acquire, and release reads only the
record.**

The record is an annotation on the holder, with a versioned key:

```
credential-use.krateo.io/refs.v1: [{"group":"","kind":"Secret","namespace":"rlgov-probe","name":"probe-dummy-creds"}]
```

The list is sorted and deduplicated. It records identity only (group, kind,
namespace, name): two keys of one Secret are one ref. A future format gets a
new key (`refs.v2`), and a reader that knows both reads both.

**A record that can't be parsed counts as a user** of every protected
object its namespace's check considers. It fails safe, and is logged with
the holder's name. The holder's next acquire overwrites it with a valid
record.

**Annotation, not status.** Every kind has annotations, unstructured
objects included. Annotations survive backup, restore and GitOps re-applies
(client-side apply's three-way merge and server-side apply leave keys they
don't own). provider-runtime owns no status field in providers' CRDs.

**Editing the record by hand** is documented as unsupported (§11.9).

## 5. When to hold: only when losing the object is unrecoverable (U5)

A hold exists to stop one thing: a holder that can never finish deleting
because what it reads is gone *and can't be put back*. Losing a Secret is
unrecoverable in two cases:

- **its namespace is being deleted**, so it can't be recreated (§1);
- **one of its users is being deleted** and still needs Connect.

Otherwise, a deleted Secret can be recreated, and holding it would break
delete-and-recreate rotation: the recreate fails with AlreadyExists while
the old object waits.

The rule, where "the grace" is `Options.DeletionGrace` (60s by default,
configurable). It is counted from **the later of** P's `deletionTimestamp`
and the time this process first saw that timestamp (L4). So neither a
server clock behind the local one nor a late delivery can shorten it.

| Protected object P | P's finalizer for (K, N) is kept while |
|---|---|
| not being deleted | any user of P in N exists (§6.1) |
| being deleted, no user at all | never: released at once |
| being deleted, its namespace has a `deletionTimestamp` | any user of P in N exists |
| being deleted, its namespace live | the grace runs; after it, any user of P in N **is being deleted** |

A cluster-scoped P has no namespace, so it always takes the last row.

**The re-reads are the last step before the release patch.** When the
grace has run out, the releaser GETs the Namespace live and reads its
`deletionTimestamp`, not its phase. It then lists the holders live, to find
users that are deleting. Only then does it patch (§6.2, R2 and R3). Reading
them earlier, when P is first read, would let a namespace deletion or a
holder deletion that starts during the grace go unseen.

**Deletion passes never skip the write.** A holder that is being deleted
always runs A2, whatever the verified set says (§6.3). So its write changes
the epoch, and a release patch racing it fails its test (§6.4).

Once P is gone and is then recreated, its holders acquire the new
incarnation. It has a new UID, so nothing they held carries over (§6.3).
They find it through the missing-ref prober, or on their next pass (§10.3).

### 5.1 Helm, and the two interleavings

*Corrected from an earlier revision, which said Helm uninstall deletes
custom resources before Secrets. That is false.* Verified against Helm
v3.21.3, read from the module cache:

- **The order.** `UninstallOrder` in `pkg/releaseutil/kind_sorter.go` lists
  ConfigMap, SecretList, Secret and ServiceAccount, and later Namespace and
  PriorityClass. Kinds it doesn't know sort last (`lessByKind`: "unknown
  kind is last"). So an uninstall deletes the Secret, then the chart's own
  Namespace, then every custom resource.
- **What the wait between kinds waits for.** `batchPerform`
  (`pkg/kube/client.go:601-615`) calls `wg.Wait()` between kinds. That
  waits only for the previous kind's goroutines to **return**. Each runs
  `deleteResource` (`:630-638`): one DELETE call with a propagation policy,
  retried on conflict. **It doesn't wait for the objects to be gone.**
- **`--wait`.** Only with `--wait` does `pkg/action/uninstall.go:134-136`
  call `WaitForDelete(deletedResources, timeout)`, and it does so once,
  after every delete has been issued.

So a custom resource's DELETE lands milliseconds after the Secret's DELETE
returns, well inside the 60s grace. core-provider's composition delete is a
Helm uninstall, so this ordering is the common case, not an edge case.

**Interleaving A, a live namespace.** The uninstall, or a `kubectl delete -f`
whose file lists the Secret first, deletes P and then holder H. P's
namespace stays live.

- H's deletion starts within the grace, as it does under Helm: the live list
  after the grace sees H deleting, so P is kept until H is gone.
  **Covered.**
- H's deletion starts after the grace and after the releaser's list: P is
  released, and H fails until P is recreated. The namespace is live, so this
  is recoverable, and it is §5.2's residual. Helm doesn't produce this case.
  A slow manual sequence can.

**Interleaving B, a chart that ships its Namespace.** The uninstall deletes
P, then the chart's Namespace, then H, each a few milliseconds after the
last. The namespace controller also deletes H.

- The Namespace has its `deletionTimestamp` long before the grace ends, so
  the live re-read sees it. P is kept until its users are gone, and H's
  deletion runs with P readable. **Covered.**

**If core-provider moves to Helm 4, re-verify this section.** The facts
above are Helm v3.21.3's. Helm 4 reworked waiting and deletion, and it must
be read again before relying on the same ordering.

**Tools that wait for objects to disappear between kinds.** A tool that
deleted the Secret and then waited for it to be gone before deleting the
holders would defeat the grace: nothing would start deleting until the
Secret was released. Helm v3.21.3 doesn't do this, as shown above, and no
other tool that does is known here. If one turns up, the fix is on its side:
delete the holders first, or don't wait for the Secret.

**The cost to rotation.** A Secret with live users that is deleted in a live
namespace is held for up to the grace. During that time, creating one with
the same name fails with AlreadyExists, and `kubectl replace --force`
waits. That is up to 60s with the default. A Secret with no users is
released at once.

### 5.2 The residual, and the platform namespace

If a Secret is deleted while its namespace is live, and none of its users
starts deleting within the grace, it is released. Its live holders then
fail Connect until it is recreated. If the namespace is deleted before then,
or a holder is deleted, that holder wedges until the Secret is recreated.
The remedy, documented for consumers:

> If you delete a Secret your objects use while its namespace is live,
> recreate it before deleting those objects, or delete the objects first. If
> objects are already stuck deleting on a missing Secret in a live
> namespace, recreate the Secret, and they finish.

**Platform namespaces.** Platform Secrets live in `krateo-system` and are
used across namespaces. If one is deleted while any of its users is being
deleted, it stays Terminating for as long as that deletion takes. Recreating
it fails with AlreadyExists until then. A holder whose external Delete keeps
failing blocks the platform Secret's rotation for that whole time. U5's
"a user is being deleted" rule limits the block to that user's deletion.
**There is no time cap** (NQ2, decided). The waiting event and gauge name
the stuck holder (§12), and the operator unblocks rotation by fixing,
orphaning or deleting that holder.

## 6. The protocol: shared finalizer, write-to-acquire (U6)

### 6.1 Who counts as a user

A holder H of kind K in namespace N **uses** protected object P while all of
these hold:

- H exists;
- H's record names P, or H's record can't be parsed;
- H is not being deleted without the owner finalizer. H is excluded when it
  has a `deletionTimestamp` and lacks the runtime's own finalizer, which
  `Options.OwnerFinalizer` names (§7.1).

H is **deleting** when it has a `deletionTimestamp` and still carries the
owner finalizer. The runtime removes the record, with `Forget`, in the
same write that removes its owner finalizer (§8). The exclusion covers
everything else that can leave a stale record behind: a third party
removing the owner finalizer, or someone else's finalizer keeping H around.
It also breaks foreground-deletion cycles (§11.8).

### 6.2 The two sides

**The acquirer**, for each ref P of holder H, in every pass, before Connect:

- **A1. Announce.** If H's record differs from the refs just computed,
  write the new record with a merge patch guarded by H's resourceVersion
  (`client.MergeFromWithOptimisticLock`). A conflict requeues, for any
  holder: it is a conflict on the holder itself, and the next pass is
  immediate. The in-memory object takes the patched resourceVersion. **In
  the same pass, synchronously**, A1 drops the `observed` entries for refs it
  removed, and resets them for refs it added (§6.3).
- **A2. Write P.** It is skipped only where §6.3 allows: never on a deletion
  pass, and never for a ref this pass's A1 added.
  - **Built-in kinds** (Secret, ConfigMap): one strategic merge patch with
    no precondition. It merges the finalizer into the list, sets the epoch
    to a fresh token, and sets the held label and the kind's marker label
    (§10.1). A strategic merge patch can't drop or duplicate anyone else's
    finalizers.
  - **Custom resources:** a live GET, then a merge patch guarded by
    resourceVersion (`MergeFromWithOptimisticLock`) that writes the full
    finalizer list with the finalizer added, the fresh epoch, and both
    labels. On a conflict it GETs and retries, up to 3 times. If the
    retries run out, a new holder requeues (fail closed), and an
    established holder is **Degraded**: it logs once, records an event,
    counts the metric, and proceeds, deletion passes included (R4, M5).
  - **Outcomes:**
    - **held**: P now carries the finalizer and the fresh epoch. That
      includes a P that is being deleted *with* the finalizer: the patch
      adds no new finalizer, so it is allowed, and the epoch still moves;
    - **Deleting**: `Forbidden`, because P is being deleted and lacks the
      finalizer. A new holder gets `MustNotConnect`. For an established
      holder this is **Degraded**, the one term this doc uses for every
      fail-open (§8);
    - **Missing**: NotFound;
    - **Degraded**: any other error. A new holder fails closed and an
      established one proceeds (§8).
- **A3.** Only then Connect.

**The releaser** works on one holder kind K and one namespace N at a time,
for the set of protected objects pending there (R7). For each pending P:

- **R0. Fast path.** Ask the holder informer's index for users of P in N,
  leaving out the holders §6.1 excludes. If P is not being deleted and a
  user exists, stop. This settles the usual case, where one of several users
  leaves.
- **R1. Get P live.** Note its resourceVersion, the index *i* of the
  finalizer in `metadata.finalizers`, the epoch *e*, and its
  `deletionTimestamp`.
  - If the finalizer is absent, P is done.
  - If the epoch is absent (only a third party could cause that), set a
    fresh one and requeue.
  - If P is being deleted in a namespace that was live when last seen, has
    users by R0, and is still inside the grace, requeue the batch at the
    end of the grace (§5).
- **R2. The last reads, after every R1 in the batch.** For each P being
  deleted, GET its Namespace live and read its `deletionTimestamp`. Then
  make **one** live, paginated, metadata-only list of K in N
  (`PartialObjectMetadataList`, no resourceVersion, so it is a consistent
  read), with no label or version selector. For each P, compute its users
  and its deleting users (§6.1), and apply §5's table.
- **R3. Release** each P the table says to release, with one JSON patch:

  ```json
  [{"op":"test",   "path":"/metadata/annotations/<F escaped>", "value":"<e>"},
   {"op":"test",   "path":"/metadata/finalizers/<i>",          "value":"<F>"},
   {"op":"remove", "path":"/metadata/finalizers/<i>"},
   {"op":"remove", "path":"/metadata/annotations/<F escaped>"}]
  ```

  `<F escaped>` is the finalizer name with `/` written as `~1` (RFC 6901).
  Two cases add to the patch, and both then also test the whole
  `/metadata/finalizers` array as R1 read it:
  - when F is K's last finalizer on P (no other namespace's), the patch
    also removes K's marker label;
  - when F is the last `credential-use.` finalizer of any kind, it also
    removes the held label.

  If a test fails, the API server rejects the whole patch. The exact status
  code is pinned by a test, and is treated as a conflict: P goes back to R1.
  No other writer's change can be clobbered: the patch removes one index it
  has just tested, and keys only this kind writes. When R3 succeeds, this
  process drops its `written` entries for P at once, synchronously.

### 6.3 The verified set

A2 may skip the write only for pairs this process has written (U6), and
never on a deletion pass. The tracker keeps three maps in memory:

- `written[(P.uid, K, N)] = t_issue`: the local monotonic time at which this
  process issued a successful A2 write for P, K and N.
- `recordSince[(H.uid, P)]`: the **informer delivery time** of the first
  holder event, in the current unbroken run of H's versions whose record
  names P. The holder informer's handler maintains it, and the handler
  touches nothing else. A delivered version whose record doesn't name P
  ends the run and deletes the entry, so a later re-add starts a new run
  with a new time.
- `observed[(H.uid, P)]`, written only by passes, **synchronously**. When a
  pass's A1 adds P, it sets `observed` to the time A1's response returned,
  **overwriting** any earlier value (M1). When A1 removes P, it deletes the
  entry. That ref can't skip in the pass whose A1 added it.

t_obs for (H, P) is **the later of** `observed` and `recordSince`. If both
are absent, because the informer hasn't delivered the record yet and no
local A1 wrote it, the pass doesn't skip. Taking the later time can only
make skips rarer. `written` is keyed by P, not by holder, so A1 has no
per-holder `written` entry to reset. The comparison with t_obs is what makes
a re-added ref write again.

**A2 may skip for (H, P) only if** all of these hold:

- the pass is not a deletion pass;
- this pass's A1 didn't add P;
- `written[(P.uid, K, N)]` exists, with `t_obs < t_issue`. That is, this
  process wrote P after the informer had delivered H's record naming P.

**After a restart or a leader change**, all three maps are empty. The
holder informer's initial list delivers every holder at about the time it
syncs, t_sync, and that becomes their `recordSince`. The first pass of the
first holder of P writes P at some t_issue > t_sync. Every holder whose
record was in that initial list has `recordSince` = t_sync < t_issue, so it
skips. **So a restart costs one A2 per (P, K, N)**, as claimed, under this
definition. The exceptions are holders whose record changes after the
sync, and every deletion pass: they write again.

Entries are dropped:

- **in the pass, synchronously:** by A1, for refs it removes; by a
  successful R3, for the P it released;
- **in the protected informer's handler:** when P's UID changes or P is
  deleted, when P loses the finalizer for (K, N), or when P leaves the
  label-scoped cache (§10.2). The handler only deletes `written` entries; it
  never creates or extends one;
- **all of them** on Disown, and when the process restarts or loses
  leadership.

### 6.4 Why it is safe

Take an acquirer whose A1 lands at tA and whose A2 write lands at tW, with
tA < tW (A2 starts after A1 returns). Take a releaser whose R1 reads P at
tG, whose R2 list reads at tL > tG, and whose R3 patch lands at tR > tL.

- **tW < tG.** Then tA < tG < tL, so R2 sees H's record naming P, and H is
  a user. The releaser keeps the finalizer, except where §5's last row says
  to release (P being deleted, its namespace live, the grace over, H not
  deleting). That is U5's intended release, not a lost hold.
- **tG < tW < tR.** A2 wrote a fresh random epoch, so at tR the epoch is not
  *e*, and R3's first test fails. This holds even if another releaser
  removed the finalizer between tG and tW and A2 re-added it: the epoch is
  still fresh. The releaser starts again from R1. Its next list is after
  tW > tA, so it sees H.
- **tW > tR.** The finalizer was gone before A2. A2 adds it back (held), or
  is Forbidden because P is being deleted (Deleting), and the acquirer
  knows which.

**A holder that starts deleting during a release.** Say H's
`deletionTimestamp` lands at tD_H.

- If tD_H < tL, the list sees H deleting, so P is kept.
- If tD_H > tL, H's deletion pass runs A2 without a skip. If that write
  lands before tR, R3 fails and the retry's list sees H deleting.
- If it lands after tR, the release won. This needs tD_H inside the
  releaser's few milliseconds between the list and the patch, and after the
  grace. A2 then reports Deleting, H proceeds Degraded, and H fails until P
  is recreated. Its namespace is live, so this is recoverable, and it is
  part of §5.2's residual.

The namespace's deletion is handled the same way. Its `deletionTimestamp`
is read live after the grace, as the last step before R3. A namespace
deletion that starts after that read deletes the holders, and their
deletion passes write, which breaks an R3 that hasn't landed yet.

**That leaves an unrecoverable window, a few milliseconds wide** (L3). Say
the namespace's deletion begins between R2's Namespace GET and R3's patch,
after the grace, while no user is deleting. R3 then lands first, because
the namespace controller takes far longer than that to discover and delete
the holders. P goes, the namespace is terminating so P can't be recreated,
and the holders wedge exactly as in §1. It needs a Secret deleted in a live
namespace, then the grace running out with no user deleting, then the
namespace deleted inside those milliseconds. Nothing in this design closes
it. It is stated here, and tested in the model, rather than claimed
away.

**Error or crash after the announce** (HIGH-1). Suppose A1 succeeds and A2
fails, times out, or the process dies. Then no `written` entry is issued
after it. A ref A1 adds never skips in that pass, and after a restart
`written` is empty, so every later pass, in any process, runs A2 before its
Connect. A2 is idempotent: a write that landed but whose response was lost
is simply repeated. While A2 has not succeeded, a concurrent releaser can't
use the record against H. Either the record is visible to its R2
(tA < tL), or R2 came first, in which case the finalizer may go and the
next A2 restores it or reports Deleting. In no case does H connect on a P
that a releaser can remove underneath it without H's next write seeing so.

**The skip is safe** for the same reason as the first case. If
`t_obs < t_issue`, then tA < t_obs < t_issue < tW, because the informer
delivered the record after the API server had stored it. So every releaser
that read P before tW fails its test, and every releaser that reads after
tW sees H. The only things that can still remove the finalizer while H's
record names P are:

- §5's live-namespace release, which H's deletion passes guard against by
  never skipping;
- Disown, which is intended;
- third parties, which drop the `written` entry through the informer
  (§10.2), within the cache's lag.

**What the proof assumes.** R1's GET, R2's Namespace GET and R2's list are
consistent reads, and A2 is a write. No decision rests on a cache. Caches
serve only the fast paths (R0, and the `written` skip), and both are safe
when stale. A stale R0 that sees a user already gone only delays the
release to the next event. The skip rests on a delivery time, which can
only come after the write it reports.

### 6.5 Races, case by case

- **Two users deleted at once.** Their events coalesce into one batch for
  (K, N), and R2 sees whichever are left. Two releasers in two processes are
  serialized by R3's tests.
- **One created while another is deleted.** This is §6.4's middle case.
  Either R2 sees the new announcement, or A2's fresh epoch breaks R3.
- **The finalizer added after a concurrent removal.** This is §6.4's last
  case. A Forbidden A2 on a deleting P gives a new holder `MustNotConnect`
  (§8). It never gets its owner finalizer, so it deletes freely.
- **Error or crash after the announce**, with a concurrent releaser: §6.4.
- **A holder or namespace deletion during a release**: §6.4.
- **Third-party stripping** (`kubectl replace`, Argo CD's Replace sync
  option) removes the finalizer and the labels outside the protocol. P
  leaves the label-scoped cache. That drops the `written` entries and
  enqueues P's holders through the index (§10.2). The window is the
  informer's lag plus the holders' queue wait.
- **A relist of the holder informer** (L2). After a watch gap, client-go
  relists, and a relist shows only the final state. A record that went
  away and came back inside the gap would then keep its old `recordSince`,
  and could skip A2 against a `written` entry issued before the gap, while
  a releaser that listed during the gap may have released P. So **a relist
  resets `recordSince` to the relist's delivery time for every holder**.
  That forces one A2 per (P, K, N) after each relist, coalesced as after a
  restart (§6.6). client-go has no silent gaps: a gap always ends in a
  relist. The residual is the relist's own latency.
- **A recreate behind a cached UID** (L6). P is released and removed, and
  recreated under the same name. A process other than the releaser drops
  its `written` entries for the old UID only when its informer delivers
  the old P's deletion. Until then, that process's fast path may find the
  old P, with the finalizer, in its cache, and skip A2, while Connect reads
  the new, unheld P. **The window runs from the old P's final removal to
  that delivery**, which is watch latency. It can't open while the old P
  is held, because the recreate fails with AlreadyExists until P is gone.
  It can't open in the releasing process, which drops its entries when R3
  succeeds. It matters only if the new P is deleted inside the window.

### 6.6 Cost

- **Steady state:** no API calls. Refs are unchanged and `written` covers
  them, so everything is read from caches.
- **A new holder, or new refs:** one patch of the holder (A1), plus one
  write per new P (A2). A1 changes the holder, so it **causes one extra pass**
  of that holder: its update event re-queues it, and the pass makes an
  external Observe call. A status update in the steady state changes
  nothing, so it causes no extra pass, but A1 does change the object. The
  previous revision's "no extra pass" was wrong. The predicate shipped with
  `CredentialUseSource` (§7.5) drops holder updates whose only change is the
  record, and removes the extra pass.
- **Deletion passes:** one A2 per ref per pass, never skipped.
- **After a restart or leader change:** one A2 per (P, K, N), not per
  holder (§6.3).
- **Backfill on upgrade** (L6, M6). Every existing holder has no record, so
  its first pass makes A1 and A2: two writes per holder. A2 can't be
  coalesced, because each record is written in that same pass. Nothing
  paces this but the controller's workers and the client's request rate.
  controller-runtime's `GetConfig` disables client-side rate limiting by
  default (`pkg/client/config/config.go:101-104`), leaving API Priority and
  Fairness to do it, unless the provider sets a QPS.
  - For 1,000 holders that is 2,000 writes, plus 1,000 extra passes, each
    with an external Observe call, if the predicate isn't installed.
  - At 2 workers and about 0.5s per pass, the backfill takes about 8 minutes
    with the predicate, and about 16 without.
  - With saturation on, local slots pace it further.
- **A release:** for a batch of M pending objects in (K, N), M GETs, one
  Namespace GET per deleting namespace, one paginated metadata-only list of
  K in N, and up to M patches. A namespace teardown with 50 Secrets lists
  the holders once, not 50 times.

### 6.7 The invariant, restated so it is true

**When a holder's owner finalizer is written, every ref in its record has
either been written by A2 after the record named it, or been found absent by
a live read made after the pass's last A2 and before that finalizer write.**
That live read is the check just before `AddFinalizer` (§8, R6). A ref that
appears after it is held on a later pass, through the prober or backoff
(§10.3). Until then it is unprotected: it was created after the decision, in
a live namespace, so it is recoverable.

## 7. The package: `pkg/credentialuse`

### 7.1 The core: no manager, no controller-runtime

unstructured-runtime has no manager (C1). So the core depends only on
client-go: a metadata client, a dynamic client, a RESTMapper, and informers
that it builds itself from them. The caller runs the core's loop. Sketch:

```go
package credentialuse

// A Ref names an object a holder reads to reach its external system:
// a Secret, or an intermediate object that leads to one (§9).
type Ref struct {
	GroupKind schema.GroupKind // {"" "Secret"} for a Secret
	Namespace string           // empty for a cluster-scoped object
	Name      string

	// Next, when set, names the refs found inside this object. The tracker
	// calls it after the object is held, with the object it read (§9).
	Next func(obj *unstructured.Unstructured) ([]Ref, error)

	// Guaranteed declares that the object's own controller already keeps it
	// alive while the holder exists (§9.3). It is neither recorded nor held;
	// its Next is still followed.
	Guaranteed bool
}

func SecretRef(namespace, name string) Ref

// RefsFunc names what obj reads to reach its external system. Object in,
// refs out, no provider types. It must make no API calls. It may use
// provider configuration held in memory. It is called only to acquire,
// never to release.
type RefsFunc func(obj metav1.Object) ([]Ref, error)

type Clients struct {
	Metadata metadata.Interface
	Dynamic  dynamic.Interface
	Mapper   meta.RESTMapper
}

type Options struct {
	Holder         schema.GroupVersionKind // the kind the caller reconciles
	OwnerFinalizer string                   // the caller's own finalizer on holders (§6.1)
	Refs           RefsFunc
	Protected      []ProtectedKind          // kinds Refs and Next may name; default: Secret
	MaxDepth       int                      // levels of Next followed; default 3
	Enqueue        func(holder types.NamespacedName) // the caller's queue (§10.2)
	DeletionGrace  time.Duration            // §5; default 60s
	ProbeInterval  time.Duration            // missing-ref prober; default 15s (§10.3)
	DisownInterval time.Duration            // §7.4; default 60s
	DisownPasses   int                      // §7.4; consecutive zero passes, default 3
	Index          types.NamespacedName     // PerName mode only (§10.6)
	Logger         logging.Logger
	Metrics        MetricsRecorder
}
// Disown is not an option: it is the switch on the holder CRD (§7.4), so
// every process of the kind reads the same value.

type ProtectedKind struct {
	GVK   schema.GroupVersionKind
	Watch WatchMode // LabelScoped (default) or PerName (§13)
	// Content keeps whole objects, not metadata, in this kind's cache, for
	// refs with a Next. For Secrets this is opt-in and holds Secret data (§10.4).
	Content bool
}

// NewShared builds the process-wide state: informers per protected kind,
// shared by every tracker. Run it once, under the caller's leader election
// for the release side (§7.3).
func NewShared(c Clients, o SharedOptions) *Shared
func (s *Shared) Run(ctx context.Context) error

// Track adds a holder kind. It can be called before or after Run, for kinds
// discovered at runtime (§7.3). It is idempotent per holder GroupKind.
func (s *Shared) Track(o Options) (*Tracker, error)

func (t *Tracker) Acquire(ctx context.Context, obj metav1.Object) (Acquired, error)
func (t *Tracker) BeforeEstablish(ctx context.Context, obj metav1.Object, a Acquired) error
func (t *Tracker) Forget(obj metav1.Object) (changed bool)
func (t *Tracker) Users(ctx context.Context, p Ref, namespace string) ([]types.NamespacedName, error)

type Acquired struct {
	Held     []Ref
	Unheld   []Unheld // each with a Reason: Missing, Deleting
	Resolved map[Ref]*unstructured.Unstructured // objects read while following Next (§9.2)
	// MustNotConnect: a ref is Deleting and unheld, and obj lacks its owner
	// finalizer, or the acquire failed for an object without one (§8).
	MustNotConnect bool
	// Degraded: the acquire failed for an established object, which proceeds
	// (fail open, §8). The error is in Err.
	Degraded bool
	Err      error
}

// Resolved returns the object the tracker read for ref in this pass.
func WithResolved(ctx context.Context, a Acquired) context.Context
func Resolved(ctx context.Context, ref Ref) (*unstructured.Unstructured, bool)

func FinalizerName(holder schema.GroupKind, namespace string) (string, error)

const (
	AnnotationRefs = "credential-use.krateo.io/refs.v1"
	LabelHeld      = "credential-use.krateo.io/held"
)
```

A holder is **established** when it carries `OwnerFinalizer`. The tracker
decides that from the object it is given, so the caller doesn't pass it.

### 7.2 What an outside runtime must call, and when

For unstructured-runtime:

1. **At start:** `NewShared` once per process, `Track` once per holder
   kind, then `Run(ctx)`. Run it under the leader election the controller
   already uses, because the release side writes. Kinds discovered later
   can call `Track` at any time.
2. **In every reconcile**, after reading the object and before anything
   reads a Secret or a protected object, deletion passes included:
   `a, err := t.Acquire(ctx, obj)`.
   - On a conflict, requeue.
   - If `a.MustNotConnect`, don't read credentials. Report it (§12), and
     requeue with backoff.
   - If `a.Degraded`, proceed: log once, record an event.
   - Pass `WithResolved(ctx, a)` to whatever reads the chain.
3. **Just before adding its own finalizer:** `t.BeforeEstablish(ctx, obj, a)`.
   It live-reads refs that were Missing and holds any that now exist. On an
   error, don't add the finalizer this pass.
4. **When removing its own finalizer:** `t.Forget(obj)` on the same object,
   just before that write, so both go in one write. **This is mandatory.**
   On any deletion pass where its own finalizer is already absent but the
   record is present, it writes `Forget` with an Update of its own.
5. **Enqueue, and the holder event filter:** pass `Options.Enqueue` that adds
   a holder to its workqueue (§10.2). It is required: `Track` fails without
   it, for the reason §7.5 gives for the source (M4). Filter holder update
   events whose only change is the record, as `IgnoreCredentialUseRecord`
   does (M6).
6. RBAC as in §13.

"Find the last user" is the release loop's job, inside `Run`. `Users` is
exported for diagnostics and tests.

### 7.3 Setup: idempotent, after start, refuses to start on error

- **Idempotent per holder kind** (L8). A second `Track` with equal Options
  returns the existing tracker. With different Options it returns an error.
  The core keeps its own indexers on its own informers, so nothing registers
  a controller-runtime field index, and nothing can register one twice or
  after the cache has started.
- **After start** (C3). `Track` adds the holder informer and any protected
  informer not yet running. It starts them, and waits for their sync before
  returning. core-provider's dynamicwatch and pool can call it when a
  CompositionDefinition appears.
- **Errors.** An invalid finalizer prefix, an unmappable holder or protected
  kind, or a missing `OwnerFinalizer` is returned by `Track`. Before `Run`,
  the error makes `Run` fail, so the process refuses to start (U2).

### 7.4 Disown

Disown replaces the first revision's ReleaseOnly (HIGH-3), which left
records and finalizers behind for objects still in use, so a downgrade
wasn't safe.

**The switch is cluster-visible, on the holder CRD** (M2). It is an
annotation, `credential-use.krateo.io/disown`, on the
CustomResourceDefinition of the holder kind. Its value is `all`, or a
comma-separated list of protected GroupKinds (`Secret`,
`KubernetesTarget.core.krateo.io`) to stop protecting. Every tracker of that
GroupKind watches its CRD by name and reads the switch. Why the CRD, and not
a ConfigMap in the provider's namespace:

- **It is keyed by exactly the right thing.** A CRD is one per GroupKind,
  and every process that reconciles the kind can read it. core-provider runs
  one process per composition version, and those processes have no single
  "provider namespace" a ConfigMap would belong to.
- **It is cluster-scoped**, so no namespace teardown can delete it, and it
  needs no extra object.
- **Its weakness is a re-apply.** Whoever applies the CRD could strip the
  annotation with a full-object Update. Helm's three-way merge and
  server-side apply keep annotations they don't own. core-provider generates
  its CRDs, so it must write them with patch or apply, never a full Update.
  The doc text says so.

**What Disown does.** While the switch is on, for every tracker of the kind
in every process:

- `Acquire` acquires nothing. It returns every ref Unheld, the object
  proceeds as today, and nothing is ever `MustNotConnect`.
- The loop strips the record from every holder of K, with a merge patch
  guarded by resourceVersion.
- It removes every finalizer with K's prefix, and its epoch, from every
  protected object, using R3's JSON patch without the user check. It
  removes K's marker label, and the held label where that was the last
  `credential-use.` finalizer.

**"Safe" is observable cluster-wide**, not only on one process's gauge.
Every pass of the Disown loop, once per `DisownInterval` (60s by default),
makes a **live count**. In LabelScoped mode that is label-selected,
metadata-only, paginated lists of each protected kind by K's marker label,
counting finalizers with K's prefix. In PerName mode it reads the index and
makes a GET per line (§10.6). Either way it adds the holders that still
carry a record. The result goes on the CRD:

```
credential-use.krateo.io/held-count: "0"
credential-use.krateo.io/disown-settled: "true"
```

**Settling counts consecutive zero passes, not wall-clock time** (L4). A
process sets `disown-settled` once it has seen `DisownPasses` (3)
consecutive passes count zero. The passes must span longer than the longest
reconcile, which is the reconcile timeout plus its grace, 90s by default.
At the defaults, 3 passes at 60s span 180s. A non-zero count clears the
flag. **Downgrading the runtime, removing the interface from the connecter,
or uninstalling is safe once `held-count` is `0` and `disown-settled` is
`true`.** Any process can write the fields, because each one counts the
same cluster, live.

**Writing the fields** (L5, L6). Every write of these fields is a JSON patch
that tests the field's previous value. Of two processes racing, one fails
and re-reads, so neither overwrites a newer count with an older one. **A
CRD write makes the API server rebuild its OpenAPI**, so the fields are
written only when they change. Apart from the one-time index pointer
(§10.6), these are the only CRD writes the design makes.

**A Kind at zero replicas** has no tracker running, so nothing updates the
count. Two things cover that (L5):

- **`WithHeld` keeps the controller up while the switch is set and
  `disown-settled` isn't `true`** (§11.6). Setting the switch on a
  scaled-down Kind wakes its controller, which then performs Disown.
- **An operator can check without starting it:**
  `credentialuse.DisownPrefix` with `DryRun: true` reports, from a live
  count, what it would strip, and changes nothing.

**A pass that started before the switch** may still land an A2 after the
strip. §10.6, interleaving 3, shows why consecutive zero passes catch it.

**Namespaces outside `SharedOptions.Namespaces`.** Disown doesn't use the
caches. Its lists are live and cluster-wide, so it reaches every namespace,
including those the caches don't cover. Under `resourceNames` RBAC, where
cluster-wide lists aren't allowed, it works from the holders' records
instead: it GETs and patches each named object.

**Disown with no holder kind, for a deleted CRD** (L5). When the holder CRD
is gone, its objects are gone too: a CRD's deletion removes its custom
resources first. But finalizers with its prefix may remain on protected
objects. `credentialuse.DisownPrefix(ctx, clients, holder schema.GroupKind,
protected []schema.GroupVersionKind)` needs no holder kind and no switch. It
lists the protected kinds live by K's marker label, removes every finalizer
with the prefix `credential-use.<kind>.<group>/` and its epoch, and removes
the labels as above. In PerName mode it takes the names from the index,
whose location it can no longer read from the CRD, so it is given the index
explicitly, or a name list. It refuses to run while the CRD still exists,
even if it is being deleted, because then holders may still be deleting and
need their refs. With `DryRun: true` it only reports. It ships as a function
and as a one-shot Job recipe in the docs.

**Dropping one protected kind** goes through the same switch, by listing
that kind in the annotation.

### 7.5 provider-runtime's adapter and wiring (U2)

A connecter that implements

```go
type CredentialRefsProvider interface {
	CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error)
}
```

is discovered as `keyerOf` discovers keyers, in `WithExternalConnecter` and
`WithExternalConnectDisconnecter`. It lives on the connecter only, because
refs are needed before Connect, when there is no client yet.

`NewReconciler` then:

- gets the per-manager `Shared` (built once from `mgr.GetConfig()` and
  `mgr.GetRESTMapper()`) and adds its `Run` to the manager as a runnable
  that needs leader election;
- calls `Track`.

**The owner finalizer under a custom `WithFinalizer`.** `APIFinalizer`
gains `FinalizerName() string`, through a new optional interface,
`resource.NamedFinalizer`. A custom `resource.Finalizer` that doesn't
implement it, on a connecter that implements `CredentialRefsProvider`, is a
setup error: the runtime can't tell established holders from new ones.

**The builder line, and the predicate, are required** (NQ3, decided). The
tracker enqueues holders when a protected object changes (§10.2), so it
needs a handle on the controller the provider builds after
`NewReconciler`. The provider writes:

```go
r := reconciler.NewReconciler(mgr, kind, reconciler.WithExternalConnecter(c), ...)
return ctrl.NewControllerManagedBy(mgr).
	Named("myresource").
	For(&v1alpha1.MyResource{}, builder.WithPredicates(reconciler.IgnoreCredentialUseRecord())).
	WatchesRawSource(r.CredentialUseSource()).
	WithOptions(o.ForControllerRuntime()).
	Complete(r)
```

`IgnoreCredentialUseRecord` drops a holder update event only if its only
change is the record annotation (setting aside `resourceVersion` and
`managedFields`), **and** the new record equals the one this process last
wrote for that holder (L8). A hand edit of the record, or another process's
write, still triggers a pass. That removes the extra pass A1 would otherwise
cause (§6.6, M6), and nothing else. An outside runtime filters its holder
events the same way.

**`CredentialUseSource` is a `source.TypedSyncingSource`** (L1). Its
`WaitForSync` waits on `HasSynced` of the event-handler registration on the
protected informers. controller-runtime waits on every syncing source
before it starts the controller's workers
(`pkg/internal/controller/controller.go:378-383`). So no `Reconcile` runs
before the source has delivered the protected objects' initial state.

**A setup error refuses to start**: the runnable's `Start` returns it, so
`mgr.Start` fails and the process exits. Setup errors include:

- an invalid finalizer prefix;
- an unmappable holder or protected kind;
- a missing owner finalizer name;
- **an index location that disagrees with the holder CRD's** (§10.6);
- **a tracker whose `CredentialUseSource` was never started** (M4).
  controller-runtime starts a controller's sources, and waits for the
  syncing ones, before its workers, so the first `Reconcile` proves it. If the source hasn't been started by
  then, that `Reconcile` does nothing (no Acquire, no Connect) and signals
  the runnable, whose `Start` returns
  `credential-use source for <kind> was not started: add .WatchesRawSource(r.CredentialUseSource()) to the controller`.
  If no `Reconcile` comes, the runnable also fails 2 minutes after leader
  election with the source still unstarted.

U2's rule is that acquiring without its supporting pieces must not run.
Without the source, third-party stripping, re-pointed intermediates and
recreated objects would wait for the poll interval, so the windows of §6.5
and §9.2 would stretch to it. That is a loss of protection, not only of
latency, so it fails at start.

**Fake managers work.** `pkg/resource/fake.Manager` has a nil config. With
a nil config, the adapter doesn't build a `Shared`, logs once that
credential-use is off, and makes `CredentialUseSource` a no-op source.
Unit tests that don't care are unaffected. Tests that do inject a `Shared`
with `reconciler.WithCredentialUseShared(s)`, built on client-go fakes. A
connecter that doesn't implement the interface creates nothing (§11.10),
and `CredentialUseSource` is then a no-op source, so the builder line is
harmless there too.

## 8. Where it runs in Reconcile; the governor and the gated queue

All of this is API-server work, not external calls, so none of it is gated
per key. In order:

1. **The source check** (M4), first of all, before the Get and so before
   the orphan path too. If the tracker's `CredentialUseSource` hasn't been
   started, return at once, writing nothing, and fail the manager (§7.5).
   The process is about to exit, so the orphan path loses nothing by
   waiting for the next one.
2. **Get**, as now.
3. **Paused** (`:806`): return, as now. Nothing is acquired.
4. **Orphan path** (`:821`): `Forget(managed)`, then `RemoveFinalizer`, in
   one Update. If the owner finalizer is already absent, so that
   `RemoveFinalizer` would write nothing, and the record is present, write
   the record's removal with an Update of its own (R12). No Connect, so
   nothing to acquire.
5. **Acquire**, new, after the orphan check and before the Connect gate
   (`:879`), on every remaining pass, deletion passes included. **A deletion
   pass always writes** (A2 never skips there, §5, §6.3).
   - A conflict on A1, the holder's own record, returns
     `rateLimitedRequeue()`, as every conflict on the holder does.
   - **A new holder** (no owner finalizer) fails closed. On `MustNotConnect`
     or any other error: Synced=False/ReconcileError, Ready=NotObserved
     through `markNotObserved`, and return the error so backoff applies.
   - **An established holder** fails open on every other Acquire error
     (R4), deletion passes included. That includes a custom-resource A2
     whose retries ran out (M5). Log once per object, record a Warning
     event (`CredentialUseDegraded`), count the metric, and proceed to
     Connect as today. It never gets `MustNotConnect`. A Forbidden A2
     (outcome Deleting) is Degraded in the same way (§11.4).
   - `WithResolved(externalCtx, a)` puts the resolved chain on Connect's
     context (§9.2).
   - The gate's `pristine` copy (`:870`) is taken after Acquire, so that a
     deferral right after A1 compares against the object as it now stands.
6. **Gate and Connect**, as now.
7. **Delete path**: `Forget(managed)` before `RemoveFinalizer` (`:1125`), in
   the same Update. As in step 4, if the owner finalizer is already absent
   and the record present, write the removal on its own.
8. **`BeforeEstablish`**, new, just before `AddFinalizer` (`:1152`), and only
   when the owner finalizer is absent. On an error, write
   Synced=False/ReconcileError and return, without adding the finalizer.
9. **`AddFinalizer`**, unchanged.

**The governor.** Acquire runs before the Connect gate, so a deferred pass
has already acquired. That is cheap: cached in the steady state. A long
throttle doesn't leave a new object unprotected.

**The gated queue.** With saturation on, the local slot covers the whole
reconcile, so Acquire's API calls run inside it, like status updates.
Nothing new is gated. A holder waiting for a slot has not re-acquired yet,
which is part of the window in §9.2.

**The release loop and the prober** are not controller-runtime controllers
built with `ForControllerRuntime`, and they don't register with the
governor. They take no local slot and don't raise the ceiling. A teardown
must not queue behind a saturated external API.

## 9. Chains: object → intermediate object → Secret (U1)

core-provider's remote CompositionDefinition resolves a KubernetesTarget in
Connect, and the target names a kubeconfig Secret. oasgen's objects name a
Configuration, which names a Secret. A teardown can delete the intermediate
object first, just as it can delete the Secret. That is the same deadlock
one level up. **So v1 protects any kind a holder reads to reach its
credentials, not just Secrets.** The finalizer, the epoch and the label are
metadata, so the protocol works the same for every kind.

### 9.1 How a controller names a chain

`RefsFunc` stays pure. It names the first hop, with a `Next` that reads it:

```go
func (c *connector) CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error) {
	cd := mg.(*v1alpha1.CompositionDefinition)
	return []credentialuse.Ref{{
		GroupKind: kubernetesTargetGK, Namespace: cd.Namespace, Name: cd.Spec.Target.Name,
		Next: func(kt *unstructured.Unstructured) ([]credentialuse.Ref, error) {
			name, _, err := unstructured.NestedString(kt.Object, "spec", "kubeconfigRef", "name")
			if err != nil || name == "" {
				return nil, fmt.Errorf("KubernetesTarget %s has no kubeconfigRef: %v", kt.GetName(), err)
			}
			return []credentialuse.Ref{credentialuse.SecretRef(kt.GetNamespace(), name)}, nil
		},
	}}, nil
}
```

The tracker follows the chain level by level, and **holds each object
before it reads it**:

1. A1 for level 1, then A2 for the KubernetesTarget.
2. Read the KubernetesTarget: from its content cache, or with a live GET if
   the cache doesn't have it yet.
3. Call `Next`.
4. A1 again with the union, then A2 for the Secret.

The record names both, and both carry the finalizer. Every function the
provider supplies is pure, and the hold-before-read ordering belongs to the
runtime. `MaxDepth` bounds the chain. A `Next` error, or a chain deeper than
`MaxDepth`, is an Acquire error: a new holder fails closed, and an
established one fails open with its record as it was (§8).

**A Secret that names a Secret** is in scope the same way: the first
Secret's `Ref` has a `Next`. It needs that Secret's data, which is §10.4's
opt-in.

### 9.2 `Resolved`, and the window it doesn't close (U3)

`Resolved` is built now. Connect reads `Resolved(ctx, ref)` to get exactly
the intermediate objects that were held, not a newer cached copy. Without it,
Connect could read a re-pointed KubernetesTarget naming a Secret this pass
never held.

**`Resolved` alone doesn't close the window** (MED-5). If the
KubernetesTarget is re-pointed after this pass, the holder keeps connecting
through the old, still-held chain until it reconciles again. So the tracker
also **enqueues holders through the record index** when a held protected
object with a `Next`:

- changes generation, or changes through someone else's write. A change
  counts as the tracker's own only if the two versions are equal once every
  `credential-use.` label, annotation and finalizer, `resourceVersion` and
  `managedFields` are set aside (L2). For a metadata-only kind, a change to
  data can't be told apart, but it doesn't need to be: such a kind has no
  `Next`;
- loses the finalizer or a label;
- gets a `deletionTimestamp`, or is deleted.

For kinds without a `Next`, such as plain Secrets, holders are enqueued
only on finalizer or label loss and on deletion (L7). A data change doesn't
change what they hold, and a `deletionTimestamp` concerns only the release,
which §10.2's batch enqueue handles.

**A recreated object is not among these events** (L5). It is unlabelled, so
the label-scoped informer doesn't deliver it. **Only the missing-ref prober
sees it** (§10.3). The holders' own retries also find it, and
`BeforeEstablish` re-checks before an owner finalizer is written.

**The window that remains is reconcile latency, stated honestly.** From the
change to the holder's next pass, the holder uses the old chain. That time
covers the informer's lag and the holder's queue wait, including a gated
queue's wait for a local slot. For a recreated object it also covers up to
one prober interval. A **paused** holder doesn't reconcile at all, so it
stays on the old chain until it is unpaused. It is also deleted only after
it is unpaused, and its first pass then acquires before anything else. The
window matters only if the newly named object is deleted inside it, and §5
already makes that recoverable unless its namespace is being deleted.

### 9.3 Refs whose lifetime is already guaranteed (C2)

A CompositionDefinition refuses Delete while compositions exist
(`ErrCompositionStillExist`). So for a composition, the CompositionDefinition
can't vanish first, and holding it would add writes and nothing else. A
provider declares this with `Ref.Guaranteed: true`. A guaranteed ref:

- is not recorded and gets no A2;
- has its `Next` followed, so the chart's `passwordRef` Secret behind it is
  still held;
- is read through a lister the caller supplies for that kind
  (`SharedOptions.Listers`), or with a live GET if there is none;
- counts as covered for `AssertCredentialRefsCover` (§11.9).

**The fallback is flagged as unverified.** The guarantee depends on the
compositions carrying the labels the CompositionDefinition's check selects
on. Where they might be missing, the guarantee doesn't hold. `RefsFunc`
must then set `Guaranteed` only for objects that carry them, and return a
normal, held ref for the rest. Whether every composition carries those
labels is for core-provider to verify.

## 10. Watches, caches and memory

### 10.1 The protected-object caches (R8)

There is one informer per protected kind, shared by every tracker in the
process:

- **scoped by the label selector** `credential-use.krateo.io/held=true`, so
  it holds only objects some kind holds;
- **metadata-only** (`metadatainformer`), unless `Content` is set (§10.4);
- **with a transform** that drops `managedFields`, every annotation except
  `credential-use.*`, and every label except `credential-use.*`.

What remains is about 300 to 500 bytes per object: name, namespace, UID,
resourceVersion, generation, `deletionTimestamp`, finalizers, the epoch
annotations and the labels. 10,000 held Secrets cost about 5 MB.

**Two labels.** A2 sets both in the same write as the finalizer:

- the held label, `credential-use.krateo.io/held: "true"`, shared by every
  kind. It scopes the caches;
- **K's marker label** (L3), `credential-use.krateo.io/k-<hash>: "true"`,
  where `<hash>` is the first 16 hex digits of the SHA-256 of
  `<lowercase kind>.<group>`. It lets `scaletozero` and Disown select one
  kind's held objects and stop at the first match (§11.6). A hash collision
  is harmless: every reader then checks the finalizer prefix, and pages on
  past a mismatch.

**Who removes the labels** (L4):

- R3 removes K's marker label with K's last finalizer on P, and the held
  label with the last `credential-use.` finalizer (§6.2);
- Disown removes them likewise;
- if a third party strips the finalizers but leaves a label, any tracker
  whose informer sees a labelled object with no matching finalizer removes
  the label. That is a JSON patch testing that the whole finalizer array
  still has no `credential-use.` entry, so it can't race a new A2.

**A fast-path miss falls back to a live GET.** That covers:

- an object not yet labelled, because it was just created or has never been
  held;
- an object the informer hasn't delivered yet;
- a namespace the caller's informers don't cover (§10.5).

A2's write sets the labels, so from then on the object is in the cache.

### 10.2 The holder informer, the index, and enqueue (R5)

There is one metadata-only informer per holder kind, with a transform that
keeps the UID, resourceVersion, `deletionTimestamp`, finalizers and the
record. That is about 300 bytes plus about 100 bytes per ref, so 10,000
holders cost about 5 MB. Its handler maintains `recordSince` (§6.3).

Its index maps each recorded ref to holders. The index serves:

- **R0's fast path**;
- **mapping holder events to release batches**: a create, an update whose
  record or owner finalizer changed, and a delete map to the protected
  objects in the old and the new record, and enqueue their (K, N) batch;
- **enqueueing holders when a protected object changes** (§9.2), through
  `CredentialUseSource` or `Options.Enqueue`. A label-scoped informer
  delivers an object losing its label as a deletion. That is the signature
  of a `kubectl replace`, or an Argo CD Replace, that strips our metadata.
  So a stripped object enqueues its holders, and the handler drops the
  `written` entries for it (§6.3).

**A protected object getting a `deletionTimestamp`** (M3) enqueues release
batches, not holders (holders only for kinds with a `Next`, §9.2, L7): the
(K, N) batch of every finalizer of K it carries,

- at once, which releases it if it has no users at all;
- again when the grace ends, measured as §5 says, for §5's live-namespace
  row.

The releaser re-checks the grace itself in R1, so an early or duplicate
enqueue is harmless.

**On startup**, the protected informers' initial list enqueues a release
batch for every (K, N) of every held object. In PerName mode, the index
does instead (§10.6). So every hold is checked again after a restart, and a
holder delete missed while down can't strand a finalizer.

### 10.3 Seeing new objects: the missing-ref prober (R6)

A newly created object, such as a Secret recreated for rotation, isn't
labelled, so the label-scoped informer can't see it. The tracker keeps the
set of refs it last found Missing for some holder.

Every `ProbeInterval` (15s by default), the prober checks the missing refs
that are due:

- **each ref backs off on its own** (L9), from 15s, doubling up to 5
  minutes. It returns to 15s when a holder's pass reports it Missing
  again, so a ref under active use is probed often, and an abandoned one
  rarely;
- **per (kind, namespace)**, with 3 or fewer due names it makes a GET per
  name. With more, it makes **one live, metadata-only, paginated LIST**,
  not a GET per ref.

It enqueues the holders of every ref it finds, through the index. Their
next pass runs A2 on the new incarnation.

**Its cost** is, per tick, a few GETs, or one list per namespace with many
due refs, sized by all objects of that kind in that namespace (about 1 KB
each as metadata). It is zero while nothing is missing, and it falls off as
refs back off. A namespace with 200 Secrets
and one missing ref costs about 200 KB every 15s until the ref appears.
Under `resourceNames` RBAC, where such a list isn't allowed, the prober
falls back to one GET per missing name.

The holders' own retries find a new object too, within their backoff of 60s
at most. `BeforeEstablish` re-checks a Missing ref live just before the owner
finalizer is written (§6.7). This is how U5's "holders re-acquire the new
incarnation" happens.

### 10.4 Content caches, for kinds with a `Next` (R9)

`Next` needs the object's content, not just metadata. For a protected kind
whose refs have a `Next`, `ProtectedKind.Content` makes that kind's informer
a **dynamic (unstructured) informer**. It is scoped by the same label, and
its transform drops `managedFields` and `status`. It needs the same verbs as
§13. Memory is the size of the held intermediate objects, typically a few
per namespace.

**On Secrets, `Content` is opt-in and holds Secret data in memory**, for
held Secrets only. That is needed only for a Secret that names a Secret.
Without it, a `Next` on a Secret is given a metadata-only object, so it
can't read data. A provider that sets `Content` on Secrets takes on the
memory and the exposure. Providers whose cached client already reads all
Secrets hold that data anyway.

### 10.5 Namespace-restricted caches (R10)

The core builds cluster-wide informers. A caller that restricts them to
some namespaces (`SharedOptions.Namespaces`) can't cache refs outside those
namespaces. Such a ref is read live, every pass. If even the live read is
forbidden there, Acquire returns an error: fail closed for a new holder,
fail open for an established one (§8). The doc text says so, and that such
refs need §13's cluster-scoped RBAC.

### 10.6 PerName mode: the index ConfigMap (M1)

Under `resourceNames` RBAC a tracker can't list a protected kind, so it
can't find what it holds by label. Without help, four things would break,
because each starts from "every object this kind holds":

- the startup recheck (§10.2);
- Disown and its count (§7.4);
- `WithHeld` (§11.6);
- the rebuild after a lost event.

**The index** (the user's decision) is one ConfigMap per holder kind, in the
provider's namespace. It lists the protected objects that kind holds:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: credential-use-index.repo.git.krateo.io
  namespace: krateo-system
data:
  entries: |
    /Secret/team-a/git-token
    core.krateo.io/KubernetesTarget/team-b/prod
```

Each line is `<group>/<kind>/<namespace>/<name>`. The group is empty for
core kinds, and so is the namespace for cluster-scoped objects. The lines
are sorted.

**Its location is fixed once, on the holder CRD.** The first process of the
kind to start writes `credential-use.krateo.io/index: <namespace>/<name>` on
the CRD, with a patch that tests the annotation is absent. Every later
process reads it. **A process whose configured index (`SharedOptions.Index`)
disagrees with the CRD's refuses to start** (§7.3), so two processes of one
kind can never keep two indexes. Moving the index means Disown, then
re-pointing the annotation by hand.

**Writes, and their order.**

- **Add before A2.** When A2 is about to write a P whose line isn't in the
  index, the tracker first adds the line, with an update guarded by the
  ConfigMap's resourceVersion (retried on conflict). The index is therefore
  always a superset of what this kind holds.
- **Prune after R3.** When R3 has removed K's last finalizer from P (K's
  marker label is gone), the tracker removes the line, guarded the same
  way. A crash between R3 and the prune leaves a line for an object that
  holds nothing. The next startup recheck finds no finalizer there and
  prunes it.

**Size.** A ConfigMap holds at most 1 MiB. A typical line is about 60 bytes,
so the index holds about 17,000 names. With every field at its maximum (a
253-character name, a 63-character namespace, a long group), a line is
about 400 bytes, and the index still holds about 2,600. PerName mode is for
small held sets, whose names are known when the RBAC is generated, so this
is far above what it is used for. **When the index is full**, adding a line
fails. A2 then doesn't run: a new holder fails closed, an established one is
Degraded (§8), and the tracker logs and counts it.

**Who reads it in PerName mode.**

- **The startup recheck** reads the index, GETs each named object, and
  enqueues a release batch for every (K, N) finalizer it finds.
- **Disown** reads it to find what to strip and what to count (§7.4).
- **`WithHeld`** reads it and stops at the first line whose object still
  carries K's prefix (§11.6).

**If someone deletes it by hand,** the tracker sees that (it watches the
ConfigMap by name) and **rebuilds it from the records**. It lists the
holders of K, takes the union of their records, GETs each named object, and
writes back the ones that carry a finalizer with K's prefix.

**The residual.** An object whose last holder's record was already gone
(after `Forget`), but whose R3 hadn't landed when the index was deleted, is
in no record, so the rebuild can't find it. Its finalizer stays until
someone removes it. It isn't lost: K's marker label is still on it, so an
operator with list rights finds it with
`kubectl get secrets -A -l credential-use.krateo.io/k-<hash>=true`, and
`DisownPrefix` with an explicit name list strips it. The doc text says not
to delete the index.

**RBAC.** `get`, `update` and `patch` on that one ConfigMap, by
`resourceNames`, plus `watch` on it through a field-selected watch. Then
either `create` on ConfigMaps in the provider's namespace, used once, or
none at all if the chart ships the ConfigMap empty, which is the
recommendation. These grants are bound in the provider's own namespace. That
is not the unsafe namespaced Role of §13: the provider's own namespace is
not one whose objects it manages, and if it is deleted the provider stops
anyway (core-provider#313).

**The reviewer's three interleavings, resolved:**

1. **A restart between `Forget` and R3.** H's last pass writes `Forget`
   with its owner-finalizer removal, and H is gone. The process restarts
   before R3, so the in-memory release batch is lost. P still carries the
   finalizer, and its line is still in the index, because it is pruned
   only after R3. The startup recheck reads the index, finds the finalizer,
   and enqueues the batch. R2 finds no users, so R3 releases it.
   (LabelScoped mode reaches the same end through the protected informer's
   initial list.)
2. **A never-established holder deleted while the process was down.** H's
   pass held P (A1, the index line, A2), and then Connect failed, so H never
   got its owner finalizer. H is deleted while no process runs. It has no
   finalizer, so it vanishes at once, and no event reaches anyone. On
   restart, P's line is in the index, so the recheck enqueues its batch. R2
   finds no record naming P, and R3 releases it.
3. **A1 before the Disown switch.** A pass that began before the switch was
   set runs A1, then the switch is set, then the Disown loop strips H's
   record and P's finalizer, and then the pass's A2 lands and re-adds the
   finalizer. Two things close this:
   - every Acquire reads the switch again just before each A2. A pass that
     sees it set stops and returns its refs unheld;
   - a pass already past that check, whose A2 lands after the strip, is
     caught by the Disown loop's next pass. The live count, from the index
     in PerName mode, sees the finalizer, so the loop strips it again. The
     count is non-zero, which resets the settling.

   Disown is settled only after `DisownPasses` consecutive zero passes,
   spanning longer than the longest reconcile (§7.4). So no pass that
   started before the switch can still land a write after it is settled.

## 11. Lifecycle edges

### 11.1 A ref that changes on update

The next pass computes the new set. A1 writes it, adding new refs and
dropping old ones, and A2 holds the new ones. Dropping a ref is always safe,
because the holder will never read it again. The holder's update event maps
the dropped refs to a release batch. A ref reached through `Next` changes
the same way when an intermediate changes (§9.2).

### 11.2 A Secret that doesn't exist yet

A2 gets NotFound, so the ref is Unheld/Missing. The record keeps it. Connect
runs and usually fails, as it does today, and the holder gets no owner
finalizer. When the Secret appears, the prober or the holder's next pass
holds it, and `BeforeEstablish` re-checks it before any owner finalizer is
written. A provider that tolerates a missing optional Secret is unaffected.

### 11.3 A Secret in another namespace

Allowed. The finalizer names the holder's namespace (§3), and the release
check lists that namespace only. Under §5, the Secret's own namespace
decides whether a deletion is recoverable. A cross-namespace user keeps a
Secret in a terminating namespace alive until that user goes, so that
namespace stays Terminating until then. The waiting event names the user.

### 11.4 A Secret already being deleted when an object first references it

If it carries this (K, N)'s finalizer, A2 holds it. The epoch moves and the
holder counts, so §5 keeps the Secret while its namespace is terminating or
a user is deleting. Otherwise it is released as §5 says. If it lacks the
finalizer, A2 is Forbidden:

- **a new holder** gets `MustNotConnect`: no Connect, §12's report, and
  requeue with backoff. It never gets its owner finalizer, so it deletes
  freely;
- **an established holder** whose refs were just re-pointed to that Secret
  can't be protected: the Secret was being deleted before the reference
  existed. It is Degraded and proceeds, with a Warning event. That is a
  documented limit, and the remedy is to fix the ref.

### 11.5 A provider uninstalled before its users

Today the users' own finalizers stay and their namespaces wedge. That
doesn't get worse. What is newly stuck is every protected object those
remaining users held. **Uninstall through Disown** (§7.4). Doc text:

> Before uninstalling a provider, delete its objects and wait for them to
> go. Or annotate each holder CRD with `credential-use.krateo.io/disown=all`,
> and wait until its `credential-use.krateo.io/held-count` is `0` and
> `credential-use.krateo.io/disown-settled` is `true`. After an out-of-order
> uninstall, remove the objects' own finalizers first. Then, if the CRD is
> gone, run the `DisownPrefix` Job. Otherwise remove by hand the
> `credential-use.<kind>.<group>/<namespace>` finalizers, the annotation with
> the same key, the kind's `credential-use.krateo.io/k-<hash>` label, and the
> `credential-use.krateo.io/held` label once no `credential-use.` finalizer
> is left.

core-provider#313 is the same class: a controller that runs in the
namespace being deleted dies before it can release anything. This design
doesn't fix that. A provider must not run in a namespace whose objects it
manages, and the RBAC that keeps it working must not live there either
(§13).

### 11.6 Scale to zero (R11)

At 0 replicas, nothing releases. So **`scaletozero.Exists` also counts, as
instances, protected objects that carry a finalizer with the Kind's
prefix**. A new option asks for it,
`scaletozero.WithHeld(protected ...schema.GroupVersionKind)`. With it, if
the instance list finds nothing, `Exists` lists each protected kind
metadata-only, selected by **K's marker label** (§10.1), with `Limit: 1`. It
stops at the first object whose finalizers include the prefix
`credential-use.<kind>.<group>/`, and pages on only past a hash collision
(L3). A stale hold then keeps the controller running until it releases it,
after which the Kind can scale down.

In PerName mode, `WithHeld` reads the index instead, through
`WithHeldIndex(index types.NamespacedName)`, and GETs lines until one still
carries K's prefix (§10.6). In both modes, **a set Disown switch whose
`disown-settled` isn't `true` counts as an instance** (§7.4), so the
controller runs until Disown has finished.

This is a `pkg/scaletozero` change, shipped in the same release, and the
owners (core-provider, oasgen-provider) pass the option. The marker is per
GroupKind, so with core-provider's controller per composition version, a
hold keeps every version's controller up. That is safe, because any of them
can release it, and it costs only the running replicas.

### 11.7 An object with deletion-policy orphan

No Connect is needed (`:821`). `Forget` goes in that Update, the delete
event enqueues its batch, and the refs are released. Until then it held its
refs like any other object.

### 11.8 Foreground deletion cycles

A holder H might own the Secret it reads, a connection Secret say, with
`blockOwnerDeletion`. If H is deleted in the foreground, H waits for P to be
deleted, and P is kept because H is a deleting user. The cycle is broken in
the usual order:

1. The runtime reconciles H. P is still readable, so Connect and Delete run.
2. The runtime removes its owner finalizer and, in the same write, the
   record.
3. H no longer counts (§6.1). Its `foregroundDeletion` finalizer is not the
   owner finalizer.
4. P is released and goes, and the garbage collector then completes H.

If a third party removes the owner finalizer and leaves the record, the
§6.1 exclusion stops H from counting just the same.

### 11.9 A ref that was never recorded

These are the cases the bookkeeping misses, and how each is handled:

1. **Objects from before the upgrade:** backfill on their next pass (§6.6).
   That includes a deletion pass, if the Secret is not yet being deleted.
   Objects already wedged, whose Secret is already gone, stay wedged until a
   human removes their finalizer, as today.
2. **A ref `RefsFunc` doesn't name** but Connect reads anyway. Nothing
   protects it. This is a provider defect, detected two ways:
   - **in tests:** `reconcilertest.AssertCredentialRefsCover` runs Connect
     against a recording client. It fails when Connect reads a Secret, or a
     `Protected` kind, that isn't in the object's refs after `Next`. It also
     fails when a path resolves to nothing, so a mistyped path doesn't
     silently protect nothing (§16.2);
   - **at run time:** `credentialuse.CheckingReader(reader, tracker)` wraps
     the provider's client. A read of a protected kind absent from the
     object's record logs once per object and counts in a metric.
3. **`RefsFunc` or `Next` fails:** fail closed for a new holder, open for an
   established one (§8). Release never calls them.
4. **The record removed or edited by hand:** the next pass re-announces and
   re-writes. In between, a releaser can miss the holder. That is
   documented as "don't edit `credential-use.krateo.io/refs.v1`".

### 11.10 Consumers that don't implement the interface

No behaviour change: no `Shared`, no informer, no annotation, no write, and
the same Reconcile path as today, Connect on deletion included.

## 12. What to report (C4)

In neutral terms first, so that an outside runtime knows what to report.
Then provider-runtime's mapping.

| State | Report (neutral) | provider-runtime |
|---|---|---|
| A deleting holder whose protected object is still referenced elsewhere | Nothing extra. Its deletion runs normally, because the object stays readable: the object reports that it is deleting, and its sync state reflects the delete call | Ready=False/Deleting; Synced=True/ReconcileSuccess, or the delete's error |
| A protected object waiting for users after its deletion was requested, including during the grace | No condition, since Secrets have none. A Normal event on the object, throttled, once per state change, naming the holder kind, namespace and count, plus the deleting holders by name, so a stuck holder is named (NQ2); an Info log line; the waiting gauge | event `WaitingForCredentialUsers`, gauge `credentialuse_waiting_objects{holder_kind}` |
| A live holder whose protected object is being deleted but held | It keeps working; no condition change; a throttled Warning event naming the object and why it is kept | event `CredentialDeleting` |
| A live holder whose protected object was released under §5 and is gone | Its credential read fails, as today; the provider's error names the object | Synced=False/ReconcileError from Connect |
| A new holder that must not connect (§11.4), or whose acquire failed | Sync failed, with a message naming the object and the reason; readiness unknown | Synced=False/ReconcileError, Ready=NotObserved |
| An established holder whose acquire failed | It proceeds; a Warning event and one log line | event `CredentialUseDegraded` |

In a terminating namespace, events are rejected (`git-provider-rlgov.log`
line 7), so there the log and the gauge are the signal. The message for a
new holder that must not connect is
`credential <kind> <ns>/<name> is being deleted and can't be held; not connecting until it is replaced or the reference changes`.
No new condition type is added (U4).

**The held gauge counts finalizers, not labels** (L4).
`credentialuse_held_objects{holder_kind}` is the number of (protected
object, finalizer) pairs with the kind's prefix in the process's cache. A
labelled object whose finalizers are gone doesn't count, and the janitor in
§10.1 removes its label. The cluster-wide, live count for Disown is
separate, and is written on the CRD (§7.4).

## 13. RBAC (U7)

**Cluster-scoped, by name where the names are known.** A ClusterRole plus a
ClusterRoleBinding. They survive any namespace teardown (MED-10b). **A
namespaced Role is unsafe for this feature**, and the doc says so: the
namespace controller deletes the Role and RoleBinding in a terminating
namespace like any other content, and from then on the provider can't
remove the finalizers it holds there. That is a new wedge.

| Resource | Verbs | Why |
|---|---|---|
| Secrets, and each other `Protected` kind | `get`, `patch` | A2, R1, R3, the live fallbacks and the prober |
| the same | `list`, `watch` | the label-scoped informer |
| namespaces | `get` | §5's live `deletionTimestamp` check |
| the holder kind | `get`, `list`, `watch`, `patch`, `update` | the holder informer, R2's consistent list, A1 (patch), and `Forget` in the finalizer-removal Update and on its own (update) (L8) |
| customresourcedefinitions, by `resourceNames` (the holder CRDs) | `get`, `list`, `watch`, `patch` | the Disown switch, read through a field-selected watch; the count fields and the one-time index pointer, written only on change (§7.4, §10.6) |
| configmaps, by `resourceNames` (PerName mode: the kind's index) | `get`, `watch`, `update`, `patch`, plus `create` in the provider's namespace once, unless the chart ships the index | §10.6 |
| events | `create`, `patch` | §12 |

`update`, `create` and `delete` on protected kinds are not needed: every
write is a patch.

**A new privilege for providers with static CRDs** (L6). git-provider ships
its CRDs in its chart, and has never needed to write them. It now needs
`patch` on its own CRDs, by name. The writes are rare: the Disown fields
when they change, and the PerName index pointer once. A CRD write makes the
API server rebuild its OpenAPI, which is why nothing else writes the
CRD.

**By name.** Where refs are known when the RBAC is generated, as
core-provider generates RBAC per composition and knows the passwordRef
Secret, the ClusterRole lists them in `resourceNames`. A list or watch
authorized by `resourceNames` must name one object with a
`metadata.name` field selector. So such a protected kind uses
`Watch: PerName`: one watch per held object, field-selected, plus GETs.
That fits small held sets. Refs that can't be known in advance, such as
git-provider's user-supplied Secrets or oasgen's paths, need the verbs on
all objects of the kind, with `Watch: LabelScoped`.

Doc text for consumers:

> If your connecter implements `CredentialRefsProvider` (or your runtime
> calls `credentialuse`), grant, in a ClusterRole bound with a
> ClusterRoleBinding: `get`, `list`, `watch` and `patch` on `secrets` and on
> every other kind your refs name; `get` on `namespaces`; `patch` and
> `update` on your own kinds; `get`, `list`, `watch` and `patch` on your
> kinds' CustomResourceDefinitions, by name. Use `resourceNames` only when you know every Secret name when
> you generate the RBAC, and set `Watch: PerName` for those kinds. Don't
> use a namespaced Role for these permissions: it is deleted with its
> namespace, and objects there then can't finish deleting.

## 14. Tests, and what the reviewer should attack

**Envtest: namespace teardown** (`pkg/credentialuse`, with its own
`TestMain` and `requireAPIServer`, modelled on
`pkg/scaletozero/envtest_test.go`). A manager runs the real reconciler for
a test CRD (`Widget`). Its connecter implements `CredentialRefs` and reads
the Secret in Connect, and its client records Delete calls. envtest has no
namespace controller, so the test plays that controller, in the order seen
on krateo-057:

1. Create a namespace, a Secret, and two Widgets naming it. Wait for
   Synced=True, the finalizer, epoch and label on the Secret, and the
   record on both Widgets.
2. Delete the namespace. Assert that it is Terminating, and that creating a
   Secret in it is Forbidden. That pins the premise, and fails the test if
   NamespaceLifecycle isn't on.
3. Delete the Secret first, then both Widgets. Assert that:
   - the Secret stays, readable, with a `deletionTimestamp`;
   - Delete was called for each Widget;
   - **for the whole teardown, no Widget ever has Ready=Deleting together
     with Synced=ReconcileError and a not-found message for its
     credential** (I1). A watch on the Widgets checks every status it
     delivers;
   - both Widgets go, then the Secret goes.
4. Finalize the namespace through the `finalize` subresource, and assert
   that it goes.
5. **Control:** the same sequence with a connecter that doesn't implement
   the interface must wedge: the Widget is still there after a timeout,
   with the not-found message. That proves the test reproduces the defect.

**Further envtest cases:**

- U5 and the grace: a Secret deleted in a live namespace, with live users
  and none of them deleting, is kept for the grace and then released.
  Recreating it under the same name fails with AlreadyExists during the
  grace, and succeeds after it. The holders acquire the new UID through the
  prober. A Secret with no users is released at once.
- **Interleaving A** (§5.1): delete the Secret, then a holder 1s later, in a
  live namespace. The Secret is kept until the holder is gone. And again
  with the holder deleted after the grace, which hits the residual: the
  holder fails until the Secret is recreated.
- **Interleaving B** (§5.1): delete the Secret, then the namespace, then
  the holders, each right after the previous DELETE returns, as Helm's
  `batchPerform` does. Let the test's namespace controller delete the
  holders too. The live re-read of the namespace's `deletionTimestamp` keeps
  the Secret, and the namespace tears down. There is no variant that waits
  for the Secret to be gone before the next kind: Helm v3.21.3 doesn't wait,
  and no tool that does is known (§5.1).
- A namespace deletion that starts just after the grace, and a holder
  deletion between R2 and R3 (an interceptor delays R3), check §6.4's
  last-step reads and the deletion pass's unskippable A2.
- A Secret deleted in a live namespace while a user is deleting is kept
  until that user goes, past the grace.
- A ref changed on update releases the old Secret.
- A cross-namespace user keeps a Secret whose namespace is terminating.
- A Secret already being deleted gets a new holder with `MustNotConnect`,
  and that holder deletes freely.
- A chain Widget → Target → Secret, with the Target deleted first in the
  teardown; a re-pointed Target enqueues its holders.
- `kubectl replace`-style stripping of the finalizer and label enqueues
  holders, and they write again.
- A label-scoped watch delivers losing the label as a deletion (§2).
- R3's failed `test` op: the exact status code.
- Disown, through the CRD annotation, drains two `Shared` instances (two
  processes) to a live `held-count` of 0, including a namespace outside
  `SharedOptions.Namespaces`; `DisownPrefix` after the CRD is deleted, and
  its refusal while the CRD exists.
- Start fails when `CredentialUseSource` isn't wired (M4).
- `IgnoreCredentialUseRecord` drops a record-only update and passes every
  other change (M6).
- The prober finds a recreated Secret by GET with few missing names and by
  one LIST with many, and backs off per ref (L9).
- **PerName mode and the index** (§10.6): the pointer written once on the
  CRD; a second process with a different index refuses to start; lines
  added before A2 and pruned after R3; a hand-deleted index rebuilt from
  records, with the residual case asserted to be found by its marker label.
  The reviewer's three interleavings each run as a test: a restart between
  `Forget` and R3; a never-established holder deleted while the process is
  down; A1 before the Disown switch, with the A2 landing after the strip.
- Disown settles only after `DisownPasses` consecutive zero passes, and a
  re-added finalizer resets the count; a Kind at zero replicas wakes for the
  switch; `DisownPrefix` with `DryRun`.
- A holder informer relist resets `recordSince`, and the next passes write
  (L2).
- `IgnoreCredentialUseRecord` passes a record change it didn't write (L8).
- A foreground-deleted holder that owns its Secret completes (§11.8).
- Setup after start, and `Track` twice with equal and with different
  Options.
- `scaletozero.Exists` with `WithHeld`, stopping at the first object with
  K's marker label, and paging past a forced hash collision.
- `Watch: PerName` against `resourceNames` RBAC, on an envtest API server
  started with `--authorization-mode=RBAC`, through an impersonated
  ServiceAccount.

**Unit tests for the races (§6)** run on an in-memory API model: a
linearizable store with resourceVersions, `ValidateNoNewFinalizers`,
strategic-merge and JSON-patch semantics, a namespace phase, and a lagging
cache per actor. They enumerate every schedule of:

- the acquirer's steps: A1, A2, and Connect. Each step can succeed, fail
  with an error, crash, or lose its response;
- one or two releasers' steps: R0 to R3, each read fresh or stale;
- the deletion of P, of H, and of the namespace, at every point.

That includes **error or crash after A1, with a concurrent releaser**
(HIGH-1), and a restart that clears the verified set. The model also covers
the second review's schedules:

- a ref that this pass's A1 adds, with a stale `written` entry from an
  earlier run of the same ref (M1). Away and back, without the informer
  ever delivering the middle version, must still write;
- a holder deletion and a namespace deletion that start during the grace,
  just after it, and between R2 and R3;
- a deletion pass that holds a `written` entry: it must still write;
- a recreate behind a cached UID (L6), which is asserted to be the only
  window and to close when the deletion is delivered;
- the epoch-only and label-only self-write filter (L2).

**The model asserts safety, not only convergence** (L4). The invariant: *a
holder H that is deleting (with the owner finalizer), whose pass held P
before H's deletion began, never reads P as NotFound, unless one of these
happened first:*

- *P was deleted while its namespace was live and no user of P was deleting
  (U5's release);*
- *Disown ran;*
- *a third party stripped the finalizer.*

The model records each read and each release decision, so every violation
names its schedule. Convergence is checked separately: when no users
remain, the finalizer is removed within a bounded number of steps.

**Stress** (envtest, `-race`): many goroutines create, re-point and delete
Widgets over a small pool of Secrets and Targets, in several namespaces
with cross-namespace refs. Others delete Secrets, recreate them, and delete
namespaces. Two holder kinds and two `Shared` instances run, as two
processes would. The §6 invariant is checked on every observed read. At the
end, every namespace tears down, and no object is left with a
`credential-use.` finalizer and no users.

**Drain:** cancel the process between A1 and A2, between R1 and R3, and in
the middle of a release batch. Restart, and assert that it converges with
no safety violation.

**What the independent reviewer should attack:**

- every path to `AddFinalizer`, and whether one can be reached without A2
  or a live miss after the pass's last A2 (§6.7);
- the `written`/`observed` skip: any way `t_obs < t_issue` can hold for a
  record that wasn't visible when the write was issued (clock source,
  leader change, record rewritten away and back);
- R3's tests: the epoch, the index, and the full-array test when the label
  is removed. Whether any interleaving lets R3 remove a finalizer an A2
  re-added;
- strategic merge on built-in kinds and guarded merge on custom resources:
  whether either can drop someone else's finalizer;
- `Forget` in the same write as the owner-finalizer removal on both paths,
  the standalone Forget when the owner finalizer is already gone, and the
  §6.1 exclusion;
- §5's live Namespace check, and the claim that P's `deletionTimestamp`
  follows the namespace's in a teardown;
- the self-write filter in §9.2: can a foreign change be mistaken for the
  tracker's own, or the other way round, so that an enqueue is missed or a
  storm starts?
- the label-scoped informer's handling of objects that lose the label, and
  the prober's cap and rotation;
- fail-open for established holders on deletion passes: does it ever turn a
  safe state into a wedge?
- two processes for one GroupKind with different version selectors;
- Disown racing a still-running acquirer during a rollout, and whether
  `DisownPasses` × `DisownInterval` really exceeds every in-flight pass;
- the PerName index: add-before-A2 and prune-after-R3 under conflicts and
  crashes, the CRD pointer race between two first processes, and the
  rebuild's residual (§10.6);
- the unrecoverable namespace window of §6.4, and whether anything makes
  it wider than claimed;
- the grace: the order of the re-reads, and whether any path releases a
  deleting P in a namespace that already has a `deletionTimestamp`;
- `recordSince` and `observed`: whether any informer reordering or relist
  can make `t_obs` earlier than the record's true arrival;
- the start check for `CredentialUseSource` (M4): a controller that starts
  its workers before a source, if any controller-runtime path does.

## 15. Alternatives rejected

**15.1 One finalizer per holder object.** It needs no release check, and
its writes don't conflict, because a strategic merge patch adds or removes
one entry. It is rejected because of how many there would be. A
configuration-level Secret, oasgen's one Secret per Configuration used by
every object of a kind, carries one finalizer per user, about 90 bytes
each. Every acquire and release re-sends the whole object to the API server
and to every Secret watcher. The practical ceiling is roughly 1,000 to
2,000 users, and that size of fleet is well within reach. The shared
finalizer (§3) is bounded by kinds × namespaces.

**15.2 Skip Connect on deletion when the credential is missing.** It leaks
the external resource. A missing Secret says nothing about whether the
external resource exists. For a kind whose Delete calls out, removing the
finalizer without Connect leaves an orphan with nothing left that knows
about it.

**15.3 A "Delete needs no external call" marker.** It fixes git-provider's
Repo, whose Delete is a no-op, and nothing else. The defect is generic. As
a provider-side practice it remains worth recommending (§16.4).

**15.4 Owner references from the Secret to its users.** An owner reference
says the owner's deletion cascades to the dependent, the opposite of what
is needed. `blockOwnerDeletion` only blocks foreground deletion, and
namespace deletion uses background deletion. Owner references can't cross
namespaces.

**15.5 A usage object per holder (ProviderConfigUsage).** It needs a CRD in
every chart, doubles the objects and writes, needs garbage collection, and
still needs a list to find the last user. The record on the holder is the
same information without a second object. Crossplane counts usages from a
cache, which is the race §6 closes.

**15.6 Recomputing refs at release time.** See §4.

**15.7 A drain mark** (the first revision): the releaser marked P, listed,
then removed. It is replaced by write-to-acquire. The mark let a holder
connect on a record alone, and that was HIGH-1. Write-to-acquire makes every
acquirer's write the thing that breaks a concurrent release, and drops the
second annotation.

**15.8 Labels as the record.** A label value is limited to 63 characters,
so the record would need hashed identities and one label per ref. Users and
tools also edit labels far more often than annotations. The one label kept
(`held`) only scopes caches.

## 16. Consumer adoption

### 16.1 git-provider

git-provider implements `CredentialRefs` on both connecters, wires
`CredentialUseSource`, and changes its RBAC as §13 says (ClusterRole:
`patch` on `secrets`, `get` on `namespaces`, `patch` on its kinds).

- **Repo:** `spec.fromRepo.secretRef`, `spec.fromRepo.usernameRef`,
  `spec.toRepo.secretRef` and `spec.toRepo.usernameRef`, read in Connect
  through `loadExternalClientOpts` (`internal/controllers/repo/utils.go` at
  0.17.0: `resource.GetSecret` at lines 65, 76 and 93). Skip nil refs. The
  probe's Repo named one Secret twice, which becomes one ref.
- **LocalResource:** `spec.toRepo.credentials.secretRef` and `usernameRef`,
  read in Connect (`internal/controllers/localresource/localresource.go`,
  read at `69d98eb`; confirm at the tag that adopts this).
- **`spec.configMapKeyRef` on Repo** is a ConfigMap. If it is read on the
  delete path, git-provider adds it as a protected ConfigMap ref. Its
  maintainers should check; this design didn't.

### 16.2 oasgen-provider (rest-dynamic-controller)

rest-dynamic-controller adopts through unstructured-runtime's integration
(§7.2), not `CredentialRefsProvider`. Its `RefsFunc` is the collector it is
writing for #510:

- **Per-object Secrets** resolved from paths in the object's own spec come
  straight from `RefsFunc`.
- **The Configuration-level Secret** is a chain: a ref to the Configuration,
  with a `Next` that names its Secret. The Configuration is held too.
- **`RefsFunc` needs the RestDefinition** that describes the paths to be
  loaded and readable. It is called only on acquire. **Release never calls
  it**, and must not, or the deadlock comes back one level out: a teardown
  that deletes the RestDefinition first would leave nothing able to
  release. When the RestDefinition can't be read, `RefsFunc` errors. A new
  holder fails closed, an established one fails open with its record (§8).
- **A mistyped or unmatched path resolves to not found**, so it silently
  protects nothing. `AssertCredentialRefsCover` (§11.9) fails on it. The
  collector's tests should run it for every RestDefinition fixture.

### 16.3 unstructured-runtime (core-provider)

It has the same deadlock (§1, and core-provider#402 for the chart
`passwordRef` in the composition controller's Delete). It integrates the
core as §7.2 says, using `Track` after start for kinds discovered at
runtime (§7.3).

- A remote CompositionDefinition names its KubernetesTarget with a `Next`
  to the kubeconfig Secret (§9.1).
- Compositions name their CompositionDefinition as `Guaranteed`, with a
  `Next` to the chart `passwordRef` Secret (§9.3). The label fallback
  there is unverified.
- RBAC is by name where generated (§13), and **its lifetime must outlast
  the holds** (see below).
- With per-version controllers, the release list covers the whole
  GroupKind (§3).
- **Its composition delete is a Helm uninstall**, which deletes Secrets, and
  a chart's own Namespace, before custom resources. It doesn't wait for one
  kind to be gone before deleting the next (§5.1, verified), so the grace
  covers it. core-provider needs no change to its uninstall order.

**Generated RBAC must outlast the holds.** Read from a local checkout of
core-provider (`go/core-provider/internal/tools/deploy/deploy.go`). The
commit couldn't be determined, so core-provider should confirm this at
main. `Undeploy`:

- refuses while composition instances exist (`ErrCompositionStillExist`),
  but only when it will delete the CRD (`!SkipCRD`);
- then deletes the controller's Deployment, and waits for its pod to drain;
- then deletes its ConfigMap and its RBAC: the ClusterRole and
  ClusterRoleBinding unless `KeepShared`, the Role and RoleBinding, and the
  ServiceAccount.

Holds are released by the controller's release loop, moments after the last
instance goes. Two cases matter:

1. **The release can be cut off.** If `Undeploy` runs right after the last
   instance goes, the controller is shut down before its release batch
   lands. Its graceful shutdown cancels the loop. The RBAC that would let
   any process release is then deleted, and so is the CRD, so the holds
   stay until `DisownPrefix` runs.
2. **The instance guard is skipped with `SkipCRD`.** That is the case where
   another definition keeps serving the kind. The retiring controller's
   RBAC goes regardless of holds. The holds are per GroupKind, so a
   surviving controller of the same kind can release them, but only if its
   RBAC covers the same protected names.

**What core-provider must change:**

- `Undeploy` treats held objects as instances, in both the `!SkipCRD` and
  the `SkipCRD` paths. It refuses with `ErrCompositionStillExist`, or a new
  error, while `scaletozero.Exists(... WithHeld(...))` (or `WithHeldIndex`
  in PerName mode) finds a hold under the kind's prefix that this
  definition's RBAC is the only grant for.
- With `SkipCRD`, the surviving definition's generated `resourceNames` must
  include every name the retiring one held, before the retiring RBAC is
  deleted.
- After `crd.Uninstall`, it runs `DisownPrefix` for the kind as a backstop.

### 16.4 "Answer from status" is a complement worth recommending

core-provider#314 fixed a Delete that couldn't proceed with an unreadable
chart, by answering from status. The same practice is worth recommending to
any provider whose Delete, or whose decision that the external resource is
gone, can be made without credentials. git-provider's Repo is one: its
Delete is a no-op. Read credentials lazily, in the client calls that need
them, not in Connect. A deleting object whose Delete doesn't call out then
never reads its Secret. It is a complement, not a substitute: it does
nothing for kinds whose Delete calls out (§15.3), and the runtime still
connects.

## 17. Review history

The first revision is `a2ee5d1`. This revision answers its review.

**The user's decisions:**

| Item | Resolved in |
|---|---|
| U1 Chains: protect any kind in v1, with `Next` | §9, §9.1 |
| U2 Implicit wiring; setup failure refuses to start | §7.3, §7.5 (the builder line: NQ3, below) |
| U3 Build `Resolved` now; alone it doesn't close the window | §9.2 |
| U4 Events, a log line and a gauge; no new condition | §12 |
| U5 Hold only when unrecoverable; the residual and its remedy | §5 |
| U6 Shared finalizer per (K, N), write-to-acquire, no drain mark, JSON-patch tests, the proof, the in-memory verified set, §15's grounds | §3, §6, §15.1, §15.7 |
| U7 ClusterRole and ClusterRoleBinding, `resourceNames`, a namespaced Role is unsafe, core-provider's point 5 | §13 |

**core-provider's review:**

| Item | Resolved in |
|---|---|
| C1 A core without a manager, and a controller-runtime adapter | §7.1, §7.2, §7.5 |
| C2 Guaranteed refs; the label fallback flagged as unverified | §9.3, §16.3 |
| C3 Setup after start; L8 idempotence; fake managers | §7.3, §7.5 |
| C4 §11's report in neutral terms | §12 |
| C5 core-provider#402 cited | §1, §16.3 |

**oasgen's review:**

| Item | Resolved in |
|---|---|
| O1 RefsFunc only on acquire; release never calls it | §4, §16.2 |
| O2 Unmatched paths protect nothing; AssertCredentialRefsCover | §11.9, §16.2 |

**installer:**

| Item | Resolved in |
|---|---|
| I1 No Ready=Deleting with a credential not-found ReconcileError during teardown | §14, envtest step 3 |

**The independent review:**

| Item | Resolved in |
|---|---|
| R1 HIGH-1 Error or crash after the announce | §6.3, §6.4; model schedules in §14 |
| R2 HIGH-2 | §5 (by U5) |
| R3 HIGH-3 Disown replaces ReleaseOnly; L5 | §7.4 |
| R4 HIGH/MED-4 Fail open for established holders, fail closed for new ones; the §7 vs §8/§10.3 contradiction removed | §8, §9.1, §11.9 |
| R5 MED-5 Enqueue holders on protected changes; paused, slot-waiting and re-point cases; third-party stripping; the remaining window | §9.2, §10.2, §6.5 |
| R6 MED-6 Live re-check before `AddFinalizer`; seeing new objects; the invariant restated | §6.7, §8 step 8, §10.3 |
| R7 MED-7 Releases coalesced per (K, N), with the cost | §6.2, §6.6 |
| R8 MED-8 The held label, label-scoped caches, the transform, the live fallback, memory | §3, §10.1 |
| R9 MED-9 Informers for kinds with a `Next`; Secret data opt-in | §10.4 |
| R10 MED-10 Namespace-restricted caches | §10.5 (RBAC: §13) |
| R11 MED-11 `scaletozero.Exists` counts held objects | §11.6 |
| R12 MED-12 `Forget` mandatory; the standalone write; the owner finalizer in Options; `established` under `WithFinalizer`; foreground cycles | §6.1, §7.2, §7.5, §8, §11.8 |
| MED-13 JSON patch `test` ops on the epoch and the index | §6.2 R3 |
| L1 | gone with U6 (§15.7) |
| L2 Patch types pinned | §6.2: `MergeFromWithOptimisticLock` for holders and custom resources, strategic merge for built-in kinds, JSON patch with `test` for release |
| L3 An unparseable record counts as a user; the key is versioned | §4 |
| L4 The model invariant fixed | §14 |
| L5 Deleting a holder CRD goes through Disown | §7.4 |
| L6 The backfill herd and its QPS | §6.6. Its "no extra pass" was wrong, and is corrected by the second review's M6 |
| L7 The `krateo-system` wedge, and how U5 limits it | §5.2 (no cap: NQ2, below) |
| L8 Idempotent setup; fake managers | §7.3, §7.5 |
| L9 Cosmetic | noted; no design change |

**The second review** (the re-review of `ffc41cb`, and the user's answers
to NQ1 to NQ4):

| Item | Resolved in |
|---|---|
| NQ1 A 60s configurable grace; the Namespace's `deletionTimestamp` and the deleting users re-read live as the last step before R3; deletion passes never skip; the Helm text corrected and verified; interleavings A and B; the cost to rotation | §5, §5.1, §6.2 R1 to R3, §6.3, §6.4; tests in §14 |
| NQ2 No cap; the event and gauge name the stuck holder | §5.2, §12 |
| NQ3 The builder line `.WatchesRawSource(r.CredentialUseSource())` | §7.5 |
| NQ4 Filed as provider-runtime#46, out of scope | §3 |
| M1 A1-added refs never skip; drops and resets in the pass; `t_obs` from the informer's delivery time, overwritten; §6.6's restart claim re-checked | §6.2 A1, §6.3, §6.6 |
| M2 Disown as a cluster-visible switch on the holder CRD; a live cluster-wide count; `DisownPrefix` for a deleted CRD; namespaces outside the caches | §7.4, §11.5 |
| M3 A `deletionTimestamp` on a protected object enqueues its batches, at once and after the grace | §10.2 |
| M4 Start fails when `CredentialUseSource` wasn't started; "safety is unchanged" removed | §7.5, §8 step 1, §7.2 item 5 |
| M5 A custom-resource A2 whose retries ran out is Degraded for established holders | §6.2 A2, §8 step 5 |
| M6 `IgnoreCredentialUseRecord`; the extra pass corrected; backfill paced only by workers and request rate, with figures | §6.6, §7.5 |
| L1 The prober lists per namespace, with its cost | §10.3 |
| L2 Self-writes: equal once the `credential-use.` keys are stripped | §9.2 |
| L3 K's marker label, hashed, so `WithHeld` stops at the first match | §3, §10.1, §11.6 |
| L4 The gauge counts finalizers; who removes the labels | §10.1, §12 |
| L5 A recreated unlabelled object is seen only by the prober | §9.2 |
| L6 The window around a recreate behind a cached UID | §6.5 |
| L8 Holder RBAC needs `update` | §13 |

The re-review's L7 (read the `deletionTimestamp`, not the phase) is folded
into NQ1 and §5.

**The Helm facts in §5.1 are verified.** The coordinator read them from
Helm v3.21.3 in the module cache: `pkg/releaseutil/kind_sorter.go`
(`UninstallOrder`, `lessByKind`), `pkg/kube/client.go:601-615` and
`:630-638`, and `pkg/action/uninstall.go:134-136`. They show that Helm
doesn't wait for objects to be gone between kinds. So the worst case the
third revision flagged doesn't happen. Its pending-verification item is
removed, and so is the waiting-uninstall envtest variant (§14). This
session's own reads of Helm's source were blocked by its permissions, and
were not retried.

**The final review** (the reviewer's last pass over `c7f3fb2`; nothing High):

| Item | Resolved in |
|---|---|
| M1 PerName mode: a ConfigMap index per holder kind, its location fixed on the CRD, a start failure on disagreement; written before A2 and pruned after R3; read by the startup recheck, Disown and its count, and `WithHeld`; its size, RBAC, and the rebuild after a hand delete with its residual; the reviewer's three interleavings | §10.6, §7.3, §7.4, §10.2, §11.6, §13; tests in §14 |
| M1, core-provider's generated RBAC lifetime: what can remove it before the holds are released, and what core-provider must change | §16.3 |
| L1 `CredentialUseSource` is a `TypedSyncingSource`; `WaitForSync` waits on the registration's `HasSynced` | §7.5 |
| L2 The relist residual | §6.5 |
| L3 The unrecoverable few-millisecond namespace window, stated | §6.4 |
| L4 The grace runs from the later of the `deletionTimestamp` and local first sight; Disown settles on consecutive zero passes | §5, §7.4 |
| L5 Guarded patches for the count fields; checking a Kind at zero replicas | §7.4, §11.6 |
| L6 CRD writes kept to the index pointer and the Disown fields, only on change; the new CRD `patch` privilege for static CRDs | §7.4, §13 |
| L7 No holder enqueue on a `deletionTimestamp` for kinds without a `Next` | §9.2, §10.2 |
| L8 The predicate drops only record changes equal to this process's last write | §7.5 |
| L9 Per-ref backoff in the prober, and GETs for few names | §10.3 |
| L10 §8 renumbered, with the source check first, before the orphan path; "Degraded" as the one fail-open term; re-verify §5.1 for Helm 4 | §8, §6.2, §11.4, §5.1 |

## 18. Open questions for the user

**Decided since the last revision**: NQ1 (a 60s grace, §5), NQ2 (no cap,
§5.2), NQ3 (the builder line, §7.5) and NQ4 (provider-runtime#46).

Nothing is open. No question needs the user.
