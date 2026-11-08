---
type: Architecture
title: provider-runtime — credential-use finalizer
description: Design, not built, for holding the Secrets (and the objects that lead to them) that a managed resource reads, for as long as losing them would be unrecoverable, so a namespace deletion can't wedge on a managed resource whose credentials were deleted first.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, finalizer, deletion, credentials, reconciler]
timestamp: 2026-11-08T00:00:00Z
---

# Credential-use finalizer

**Status:** design, second revision, for re-review. Nothing is built. The
user chose the approach: a finalizer on each Secret an object uses, the
analog of Crossplane's ProviderConfigUsage. The user's decisions on the
first review (U1 to U7) are binding here. §17 maps every review item to the
section that resolves it, and §18 holds the questions only the user can
decide.

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
- **The record** lives on the holder, not here (§4).

**Validity.** The name part is a namespace name, a DNS label of at most 63
characters. The prefix is a DNS subdomain: a CRD's lowercased kind must be a
DNS-1035 label, and its group is a DNS subdomain. Only the total length,
`15 + len(kind) + 1 + len(group) <= 253`, can fail, and only for a
pathological group. Setup checks it and refuses to start (§7.3). The name
contains a `/`, so the API server doesn't warn on it.

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

- **its namespace is terminating**, so it can't be recreated (§1);
- **one of its users is being deleted** and still needs Connect.

Otherwise, a deleted Secret can be recreated, and holding it would break
delete-and-recreate rotation: the recreate fails with AlreadyExists while
the old object waits.

The rule:

| Protected object P | P's finalizer for (K, N) is kept while |
|---|---|
| not being deleted | any user of P in N exists (§6.1) |
| being deleted, its namespace terminating | any user of P in N exists |
| being deleted, its namespace live | any user of P in N **is being deleted** |

A cluster-scoped P has no namespace, so it always takes the third row.

Once P is gone and is then recreated, its holders acquire the new
incarnation. It has a new UID, so nothing they held carries over (§6.3).
They find it through the missing-ref prober, or on their next pass (§10.3).

**"Its namespace is terminating" is read live.** The releaser GETs the
Namespace from the API server, never from a cache, before it releases a
deleting P under the third row. In a teardown, P's `deletionTimestamp` is
set after the namespace's, so a live read made after P's
`deletionTimestamp` always sees the namespace terminating. A cache could
lag behind the Secret's event and say the namespace is live.

**The residual, stated plainly.** If a Secret is deleted while its
namespace is live and no user is being deleted, it is released at once. Its
live holders then fail Connect until it is recreated. If the namespace is
deleted before then, the teardown wedges on those holders, exactly as today.
The same happens when a user starts deleting just after the release
decision, and when a Secret and its holder are deleted together and the
Secret goes first, as `kubectl delete -f` of a file that lists the Secret
before the Repo does (§18 NQ1). The remedy, documented for consumers:

> If you delete a Secret your objects use while its namespace is live,
> recreate it before deleting those objects. Or delete the objects first. If
> objects are already stuck deleting on a missing Secret in a live namespace,
> recreate the Secret, and they finish.

**A new consequence for platform namespaces** (L7). Platform Secrets live in
`krateo-system` and are used across namespaces. If one is deleted, for
rotation say, while any of its users is being deleted, it stays Terminating
for as long as that deletion takes. Recreating it fails with AlreadyExists
until then. A holder whose external Delete keeps failing blocks the
platform Secret's rotation for that whole time. Before U5's
"a user is being deleted" rule, there was no hold, and that holder wedged
instead. U5 limits the block to the deleting user's lifetime: it ends when
that deletion finishes or is abandoned. It doesn't bound it in time. The
waiting event and gauge name the deleting holder (§12). §18 NQ2 asks
whether to cap it.

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

- **A1. Announce.** If H's record doesn't name P, write the new record
  with a merge patch guarded by H's resourceVersion
  (`client.MergeFromWithOptimisticLock`). On a conflict, requeue. The
  in-memory object takes the patched resourceVersion, so the pass's later
  writes don't conflict with it.
- **A2. Write P**, unless the verified set allows skipping it (§6.3).
  - Built-in kinds (Secret, ConfigMap): one **strategic merge patch** with
    no precondition. It merges the finalizer into the list, sets the epoch
    to a fresh token, and sets the held label. A strategic merge patch
    can't drop or duplicate anyone else's finalizers.
  - Custom resources: a live GET, then a **merge patch guarded by
    resourceVersion** (`MergeFromWithOptimisticLock`) that writes the full
    finalizer list plus the finalizer, the fresh epoch, and the label. On a
    conflict, GET again and retry, up to 3 times, then requeue.
  - Outcomes: **held** (P now carries the finalizer and the fresh epoch),
    **Deleting** (`Forbidden`: P is being deleted and lacks the finalizer),
    or **Missing** (NotFound). P being deleted *with* the finalizer is held:
    the patch adds no new finalizer, so it is allowed, and the epoch still
    moves.
- **A3.** Only then Connect.

**The releaser** works on one holder kind K and one namespace N at a time,
for the set of protected objects pending there (R7). For each pending P:

- **R0. Fast path.** Ask the holder informer's index for users of P in N.
  Holders that §6.1 excludes don't count. If a user exists and §5 says
  keep, stop. This settles the usual case, where one of several users
  leaves.
- **R1. Get P live.** Note its resourceVersion, the index *i* of the
  finalizer in `metadata.finalizers`, and the epoch *e*. If the finalizer
  is absent, P is done. If the epoch is absent (only a third party could
  cause that), set a fresh one with a strategic merge or guarded patch, and
  requeue. For a deleting namespaced P, GET its Namespace live (§5).
- **R2. List consistently, once for the whole batch.** After every R1 GET
  in the batch, make one live, paginated, metadata-only list of K in N
  (`PartialObjectMetadataList`, no resourceVersion, so it is a consistent
  read), with no label or version selector. For each P, compute its users
  (§6.1) and its deleting users.
- **R3. Release** each P that §5 says to release. Apply one **JSON patch**:

  ```json
  [{"op":"test",   "path":"/metadata/annotations/<F escaped>", "value":"<e>"},
   {"op":"test",   "path":"/metadata/finalizers/<i>",          "value":"<F>"},
   {"op":"remove", "path":"/metadata/finalizers/<i>"},
   {"op":"remove", "path":"/metadata/annotations/<F escaped>"}]
  ```

  `<F escaped>` is the finalizer name with `/` written as `~1` (RFC 6901).
  When the finalizer is the last `credential-use.` one on P, the patch also
  tests the whole `/metadata/finalizers` array as read and removes the held
  label. Otherwise the label stays, because another kind's hold needs it.
  If a test fails, the API server rejects the whole patch. The exact status
  code is pinned by a test, and is treated as a conflict: P goes back to
  R1. No other writer's change can be clobbered: the patch removes one
  index it has just tested, and one annotation.

### 6.3 The verified set

The fast path in A2 may skip the write only for pairs this process has
written. The tracker keeps them in memory:

- `written[(P.uid, K, N)] = t_issue`: the local monotonic time at which this
  process issued a successful A2 write for P, K and N;
- `observed[(H.uid, P)] = t_obs`: the local time at which this process first
  saw H's record naming P, by reading it from the holder informer at the
  start of a pass or from the response to its own A1.

**A2 may skip for (H, P) only if `t_obs < t_issue`**, that is, this process
wrote P *after* it had seen H's record naming P. This is U6's rule ("only
for pairs this process wrote") in its exact form. One write covers every
holder whose record was already visible when the write was issued, and that
is what keeps a restart from re-writing P once per holder (§6.6).

Entries are dropped:

- when H's record stops naming P, so an away-and-back re-point writes again;
- when P's UID changes or P disappears;
- when P loses the finalizer for (K, N) or leaves the label-scoped cache
  (third-party stripping, §10.2);
- when Disown runs, and when the process restarts or loses leadership. The
  memory is gone, so everything is written again.

### 6.4 Why it is safe

Take an acquirer whose A1 lands at tA and whose A2 write lands at tW, with
tA < tW (A2 starts after A1 returns). Take a releaser whose R1 reads P at
tG, whose R2 lists at tL > tG, and whose R3 patch lands at tR > tL.

- **tW < tG.** Then tA < tG < tL, so R2 sees H's record naming P, and H is
  a user. The releaser keeps the finalizer, except where §5's third row
  says to release (P being deleted, namespace live, H not deleting). That
  is U5's intended release, not a lost hold.
- **tG < tW < tR.** A2 wrote a fresh random epoch, so at tR the epoch is not
  *e*, and R3's first test fails. This holds even if another releaser
  removed the finalizer between tG and tW and A2 re-added it: the epoch is
  still fresh. The releaser starts again from R1. Its next list is after
  tW > tA, so it sees H.
- **tW > tR.** The finalizer was gone before A2. A2 adds it back (held), or
  is Forbidden because P is being deleted (Deleting), and the acquirer
  knows which.

**Error or crash after the announce** (HIGH-1). Suppose A1 succeeds and A2
fails, times out, or the process dies. No `written` entry exists, so every
later pass, in any process, runs A2 again before its Connect. The record
alone never lets a pass skip the write. A2 is idempotent: a write that
landed but whose response was lost is simply repeated. While A2 has not
succeeded, a concurrent releaser can't use the record against H: the record
is visible to its R2 (tA < tL), or R2 came first, in which case the
finalizer may go and the next A2 restores it or reports Deleting. In no
case does H connect on a P that a releaser can remove underneath it.

**The skip is safe** for the same reason as the first case. If
`t_obs < t_issue`, then tA < t_obs < t_issue < tW. Every releaser that read
P before tW fails its test, and every releaser that reads after tW sees H.
The only things that can still remove the finalizer while H's record names
P are §5's live-namespace release, Disown, and third parties. The first two
are intended. Third parties drop the `written` entry through the informer
(§10.2), within the cache's lag.

**What the proof assumes.** R1's GET and R2's list are consistent reads,
and A2 is a write. No decision rests on a cache. Caches serve only the fast
paths (R0, the `written` skip), and both are safe when stale: a stale R0
that sees a user already gone only delays the release to the next event.

### 6.5 Races, case by case

- **Two users deleted at once.** Their events coalesce into one batch for
  (K, N), and R2 sees whichever are left. Two releasers in two processes are
  serialized by R3's tests.
- **One created while another is deleted.** This is §6.4's middle case:
  either R2 sees the new announcement, or A2's fresh epoch breaks R3.
- **The finalizer added after a concurrent removal.** This is §6.4's last
  case. A Forbidden A2 on a deleting P gives a new holder `MustNotConnect`
  (§8). It never gets its owner finalizer, so it deletes freely.
- **Error or crash after the announce**, with a concurrent releaser: §6.4.
- **Third-party stripping** (`kubectl replace`, Argo CD's Replace sync
  option) removes the finalizer and the label outside the protocol. P
  leaves the label-scoped cache, which drops the `written` entries and
  enqueues P's holders through the index (§10.2). The window is the
  informer's lag plus the holders' queue wait.

### 6.6 Cost

- **Steady state:** none. Refs are unchanged and `written` covers them, so
  everything is read from caches.
- **A new holder, or new refs:** one patch of the holder (A1), plus one
  write per new P (A2).
- **After a restart or leader change:** one A2 per (P, K, N), not per
  holder. The first pass of the first holder of each P writes it, and every
  holder whose record was visible then is covered.
- **Backfill on upgrade** (L6): every existing holder has no record, so it
  needs A1 and A2 on its first pass. That is two writes per holder. A2
  can't be coalesced here, because each record is only written in that
  pass. 1,000 holders make about 2,000 writes over the first poll interval
  (1 minute by default), about 33 per second. With saturation on, local
  slots spread them, and so does the REST config's client-side limiter when
  one is set. The A1 write changes the holder's resourceVersion, but it
  causes no extra pass beyond the one the pass's own status update already
  causes: both events coalesce in the queue while the item is being
  processed. So there is nothing to skip.
- **A release:** for a batch of M pending objects in (K, N), M GETs (plus
  one Namespace GET), one paginated metadata-only list of K in N, and up to
  M patches. A namespace teardown with 50 Secrets lists the holders once,
  not 50 times.

### 6.7 The invariant, restated so it is true

**When a holder's owner finalizer is written, every ref in its record has
either been written by A2 after the record named it, or been found absent by
a live read made after the pass's last A2 and before that finalizer write.**
That live read is the check just before `AddFinalizer` (§8, R6). A ref that
appears after it is held on a later pass, through the prober or backoff
(§10.3). It is unprotected until then: it was created after the decision,
in a live namespace, so it is recoverable.

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
	Disown         bool                     // §7.4
	ProbeInterval  time.Duration            // missing-ref prober; default 15s (§10.3)
	Logger         logging.Logger
	Metrics        MetricsRecorder
}

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
5. **Enqueue:** pass `Options.Enqueue` that adds a holder to its workqueue
   (§10.2).
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

With `Options.Disown`:

- `Acquire` acquires nothing. It returns all refs Unheld, the object
  proceeds as it does today, and nothing is ever `MustNotConnect`.
- The loop strips the record from every holder of K (a merge patch guarded
  by resourceVersion).
- It removes every finalizer with K's prefix, and its epoch, from every
  labelled protected object (R3's JSON patch, without the user check). It
  removes the label where that was the last `credential-use.` finalizer.
- It reports `credentialuse_held_objects{holder_kind}`, which is exported in
  every mode.

**At 0, downgrading the runtime, removing the interface from the
connecter, or uninstalling is safe.** Disown is also the way to:

- **drop a kind from `Protected`.** Disown runs for that protected kind
  only: `Options.DisownProtected`.
- **delete a holder CRD** (L5). The holders are gone, so the loop skips the
  record step. It finds the protected objects by label and prefix.

The release notes must say: set Disown, wait for the gauge to reach 0, then
change the version.

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

**A setup error refuses to start**: the runnable's `Start` returns it, so
`mgr.Start` fails. The process exits with the error. It doesn't run with the
feature half on.

**Fake managers work.** `pkg/resource/fake.Manager` has a nil config. With
a nil config, the adapter doesn't build a `Shared` and logs once that
credential-use is off. Unit tests that don't care are unaffected. Tests
that do inject one: `reconciler.WithCredentialUseShared(s)`, built on
client-go fakes. A connecter that doesn't implement the interface creates
nothing (§11.10).

**Prompt requeue needs one builder line.** The tracker enqueues holders
when a protected object changes (§10.2). `NewReconciler` has no handle on
the controller the provider builds afterwards, so the reconciler exposes
`CredentialUseSource()` and the provider adds
`.WatchesRawSource(r.CredentialUseSource())`. Without it, safety is
unchanged and re-acquiring waits for the poll interval or backoff. The
reconciler logs a warning if the source hasn't been started 30s after the
manager started. §18 NQ3 asks whether to keep this explicit.

## 8. Where it runs in Reconcile; the governor and the gated queue

All of this is API-server work, not external calls, so none of it is gated
per key. In order:

1. **Get**, as now.
2. **Paused** (`:806`): return, as now. Nothing is acquired.
3. **Orphan path** (`:821`): `Forget(managed)`, then `RemoveFinalizer`, in
   one Update. If the owner finalizer is already absent, so that
   `RemoveFinalizer` would write nothing, and the record is present, write
   the record's removal with an Update of its own (R12). No Connect, so
   nothing to acquire.
4. **Acquire**, new, after the orphan check and before the Connect gate
   (`:879`), on every remaining pass, deletion passes included.
   - A conflict returns `rateLimitedRequeue()`, as every conflict does.
   - **A new holder** (no owner finalizer) fails closed. On `MustNotConnect`
     or any other error: Synced=False/ReconcileError, Ready=NotObserved
     through `markNotObserved`, and return the error so backoff applies.
   - **An established holder** fails open on every Acquire error (R4),
     deletion passes included. Log once per object, record a Warning event
     (`CredentialUseDegraded`), and proceed to Connect as today. It never
     gets `MustNotConnect`. An established holder whose ref is Deleting and
     unheld also proceeds (§11.4).
   - `WithResolved(externalCtx, a)` puts the resolved chain on Connect's
     context (§9.2).
   - The gate's `pristine` copy (`:870`) is taken after Acquire, so that a
     deferral right after A1 compares against the object as it now stands.
5. **Gate and Connect**, as now.
6. **Delete path**: `Forget(managed)` before `RemoveFinalizer` (`:1125`), in
   the same Update. As in step 3, if the owner finalizer is already absent
   and the record present, write the removal on its own.
7. **`BeforeEstablish`**, new, just before `AddFinalizer` (`:1152`), and only
   when the owner finalizer is absent. On an error, write
   Synced=False/ReconcileError and return, without adding the finalizer.
8. **`AddFinalizer`**, unchanged.

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
also **enqueues holders through the record index** when a protected object
with a `Next`:

- changes generation, or changes resourceVersion through someone else's
  write (not one of the tracker's own epoch or label writes, which it
  recognises by the `credential-use.` keys that changed);
- loses the finalizer or the label;
- is deleted or recreated with a new UID.

Plain Secrets without a `Next` are enqueued only on finalizer or label loss
and on deletion or recreation, because a data change doesn't change what
they hold.

**The window that remains is reconcile latency, stated honestly.** From the
change to the holder's next pass, the holder uses the old chain. That time
covers the informer's lag, the holder's queue wait (including a gated
queue's wait for a local slot), and, if `CredentialUseSource` isn't wired,
the poll interval. A **paused** holder doesn't reconcile at all, so it stays
on the old chain until it is unpaused. It is also deleted only after it is
unpaused, and its first pass then acquires before anything else. The window
matters only if the newly named Secret is deleted inside it, and §5 already
makes that recoverable unless its namespace is terminating.

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
  `credential-use.*`, and every label except the held label.

What remains is about 300 to 500 bytes per object: name, namespace, UID,
resourceVersion, generation, `deletionTimestamp`, finalizers, the epoch
annotations and the label. 10,000 held Secrets cost about 5 MB.

**A fast-path miss falls back to a live GET.** That covers an object not yet
labelled (it was just created, or it has never been held), an object the
informer hasn't delivered yet, and a namespace the caller's informers don't
cover (§10.5). A2's write sets the label, so from then on the object is in
the cache.

### 10.2 The holder informer, the index, and enqueue (R5)

There is one metadata-only informer per holder kind, with a transform that
keeps the UID, resourceVersion, `deletionTimestamp`, finalizers and the
record. That is about 300 bytes plus about 100 bytes per ref, so 10,000
holders cost about 5 MB.

Its index maps each recorded ref to holders. The index serves:

- **R0's fast path**;
- **mapping holder events to release batches**: a create, an update whose
  record or owner finalizer changed, and a delete map to the protected
  objects in the old and the new record, and enqueue their (K, N) batch;
- **enqueueing holders when a protected object changes** (§9.2). Events from
  the protected-object informer go through the index to `Options.Enqueue`.
  A label-scoped informer delivers losing the label, the signature of a
  `kubectl replace` or an Argo CD Replace that strips our metadata, as a
  deletion. So a stripped object enqueues its holders, and the tracker drops
  the `written` entries for it (§6.3).

On startup, the protected informers' initial list enqueues a release batch
for every (K, N) of every held object. So every hold is checked again after
a restart, and a holder delete missed while down can't strand a finalizer.

### 10.3 Seeing new objects: the missing-ref prober (R6)

A newly created object, such as a Secret recreated for rotation, isn't
labelled, so the label-scoped informer can't see it. The tracker keeps the
set of refs it last found Missing for some holder. Every `ProbeInterval`
(15s by default), the prober live-GETs each distinct missing ref, at most
100 per tick, rotating through the set. When one exists, it enqueues the
ref's holders through the index. Their next pass runs A2 on the new
incarnation.

The holders' own retries find it too, within their backoff of 60s at most.
`BeforeEstablish` re-checks a Missing ref live just before the owner
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
  existed. It proceeds (fail open) with a Warning event. That is a
  documented limit, and the remedy is to fix the ref.

### 11.5 A provider uninstalled before its users

Today the users' own finalizers stay and their namespaces wedge. That
doesn't get worse. What is newly stuck is every protected object those
remaining users held. **Uninstall through Disown** (§7.4). Doc text:

> Before uninstalling a provider, delete its objects and wait for them to
> go. Or set Disown and wait for `credentialuse_held_objects` to reach 0.
> After an out-of-order uninstall, remove the objects' own finalizers, then
> the `credential-use.<kind>.<group>/<namespace>` finalizers, the annotation
> with the same key, and the `credential-use.krateo.io/held` label once no
> `credential-use.` finalizer is left.

core-provider#313 is the same class: a controller that runs in the
namespace being deleted dies before it can release anything. This design
doesn't fix that. A provider must not run in a namespace whose objects it
manages, and the RBAC that keeps it working must not live there either
(§13).

### 11.6 Scale to zero (R11)

At 0 replicas, nothing releases. So **`scaletozero.Exists` also counts, as
instances, protected objects that carry a finalizer with the Kind's
prefix**. A new option asks for it,
`scaletozero.WithHeld(protected ...schema.GroupVersionKind)`. With it,
after the instance list finds nothing, `Exists` runs a metadata-only,
paginated list of each protected kind, selected by the held label, and
stops at the first object with a finalizer starting
`credential-use.<kind>.<group>/`. A stale hold then keeps the controller
running until it releases it, after which the Kind can scale down.

This is a `pkg/scaletozero` change, shipped in the same release, and the
owners (core-provider, oasgen-provider) pass the option. The prefix is per
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
| A protected object waiting for users after its deletion was requested | No condition, since Secrets have none. A Normal event on the object, throttled, once per state change, naming the holder kind, namespace and count, plus the deleting holders by name; an Info log line; the waiting gauge | event `WaitingForCredentialUsers`, gauge `credentialuse_waiting_objects{holder_kind}` |
| A live holder whose protected object is being deleted but held | It keeps working; no condition change; a throttled Warning event naming the object and why it is kept | event `CredentialDeleting` |
| A live holder whose protected object was released under §5 and is gone | Its credential read fails, as today; the provider's error names the object | Synced=False/ReconcileError from Connect |
| A new holder that must not connect (§11.4), or whose acquire failed | Sync failed, with a message naming the object and the reason; readiness unknown | Synced=False/ReconcileError, Ready=NotObserved |
| An established holder whose acquire failed | It proceeds; a Warning event and one log line | event `CredentialUseDegraded` |

In a terminating namespace, events are rejected (`git-provider-rlgov.log`
line 7), so there the log and the gauge are the signal. The message for a
new holder that must not connect is
`credential <kind> <ns>/<name> is being deleted and can't be held; not connecting until it is replaced or the reference changes`.
No new condition type is added (U4).

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
| namespaces | `get` | §5's live terminating check |
| the holder kind | `get`, `list`, `watch`, `patch` | the holder informer, R2's consistent list, A1 and `Forget` |
| events | `create`, `patch` | §12 |

`update`, `create` and `delete` on protected kinds are not needed: every
write is a patch.

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
> every other kind your refs name; `get` on `namespaces`; `patch` on your
> own kinds. Use `resourceNames` only when you know every Secret name when
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

- U5: a Secret deleted in a live namespace with live, non-deleting users is
  released at once, and recreating it under the same name succeeds. The
  holders acquire the new UID, through the prober.
- A Secret deleted in a live namespace while a user is deleting is kept
  until that user goes.
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
- Disown drains to a gauge of 0.
- A foreground-deleted holder that owns its Secret completes (§11.8).
- Setup after start, and `Track` twice with equal and with different
  Options.
- `scaletozero.Exists` with `WithHeld`.
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
(HIGH-1), and a restart that clears the verified set.

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
- Disown racing a still-running acquirer during a rollout.

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
- RBAC is by name where generated (§13).
- With per-version controllers, the release list covers the whole
  GroupKind (§3).

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
| U2 Implicit wiring; setup failure refuses to start | §7.3, §7.5 (prompt requeue: §18 NQ3) |
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
| R6 MED-6 Live re-check before `AddFinalizer`; seeing new objects; the invariant restated | §6.7, §8 step 7, §10.3 |
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
| L6 The backfill herd and its QPS; no extra pass to skip | §6.6 |
| L7 The `krateo-system` wedge, and how U5 limits it | §5 (cap: §18 NQ2) |
| L8 Idempotent setup; fake managers | §7.3, §7.5 |
| L9 Cosmetic | noted; no design change |

## 18. Open questions for the user

**NQ1. A grace period before releasing a deleted Secret in a live
namespace?** U5 releases at once. The cost: when a Secret and its holder
are deleted together and the Secret goes first (`kubectl delete -f` of a
file that lists it first), the release can win the race. The holder then
wedges in a live namespace until the Secret is recreated. Helm uninstall
deletes custom resources before Secrets, so it isn't affected. Options:

- (a) release at once, as decided;
- (b) a fixed grace, such as 30s, during which a user that starts deleting
  still keeps the Secret. Delete-then-create rotation then fails with
  AlreadyExists for that long;
- (c) a configurable grace that defaults to 0.

*Recommendation: (c).* It keeps your decision as the default, and lets an
operator whose workflows delete bundles turn it on without a release.

**NQ2. Cap the "a user is being deleted" hold in a live namespace?** A
holder stuck deleting blocks the rotation of a platform Secret for as long
as it is stuck (§5, L7). Options:

- (a) no cap: the event and gauge name the stuck holder;
- (b) a cap, such as 15 minutes, after which the Secret is released and the
  stuck holder wedges instead.

*Recommendation: (a).* A cap trades a visible, recoverable block for a
permanent wedge of the holder, and the operator can always resolve (a) by
fixing or orphaning that holder.

**NQ3. Prompt holder requeue: one explicit builder line, or a hook?**
Safety is implicit (U2), but enqueueing holders when a protected object
changes needs a handle on the provider's controller, which `NewReconciler`
doesn't have. Options:

- (a) the provider adds `.WatchesRawSource(r.CredentialUseSource())`, and
  the reconciler warns at start if it wasn't wired;
- (b) `ForControllerRuntime` always wraps the queue and registers it by
  controller name, and the reconciler finds it by that name.

*Recommendation: (a).* The controller name is the provider's choice
(`Named("myresource")` in `docs/usage.md`), so the lookup in (b) is
fragile. Without the line, re-acquiring waits for the poll interval, and
nothing becomes unsafe.

**NQ4. The existing finalizer name**, `finalizer.managedresource.krateo.io`,
draws the API server's warning for lacking a `/`. Options: (a) leave it;
(b) migrate it in a separate change. *Recommendation: (b), as its own
issue*, unchanged from the first revision. It has nothing to do with this
deadlock.
