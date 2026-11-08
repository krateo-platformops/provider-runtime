---
type: Architecture
title: provider-runtime — credential-use finalizer
description: Design, not built, for keeping the Secrets (and the objects that lead to them) a managed resource reads alive until the last object that reads them is gone, so a namespace deletion can't wedge on a managed resource whose credentials were deleted first.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, finalizer, deletion, credentials, reconciler]
timestamp: 2026-11-08T00:00:00Z
---

# Credential-use finalizer

**Status:** design, for review. Nothing is built. The user chose the approach,
a finalizer on each Secret an object uses (the analog of Crossplane's
ProviderConfigUsage). This document settles how it works. Code references are
to provider-runtime at `77cce67`, and to controller-runtime v0.25.2 and
k8s.io/apimachinery v0.37.2 (`AM/` is `k8s.io/apimachinery@v0.37.2`).

Where the brief's points are answered: naming and scope §3; what is recorded
§4; finding the last user and the ordering §5; the package and what an
outside runtime calls §6; the Reconcile flow, governor and gated queue §7;
chains of objects §8; lifecycle edges §9; refs that were never recorded §10;
conditions §11; RBAC §12; tests and review §13; rejected alternatives §14;
consumer adoption §15; open questions §16.

## 1. The problem

The deadlock was found on krateo-057 on 2026-11-08. The evidence is in
`~/.krateo-baselines/rlgov-probe-evidence/` (`README.md`, `repo.json`,
`namespace.json`, `git-provider-rlgov.log`, `git-provider-full-20m.log`).
`events.json` there is empty because the events had been swept.

- A Repo, `rlgov-probe/rlgov-probe-repo` (git-provider 0.17.0), named the
  Secret `rlgov-probe/probe-dummy-creds` in both `fromRepo.secretRef` and
  `toRepo.secretRef` (`repo.json`).
- The namespace was deleted at 16:26:41Z. The Repo got its
  `deletionTimestamp` at 16:27:34Z, and its first pass after that already
  failed in Connect:
  `connect failed: retrieving .fromRepo credentials: cannot get probe-dummy-creds secret: Secret "probe-dummy-creds" not found`
  (`repo.json` status, Synced=False/ReconcileError; `git-provider-rlgov.log`
  line 6). The namespace controller had deleted the Secret first.
- It never recovered. `git-provider-full-20m.log` has 38 lines naming the
  missing Secret, from line 477 (16:27:34Z) to line 1014 (16:38:01Z), with
  the backoff growing to the 60s cap.
- The Secret couldn't be put back. The README records the attempt:
  `secrets "probe-dummy-creds" is forbidden: unable to create new content in namespace rlgov-probe because it is being terminated`.
  The provider's own events were rejected the same way
  (`git-provider-rlgov.log` line 7).
- The namespace stayed Terminating. Its conditions name the blocker
  (`namespace.json`): `NamespaceContentRemaining: repoes.git.krateo.io has 1 resource instances`
  and `NamespaceFinalizersRemaining: finalizer.managedresource.krateo.io in 1 resource instances`.
  A human removed the finalizer.

The probe's rate-limit hold only widened the window. git-provider's Repo
Delete is a no-op, yet the runtime still needs Connect to remove the
finalizer. **The defect is generic: any provider, any kind, whose Connect
reads a Secret that a namespace deletion can delete first.**

The same deadlock exists outside this runtime. oasgen-provider's
rest-dynamic-controller is built on unstructured-runtime, which core-provider
owns, and its delete path reads Secrets too: `handler.Delete` →
`ensureSecretRefRBACAndResolve` → `secretref.GetSecretValue`. **This is
flagged for core-provider**, and is why §6 makes the bookkeeping a
standalone package. Related prior art in core-provider: #313 (a namespace
deletion deadlocks because the controller dies with the namespace) and #314
(Delete couldn't proceed with an unreadable chart; fixed by answering from
status). §9.6 and §15.4 come back to both.

## 2. What we have to work with

- **Connect is unconditional on deletion.** On a deleted object with the
  default deletion policy, Reconcile gates and calls `Connect`
  (`pkg/reconciler/reconciler.go:879-885`) before Observe, Delete and
  `RemoveFinalizer` (`:1089`, `:1125`). Only `krateo.io/deletion-policy: orphan`
  (or a management policy that forbids delete) skips it
  (`:821-848`, `meta.ShouldDelete`).
- **The runtime's own finalizer is added after the first Connect.**
  `AddFinalizer` runs at `:1152`, after Connect and Observe have succeeded.
  An object whose first pass fails in Connect has no finalizer and is
  deleted at once.
- **Finalizers are written with a plain Update.** `APIFinalizer.AddFinalizer`
  and `RemoveFinalizer` (`pkg/resource/api.go:40-55`) mutate the object and
  `Update` it, which is resourceVersion-guarded. The reconciler returns
  `rateLimitedRequeue()` on a conflict.
- **Optional provider capabilities are discovered by type.** `keyerOf`
  (`pkg/reconciler/ratelimit.go:112`) checks whether the connecter implements
  `RateLimitKeysProvider` or `RateLimitKeyer`. `WithExternalConnecter` and
  `WithExternalConnectDisconnecter` take it from the connecter before
  `NewNopDisconnecter` hides it (`reconciler.go:638`, `:647`). The new
  interface is discovered the same way.
- **Credential refs are `commonv1.SecretKeySelector`**: a `Reference` (name,
  namespace) plus a key (`apis/common/v1/resource.go:31-37`). The namespace
  is explicit, so refs can cross namespaces. `resource.GetSecret` reads it
  through whatever client the provider passes, usually the manager's cached
  client (`pkg/resource/resource.go:94-105`).
- **Finalizer names are qualified names.** `ValidateFinalizerName` applies
  `IsQualifiedName` (`AM/pkg/api/validation/objectmeta.go:117-124`): an
  optional DNS-subdomain prefix (at most 253 characters) and `/`, then a name
  of at most 63 characters matching `[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?`.
  The API server also warns on a name with no `/`. The probe hit that
  warning for the existing finalizer (`git-provider-rlgov.log` line 2:
  `prefer a domain-qualified finalizer name including a path (/)`).
- **No finalizer can be added to an object that is being deleted.**
  `ValidateNoNewFinalizers` (`objectmeta.go:126-134`) rejects it with
  `Forbidden`. Removing finalizers, and changing annotations, is still
  allowed. The NamespaceLifecycle admission plugin forbids *creating*
  objects in a terminating namespace, but allows updates and patches.
  Without that, no finalizer could ever be removed there.
- **A Secret with a finalizer stays readable while it is being deleted.**
  This is the whole fix: the namespace controller sets a `deletionTimestamp`,
  and Connect's `Get` keeps working until the finalizer is removed.
- **envtest is kube-apiserver and etcd only.** It has no namespace
  controller and no garbage collector. `pkg/scaletozero/envtest_test.go`
  starts it in `TestMain` when `KUBEBUILDER_ASSETS` is set, and
  `requireAPIServer` (`:94`) fails rather than skips under CI. CI installs
  the binaries with setup-envtest for 1.37.0
  (`.github/workflows/run-tests.yaml`).

## 3. Finalizer naming and scope

A Secret can be used by several kinds, from several providers and
processes, at once. Each holder must track only its own users, and must not
remove or depend on another holder's mark.

| | Scope of one finalizer | Count on a Secret | Release check | Verdict |
|---|---|---|---|---|
| A | One per holder object (`.../<uid>`) | unbounded: one per user | none needed | Rejected. A Secret shared by 1,000 objects carries 1,000 finalizers, every acquire and release is a write on one hot object, and they conflict with each other. |
| B | One per holder kind (GroupKind) | number of kinds | list every object of the kind, cluster-wide | Bounded, but the release check is a cluster-wide list of the kind. |
| C | **One per holder kind and holder namespace** | kinds × namespaces that use it, usually 1 | list the kind in one namespace | **Chosen.** |
| D | One per process or controller instance | processes | the process's own objects | Rejected. Pod names change, and a process that is gone strands its finalizer. |
| E | A usage object per holder (Crossplane's ProviderConfigUsage) | 1 | list usages | Rejected in §14.4. |

**C is the choice.** The finalizer is:

```
credential-use.<lowercase kind>.<group>/<holder namespace>
```

For example `credential-use.repo.git.krateo.io/rlgov-probe` and
`credential-use.localresource.git.krateo.io/rlgov-probe`. A cluster-scoped
holder kind uses the name `cluster-scoped`. A GroupKind is either namespaced
or cluster-scoped, never both, so this can't collide with a namespace of
that name.

Why it is valid, and when it isn't:

- The name part is a namespace name, a DNS label of at most 63 characters,
  which `IsQualifiedName` accepts.
- The prefix is a DNS subdomain. A CRD's lowercased kind must be a DNS-1035
  label (63 characters at most), and its group is a DNS subdomain. Only the
  total length, `15 + len(kind) + 1 + len(group) <= 253`, can fail, and only
  for a pathological group. Setup computes the prefix and fails if it is
  invalid (§6), so this shows when the provider starts, not on first use.
- It has a `/`, so the API server doesn't warn on it. The runtime's existing
  `finalizer.managedresource.krateo.io` has no `/` and draws that warning.
  Renaming it needs a migration and is out of scope (§16, Q5).

Why C over B: the namespace in the name makes the release check a list of
one namespace, not of the whole cluster (§5.3). It also tells an operator
exactly who holds a stuck Secret: `kubectl get secret -o yaml` shows "Repos
in rlgov-probe". And it stays bounded. A configuration-level Secret, such as
oasgen's one Secret per Configuration used by every object of a kind, gets
one finalizer per kind and per namespace that uses it, whatever the number
of objects.

**Scope and ownership.** Each holder kind touches only finalizers with its
own prefix. Several kinds sharing a Secret never see each other's
finalizers. Two processes reconciling the same kind would share the
finalizer, and that is still safe. The protocol in §5 makes every decision
from consistent reads and resourceVersion-guarded writes on the API server,
and from records persisted on the objects, never from one process's memory
or cache.

## 4. What is recorded: the set of refs an object holds, on the object

The release side must not recompute refs. Recomputing fails exactly when
it matters most:

- path resolution fails, or the object's shape changed (a new CRD version,
  a field removed);
- the ref changed between acquire and delete, so recomputing gives the new
  Secret and leaks the old one;
- the ref is indirect (object → KubernetesTarget → Secret, §8), and
  recomputing needs reads of other objects that may already be gone;
- recomputing cluster-wide needs every object and the provider's code, so an
  outside runtime or another process can't check another kind's users.

**So the holder records the set of refs it holds on itself, and release
reads only that record.** The record is an annotation on the holder:

```
credential-use.krateo.io/refs: [{"group":"","kind":"Secret","namespace":"rlgov-probe","name":"probe-dummy-creds"}]
```

The list is sorted and deduplicated. It records identity (group, kind,
namespace, name), not version or key: two keys of one Secret are one ref.

Annotation or a status field the runtime owns:

- **An annotation, chosen.** Every kind has annotations, including
  unstructured objects, so an outside runtime can use the same record. It
  survives backup and restore, and GitOps re-applies (client-side apply's
  three-way merge and server-side apply both leave keys they don't own).
  It is written with the same resourceVersion-guarded Update the runtime
  already uses for its finalizer.
- **A status field, rejected.** provider-runtime owns no status field in
  providers' CRDs, only conditions in them. A condition holding a list is a
  misuse. Status is also commonly dropped on restore, and the provider's
  whole-status writes could clobber it.

What a user can do to the annotation: deleting or editing it makes the next
pass see a difference and re-announce (§5.2), which is safe. In the window
before that, a release could miss the object (§10.4). The doc text says
not to edit it.

## 5. Finding the last user

### 5.1 Who counts as a user

A holder H of kind K in namespace N **uses** protected object P while H
exists and its record names P. That includes an H that is being deleted and
still carries its own finalizer. When the runtime removes its own finalizer
from H, it removes the record in the same write (`Forget`, §6). An H whose
own finalizer is gone may linger because of someone else's finalizer, but it
no longer counts.

The finalizer `credential-use.<K>/<N>` may be removed from P only when no
user of P exists in N.

### 5.2 The ordering that makes it safe

Two actors race. One is an **acquirer**, the reconcile of a holder that
starts using P. The other is a **releaser**, the reconcile of P that
decides it has no users. The failure to prevent: the releaser decides "no
users" from a view that misses a new user, removes the finalizer, P is
deleted, and the new user, holding its own finalizer, can never Connect
again.

A check followed by an act across two objects can't be made atomic, so each
side **writes its intent first and reads the other side's state second,
with a consistent read**. Then at least one of the two always sees the
other. (This is the pattern of Dekker's mutual exclusion: raise your flag,
then look at theirs.)

**The acquirer, for each ref P its object newly names** (in the current
refs, not in the record):

1. **Announce.** Write the record on H to include P: an Update guarded by
   H's resourceVersion. On a conflict, requeue.
2. **Verify, consistently.** Read P's metadata live, with the API reader,
   not the cache:
   - P has the finalizer and no drain mark (step R1 below): done.
   - P has the finalizer and a drain mark: clear the mark with a patch
     guarded by P's resourceVersion. That write makes the releaser's final
     guarded write fail. On a conflict, re-read and retry.
   - P has no finalizer and is not being deleted: add it with a guarded
     patch. On a conflict, re-read and retry.
   - P has no finalizer and is being deleted: it can't be held (§2,
     `ValidateNoNewFinalizers`). See §9.4.
   - P doesn't exist: nothing to hold. See §9.2.
3. Only then call Connect.

**For refs already in the record**, the acquirer reads P from the cache
only. If the finalizer is missing and P isn't being deleted, it adds it
with a guarded patch, which also covers backfill and tampering. It ignores
a drain mark: the record was announced before an earlier consistent verify,
so every releaser's list sees it.

**The releaser, for protected object P and finalizer `credential-use.<K>/<N>`:**

- R0. **Fast path.** Ask the cache's field index on the record for users of
  P in N. If there are any, keep the finalizer, clear a stale drain mark if
  there is one, and stop. This is the usual case when one of several users
  goes.
- R1. **Mark.** Patch P, guarded by its resourceVersion, to set the drain
  mark: an annotation whose key is the finalizer name and whose value is a
  timestamp. Annotation keys follow the same qualified-name rules, so the
  key is valid wherever the finalizer is.
- R2. **List, consistently.** A live, paginated, metadata-only list of K in
  N (`PartialObjectMetadataList` through the API reader), filtered on the
  record annotation.
- R3. If anyone uses P, clear the mark (guarded) and stop. If not, remove
  the finalizer and the mark in one patch, guarded by the resourceVersion
  that R1 returned. If the patch conflicts (an acquirer cleared the mark, or
  anything else wrote P), start again from R0.

**Why this is safe.** Take an acquirer whose announce lands at time tA and
whose consistent verify reads at tV > tA, and a releaser that marks at tM,
lists at tL > tM, and removes at tR > tL.

- If tV < tM, then tA < tV < tM < tL, so the releaser's consistent list
  sees the announcement and keeps the finalizer.
- If tM < tV < tR, the acquirer sees the mark and clears it with a write.
  The releaser's guarded write at tR then conflicts, and it starts again.
  Its next list sees the announcement, because tA < tV.
- If tV > tR, the acquirer sees no finalizer and adds it. If P is being
  deleted by then it can't, and §9.4 applies.

The one assumption is that both reads are consistent. That holds because
the verify is a live GET and the list a live LIST, never the cache. No
step depends on cache freshness: the caches only make the fast paths (R0,
and refs already in the record) cheap, and both are safe when stale. A
stale R0 that sees a user that has already left only delays the release
until the next event. A stale cached read of a recorded ref can only miss
a finalizer that is there, and the guarded add then conflicts.

**Relative to the first Connect:** the finalizer on P is in place, and
verified with a consistent read, before the object's first Connect. The
runtime's own finalizer on H is still added after Observe (`:1152`). So
**an object can only carry its own finalizer if, in a pass that got that
far, every ref it names that exists was held**. That is the invariant the
namespace teardown needs.

### 5.3 Races, case by case

- **Two users deleted at once.** Both deletions enqueue the same P, and the
  workqueue deduplicates it. The releaser's consistent list sees whichever
  are left. If one is still being deleted with its own finalizer, the
  finalizer on P stays, and that user's later disappearance enqueues P
  again. Two releasers, in two processes, are serialized by the guarded
  writes in R1 and R3.
- **One created while another is deleted.** This is exactly the case in
  §5.2. Either the releaser's list sees the new announcement, or the
  acquirer sees the mark and breaks the release.
- **The finalizer added after a concurrent removal.** The acquirer's verify
  happens after tR and finds no finalizer, so it adds one if P is not being
  deleted. If P is being deleted, the add is forbidden, and the new object
  doesn't connect (§9.4). It never gets its own finalizer, so it can always
  be deleted.
- **A drain mark left behind** by a releaser that crashed between R1 and
  R3. The next release run (on startup, or on any event) finds it. An
  acquirer of a new ref clears it, and acquirers of refs already in the
  record ignore it.
- **The cache lags.** It's harmless, as above: only fast paths read it.
- **A cross-namespace user.** It has its own finalizer, for its own
  namespace, so it is invisible to the other namespace's check, as it
  should be.

**Cost.** In the steady state, an object whose refs haven't changed makes
no API calls for this: everything is read from caches. Its first pass, and
any pass after its refs change, makes one Update of the object and one live
GET per new ref, plus a patch the first time each Secret is held by this
kind and namespace. A release is two patches and one metadata-only list of
one namespace, paid only when the cache says the last user has left.

## 6. The package: `pkg/credentialuse`

The bookkeeping is a standalone package with no dependency on
`pkg/reconciler`. provider-runtime's reconciler uses it, and so can an
outside runtime such as unstructured-runtime. Sketch:

```go
package credentialuse

// A Ref names an object that a holder reads to reach its external system:
// a Secret, or an intermediate object that leads to one (§8).
type Ref struct {
	GroupKind schema.GroupKind // {"" "Secret"} for a Secret
	Namespace string           // empty for a cluster-scoped object
	Name      string

	// Next, when set, names the refs found inside this object. The tracker
	// calls it only after the object is held, with the object read from the
	// cache. Not recorded.
	Next func(obj client.Object) ([]Ref, error)
}

func SecretRef(namespace, name string) Ref

// RefsFunc names what obj reads to reach its external system. Object in,
// refs out: no provider types. It must make no API calls and may not read
// any cache. Anything indirect is found through Ref.Next. It is called
// before every Connect, including on throttled and deleting objects.
type RefsFunc func(obj client.Object) ([]Ref, error)

type Options struct {
	Holder    schema.GroupVersionKind   // the kind the caller reconciles
	Refs      RefsFunc
	Protected []schema.GroupVersionKind // kinds Refs and Next may name; default Secret v1
	MaxDepth  int                       // levels of Next followed; default 3
	// ReleaseOnly stops acquiring and keeps releasing, so a provider can
	// drop the feature, or downgrade, without stranding finalizers (§9.7).
	ReleaseOnly bool
	Logger    logging.Logger
}

// Setup registers the field index on the record, the metadata-only watches
// of the protected kinds, and the release controller with mgr. It validates
// the finalizer prefix. Call once per holder kind, before mgr.Start.
func Setup(mgr manager.Manager, o Options) (*Tracker, error)

// Acquire computes obj's refs, announces and holds them (§5.2), and returns
// what it couldn't hold. established says whether obj already carries the
// caller's own finalizer. An error is an API failure or a conflict: requeue.
func (t *Tracker) Acquire(ctx context.Context, obj client.Object, established bool) (Acquired, error)

type Acquired struct {
	Held   []Ref
	Unheld []Unheld // each with a Reason: Missing, Deleting
	// MustNotConnect is true when a ref is Deleting and unheld and
	// established is false (§9.4).
	MustNotConnect bool
}

// Forget removes the record from obj in memory. Call it just before the
// write that removes the caller's own finalizer, so both go in one write.
func (t *Tracker) Forget(obj client.Object)

// Users lists, with a consistent read, the holders in namespace that use p.
// For diagnostics, tests, and the release controller itself.
func (t *Tracker) Users(ctx context.Context, p Ref, namespace string) ([]types.NamespacedName, error)

// FinalizerName is credential-use.<kind>.<group>/<namespace> (§3).
func FinalizerName(holder schema.GroupKind, namespace string) (string, error)

const AnnotationRefs = "credential-use.krateo.io/refs"
```

**The release controller.** It is one per tracker, with a typed request
(protected GroupVersionKind, namespace, name). It is enqueued by:

- metadata-only events on the protected kinds, filtered to objects that
  carry a finalizer with the tracker's prefix. Its initial list enqueues
  every held object, so after a restart every hold is checked again, and a
  delete event missed while down can't leak a finalizer;
- holder events, mapped through the old and the new record: a create, an
  update whose record changed, a delete, and the removal of the runtime's
  own finalizer.

For each finalizer of its prefix on the object, it runs R0 to R3.
`MaxConcurrentReconciles` is small (2). It makes no external calls.

**What an outside runtime must call, and when** (for unstructured-runtime):

1. At startup, before the manager starts: `Setup` once per kind it
   reconciles.
2. In every reconcile, after reading the object and before anything reads a
   Secret, on the delete path too: `Acquire(ctx, obj, hasOwnFinalizer)`.
   If `MustNotConnect`, don't read the credentials. Report it (§11) and
   requeue. On an error, requeue. Then add its own finalizer only in a pass
   where Acquire succeeded, as now.
3. When it removes its own finalizer on final delete: `Forget(obj)` on the
   same object, just before that write, so both are one Update. If its
   finalizer write can't carry annotations (a finalizers-only JSON patch),
   it may skip `Forget`. The refs are then released when the object is
   gone, which is later but still safe. **It must never remove the record
   before its own finalizer.** The object would still need its credentials
   with nothing counting it.
4. RBAC as in §12.

"Find the last user" is the release controller's job, so an outside runtime
doesn't call it. `Users` is exported for diagnostics and tests.

**How provider-runtime wires it.** A connecter that implements

```go
type CredentialRefsProvider interface {
	CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error)
}
```

is discovered as `keyerOf` discovers keyers, in `WithExternalConnecter` and
`WithExternalConnectDisconnecter`, before `NewNopDisconnecter` hides it. It
lives on the connecter only, not the client: the refs are needed before
Connect, when there is no client yet. `NewReconciler` then calls
`credentialuse.Setup` with the manager it already has. It panics if setup
fails, as it already does for a kind missing from the scheme
(`reconciler.go:709-711`). So implementing the interface can't half-enable
the feature: acquiring without the release controller would make Secrets
undeletable. A connecter that doesn't implement it changes nothing: no
tracker, no watch, no annotation, no Secret writes (§9.8). §16 Q2 asks
whether to keep it implicit.

## 7. Where it runs in Reconcile; the governor and the gated queue

Every write here goes to the API server, not the external system, so none
of it is gated per key by the governor. In order:

1. Get, as now.
2. Paused (`:806`): return, as now. Nothing is acquired: paused means hands
   off.
3. **Orphan path** (`:821`): `Forget(managed)`, then `RemoveFinalizer`, in
   its one Update. No Connect, so nothing to acquire.
4. **Acquire**, new. It runs after the orphan check and before the Connect
   gate (`:879`), on every remaining pass, deleting ones included. A
   conflict returns `rateLimitedRequeue()`, as for every other conflict in
   Reconcile. Any other error, or `MustNotConnect`, writes
   Synced=False/ReconcileError and returns the error, so backoff applies
   (§11). Taking the gate's `pristine` copy (`:870`) moves after Acquire, so
   a deferral right after an announce still compares against the object as
   it now stands.
5. Gate and Connect, as now.
6. Delete path: `Forget(managed)` before `RemoveFinalizer` (`:1125`), in the
   same Update.
7. `AddFinalizer` (`:1152`), unchanged. It can only be reached after a
   successful Acquire.

**The governor.** Acquire runs before the Connect gate, so a deferred pass
has already acquired. That is cheap (cached reads in the steady state) and
means a long throttle doesn't leave a new object unprotected. A key
deferral never skips Acquire.

**The gated queue.** With saturation on, the local slot covers the whole
reconcile (`ratelimit.NewGatedQueue`), so Acquire's API calls run inside it,
like the status updates. Nothing new is gated.

**The release controller.** It is not built with `ForControllerRuntime` and
doesn't register with the governor, so it neither raises the governor's
ceiling (`RegisterReconciler`) nor waits for a local slot. Its work is a
few API calls per release. A teardown must not queue behind a saturated
external API.

## 8. Chains: object → intermediate object → Secret

core-provider's remote CompositionDefinition resolves a KubernetesTarget in
its namespace in Connect, and the target names a kubeconfig Secret. oasgen's
objects name a Configuration that names a Secret. The intermediate object
is the same deadlock one level up: a teardown can delete the
KubernetesTarget first, just as it can the Secret. **So the design protects
any object a holder reads to reach its credentials, not just Secrets.** The
mechanism is the same for every kind: the finalizer and the drain mark are
metadata, which every object has.

**How a controller names the chain.** `RefsFunc` stays pure: object in,
refs out. It can't read the KubernetesTarget, and doesn't need to. It
returns the first hop with a `Next` that knows how to read the target:

```go
func (c *connector) CredentialRefs(mg resource.Managed) ([]credentialuse.Ref, error) {
	cd := mg.(*v1alpha1.CompositionDefinition)
	t := cd.Spec.Target // the KubernetesTarget, in cd's namespace
	return []credentialuse.Ref{{
		GroupKind: kubernetesTargetGK, Namespace: cd.Namespace, Name: t.Name,
		Next: func(obj client.Object) ([]credentialuse.Ref, error) {
			kt := obj.(*v1alpha1.KubernetesTarget)
			return []credentialuse.Ref{credentialuse.SecretRef(kt.Namespace, kt.Spec.KubeconfigRef.Name)}, nil
		},
	}}, nil
}
```

The tracker follows the chain level by level, and **holds each object
before it reads it**. It announces and holds the KubernetesTarget, reads it
from the cache, calls `Next`, then announces and holds the Secret. The
record names both, and both carry the finalizer. Every function the
provider supplies stays pure, and the hold-before-read ordering is the
runtime's, not each provider's. `MaxDepth` (default 3) bounds the chain. A
`Next` that fails, or a chain deeper than that, is an Acquire error: the
object doesn't connect, and if it already has its own finalizer it proceeds
on the record it has (§10.3).

**A Secret that names a Secret** (a ref resolved from another Secret's
content) works the same way, and is in scope: the first Secret's `Ref` has
a `Next` that reads its data. The runtime never parses Secret contents
itself.

**What a chain can't close.** The intermediate object is read from the
cache, so for one pass Connect may read a newer target that names a Secret
not yet held. The next pass holds it. The window is one pass and the
cache's lag, and it matters only if that new Secret is deleted inside it.
To close it, a provider can connect with exactly what was held:
`credentialuse.Resolved(ctx, ref)` would return the object the tracker read.
That is optional, and §16 Q3 asks whether to build it now.

**Non-Secret kinds in RBAC and watches.** Each kind in `Options.Protected`
gets a metadata-only watch and needs the verbs in §12. A `Ref` of a kind
not in `Protected` is an Acquire error, so a provider can't silently hold
something whose release nobody watches.

## 9. Lifecycle edges

### 9.1 A ref that changes on update

The next pass computes the new set. Announce writes the record as the new
set: new refs added, dropped refs removed. It then verifies the new refs.
Dropping a ref is always safe, because the object will never read it
again. The holder's update event carries the old and the new record, so the
release controller is enqueued for the dropped Secret and releases it if it
has no other user. Refs reached through `Next` change the same way when an
intermediate object changes, on the next pass.

### 9.2 A Secret that doesn't exist yet

Verify finds it missing, so there is nothing to hold, and `Unheld` reports
`Missing`. The record keeps the ref. Connect runs and usually fails, as it
does today, so the object gets no finalizer of its own. When the Secret
appears, the next pass (backoff, at most 60s) reads it from the cache with
the ref already in the record, and adds the finalizer. A provider that
tolerates a missing optional Secret is unaffected. A watch from the Secret
back to its holders, for an immediate requeue, is possible through the same
index, but isn't part of this design.

### 9.3 A Secret in another namespace

Allowed. The finalizer names the holder's namespace (§3). The release check
lists that namespace, and the Secret's namespace plays no part in it. With a
namespace-restricted cache (`cache.Options.DefaultNamespaces`), a ref into
an uncached namespace can't be read from the cache. The tracker then uses
the API reader for it, every pass, and logs once that the cache doesn't
cover that namespace.

### 9.4 A Secret already being deleted when an object first references it

If it still carries this kind and namespace's finalizer, the object can use
it. The release check will count the object, and the Secret stays until its
users are gone, as a PersistentVolumeClaim does with the pods that use it.
If it doesn't, no finalizer can be added (`ValidateNoNewFinalizers`). Then:

- **A new object** (no finalizer of its own yet) gets `MustNotConnect`. It
  doesn't connect, reports §11's condition, and requeues with backoff. It
  never gets its own finalizer, so deleting it never waits on the Secret.
- **An existing object** whose refs were just changed to point at that
  Secret can't be protected by any design: the Secret was being deleted
  before anything referenced it. The tracker reports it unheld, the runtime
  connects anyway (as today), and records a Warning event. That is a
  documented limit, and the remedy is to fix the ref.

### 9.5 An object with deletion-policy orphan

No Connect is needed to remove its finalizer (`:821`). `Forget` removes the
record in that same Update, the delete event enqueues its Secrets, and they
are released. Until it was deleted it held its refs like any other object.
It connects to observe, so its Secrets need the same protection while it
lives.

### 9.6 A provider uninstalled before its users

Today the users' own finalizers stay, and their namespaces wedge. Under
this design that doesn't get worse for them. What is newly stuck: **every
Secret, and every protected intermediate, that a remaining user held**,
because nothing runs to release `credential-use.<kind>.<group>/<ns>`. Doc
text for consumers:

> Delete a provider's objects, and wait for them to go, before uninstalling
> the provider. Objects left behind keep their own finalizer, and the
> Secrets they use keep a `credential-use.<kind>.<group>/<namespace>`
> finalizer. To clean up after an out-of-order uninstall, remove the
> objects' finalizers first, then the `credential-use.` finalizers of that
> kind:
> `kubectl patch secret <name> -n <ns> --type=json -p '[{"op":"remove","path":"/metadata/finalizers/<index>"}]'`.

core-provider's #313 is the same class: a controller that runs inside the
namespace being deleted dies before it can release anything. This design
doesn't fix that, and the doc says so: a provider must not run in a
namespace whose objects it manages.

**With scale to zero** (`pkg/scaletozero`), the last delete's release has to
run before the controller scales down. The hold-down (`Decide`) gives it
time, since a release takes a few API calls. A release that keeps failing
through the hold-down (missing RBAC, say) is stranded until the Kind's next
instance wakes the controller, and the controller's startup list rechecks
every hold.

### 9.7 Upgrade, and downgrade

**Upgrade.** Existing objects have no record, so their first pass after
the upgrade differs from the record. It announces and holds, which is the
backfill. It costs one Update per object and one GET per Secret, spread
over the first poll interval (and over local slots, with saturation on).
Objects already wedged, whose Secret is already gone, can't be backfilled.
They stay wedged until a human removes their finalizer, as today. An
existing object deleted before its first pass after the upgrade gets the
same backfill on its deletion pass, if its Secret is not yet being deleted.

**Downgrade, or a provider dropping the interface.** An older runtime
neither releases nor knows the finalizers, so every held Secret would
become undeletable. So `Options.ReleaseOnly` exists: run one release with
`ReleaseOnly` set (provider-runtime: the connecter keeps the interface, and
a reconciler option turns acquisition off). It releases everything as users
go, and announces nothing new. Then drop it. The release notes must say so.

### 9.8 Consumers that don't implement the interface

No behaviour change. No tracker is set up, so there is no Secret watch, no
annotation and no Secret write. Reconcile takes the same path as today,
including Connect on deletion.

## 10. A ref that was never recorded

These are the cases the bookkeeping misses, and how each is handled.

1. **Objects from before the upgrade:** backfill on the next pass (§9.7).
2. **A ref the provider's `CredentialRefs` doesn't name** but Connect reads
   anyway. Nothing protects it. This is a provider defect, and it is
   detected in two ways:
   - in the provider's tests: `reconcilertest.AssertCredentialRefsCover`
     runs Connect against a recording client and fails if Connect reads a
     Secret, or a kind in `Protected`, that the object's refs (after `Next`)
     don't name;
   - at run time: `credentialuse.CheckingReader(reader, tracker)` wraps the
     provider's client. A read of a Secret absent from the object's record
     logs once per object and counts in a metric. It is off unless the
     provider wraps its client.
3. **`CredentialRefs` fails.** For a new object, Acquire returns the error
   and the object doesn't connect, so it gets no finalizer of its own while
   its refs are unknown (fail closed). For an existing object, the error is
   logged once per object, the record stays as it was, and Connect proceeds
   (fail open, today's behaviour). Release never needs `CredentialRefs`, so
   a failure can't strand or wrongly release a Secret.
4. **The record removed or edited by hand.** The next pass re-announces. In
   between, a release can miss the object, and if the Secret is also being
   deleted then, the object can wedge. That is documented as "don't edit
   `credential-use.krateo.io/refs`".

## 11. What users see: conditions and events

**A deleted object whose Secret is still referenced elsewhere** isn't held
up by the others. The Secret stays readable, so its deletion runs as
today: Ready=False/Deleting, Synced=True/ReconcileSuccess (or the delete's
error), then it is gone. Its release leaves the finalizers of other kinds
and namespaces, and its own namespace's if other users remain there.

**The Secret, while it waits** for users after its deletion was requested,
has a `deletionTimestamp` and keeps its `credential-use.*` finalizers.
Secrets have no conditions. The release controller records a Normal event
on it, throttled, once per state change:
`WaitingForCredentialUsers: in use by 3 repo.git.krateo.io in team-a`. It
also logs the same at Info, and exposes a gauge of protected objects that
are waiting, by holder kind. In a terminating namespace the event is
rejected (`git-provider-rlgov.log` line 7), so the log and the gauge are
the signals there.

**A live object whose Secret is being deleted but still held** keeps
working. Its conditions don't change. It gets a throttled Warning event,
`CredentialSecretDeleting`, which names the Secret and says it is kept
until this object stops using it.

**A new object that must not connect** (§9.4) gets
Synced=False/ReconcileError with the message
`credential secret <ns>/<name> is being deleted and can't be held; not connecting until it is replaced or the reference changes`,
and Ready=NotObserved through `markNotObserved`, as for any Connect
failure. The same goes for an Acquire error, with that error as the
message.

## 12. RBAC

Every consumer chart needs, for **Secrets**, and for every other kind in
`Options.Protected`:

| Verb | Why |
|---|---|
| `get` | the consistent verify of a new ref, and reads outside the cache |
| `list`, `watch` | the metadata-only informer of the release controller |
| `patch` | adding and removing the finalizer and the drain mark |

`update` is not needed: every write to a protected object is a
resourceVersion-guarded patch of metadata. `create` and `delete` are not
needed either. Refs can cross namespaces, so these go in a ClusterRole
unless the provider's cache is namespace-restricted, in which case they go
in a Role per namespace. Most providers already have `get`, `list` and
`watch` on Secrets, because their cached client reads them. What they add
is `patch`.

On the **holder kind**: `list` (the consistent release check), and `update`
(the record). Both are already granted, because the reconciler lists the
kind and updates it to add its finalizer.

Doc text for consumers:

> If your connecter implements `CredentialRefsProvider`, the runtime keeps a
> finalizer on each Secret your objects use until the last of them is gone.
> Grant the provider `get`, `list`, `watch` and `patch` on `secrets`
> (ClusterRole, since references may cross namespaces), and the same on any
> other kind your refs name through `Next`. Without `patch`, objects can't
> acquire their Secrets: their reconcile fails with a Forbidden error
> naming the Secret, and they never get the runtime's finalizer.

## 13. Tests, and what the reviewer should attack

**Envtest: namespace teardown** (`pkg/credentialuse`, with its own `TestMain`
and `requireAPIServer`, copied from `pkg/scaletozero/envtest_test.go`). A
manager runs the real reconciler with a test CRD (`Widget`), a connecter
that implements `CredentialRefs` and reads the Secret in Connect, and a
client whose Delete records the call. envtest has no namespace controller,
so the test plays it, in the adversarial order seen on krateo-057:

1. Create a namespace, a Secret, and two Widgets naming it. Wait for
   Synced=True, the Secret's `credential-use.widget.<group>/<ns>` finalizer,
   and the record on both Widgets.
2. Delete the namespace. Assert it is Terminating, and that creating a
   Secret in it fails Forbidden. That pins the premise, and fails the test
   if NamespaceLifecycle isn't on in envtest.
3. Delete the Secret first, then both Widgets. Assert that the Secret stays,
   readable, with a `deletionTimestamp`; that Delete was called for each
   Widget; that both Widgets go; and that the Secret then goes.
4. Finalize the namespace, as the namespace controller would, through the
   `finalize` subresource, and assert that it goes.
5. **Control:** the same sequence with a connecter that doesn't implement
   the interface must wedge (Widget still present after a timeout, Synced
   naming the missing Secret). That proves the test reproduces the defect.

Further envtest cases: a ref changed on update releases the old Secret; a
cross-namespace user keeps a Secret whose own namespace is being deleted,
until it goes; a Secret already being deleted gets a new object with
`MustNotConnect`, and that object deletes freely; restart with a drain
mark left behind; `ReleaseOnly` drains; a chain Widget → Target → Secret
with the Target deleted first.

**Unit tests for the races in §5** run on a small in-memory API model: a
linearizable store with resourceVersions, `ValidateNoNewFinalizers`, and a
lagging cache per actor. The fake client can't enforce terminating
semantics. The model tests:

- **Exhaustive interleavings.** The acquirer's steps (announce, verify,
  clear mark or add finalizer) against the releaser's (R0, mark, list,
  remove), with each read either fresh or stale. That is a few hundred
  schedules. Assert after each: no holder with a record naming P exists
  while P lacks the finalizer *and* P is being deleted.
- Two releasers in two processes; two users deleted at once; one created
  while another is deleted; the finalizer added after a concurrent removal;
  a crash after R1.
- `CredentialRefs` errors, for new and existing objects; `Next` errors;
  the depth bound.

**Stress** (envtest, `-race`): many goroutines create, re-point and delete
Widgets over a small pool of Secrets, in several namespaces with
cross-namespace refs, while other goroutines delete Secrets and namespaces.
Two trackers run for two kinds. Invariant, checked continuously: every
Widget that carries the runtime's finalizer and a record naming a Secret
being deleted has that Secret still present with its finalizer. At the end,
every namespace tears down, and no Secret is left with a `credential-use.`
finalizer and no users.

**Drain:** cancel the manager mid-protocol (between R1 and R3, and between
announce and verify), restart it, and assert it converges.

**What the independent reviewer should attack:**

- every path that reaches `AddFinalizer`, and whether one can do so
  without a successful Acquire in the same pass;
- the announce-before-verify and mark-before-list orderings, and whether any
  read on the decision path can come from a cache;
- that R3's patch is guarded on the resourceVersion that R1 returned, and
  that no merge patch of `metadata.finalizers` can clobber a finalizer
  written concurrently by someone else;
- the fast path for refs already in the record: any way a ref enters the
  record without a consistent verify after it, including tampering and
  backfill;
- `Forget` landing in the same write as the finalizer removal on both
  delete paths, and never before it;
- the release controller's startup list, as the only defense against a
  missed delete event;
- chains: hold-before-read at every level, and the record after a `Next`
  error;
- the deletion pass with an unheld ref on an established object;
- finalizer-name construction for long groups and cluster-scoped kinds;
- the namespace-restricted cache fallback.

## 14. Alternatives rejected

**14.1 Skip Connect on deletion when the credential is missing.** It leaks
the external resource. A missing Secret says nothing about whether the
external resource exists. For a kind whose Delete calls out, removing the
finalizer without Connect leaves an orphan in the external system with
nothing left that knows about it. It would also make Secret deletion an
unintended "orphan everything" switch.

**14.2 A "Delete needs no external call" marker.** It fixes git-provider's
Repo, whose Delete is a no-op, and nothing else: every kind whose Delete
calls out still wedges. The defect is generic, so the fix must be too.
Lazy credentials as a provider-side practice are still worth recommending
as defense in depth (§15.4).

**14.3 Owner references from the Secret to its users.** An owner reference
says the *owner's* deletion cascades to the dependent, which is the
opposite of what is needed. `blockOwnerDeletion` only blocks foreground
deletion of the owner, and namespace deletion uses background deletion. It
also has no cross-namespace form: owner references must be in the same
namespace, or cluster-scoped. And it needs `update` on the users'
`finalizers` subresource under OwnerReferencesPermissionEnforcement.

**14.4 A usage object per holder (ProviderConfigUsage).** It would need a
CRD shipped in every provider chart, an object per holder (twice the
objects and the writes), and garbage collection of the usages. And it
still needs a list to find the last user. The record on the holder is the
same information without the second object. Crossplane's version also
counts usages from the cache, the race §5.2 closes.

**14.5 Recomputing refs at release time** (a field index on
`CredentialRefs`, or a list of the kind filtered through it). See §4: it
fails on unresolvable paths, changed refs and chains, and it needs the
provider's code in the releaser.

**14.6 Labels on the holder** in place of an annotation. A server-side
label selector would make the release list cheaper. But label values are
limited to 63 characters, so they would need hashed Secret identities, and
a holder has one label per ref. Users and tools (`kubectl label`, GitOps
label policies) also edit labels far more often than annotations. The
consistent list in §5.2 is already metadata-only and limited to one
namespace.

## 15. Consumer adoption

### 15.1 git-provider

Implement `CredentialRefs` on both connecters, and add `patch` on `secrets`
to the chart's ClusterRole.

- **Repo:** `spec.fromRepo.secretRef`, `spec.fromRepo.usernameRef`,
  `spec.toRepo.secretRef`, `spec.toRepo.usernameRef`, each a
  `SecretKeySelector` read in Connect through `loadExternalClientOpts`
  (`internal/controllers/repo/utils.go` at 0.17.0: `resource.GetSecret` at
  lines 65, 76 and 93). Skip nil refs. The probe's Repo named one Secret
  twice, which becomes one ref.
- **LocalResource:** `spec.toRepo.credentials.secretRef` and
  `spec.toRepo.credentials.usernameRef`, read in Connect
  (`internal/controllers/localresource/localresource.go`, read at
  `69d98eb`; confirm at the tag that adopts this).
- `spec.configMapKeyRef` on Repo is a ConfigMap. If it is read on the
  delete path, add it as a protected ConfigMap ref. git-provider should
  check this; this design didn't.

### 15.2 oasgen-provider

rest-dynamic-controller is on unstructured-runtime, so it adopts through
core-provider's integration (§6, "What an outside runtime must call"), not
`CredentialRefsProvider`. Its `RefsFunc` is the collector it is writing for
#510. Per-object Secrets resolved from paths in the object's own spec come
straight from `RefsFunc`. The Configuration-level Secret is a chain:
a ref to the Configuration, with a `Next` that names its Secret, so the
Configuration is protected too.

### 15.3 unstructured-runtime (core-provider)

It has the same deadlock (§1). It integrates `pkg/credentialuse` as §6
describes. A remote CompositionDefinition names its KubernetesTarget with a
`Next` to the kubeconfig Secret (§8). A chart's
`spec.chart.credentials.passwordRef` Secret is read by the composition
controller. Whether that read is on the delete path is unverified, and
core-provider should check. If it is, it is a ref like any other.

### 15.4 "Answer from status" is a complement worth recommending

core-provider's #314 fixed a Delete that couldn't proceed with an
unreadable chart, by answering from status. It is worth recommending to
any provider whose Delete, or whose decision that the external resource is
gone, can be made without credentials. git-provider's Repo is one: its
Delete is a no-op. The practice is to read credentials lazily, in the
client calls that need them, not in Connect. Then a deleting object whose
Delete doesn't call out never reads its Secret. It is a complement, not a
substitute: it does nothing for kinds whose Delete calls out (§14.2), and
the runtime still connects.

## 16. Open questions for the user

**Q1. Protect non-Secret intermediates in the first version?**
(a) Secrets only, chains later. (b) The general `Ref` with a GroupKind and
`Next`, as designed. *Recommendation: (b).* The KubernetesTarget and the
Configuration are the same deadlock one level up (§8). Retrofitting the
record format and the RBAC later would mean a second migration.

**Q2. Implicit or explicit wiring in provider-runtime?**
(a) Implicit: `NewReconciler` sets up the tracker when the connecter
implements `CredentialRefsProvider`, and panics if setup fails. (b)
Explicit: a `WithCredentialUse(tracker)` option, after
`credentialuse.Setup`. *Recommendation: (a)*, because it can't half-enable
the feature (acquire without release makes Secrets undeletable). (b) is
more conventional, but a provider that implements the interface and
forgets the option gets nothing, and the same is true the other way round.

**Q3. Build `credentialuse.Resolved` now** (Connect uses exactly the
objects that were held, closing the one-pass window for chains in §8)?
(a) Now. (b) Later, if a chain provider asks. *Recommendation: (b).* The
window is one pass of cache lag, and only matters if the new Secret is
deleted inside it. Recording and holding the chain is what removes the
deadlock.

**Q4. A condition for a live object whose Secret is waiting** (§11)?
(a) Events, log and a gauge only, as designed. (b) A new condition type,
such as `CredentialsHeld`. *Recommendation: (a).* Its conditions are
accurate, since it still works, and a new condition type would be the
first the runtime adds to every provider's objects.

**Q5. The existing finalizer name**, `finalizer.managedresource.krateo.io`,
draws an API-server warning for lacking a `/` (`git-provider-rlgov.log`
line 2). (a) Leave it. (b) A separate change that migrates it.
*Recommendation: (b), as its own issue.* Renaming a finalizer needs a
migration of every live object, and has nothing to do with this deadlock.
