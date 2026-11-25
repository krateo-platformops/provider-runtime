---
type: Architecture
title: provider-runtime — credential-use finalizer
description: Design, not built, for holding the Secrets (and the objects that lead to them) that a managed resource reads, for as long as losing them would be unrecoverable, so a namespace deletion can't wedge on a managed resource whose credentials were deleted first.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, finalizer, deletion, credentials, reconciler]
timestamp: 2026-11-09T00:00:00Z
---

# Credential-use finalizer

**Status:** design, revision 18. Nothing is built yet; the build follows
[the implementation plan](./plan-credential-use-implementation.md), eleven
stacked PRs that ship as v1.13.0. The
user chose the approach: a finalizer on each Secret an object uses, the
analog of Crossplane's ProviderConfigUsage. The user's decisions are
binding here: U1 to U7 on the first review, NQ1 to NQ4 on the second, and
IP1 to IP5 on the implementation plan. Revision 16 amends the text where
the plan found it ambiguous once coded (§17, "Revision 16"). Revision 17
amends the skip rule and the grace's clock after the independent review of
the protocol's code (§17, "Revision 17"). Revision 18 amends the index's
dropped lines and Disown's settling (§17, "Revision 18").

**The first tag holds direct refs only** (the user's decision, 2026-11-24).
A ref with a `Next`, a chain, is refused with a clear error, before the pass
looks at the list. §9 says what that looks like and why; the chain design in §9 and
§10.4 is built and tested, and ships in the next release.

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

**It exists outside this runtime too.** In core-provider, the composition
controller's Delete reads the chart's `passwordRef` Secret
(core-provider#402). That is **the known second instance**, built on
unstructured-runtime, which core-provider owns. **This is flagged for
core-provider.** It is why the bookkeeping is a standalone package that
doesn't need a manager (§7).

oasgen-provider's rest-dynamic-controller, also on unstructured-runtime, is
**not** a second known instance (corrected by oasgen). Its Delete
(`restResources.go:1574`) reads a Secret, through `handler.Delete` →
`ensureSecretRefRBACAndResolve` → `secretref.GetSecretValue`, but tolerates
the failure: it logs a Warn and continues with `resolved = nil`
(`restResources.go:1692`). It is exposed only where a resolved value is
needed to build the delete request itself, such as a path parameter. There,
`BuildCallConfig` returns nil.

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
  `ValidateNoNewFinalizers` (`objectmeta.go:126-134`) rejects that with a
  field error of type Forbidden. The API server returns it as **422
  Invalid** (`k8s.io/apiserver@v0.37.2/pkg/registry/rest/update.go:157`),
  not as an RBAC 403. Removing finalizers is still allowed, and so is changing
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
object its namespace's check considers. It fails safe, and each process whose
informer delivers it, while it counts so, logs a warning, once per holder, with
its namespace, name and kind, and nothing of the record. The holder's next
acquire that reaches its last A1 overwrites it with a valid record. Until then
it counts, for every object held for its namespace, as a holder that names it
does (§5); when it stops counting as a user of them all, they are all checked
again (§10.2). An acquire that ends before its last A1 doesn't overwrite it: a
`Refs` error, a holder in the index namespace, a refused chain (§9). The record
then stays unparseable, and keeps what it counts as used, as a paused holder's
record does (§11.3).

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

**The table covers held refs.** A ref in another namespace is held only with
the target's consent (§11.3). One that isn't held isn't covered here, and
the original deadlock doesn't apply to it: the holder's namespace teardown
never deletes a Secret in another namespace.

**The re-reads are the last reads before the release patch.** When the
grace has run out, the releaser GETs the Namespace live and reads its
`deletionTimestamp`, not its phase. It then lists the holders live, to find
users that are deleting. Only then does it patch (§6.2, R2 and R3); how long
that can be, and what can start in it, is in §6.4. Reading
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
  has a `deletionTimestamp` and lacks the runtime's own finalizer, every
  name of which `Options.OwnerFinalizers` lists (§7.1). For the whole of
  provider-runtime#46's migration, that is both the old and the new name,
  and "has the owner finalizer" means "has either".

H is **deleting** when it has a `deletionTimestamp` and still carries the
owner finalizer. The runtime removes the record, with `Forget`, in the
same write that removes its owner finalizer (§8). The exclusion covers
everything else that can leave a stale record behind: a third party
removing the owner finalizer, or someone else's finalizer keeping H around.
It also breaks foreground-deletion cycles (§11.8).

A holder whose record can't be parsed uses every P in question, and stops when
it is gone, when its record is repaired or removed, or when it is being deleted
without the owner finalizer. §10.2 queues what it kept: its record can't.

### 6.2 The two sides

**The acquirer**, for each ref P of holder H, in every pass, before Connect:

- **A1. Announce.** If H's record differs from the refs just computed,
  write the new record with a merge patch guarded by H's resourceVersion.
  The core builds it itself, as a JSON merge patch that carries
  `metadata.resourceVersion`, sent through the metadata client. That is the
  patch controller-runtime's `client.MergeFromWithOptimisticLock` makes,
  without depending on controller-runtime (§7.1). The whole returned
  `ObjectMeta` is copied onto the caller's object (L2 of the review of
  revision 16). The patch touches only annotations, so the rest of the object
  is unchanged, unless a mutating webhook or defaulting changed it. So if
  the returned generation or labels differ from what the object had before
  the patch, the caller's object may be stale beyond its metadata too, and it
  is re-read before the pass makes its next write. A conflict requeues, for any
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
    resourceVersion (built as A1's is) that writes the full
    finalizer list with the finalizer added, the fresh epoch, and both
    labels. On a conflict it GETs and retries, up to 3 times. If the
    retries run out, a new holder requeues (fail closed), and an
    established holder is **Degraded**: it logs once, records an event,
    counts the metric, and proceeds, deletion passes included (R4, M5).
  - **Outcomes:**
    - **held**: P now carries the finalizer and the fresh epoch. That
      includes a P that is being deleted *with* the finalizer: the patch
      adds no new finalizer, so it is allowed, and the epoch still moves;
    - **Deleting**: the patch is rejected as **422 Invalid**, with a
      Forbidden-type cause on `metadata.finalizers`, because P is being
      deleted and lacks the finalizer (§2). That is class (a) below, and
      nothing else is. A new holder gets `MustNotConnect`. For an
      established holder this is **Degraded**, the one term this doc uses
      for every fail-open (§8);
    - **Missing**: NotFound, of the object (below);
    - **Degraded**: any other error. A new holder fails closed and an
      established one proceeds (§8).
- **A3.** Only then Connect.

**Classifying A2 and index-write errors.** Every place that classifies an
error from A2, from an index write (§10.6), or from a live read in the
gate, reads the status's code, reason, message and **causes**. It never
uses `IsForbidden` or `IsInvalid` alone, because two different 422s and two
different 403s exist (core-provider found this; verified in
`k8s.io/apiserver@v0.37.2`):

| Class | Status | How it is recognised | Meaning |
|---|---|---|---|
| (a) **Deleting** | 422, reason Invalid | a cause of type `FieldValueForbidden` on field `metadata.finalizers`, whose message starts `Forbidden: no new finalizers can be added if the object is being deleted` (`ValidateNoNewFinalizers`, `objectmeta.go:131`, via `NewInvalid` in `pkg/registry/rest/update.go:157`) | P is being deleted and lacks the finalizer |
| (b) **policy denial** | 403, reason Forbidden | a policy's text (below), and not (c). Our policy sets `reason: Forbidden` on its validation (§10.6.1), so its denials are 403 | an admission policy refused the write |
| (c) **policy config error** | 422, reason Invalid | a policy's text containing `failed to configure binding`, and **no** `metadata.finalizers` cause | a policy binding couldn't be configured, for example `cu-policy-params` missing (§10.6.1) |
| (d) **RBAC** | 403, reason Forbidden | the authorizer's own message, `User "<name>" cannot <verb> resource "<resource>" in API group "<group>"` (each part written with `%q`, so escaped quotes are allowed), followed by ` in the namespace "<ns>"` or ` at the cluster scope` (`pkg/endpoints/handlers/responsewriters/errors.go:60-80`). The exact text is pinned by an envtest | the caller lacks the permission |
| (e) **other admission denial** | 403 | neither (b) nor (d), or a 403 whose reason isn't Forbidden: a webhook denial, or NamespaceLifecycle refusing a create in a terminating namespace (`pkg/admission/plugin/namespace/lifecycle/admission.go:179`). Counted in `credentialuse_admission_denied_total`, so an RBAC message the pattern no longer matches shows up | an admission plugin refused the write |

**A policy's text is not a message prefix** (corrected in revision 16). The
dispatcher wraps a denial in `admission.NewForbidden`, so the status message
reads `configmaps "cu-index-x" is forbidden: ValidatingAdmissionPolicy '…' with binding '…' denied request: …`.
It also appends a cause, with no type and no field, whose message is the bare
text starting `ValidatingAdmissionPolicy '`
(`pkg/admission/plugin/policy/validating/dispatcher.go:300-317`;
apimachinery `pkg/api/errors/errors.go:209-229`). (b) and (c) are recognised
by that cause, falling back to the substring
`is forbidden: ValidatingAdmissionPolicy '` in the message.

**(d) is recognised positively** (IP1, the user's decision). An earlier
revision defined it as "a 403 without a policy's text", which also caught
webhook denials and NamespaceLifecycle's 403. PerNamePoll would have retried
those as RBAC propagation.

**The RBAC message across Kubernetes versions** (L3 of the review of
revision 16). The format is identical in `k8s.io/apiserver` v0.31.0, v0.33.0,
v0.35.0, v0.36.5 and v0.37.2, read from the module cache
(`pkg/endpoints/handlers/responsewriters/errors.go`, `forbiddenMessage`); in
v0.31.0 `Forbidden` builds the message inline, with the same `: <reason>`
suffix. **Krateo's minimum Kubernetes is 1.36** (the user's decision,
§18). CI pins the exact text on envtest 1.37.0 and on 1.36.2, the latest
1.36 patch, in `.github/workflows/run-tests.yaml`, and the same test also
passes locally on 1.34 and 1.35.

**A 404 is NotFound only when it names the object.** A resource the API
server doesn't serve also answers 404, and that says nothing of the object,
which may be there and carry a finalizer (P19):

- Outside every group-version it serves, as for a custom resource whose CRD no
  longer serves the version the tracker mapped, whose CRD is gone, or whose CRD
  this API server hasn't seen yet, the body is plain text, from the server's
  last handler, not a Status. client-go marks it with the cause
  `UnexpectedServerResponse`, and may fill the request's name in.
- Inside one, as for a built-in resource removed from a version that is still
  served, or on an aggregated API server, it is a Status with no name in its
  details.
- An admission webhook may refuse a write with a 404 of its own.

A missing object's 404 is a Status whose details name it, without that cause.
Any other 404 is an ordinary failure everywhere in the package, never NotFound.
(An API server that is still starting answers 503 instead, until its CRD
informer has synced.)

**And only a read's 404 says that an object is gone** (P20). A write's may name
something and be about something else, since admission answers it:

- a webhook or a policy with a namespaceSelector looks the Namespace up, and
  when that is gone, as after someone forced its finalizer while a hold kept it
  Terminating, the write of an object still in it is answered with a 404 that
  names the Namespace;
- a validator may return what its own lookup gave it, a NotFound of any object,
  this one included.

Taken for "gone", those made a hold Missing, so a new holder connected without
it, though `BeforeEstablish` kept it from being established; and reported a held object bare, whose index line a prune then took. So
`clientServer.Patch` confirms a 404 with a GET of the same object, which
admission isn't asked about. If that GET's 404 names the object, it is NotFound.
If it finds the object, or fails in any other way, its kind not served, a 403 or
a timeout included, the patch's error is an ordinary one, with both answers in
its text: A2 fails its pass, R3 and the epoch's repair their batch, the janitor
and the strips are retried, and `DisownPrefix` returns it. An object created
between the two is such an error too, and the next pass finds it. *The cost:*
one GET more for each Missing ref of a built-in kind, at each pass (A2 reads a
custom resource before it patches it), and for an object deleted just before R3.
None for an object named without its namespace. *Not through it:* the index
ConfigMap's create and update, and the CRD patches, none of which reads a 404 as
gone: only their conflicts are told apart, any other error fails the call. The
package deletes nothing. What it can't cover is a holder's own write, which the
reconciler makes ("A holder kind that isn't served", below).

**A holder kind's CRD is gone by the same rule.** R2's list of a holder kind
that isn't served is a 404 that names nothing, or the mapper's no match, and
says nothing of the holders. The GET of the CRD that it leads to (§6.4, "The
holder kind itself gone") is a read, and its 404 is taken for the CRD's absence
only when it is NotFound as above, and the object it names is that CRD: the
name the tracker was given, in `apiextensions.k8s.io`. The group's discovery is
asked after it, as a veto (§6.4).

**A namespaced kind's object named without a namespace can't exist**, so it is
NotFound, as a cluster-scoped holder's data may name one: the scope is the
mapping's, and no call is made, since that 404 would name nothing. A2 finds it
Missing, reported as any Missing ref is, and the holder goes on; a release
batch, an index check, the prober and Disown find it gone. A namespaced
holder's is a ref to another namespace, so the gate (§11.3) asks for its
consent first, from a Namespace that can't exist: the ref is
`ConsentUnreadable`, and the holder goes on. A list without a namespace is of
every one, and isn't this. Such a ref with a `Next` leaves its chain unfollowed
on every pass (§9.1).

**Nor can one whose name, or namespace, isn't a single path segment**, as
`<namespace>/<name>` where a name is meant: the API server's own registry admits
none, whatever the kind. **Nor can a cluster-scoped kind's that is named with a
namespace.** Each names no object, and is NotFound in the same way, without a
call, whoever asks: a ref is Missing and its holder goes on, and what a record or
an index line names so is gone, for a release batch, an index check and Disown
alike, which settles. A list in such a namespace has nothing, nor has one of a
cluster-scoped kind in any, but the holder kind's: `spec.scope` can't change on
an established CRD, so a kind that returns with another scope is a new CRD,
which a process that mapped the old one still lists as it was (below); R2 and
`Users` therefore list a holder kind mapped cluster-scoped in every namespace,
and keep the holders in theirs. Of one mapped namespaced that is cluster-scoped
now, the list in a namespace is the API server's 404, an error, and the list
without one has every holder. In such a process the passes of that kind's
holders fail until a restart, so this keeps what is held and takes no new hold.
(Disown's strip of such a holder's record finds
the holder gone without a call, and says nothing; its count is of the list, which
still has the record, so it doesn't settle.) No cache has such an object, and no per-name
watch is made of it (§13 shape 1), nor of a namespaced kind's named without its
namespace. *Before this,* such a ref could fail its holder's passes, keep its
index line and the startup recheck from being done, and Disown from settling, or
leave a hold no record names. LabelScoped finds that one by its labels, and
releases it as any hold nobody names. PerName has no line of it: list the objects
with the kind's marker label, and add the line
`<group>/<kind>/<namespace>/<name>` of any that has the kind's finalizer and no
line. Its watch, or the next startup recheck, then releases it, unless a holder's
record names it.

**A limit the user accepted (2026-11-24): a protected kind that returns with
another scope.** Until a process that mapped the old CRD restarts, a ref with a
namespace, of a kind it still maps cluster-scoped, or the reverse, is Missing
without a call: a new holder connects without its hold, and, in PerName, a
hold's index line may be pruned, and Disown counts the hold as gone. (In
LabelScoped Disown counts it from its list, can't strip it, and doesn't settle.)
Restart the providers that track the kind (§13). A discovery check on a mismatch is for the next release.

**A limit:** a namespaced holder can't hold a cluster-scoped object. Named
without a namespace, as it must be, it is a ref to another namespace, whose
consent (§11.3) nobody can give, since there is no Namespace to read:
`ConsentUnreadable`, and the holder goes on. A cluster-scoped holder's is in its
own.

While a kind isn't served:

| Who | What it does |
|---|---|
| Acquire, `BeforeEstablish` | A2 fails, and ends the pass at that ref: the refs after it wait for a later pass. A new holder doesn't connect, or establish; an established one is Degraded |
| a hop that isn't held, for want of consent or under Disown (§9) | its read fails the pass, as any read error does: what lies behind it is unknown. A new holder doesn't connect, under Disown too; an established one is Degraded |
| the consent gate (§11.3) | unknown: a hold is kept, a new one waits, and the pass requeues |
| the prober (§10.3) | can't tell: the ref's backoff stays as it is |
| R1, R3, the epoch's repair | undecided: the batch is retried with backoff, and nothing is reported bare |
| R2, when the **holder** kind isn't served | the list fails, which is never "no users". A live GET of the CRD, and then its group's discovery, tell: the kind is gone, and so are the holders, or the batch is retried (§6.4) |
| the index's checks (§10.6) | the line is kept, or put back, and the check is retried |
| the janitor | retried with backoff |
| Disown (§7.4) | `held-count` is `"unknown"`, and `CredentialUseDisownUnreadable` names the object, or what it can't read as a whole (a LabelScoped kind, the holders, or the index): its kind isn't served. A strip that fails is left for the next pass, and so is the record that names the object. A switch that can't be read fails the hold |
| `DisownPrefix` | reports the failure, and goes on with the rest |

**The cost, and the way out.** A protected kind whose CRD is removed for good
while objects of it are held, which takes forcing the CRD's cleanup finalizer
since the holds block it, is never read as gone: batches for its objects are
retried, at most every 5 minutes, and Disown's count stays unknown. Taking such
a 404 for "gone" let Disown settle at zero, and dropped index lines, with
finalizers left on objects that a CRD installed again would serve.

- **What is logged.** Each failed batch, with the delay and the error ("a
  release batch failed"), and each failed index check; a Degraded pass, once
  per holder, with its event; and, while Disown is on, on each pass, the
  objects it can't read, or what it can't read as a whole, a LabelScoped kind,
  the holders, or the index: a log line, the event
  `CredentialUseDisownUnreadable` on the holder CRD, throttled, and, for
  objects, the gauge `credentialuse_disown_unreadable`. Disown's say that the
  kind isn't served, and its event has the failure's class, and what to do about
  that class, while the error's text, which may vary at every pass, is in the
  log alone. The others carry client-go's error, "the server could not find the
  requested resource".
- **The mapping isn't refreshed while the process lives.** The resource, the
  version and the scope a kind maps to are the ones the RESTMapper gave at
  first, and a mapper that finds new kinds by itself doesn't look again at one
  it knows. A change of any of them, the scope too, needs a restart. A protected
  kind's version is the mapper's, not `Protected`'s: the reads by name, the writes
  and the live lists map a GroupKind (below); only the informers are of the
  version `Protected` names. The holder kind's is `Holder`'s, for every call.
- **Restore the CRD**, at that version, whose objects are then served as they
  were. Everything goes on by itself. Only serving the kind again lets a
  finalizer an object still has be stripped, since nothing can write it
  meanwhile.
- **If only that version is gone, restart at one that is served**, with
  `Protected` naming it: the objects are there, at the other versions. `Track`
  refuses a GVK it can't map (§7.3), so a process that still names the old one
  doesn't start.
- **A switch that lists the other kinds** lets Disown settle for them, in both
  modes: the kind is then neither counted nor stripped (§7.4), so it settles
  with the finalizers its objects have left on them.
- **Or say it is gone**, once the objects are. *PerName:* vouch for each object
  (§10.6.6): Disown skips what it can't read of a vouched object, so it
  settles, and then drops the line. *LabelScoped:* no vouch applies; only
  the next way settles Disown with the switch at `all`.
- **And take the kind out of the tracker's `Protected`, and restart**, in this
  order: stop naming it, let the holders' passes drop it from their records,
  then restart without it. That settles Disown for either mode, and ends the
  batches, which live in the process's memory. While a record still names the
  object, each event of its holder queues a batch for it again, whatever
  `Protected` says, and that one fails as before. While the kind stays in
  `Protected` with no CRD, a restarted process refuses to start, since it can't
  map it (§7.3).
- **`DisownPrefix`** reads no more than the others: it strips what is served,
  and returns an error naming what isn't, on every run. With `Index`, it reads
  every line, and not the vouches: a vouched object of a removed kind is an
  error on every run too. Leave the kind out of `Protected` and `Names`, and
  its lines out of the index it is given.

**A holder kind that isn't served.** The rule is this package's. The
reconciler's own reads and writes of a holder, and `resource.IgnoreNotFound`,
use apimachinery's `IsNotFound`, true of every 404, and for a kind that isn't
served that loses no hold and no record. The write that removes the owner finalizer and the record (§8
steps 4 and 7) fails with nothing written, and is taken for done: the pass logs
that the object is deleted, and doesn't requeue. The read after A1 (§6.2
A1, L2), and the one that begins a pass, when they are live, end the pass as for
an object that is gone, without connecting. Either way the holder keeps the
finalizer and the record it had; R2's list of holders fails and its CRD exists,
since the holder does, so nothing is released meanwhile; and once the kind is
served again, the informer's relist or
resync reconciles the holder again, which finishes. What `Forget` drops in
memory, the holder's Missing refs, the next pass finds again. With several API
servers, which stop and start serving a kind at different times, the informer
may see no relist at all, and the resync, of 10 hours by default, is what finishes.
*A known gap:* the same write, refused with a 404 of a validator's own while the
kind is served, is taken for done too, so every pass logs that the holder was
deleted, with no backoff, while it stays. Its record and its holds stay with
it. Every consumer of the reconciler has it, with credential use or without, and
its fix is its own.

**The version a custom resource is written through.** Every call on the
**holder kind** goes through `Options.Holder`'s version, the one the holder's
own controller reads and writes: A1, R2's list and `Users`, the index's list of
the holders (§10.6.6), Disown's list and its strip of records, and the holder
informer. `Forget` makes no call: it changes the object that the runtime then
writes itself, in provider-runtime's reconciler with the typed client, whose
version is the one it passes to `Track`. So A1 loses nothing that the
controller's own writes don't lose. An empty version is the one `Track` maps the
kind at.

A **protected kind** is addressed by its group and kind, at the **mapper's
version**, the one the RESTMapper answers with for the kind alone: A2, R1, R3,
the gate, the prober, the janitor, Disown's strips of the objects, and
`DisownPrefix`. (In the calls of a tracker whose holder kind protects itself,
that kind is a holder kind.) All of these calls read and write metadata only.

The mapper's version is the one the API server prefers, or not.
controller-runtime's lazy mapper fetches only the version it is asked for, of a
group that its first discovery didn't have, and then answers with that one:
`Track` asks for the declared version, so in such a process the mapper's version
is the declared one (controller-runtime v0.25.2
`pkg/client/apiutil/restmapper.go:160-215`). Two processes can then use two
versions of one kind. Nothing here needs them to agree, if the conversions keep
the metadata they write.

A patch of metadata can still change the rest of the object. The API server
decodes the stored object at the request's version, and encodes it back at the
storage version, and at each it prunes the fields that version's schema lacks
(apiextensions-apiserver v0.37.2 `pkg/apiserver/customresource_handler.go:874`,
`:1272-1380`). And after a conversion it puts back all of an object's metadata
but the labels and the annotations, which are as the webhook answers
(`pkg/apiserver/conversion/webhook_converter.go:407-448`). So **the conversions
of a protected kind must keep every field, and every label and annotation**:
either every version's schema has every field, or a conversion webhook keeps what
a version lacks. With the conversion strategy `None`, that means equal schemas.

- **A field a version lacks** is dropped from the object by A2 and R3, though its
  writer never used the version we patch through. This is true of **any client's
  write** through such a version, a `kubectl label` included. The CRD types say
  that `None` "only change the apiVersion and would not touch any other field",
  and that a version's schema is the one used for its "validation, pruning, and
  defaulting" (apiextensions-apiserver v0.37.2
  `pkg/apis/apiextensions/v1/types.go:78`, `:190`). They don't say that the
  schemas must then be equal: that follows from the two, and we read and tested
  it (P21, envtest 1.37.0 and 1.36.2, `TestPremisePatchThroughAnotherVersion`
  and `TestPremisePatchAfterTheStorageVersionMoved`).
- **A webhook that builds its own ObjectMeta**, instead of copying the object's,
  keeps our finalizer, and drops the epoch, the marker and the held label
  (`TestPremiseConversionThatDropsMetadata`). Where the mapper's version isn't
  the storage version, **the finalizer is then never released**: R1 reads it
  without its epoch and restores that, the write drops it again, and a second
  later R1 is back, for good, with one patch each time. If the object is deleted,
  it stays, and so does its namespace
  (`TestAnEpochTheConversionDropsIsNeverReleased`). A2, R1 and R3 have always
  worked so.
  - *For the operator:* mend the webhook; the loop then releases at its next
    look. Or switch Disown on: its strip tests the object's resourceVersion, not
    the epoch. Where the label is dropped too, a LabelScoped kind's list at the
    mapper's version doesn't find the object, and Disown neither strips nor
    counts it: remove the finalizer by hand.
- **On the holder kind**, the conversions must keep the record, an annotation, **at
  every version that any process of the kind writes or lists it at**: each process
  writes and lists at its own declared one. Where one drops annotations, A1's
  record isn't stored (a declared version that isn't the storage one), or another
  process's list, at its own version, shows the holder without it. Either way that
  release loop finds no record that names the refs, and releases, while the pass
  went on to A2 and reported them held. The protection is undone, silently
  (`TestARecordAConversionDrops`). Such a webhook drops every other annotation of
  the holder, too, at each write through that version.

**Limits, of a conversion and of a mutating webhook or policy alike.** The API
server accepts a write, and stores it as they leave it; the package doesn't
compare the answer with what it wrote. So, whatever the kind:

- *A record, or an A2's finalizer, that isn't stored* leaves the ref unprotected,
  and the pass reports it held (above).
- *An epoch that isn't stored* leaves a hold that is never released (above).
- *The held label, if it isn't stored, of a LabelScoped kind* (a PerName watch sees
  the object without it): no informer sees P. Its passes write A2 each time; P deleted with live users isn't released when its
  grace ends, until an event of a holder queues its batch; a restart's rescan
  doesn't find it; and Disown, which counts by the marker label, can settle with
  the finalizer still there, where that label is dropped too.
- *A finalizer that a policy puts back* after R3, but not its epoch: R3's write
  queues the batch again, R1 restores the epoch, R3 removes the finalizer, and so
  on, as fast as the API server answers. The backoff bounds failures, and none of
  these fails.

We know of nothing that edits these keys. A comparison of each write with its
answer is designed, and not in this release.

| Conversion | Patched through | A field only the storage version has |
|---|---|---|
| None | the storage version | kept |
| None | another version | pruned |
| None, and the storage version has moved since the object was stored | any version | pruned |
| a webhook that round-trips | any version | kept |

For a protected kind, only the storage version loses nothing, until it moves, and
it is in the CRD, which the tracker may not read for a protected kind.

Of the holder kind's version:

- **Several processes of one kind, each at its own version**, as one controller
  per version (§3), share the finalizers and the records (§6.8). A1 is written by
  the process whose pass it is, at its version, which is its runtime's, so each
  controller's holders go through their own. R2 in any of them lists every
  holder of the kind, whatever its version, *at its own*: so every one of their
  versions must show the record and the finalizers (above). One `Shared` tracks a
  holder kind at one version.
- **Disown's strip is the exception.** Any process strips every holder's record,
  another version's too, through its own version. Where the versions don't
  round-trip, that drops fields, and no version is known to be the holder's.
- **A holder that can't be converted** to the list's version, by a webhook that
  fails, isn't left out of R2's list: the list fails as a whole, the batch is
  retried, and nothing is released (P22, `TestPremiseListWithAnUnconvertibleObject`).
- **A declared version that isn't served** is refused by `Track` (§7.3). One that
  stops being served is as a holder kind that isn't (above), though others are:
  A1 fails, so a new holder doesn't connect and an established one is Degraded;
  R2's list fails, which is never "no users", and the kind's CRD is there (§6.4),
  so holds are kept; Disown's count stays unknown. The process must restart with a version that is served, as its
  runtime must anyway.

**Only (a) is the Deleting outcome.** (b) to (e) are Acquire errors
under R4's rules: fail closed for a new holder, fail open (Degraded) for an
established one, at once. The brief retry for RBAC propagation in PerNamePoll
(§10.7.2) applies to (d) only. A 422 is never read as Deleting without the
`metadata.finalizers` cause, so (c) can't be mistaken for (a).

**JSON patches are classified apart.** R3's patch, Disown's strips and the
label janitor's patch (§10.1) are never classified with the table above. A
failed `test` op there is also a 422 Invalid, but the patch handler builds it
with `NewGenericServerResponse(422, …)` (`pkg/endpoints/handlers/patch.go:423-426`),
which drops json-patch's text. What remains is the generic message
"the server rejected our request due to an error in our request", and no
causes. A `remove` at an index that has moved looks the same. Either means
**the state changed**: the releaser goes back to R1, at most 4 times, and
at its fifth such patch requeues the object with backoff. Each retry rebuilds the
patch from R1's new read. A 422 with a policy's text
is (c) there too, and anything else is an error for the batch.

**A "state changed" that didn't change is an error** (L1 of the review of
revision 16). If R1's fresh GET returns the very resourceVersion the failed
patch was built from, nothing changed, so the 422 isn't contention: the
patch didn't match the state it was built from, which is a defect. The
releaser doesn't retry it in that batch. It logs it, records a Warning event on P
(`CredentialReleaseRejected`), counts `credentialuse_release_rejected_total`,
and requeues the batch with backoff. The same holds for Disown's strips and
the janitor's patch.

**What fails is retried, and only that.** A batch returns the objects it
couldn't decide, and those alone are requeued with backoff; the rest are done,
or looked at again at the time they asked for, as at a grace's end. So an
object that fails for good, as of a kind that isn't served, delays no other.
**The backoff is the object's**, per (K, N): 1s, doubling to 5 minutes at each
of its retries that fails, and forgotten when a batch decides it. It has one
pending retry: failing again before that, in a batch an event queued, neither
moves nor lengthens it. Objects that fail together, as often, share a timer, so
failures set no more timers than objects that fail. A time an object asks for
again, as each batch that finds it in its grace does, has one timer too. A
namespace's live list (§10.5) that fails for one kind is retried
so too, as a whole, deciding what it can list again, and doesn't hold up the
kinds that could be listed. An object that fails stays in memory, and is read
again every 5 minutes, for as long as it fails: it isn't dropped when no record
names it any more, since a hold may still be there. That has no bound, a name
of a kind that can't be mapped included, and neither has, while the write loops run,
what waits for its first read, when names are added faster than the turns below read
them. What each costs is bounded (below), not how many there are; that is left to a
later release. While they don't run, it is bounded ("Without the write loops",
below).
R0's keep is a decision as any other, so it ends a pending retry: its user's
event runs the batch again, that of one whose record can't be parsed included,
which queues all that the caches show as held for its namespace (§10.2), and so
every hold R0 keeps for it. What isn't held isn't queued: a PerName object whose
index prune failed, live and watched, and bare, keeps its line until the write
loops' next start, or the next batch that names it. A ghost of a kind whose CRD
is gone, which the cache keeps, had no such event: the CRD's deletion now runs
those batches (§10.2), and R0 doesn't take the cache's word then (§6.4). The protocol's
model decides one batch (`RequeueAt` is all it reads of a schedule), and has
none for what failed, what asked, or the backoff: those are the release loop's,
tested on the queue by enumeration and on the loop. A time a batch asks for, a
grace's end or a repaired epoch's recheck, is for the object that asked, each at
its own time, not for the rest of the batch, and waits for no backoff. An object
whose release must wait (R1, §6.4) is neither decided nor failed: it is queued
again when its wait ends, and its backoff stays as it was. What is logged of a
failed batch is one line for each turn (below) that fails at something, with how
many objects that is, the first few errors, and how many more there are, and a line
is about 2 KB at most: 500 names that all fail at once are 8 lines, each time they
are retried, and 450,000, 7,000. A name that isn't answered takes a turn's time, so
500 of them are up to 500 lines.

**A batch is a turn of its key.** Whoever can edit a holder's record decides how
many objects wait in its (K, N), and must not decide how long the other keys'
releases wait. So:

- **A turn reads 64 objects live at most**, at R1, retried or not. R0, below, which
  reads the caches alone, covers what the turn took from the queue: 2,048 at most,
  and a namespace's list (§10.5). An object whose release meets a change is read
  again in its own turn, past the 64 and its time, at most 4 times: put back, it
  would lose which resourceVersion its patch was built from.
- **What a turn doesn't get to goes back where it was**, before what was added since
  (and, past 1,024 that were put back, before what was put back before it), and the key
  behind the other keys. It is neither decided nor failed: its failures, its pending retry and
  what is counted for it stay as they were.
- **The queue is first in, first out**, and adding an object again doesn't move it.
  A key has two lines: of what is known to carry K's finalizer for N, as what the
  protected informers, the rescan and the same requeue as a holder kind's CRD goes, the
  rechecks, a namespace's list, the batch an A2
  asks for (§10.2) and a grace's end queue, a record's names that the caches show
  carrying it, and what a read of a batch showed, do; and of the rest. What is known
  stays so while it waits, is put back, fails, or is held up by a Track. A turn takes 1,024 of
  each at most, and reads the known ones first, with 16 of the 64 for the others,
  which may be real holds the caches don't show (§10.5, a per-name watch that
  hasn't synced, a label a third party stripped). The first of each sort is read, and
  its Namespace, in every turn. So an object waits for no more turns than objects were in its line
  before it, whatever is added since; one that is known to carry the finalizer waits
  for none of the rest; and nothing is moved by a record's being edited.
- **A turn's reads begin within 10s**: none of R1's begins after that, in its first
  round, but the first of each sort, and what wasn't begun goes back. **Each call that
  is sent must be answered in a time of its own**: R1's 10s, doubled at each failure of
  its object in a row, to 60s, the API server's own for a request; a Namespace's, read
  within the same 10s for them all, the longest of those of the objects that ask for
  it, and its failure is one for all of them; a page of R2's list, or of any live list,
  a minute, not the list. So an object whose reads are slow is released late, not
  never. The time starts when the call is sent, or a connection for it is asked for
  (`answered`), and again at each attempt that client-go makes by itself, as after a
  429: **what the client's own rate limiter makes it wait isn't limited**, before the
  first or, for 5 minutes, before another, and makes it late, as it always did; a body
  has those 5 minutes too. (But an attempt that ends in a reset gives no sign: what
  client-go waits before its next counts in the time of the one that was reset.) A deadline would be refused by that limiter at once,
  unsent, whenever its wait passes it (`TestPremiseAClientRefusesAReadItCantWaitFor`),
  so behind a standing backlog of calls nothing would ever be released; and a time that
  a turn's reads shared would leave R2 none. A transport that gives no sign of what is
  sent, as one that isn't built on net/http's, has no such time, as before.
- **How long one turn can hold a worker**, with nothing that is sent answered, beside
  the limiter's wait: R1 30s for names that are new, 130s if two that failed three times
  or more are among them; the Namespaces 10s and twice the longest of their objects'
  times; the list a minute a page, and before it a minute for each protected kind when
  the batch is a namespace's outside the caches; in PerName mode, the prune: each write of
  up to 50 lines, with its reads, in 15s and its check in 10s (§10.6.2), so 30s for a
  turn's 64; and in the write loops' recheck a minute for the index and for each line. An answer that
  begins and doesn't end, its first byte read, has 5 minutes more; and client-go makes a
  call that is answered 429 up to 10 times, each with its time and 5 minutes more, so a
  server that keeps answering so can keep one call for about an hour, where nothing
  bounded it before. **Behind a backlog B of calls in the limiter**, whose wait counts in R1's
  10s, a turn makes a read of each sort, a Namespace's, a page and a patch, B each: with
  B over 10s about 4 to 5 B of a worker for the release of one known object, and B for
  a made-up name, as each read of every name cost before. Not limited here, and not more than before: the
  mapper, which takes no context, and whose discovery client-go gives up after 32s (a
  turn's window lets one or two of them begin); the epoch's repair in R1, and R3's
  patches, at most 64 and a conflict's, each of which the API server answers or cuts; and a conflict's rounds, up to 4 more, of objects that were read.
- **Once the write loops' context ends**, nothing more is read, and what wasn't
  reached goes back for the next ones.
- **Each holder's record is parsed once a turn**, by R0 and R2 together, one that
  can't be parsed too, whatever the objects and the holders are. Nothing of it
  outlives the turn.
- **A kind that can't be mapped is looked for once a minute**, which costs a
  discovery: R1 remembers the miss, of 256 kinds at most, the oldest out, and each
  of its objects fails as it would have. So once its CRD is back, a release waits for
  the rest of that minute, and then for the retry after: up to a minute and one
  backoff later than without it. More kinds than 256, and a discovery that fails
  rather than says that there is no such kind, as of an aggregated API that is down,
  aren't remembered: each read of one is a discovery (§6.6). Acquire asks no such
  memory. A kind that is no
  longer in `Protected` is read as any other: only these batches release what is
  left of it.

**Without the write loops**, as in a process that waits for election, nothing drains
the queue, and the informers fill it, with every name that any record has had, and
every object that was held, since the process started. So until the loops run, **what
waits is bounded**: 65,536 names, more than 10,000 holders' of a few refs each; 32 MiB
of them, a name costing its text and 256 bytes, since one may be as long as a record
lets it, which is about what it takes; and 16,384 keys of all holder kinds, of some
0.6 KB each beside their names. Past the names or the
bytes, the key whose names take the most is **collapsed**: all that waits of it is
dropped, what is known to be held too, its namespace's list waits instead (§10.5), and
what it is added is dropped until the loops have made up for it. Past the keys, the
holder kind with the most of them is, as a whole. So a flood costs its own (K, N)
first, or its kind, but one with more honest names than it goes before it. When the
loops end, as when their leadership does, what waited for a retry or a time waits in
the queue, under the same bound, and its counts are forgotten. Not bounded by it: the
graces' timers, one for each deletion of a held object and each finalizer it has, for
the minute each lasts; and the release waits' timers (§6.4), below 2s, one for each
object that waits. When either fires, what it adds is under the bound.

At their start, and when a Track ends while they run, the loops make up for what
collapsed (decided by the user, 2026-11-24), after the rescan, §10.5's lists and the
index's recheck, which run whatever was dropped:

- the names that the records of the cached holders have now are queued again, of the
  collapsed namespaces, or of all of a collapsed kind's. They may be the flood's, if
  its records still have them: that many GETs, 64 to a turn;
- **every object of each LabelScoped protected kind is listed once**, in every
  namespace, with no selector, metadata only, 500 to a page, a page at a time in memory
  and a minute for each; what carries K's finalizer for one of those N, or any N of a
  collapsed kind, is queued, its labels stripped or not, the caches or their lists
  showing it or not. At 1 to 2 KB an object, 10,000 Secrets are 20 pages and 10 to 20 MB
  on the wire. It is made once for an overflow, however much that collapsed, and again
  by each Track that ends while the loops run, of a kind that did: of the kinds it
  protects, for what it has collapsed. One that fails is retried with the release loop's
  backoff, kind by kind, and write loops that end before all succeeded leave it to the
  next, whole: a kind that can never be listed keeps it owed, so each election lists the
  others again.

*What finds what, after an overflow:*

| A hold that nothing uses | Found by |
|---|---|
| in the caches | the rescan |
| outside the caches, with its label, in any namespace | §10.5's lists |
| LabelScoped, its labels stripped, in any namespace | the list above |
| PerName, its labels stripped or not | the index's recheck: a GET for each line. Such a kind isn't listed, which its RBAC would refuse (§13) |
| PerName, with no line in the index: gone (§10.6.6's residual), or never written, as of a hold made when the kind was LabelScoped | **nothing**, until something names it again. A restart doesn't find it either. While its labels are there, an operator who may list the kind finds it by its marker label; or `DisownPrefix` with its name (§10.6.6) |
| PerNamePoll (not built) | its index, the same way |
| of a kind that a record names and `Protected` no longer lists | the record, while a cached holder has it |
| of such a kind, that no record names any more | **nothing**, until something names it again |

*Who overflows at each start.* The bound holds until the loops run, which is after the
informers' first sync, with leader election or without; and the informers' first events
queue what is held and what the records name. So a process with **more than 65,536 held
objects and recorded names**, a Secret that two holder kinds name counting for each, or
more than 16,384 namespaces of holders, each counted for each of its holder kinds, makes
that one paged list at **each** start, before its leadership or not. Past the keys it is
the kind with the most that collapses, whichever made them too many. It costs no more than that: a page is
dropped once it is read, and the holds it finds are the rescan's.

Who else can make it happen is whoever can edit a holder's record, or a few of them: 15
records of 4,500 names, or 140 names of 250,000 characters. The list follows an
election, or a Track, not the edit, so they can't make it more often than that. An
authorizer that lets a kind be listed only with the label selector (RBAC can't) refuses
it: it is retried and logged, and what it would have found stays.

*Why this delays, and loses nothing.* Nothing of the rest is decided; what a turn
does decide, it decides on R1's and R2's reads, all of its own and in their order,
and on R3's tests, unchanged. R2's list is live; the text of a record alone says
what it names, so R0's parse of a text serves R2's. An object leaves the queue only
decided, failed, which has its retry, asked for at a time, waiting (§6.4), which
queues it again when its wait ends, or parked behind a Track of its kind, which puts
it back. By mode:
LabelScoped, every event and the rescan queue what they did. PerName, the same of
the per-name watches and the index's recheck; a line is pruned only for an object
that its turn read, live, without a finalizer of the kind, so one of the rest keeps
its line (§10.6.3). PerNamePoll (§10.7, not built): an A2's own batch is of the
known line, and nothing here needs a watch, which only orders. Outside the caches
(§10.5): the live lists' objects are of the known line; those only a record names
are of the other one, which no number of the former can starve.

**The releaser** works on one holder kind K and one namespace N at a time,
for the set of protected objects pending there (R7), a turn of them. For each
pending P:

- **R0. Fast path.** Ask the holder informer's index for users of P in N,
  leaving out the holders §6.1 excludes. Stop if either:
  - P is not being deleted, and a user exists. This settles the usual case,
    where one of several users leaves;
  - P is being deleted, and the cache shows a **deleting** user (L1). §5
    keeps P in every row while a user is deleting.

  A stale cache can only keep a hold longer, which is always safe. The
  release then runs on the event that shows the user gone.

  That rests on the cache catching up, and while K's CRD is gone it doesn't:
  the holder informer's every list is a 404, so a holder whose deletion it
  missed, as one whose connection was down does, stays in the cache as a ghost
  (the probes of 2026-11-24). So **while K's CRD is absent from its informer,
  which has synced, R0 settles nothing**, nor does R1's shortcut inside the
  grace: the batch goes on to R1 and R2, which are live, and the GET of the CRD
  decides (§6.4). The CRD informer is a hint, as every cache is: a stale one
  costs live reads, or keeps a hold.

  **The teardown cost**, for a namespace with M held Secrets and H holders
  deleting:
  - *before*, every holder event re-ran the whole batch: up to H × (M GETs,
    one Namespace GET and one list);
  - *after*, those batches stop at R0, and one full batch (M GETs, and one
    Namespace GET and one list for each 64 of them) runs once the cache shows
    the last deleting user gone.
- **R1. Get P live.** Note its resourceVersion, the index *i* of the
  finalizer in `metadata.finalizers`, the epoch *e*, and its
  `deletionTimestamp`.
  - If the finalizer is absent, P is done.
  - If the epoch is absent (only a third party could cause that), set a
    fresh one and requeue.
  - If P is being deleted in a namespace that was live when last seen, has
    users by R0, and is still inside the grace, requeue the batch at the
    end of the grace (§5).
  - Otherwise, if P is being deleted, and its namespace was live when last seen,
    **wait a random time below 2s** first (§6.4, H1): P leaves the batch, which
    goes on with the others, and is queued again when its wait ends; R1 then reads
    it again. The wait is the object's own, of that incarnation, begins once, and
    is forgotten when a batch decides P: a retry doesn't wait again, and a P that
    was kept does, at its next release. It holds no worker, moves neither a grace's end nor a retry, and is
    dropped when the write loops stop. Nothing that isn't being deleted waits.
- **R2. The last reads, after every R1 in the batch.** First read the
  clock: whether each P's grace is over is decided on that reading, taken
  before any of R2's reads (revision 17, §6.4). For each P being
  deleted, GET its Namespace live and read its `deletionTimestamp`. Then
  make **one** live, paginated, metadata-only list of K in N
  (`PartialObjectMetadataList`, no resourceVersion, so it is a consistent
  read), with no label or version selector. For each P, compute its users
  and its deleting users (§6.1), and apply §5's table. If the list fails
  because K isn't served, one live GET of K's CRD tells whether K is gone,
  and its holders with it: the list is then empty (§6.4, "The holder kind
  itself gone"). Any other failure keeps every hold, and the batch is
  retried.
- **R3. Release** each P the table says to release, with one JSON patch:

  ```json
  [{"op":"test",   "path":"/metadata/uid",                     "value":"<P's uid>"},
   {"op":"test",   "path":"/metadata/annotations/<F escaped>", "value":"<e>"},
   {"op":"test",   "path":"/metadata/finalizers/<i>",          "value":"<F>"},
   {"op":"test",   "path":"/metadata/finalizers",              "value":[<the array R1 read>]},
   {"op":"remove", "path":"/metadata/finalizers/<i>"},
   {"op":"remove", "path":"/metadata/annotations/<F escaped>"}]
  ```

  `<F escaped>` is the finalizer name with `/` written as `~1` (RFC 6901).
  Two cases add to the patch:
  - when F is K's last finalizer on P (no other namespace's), the patch
    also removes K's marker label;
  - when F is the last `credential-use.` finalizer of any kind, it also
    removes the held label.

  **The uid and the whole array are always tested** (L5 and L1 of the review
  of #53, the latter the user's decision). An earlier revision tested the
  array only when a label went. The uid test keeps a release built from one
  incarnation off another with the same name and keys. The array test means
  no two releasers can apply from one read: two releasers of K, for
  namespaces a and b, that both read `[Fa, Fb]` are neither last, so without
  it both would apply and leave the labels on an object nothing holds. With
  it, the second fails, re-reads `[Fb]`, is last, and removes them. The cost
  is a retry whenever the array changed since R1, for any reason. Finalizer
  arrays change rarely, and the retries are bounded (above).

  **Every value the patch removes is tested first** (L1 of the review of
  revision 16): the epoch, the finalizer at *i*, and each label it removes,
  with its value as R1 read it. A label that is already absent isn't
  removed, so that its remove can't fail.

  If a test fails, the API server rejects the whole patch, with the 422
  described under "JSON patches are classified apart" above. The exact
  status is pinned by a test, and is treated as a conflict: P goes back to R1.
  No other writer's change can be clobbered: the patch removes one index it
  has just tested, and keys only this kind writes. When R3 succeeds, this
  process drops its `written` entry for P and N at once, synchronously.

### 6.3 The verified set

A2 may skip the write only for pairs this process has written (U6), and
never on a deletion pass. The tracker keeps four maps in memory:

- `written[(P.uid, K, N)] = t_issue`: the local monotonic time at which this
  process issued a successful A2 write for P, K and N.
- `delivered[H.uid]`: the resourceVersion of the version of H that the
  holder informer delivered last (revision 17).
- `recordSince[(H.uid, P)]`: the **informer delivery time** of the first
  holder event, in the current unbroken run of H's versions whose record
  names P. A delivered version whose record doesn't name P ends the run and
  deletes the entry, so a later re-add starts a new run with a new time.
- `observed[(H.uid, P)]`, written by passes, **synchronously**. When a
  pass's A1 adds P, it sets `observed` to the time A1's response returned,
  **overwriting** any earlier value (M1). When A1 removes P, it deletes the
  entry. That ref can't skip in the pass whose A1 added it. A delivered
  version that ends P's run deletes it too (revision 17), so a later run
  can't lean on an A1 of an earlier one.

The holder informer's handler maintains `delivered` and `recordSince`, and
deletes `observed` entries; it touches nothing else.

t_obs for (H, P) is **the later of** `observed` and `recordSince` (and
the last list, §6.5 L2). Without `recordSince`, because the informer hasn't
delivered a version naming P, the pass doesn't skip: an A1 alone never lets
it (revision 17). Taking the later time can only make skips rarer. `written` is keyed by P, not by holder, so A1 has no
per-holder `written` entry to reset. The comparison with t_obs is what makes
a re-added ref write again.

**A2 may skip for (H, P) only if** all of these hold:

- the pass is not a deletion pass;
- this pass's A1 didn't add P;
- the pass works from H at resourceVersion *rv*, `delivered[H.uid]` is
  exactly *rv*, and that version's record names P (revision 17);
- `written[(P.uid, K, N)]` exists, with `t_obs < t_issue`. That is, this
  process wrote P after the informer had delivered H's record naming P.

**Why the exact version** (H1 of the review of the protocol's code,
revision 17). The maps describe the versions the informer has delivered.
A pass that works from any other version, newer or older, may be looking at
another run of the record. Revision 16 let it skip anyway, which three
interleavings break, each reproduced by a test:

- in one process, A1 removes P; before the informer delivers that version,
  another process releases P; A1 puts P back and its response is lost; the
  next pass, which runs no A1, skips on the first run's `recordSince` and
  reports P held while P has no finalizer, and a teardown then wedges;
- across two processes, B removes P and puts it back, and B's A2 fails; A's
  informer has delivered the removal, or nothing since the first run, and
  A's surviving `observed` or `recordSince` lets A skip, so A's runtime can
  reach `AddFinalizer` with no A2 behind the record (§6.7);
- the manager's typed cache, from which a runtime's pass reads H, can run
  ahead of the core's holder informer, and so give the pass a version of a
  later run.

With equality, the delivered versions up to *rv* are all the informer has
seen, in order, so the run containing *rv* began at a version it delivered,
or at a list, and `t_obs` is no earlier than that version's delivery
(§6.4). A pass whose view of H differs from the informer's writes A2. That
costs a write, never safety: after an A1 the pass works from A1's response,
which the informer hasn't delivered yet, but A1 writes only when the record
changes; and the typed cache and the core's informer agree once both have
caught up.

**After a restart or a leader change**, all four maps are empty. The
holder informer's initial list delivers every holder at about the time it
syncs, t_sync, and that becomes their `recordSince`. The first pass of the
first holder of P writes P at some t_issue > t_sync. Every holder whose
record was in that initial list has `recordSince` = t_sync < t_issue, so it
skips. **So a restart costs one A2 per (P, K, N)**, as claimed, under this
definition. The exceptions are holders whose record changes after the
sync, passes that work from a version the informer hasn't delivered yet,
and every deletion pass: they write again.

Entries are dropped:

- **in the pass, synchronously:** by A1, for refs it removes; by a
  successful R3, for the P and N it released;
- **in the holder informer's handler:** `observed`, when a delivered
  version of H ends P's run (revision 17);
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
  tW > tA, so it sees H. R3 also tests the uid and the whole finalizer array
  as R1 read it (§6.2), so any other write to the array in between breaks
  it too, and so does a recreate behind the same name.
- **tW > tR.** The finalizer was gone before A2. A2 adds it back (held), or
  is rejected because P is being deleted (Deleting), and the acquirer
  knows which.

**A holder that starts deleting during a release.** Say H's
`deletionTimestamp` lands at tD_H.

- If tD_H < tL, the list sees H deleting, so P is kept.
- If tD_H > tL, H's deletion pass runs A2 without a skip. If that write
  lands before tR, R3 fails and the retry's list sees H deleting.
- If it lands after tR, the release won. This needs tD_H between what the
  list saw and the patch (a few milliseconds for a batch of one, about M
  round trips for the last of M), and after the grace. A2 then reports
  Deleting, H proceeds Degraded, and H fails until P is recreated. Its
  namespace is live, so this is recoverable, and it is part of §5.2's
  residual.

The namespace's deletion is handled the same way, with an earlier read. R2
GETs each Namespace live, after the grace, and **then** makes the list. So
the list, with all its pages, and the batch's other Namespaces may come
between a Namespace's read and R3. A namespace deletion that starts after
that read deletes the holders, and their deletion passes write, which breaks
an R3 that hasn't landed yet.

**The grace is measured before R2's reads** (M4 of the review of the
protocol's code, revision 17). R2 reads the clock first, and decides
whether the grace is over on that reading. So the Namespace GET comes after
the grace's end, and a namespace deletion it doesn't see began after the
grace too. Revision 16's code read the clock after the list: a namespace
deletion that began inside the grace, after the GET, was then released over
as if it had begun after the grace. A test reproduces it, and the model
checks every release against it.

**That leaves an unrecoverable window: the rest of R2's reads, usually below
100ms and not bounded by that, and about M round trips for M R3 patches in a
row** (L3), at most 64 since a turn's are (§6.2). Say
the namespace's deletion begins between R2's Namespace GET and R3's patch,
after the grace, while no user is deleting. R3 then lands first, because
the namespace controller takes far longer than that to discover and delete
the holders. P goes, the namespace is terminating so P can't be recreated,
and the holders wedge exactly as in §1. It needs a Secret deleted in a live
namespace, then the grace running out with no user deleting, then the
namespace deleted inside that window. Nothing in this design closes
it. It is stated here, and tested in the model, rather than claimed
away.

*Why the list takes that long, by the source.* The API server answers a consistent
list from its watch cache, once the cache has reached the revision that etcd is at.
If something in etcd was written since the cache's last event, of whatever kind, and
the holder kind isn't being written, the cache learns that it is there from a progress
notification. The API server asks etcd for one at most once in 100ms, while a read
waits (apiserver v0.37.2 and v0.36.2, `storage/cacher/progress/watch_progress.go:34`
and `:77-108`). So a list made within 100ms of such a request waits for the next one,
and a list after a quieter time asks at once. The requests are each holder kind's own,
on each API server, as far as the API server's side shows: each resource has an etcd
client of its own. **100ms isn't a bound:** a cache that can't catch up makes the list
wait 3s (`blockTimeout`, `storage/cacher/watch_cache.go`), and the API server then
reads etcd itself (v0.37.2 `storage/cacher/delegator.go`; behind an alpha gate that is
off by default, it answers a 429). The longest list seen was 108ms, in the freshness
runs below.

Measured on envtest 1.37.0 (4 CPUs, one API server), for batches of one, from the start
of the Namespace's read to the start of R3. In every row a creator tries each Secret
again each 1ms, which loads the API server:

| | R3s | p50 | p99 | longest |
|---|---|---|---|---|
| one release alone | 250 first R3s | 3.0ms | 4.6ms | 14.7ms |
| 20 at once, each of a kind of its own | 1,000 first | 3.6ms | 13.1ms | 17.7ms |
| | 9 later | 96.6ms | | 100.3ms |
| 40 at once of one kind, written each 10ms | 1,000 first | 4.2ms | 26.4ms | 49.4ms |
| | 8 later | 4.7ms | | 28.4ms |
| 40 at once of one kind, a wait below 10s, not 2s | 1,000 first | 4.8ms | 96.4ms | 104.8ms |
| | 4 later | 85.0ms | | 98.2ms |

The later ones are the R3s of an object after its first. Of the 21, 12 overlapped the
first: in 7 of those pairs one R3 got a NotFound, and in 5 both passed. One that fails
opens no window. "At once" is graces that end together. A kind
that is written often makes a list wait less, not seldom: at most 255 of the 1,008
lists of the written-each-10ms row waited, and at most 1,391 of 2,000 in the freshness
run at 10ms, none over 25ms. (The counter has other reads of that cache, too.)

With the 2s wait and a kind that nothing else writes, only R1 to R3 was measured,
which is one read more: p50 69ms and 67ms (959 R3s that didn't overlap, and 86 that
did), longest 115ms, at a poll of 1ms; p50 81ms, longest 105ms, at 200ms. For larger M
it is about M round trips, and then the batch's own reads are R1's, too: in batches of
the 200 in one namespace, R1 to R3 was p50 443ms, longest 739ms, with the wait, and
p50 0.94s, longest 1.55s, without it (the p50s of the R3s that didn't overlap; of the
others 477ms and 1.36s).

*What the list saw is not that old (by the source).* It answers with the cache's
latest state when its wait ends, so the windows that begin at what the list saw
are no wider for it. An A2 while the list waits breaks R3 (the second case above),
which costs a retry. `TestAListIsNotBehindAnAnsweredWrite`, on request, checks two
sides of it. A list isn't behind a write of its kind that was answered *before it
began*: 0 of 8,000, of which the 2,000 with a write each 10ms are the strong case; the
others begin, mostly, long after a write. And a list shows most of what was answered
*while it went on*: of 1,983 pairs of a list and a write answered during it, 2 lists of
8,000 ended without one that was answered by then, both at 10ms, the write answered
below 0.1ms before the end. It is one API server and one etcd; for several, this rests
on each list asking etcd for its revision first.

**What the cases above assume, and the API server doesn't do (H1, H3).** They
take the removal of an object for one step. It is two: the API server tests a
request against the object, and then removes what carries that name, with no
test (apiserver v0.37.2 `pkg/registry/generic/registry/store.go`: `:847` and
`:869-870`, then `:658-680`, for a patch that empties the finalizers;
`:1210-1279` for a user's DELETE). In between:

- **a lost A2** (H1): an R3 that empties the finalizers of an object being deleted
  passes its tests, an A2 lands with a fresh epoch and succeeds, and the object
  goes. The holder thinks for one pass that it holds P.
- **the recreate** (H1): the R3s of two processes, from one incarnation, both pass
  their tests; the first removes it, the object is created again, and the second
  removes the *new* one, whatever its finalizers, though nobody deleted it.
- **a lost first hold** (H3): a user's DELETE finds no finalizer, a first A2 lands
  and succeeds, and the object goes, never marked as being deleted. It is the same
  as a DELETE an instant earlier.

`TestPremiseARemovalIsTwoSteps` pins it (P23), on 1.37.0 and 1.36.2: it fails if
Kubernetes comes to remove in one step, and then this passage is out of date. The
models' API server does it in one, so they prove nothing about these.

*How wide:* measured on envtest 1.37.0 (n2-standard-4, 2,000 races each), 1.4 to
1.9ms for an R3 and 0.8 to 1.2ms for a DELETE when idle, up to 2.3ms with 4 CPU
burners, each ±0.2 to 0.5. Every control, whose object has another finalizer, so
that nothing is removed, lost 0 of 500 races. A real cluster's isn't known; every
rate scales with it.

*How often:* a lost A2 needs an A2 in that window of a release of an object being
deleted. In a stress test that writes all the time it was 25 of 10,625 such
releases (0.24%), and no violation; at one A2 a minute it would be 1 in 40,000, an
estimate. The recreate needs two processes to release at once, and something that
creates the object again at once. The stress test's plain form met it, before the
wait, in 5 of 207 iterations (2.4%, 95% 0.8 to 5.6; 4 CPUs; each read by hand); an
iteration is a run of many releases. An earlier form met it in none of 10,600 releases
(per release: the two aren't comparable). That is to be measured again, with the
wait. Both need P deleted in a live namespace, where it can be created again, so they
are recoverable; but a pass whose Connect reads a cache may establish a holder on a
Secret that is gone.

*What is done about it:* **the recreate is made rarer, by a random wait, and the rest
is accepted** (decided 2026-11-24; and, after the measurements below, to keep the
wait as built, below 2s). How, and what it does, is ours. Two processes learn
of a holder that went, or of a grace's end, at one moment, and each then released at
once: their R3s overlapped in 16 to 24% of the stress test's releases. Now each waits a
random time of its own, below 2s, before the R1 of an object being deleted (§6.2 R1).
The second process mostly finds P gone, or the new one without the finalizer, and makes
no R3.

*What it does, measured:* the releases in which two processes' R3s of one object
overlapped: their spans at the clients, from before each call to after its answer, in
one test binary. Each is a Secret being deleted with a live user, so the graces of one
round end about together, and something tries to create it again, at the poll given.
Envtest 1.37.0, 4 CPUs, one API server; an R3 took 3 to 7ms on average.
`TestReleaseWaitSpreadsTwoProcesses`, on request; the rows are of several versions of
it.

| Shape of a round | Poll | Without the wait | With it, below 2s |
|---|---|---|---|
| 40 Secrets, each in a namespace of its own, deleted at once | 1ms | 256 of 600 (42.7%) | 43 of 1,000 (4.3%) |
| the same | 20ms | 232 of 600 (38.7%) | 31 of 600 (5.2%) |
| the same | 200ms | | 44 of 1,000 (4.4%) |
| the same, each deleted as it is made | 1ms | 996 of 1,000 | 38 of 1,000 (3.8%) |
| 200 Secrets in one namespace, in two runs | 20ms | 109 and 101 of 3,000 (3.5%) | 14 and 11 of 3,000 (0.42%) |
| 40 Secrets at once, the holder kind written each 10ms | 1ms | | 5 of 1,000 (0.50%) |
| 20 Secrets at once, each of a holder kind of its own | 1ms | | 4 of 1,000 (0.40%) |
| one Secret | 1ms | | 0 of 250 (95%: below 1.5%) |

A wait below 10s, for the first row's shape: 3 of 1,000 (0.30%, 95% 0.06 to 0.87).
Where both were run, the wait makes them 7 to 26 times rarer, by the shape. The stress
test hasn't been run with it.

- **One release alone** overlaps with probability about 2w/L, for an R3 of w and waits
  below L: 0.29 to 0.63% for the R3s with the wait here. 0.4% was measured for kinds of
  their own, where 2w/L was 0.39%.
- **Many releases of one quiet kind at once overlap more,** 3.8 to 5.2%, while an R3 was
  4 to 6ms on average. R2's lists that wait for the same progress notification (above)
  are answered together, and the R3s behind them begin together, whatever an R3 takes.
  Two processes whose lists are made in the same 100ms meet so, which is about 100ms/L,
  an estimate. In the overlaps at a poll of 1ms the two waits ended p50 40ms apart, and
  the two R3s began p50 0.2ms apart. Waits that end further apart can meet as well, when
  workers are busy: in 5 of 43 pairs they ended 100 to 221ms apart (10 of 44 at a poll of
  200ms, to 307ms).
- It isn't so for processes whose lists go to different API servers, which have caches
  of their own. That is from the source, not measured.
- **What it costs:** such an object goes 1s later on average for the wait alone, and
  later where a key or the workers are busy (§6.6).

*The recreate, estimated,* not measured: an overlap, times the chance that a try to
create lands **between the two removals**. Before the first it gets AlreadyExists, and
after the second the new object stands. The time between the two isn't measured. The
second R3 must have passed its tests before the first removal, so it is at most about
the second's own test-to-removal window, 1.5ms on average when idle, which makes these
roughly ceilings. At a try each second, with 40 at once of a quiet kind: 1 in 670 to 1,700 releases without the
wait, by the shape, and 1 in 13,000 to 17,500 with it; for a kind's only release, at
0.4%, 1 in 170,000. A try each 100ms makes them 10 times likelier. **The data says that
they are too high, by about 9 times if a try takes 5ms** (the poll and an answer; not
measured): at a poll of 1ms 46 recreated Secrets went in 1,333 overlaps of the
40-Secret shape (12 in the 256, 3 in the 43, 31 in the 996, 0 in the 38), or 3.5%,
which is 0.17ms between the removals, not 1.5ms. It is of the order of the 0.2ms by
which the R3s began apart in the 43 overlaps where that was measured, 3 of which lost a
recreated Secret. Not each overlap can: of the 87 overlaps with the wait of the
40-Secret shape of a quiet kind, at polls of 1ms and 200ms, both R3s passed in 76; in the
other shapes with the wait below 2s in 5 of 23. At polls of 20ms and 200ms 1 went in 307, of the 40-Secret shape, and none
in the 200-Secret one; the shuffled runs aren't counted.

- The wait is each object's own, by its uid. One for a whole batch would let two
  processes whose batches overlap meet on all of its objects, and one for a name would
  let the next object of the name go without.
- It is once for each decision, so an object may wait twice or more: one that a batch
  keeps waits again before it is released. A holder cache that is behind can be why:
  R0 keeps P for a user that is gone, or R1 waits where the cache doesn't show the user
  that R2's list does.
- **What has no wait.** An object that isn't being deleted at R1, and is by the time
  R3 lands, is removed by it. One in a namespace that the cache shows being deleted,
  and that has gone and been made again live since: R1 goes by the cache, and R2, which
  reads it live, may release at once. And Disown's strips, which are R3's patches: two
  processes' strips of an object being deleted can remove the next one as two R3s
  can. It is rarer, since each process has its own interval, and switched on by
  hand.
- It is no proof: it leaves the window as it is, and does nothing for a lost A2 or
  a lost first hold. A mark on the object before its removal would close the lost
  A2 and the recreate; it was rejected for now, at one more write per release
  (§15). The condition it was to be taken under, a notable share of lost A2s, isn't
  met.
- With leader election only a timed-out R3 and its own retry can overlap, and at a
  change of leader an R3 that the old one left in flight and the new one's.

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

**The skip is safe** for the same reason as the first case. The pass
works from the version *rv* the informer delivered last (§6.3). The run of
versions naming P that contains *rv* began with the A1 that landed at tA;
the informer delivered every version of that run up to *rv*, or listed
after its start, so `t_obs` is no earlier than the delivery of its first
version, or than the list. If `t_obs < t_issue`, then
tA < t_obs < t_issue < tW, because the informer delivered the record after
the API server had stored it. And the record has named P since tA, without
a break: a break would have been a version of H that the informer delivered
before *rv*, ending the run. So every releaser that read P before tW fails
its test, and every releaser that reads after tW sees H, **as of *rv***. The
proof says nothing about versions after *rv*: the informer hasn't delivered
them, and the pass doesn't see them.

**The equal-lag stale pass: an accepted window** (the re-review of #59). Say
a process's typed cache and its core informer are equally behind, both at
version V of H, which names P, under a written entry of the process's own.
Meanwhile another process removes P from H's record, a releaser releases P,
which then has no user, and the other process puts P back but its A2
fails. A pass of the first process works from V, which its informer
delivered, so it skips, and reports P held while P has no finalizer. The
pass is stale, but consistent with what its process has seen. Delivery of
the later versions ends the *permission* to skip, but P stays unheld until
a pass of that holder actually runs from a current version and writes:
- B's re-add is a record-only update, which the predicate filters out (§7.5);
- B is Degraded and doesn't requeue;
- a pass that P's label loss triggers can skip too, while it works from V.

So the window is bounded by the informer's lag **plus the time to the
holder's next pass**, at worst the poll interval. It is the same accepted
fail-open as B's failed A2, which it follows from: the holder runs on a P
that a later pass will hold again, and the damage needs P deleted inside the
window. A test pins both the window and the write of the pass that closes
it.

**The holder kind itself gone** (the user's decision, 2026-11-24, after
probes found the wedge). A CRD's deletion removes its custom resources first,
so when K's CRD is gone, so are K's holders, and no hold of K has a user. But
R2 can't list a kind that isn't served: its list is a 404 for as long as that
lasts. A hold that outlived its holder, as when the process was a standby then,
would stay, and its Secret, or its namespace, with it, until someone ran
`DisownPrefix` (§7.4). So when R2's list fails with a 404 status, or the mapper
finds no match for K, the releaser makes **one live GET of K's CRD**, by the
name its tracker was given, with no resourceVersion:

- **The API server's answer that it doesn't exist**, a 404 Status that names
  the CRD, and no veto (below): the list is empty. R3 then runs as ever, with
  its tests.
- **The CRD, one being deleted included, or any error**, a 403 or a 404 of any
  other form among them: every hold stays, and the batch is retried as after
  any failed list. A 403's error says what to grant. A CRD that is there says
  what to do if it lasts, when the kind is served no more as `Track` mapped it:
  after the CRD was created again with another scope, restart the process,
  which maps it anew; if the declared version is no longer served, restart it
  with one that is, since `Track` refuses a version that isn't (§6.2, §7.3).

*The list's 404 is never the evidence.* An API server answers it for a K that
exists while its CRD informer lags, or the version isn't served, or the scope
differs, or the names aren't accepted yet
(`apiextensions-apiserver@v0.37.2/pkg/apiserver/customresource_handler.go:257-305`);
the GET finds the CRD then. *The GET is*: without a resourceVersion it is
served from etcd, not from the watch cache
(`k8s.io/apiserver@v0.37.2/pkg/storage/cacher/delegator.go:110-114`), so it is
consistent whichever API server answers, and whether or not it lags.

*The proof above holds with the GET's time tC in place of the list's tL.* It
uses of the list that it comes after every R1, and that it shows every holder
that exists then; the GET comes after it, and shows there is none. So an A2
that landed before tG is of a holder that is gone by tC, and no user; one that
lands between tG and tR breaks R3, and the next round asks again; one after tR
adds the finalizer back. **A CRD created again** between the GET and R3 is
these same cases: its new holders are those a list just before them misses too.
So is one created again with another scope (§6.2). A process that mapped the old
one cluster-scoped lists the new one whole, and filters: its list succeeds, and
the CRD isn't asked (`TestAHolderKindMappedClusterScopedAndItsCRD`). One that
mapped it namespaced lists in a namespace, which the API server answers with a
404 while the new CRD is there: the GET finds it, and the hold stays
(`TestABatchWhoseHolderListFails`). A skip rests on the same two (below). It never matters which process did what,
so two processes are no different (§6.8). The model searches the part that is
new (§14): a releaser whose list is a 404 whether or not the CRD is there,
against the CRD's return and a new holder of it, from a state in which the CRD
is gone already. Its controls, the GET moved before R1 and no GET at all, it
must find unsafe. It doesn't search a pass in flight while the CRD goes, a
skip, two releasers, the caches, or the veto below, which only keeps holds: for
those there is the proof above.

***This CRD* is gone isn't *K* is gone.** The finalizer is a GroupKind's, and a
CRD is a plural's. Say the CRD `widgets` is replaced by `widgetries`, of the
same Kind, while a process O tracks the first. A new process N tracks the
second, and its holders hold P with the same finalizer. O finds its list and
its CRD gone, and without more it would strip each hold N's holders put back,
in a loop, for as long as it runs; and release P for good if P's namespace is
being deleted. So the GET's "gone" has **a veto, the API server's discovery**
of K's group, read live after the GET, in each round: `/apis/<group>`, then
each of its versions. Every hold stays, and the batch is retried, if any
resource there has Kind K (whatever its case, since the finalizer has it in
lower case), or if any of those documents can't be read. A 404 of the group's
says that it isn't served. Discovery isn't a consistent read, so it is never
the evidence: the GET is.

- *Not the mapper.* controller-runtime's doesn't look again at a kind it knows
  (§6.2); one that did would have given the list the new resource, if that serves
  the declared version. A mapper that finds K under another resource, at any
  version, is a veto too, and costs no request.
- *The group's own document, not the list of groups:* just after a group's
  **first** CRD is established the API server serves its objects and this
  document, through the CRD's own handlers, before it has the group's
  APIService, which the list is made of (kube-aggregator v1.36.4,
  `handler_apis.go`; seen through a summary of the source). A group that has an
  APIService is the aggregator's in both, so a **version** that comes later to
  it isn't there before its own APIService: a group whose kinds come with
  versions of their own has that gap, also on one and the same API server. N's
  mapper may find the kind sooner, in the aggregated discovery, which the CRD's
  own handlers feed without an APIService. The gap ends when the version's
  APIService is registered: the third limit below.
- *An aggregated API server* in K's group is asked through the API server: one
  that serves K vetoes, and one that is down answers 503, which keeps the holds
  for as long as it lasts (the aggregator lists a group's versions whether or not
  they are available). A partial failure of discovery elsewhere doesn't matter:
  no other group is read. One that hangs costs a release worker 10s at most for
  each round (`discoveryTimeout`).
- *The cost* is one GET and one for each of the group's versions, one after
  another, for a round that found the CRD gone, and nothing otherwise. It is per
  turn (§6.2) and per round of it that reaches R2, so at least per turn of 64
  objects of a key (K, N), and a key takes ceil(n/64) turns for n objects. While
  the CRD is absent R0 keeps nothing, so every held object goes through R1 and
  its turn. Objects of one key that are at different backoffs are retried in
  batches of their own, each with its reads. With 30 versions (32 GETs a turn:
  the CRD, the group, and its 30 versions) and holders in 50 namespaces that
  hold 1,000 objects each, one CRD's deletion is 50 x 16 turns x 32 = about
  25,600 GETs, and more for keys whose objects had failed at different times.
  The CRD's going queues every hold the caches show (§10.2), so a flood of them
  multiplies these figures. A client with a rate limit may not finish a
  large group within the 10s; the turn is then retried. And a discovery that
  hangs holds one of the two release workers for 10s for each turn of a key of K, so that
  every kind's releases wait. The answer isn't shared between batches: that
  would be state in a path that decides a release, to save reads on a rare
  event.
- *Just after the CRD goes*, discovery may still show it, or a version it no
  longer serves may be a 404: every object of the round that reached R2 is
  retried, 1s and then 2s later for one that hadn't failed before. One that had,
  its CRD being there, is at its own backoff, of up to 5 minutes; the CRD's
  deletion runs what the caches show, and asks for the recheck of what they
  don't, at once (§10.2). The error says that discovery still shows the
  resource, and asks for no restart, which would leave the hold for good: a new
  process can't track the kind (§7.3).
- *The log line* of a failed batch has its first three errors, and how many
  more there are: with several objects whose R1 failed first, the error that
  says what to grant for the CRD, or for discovery, may be among those cut, and
  only the count says there is more. The errors aren't reordered: what the log
  shows first is what failed first, the R1 reads.
- *The client* is `Clients.Discovery`, a REST client and not a discovery client,
  so that a caching one can't answer for the API server. provider-runtime sets
  it. Without it no kind is ever taken for gone: the holds stay, and the error
  says so. Neither the metadata nor the dynamic client can read a document that
  isn't an object's.
- *A 404 of the group's* isn't checked for its form, as the CRD's GET is: the API
  server's own is plain text, so it can't be. Something in front of the API
  server that doesn't pass `/apis/<group>` would be taken for a group that isn't
  served; it would have had to pass the CRD's GET, whose 404 is checked.
- *RBAC:* `get` on the non-resource URLs `/apis/*`, which `system:discovery`
  grants to every authenticated user, unless a cluster has removed that binding.
  Without it the holds stay, and the error says what to grant (§13).

*Accepted limits.* "The CRD is gone" implies "no holders" through the API
server's own cleanup, and "this CRD" is K through the tracker. Each limit is a
hold released under a holder that exists; §5's loss follows only if P is being
deleted, or is before that holder's next pass. `DisownPrefix` accepts the first
two as well.

| Limit | When | Remedy |
|---|---|---|
| Instances left behind a CRD that is gone | Its cleanup finalizer was stripped by hand, or it was deleted while not established: the cleanup deletes instances only of an established CRD (`apiextensions-apiserver@v0.37.2/pkg/controller/finalizer/crd_finalizer.go:144-170`). Nobody can read them. If the CRD returns, they do, and each one's next pass holds P again | Don't strip `customresourcecleanup.apiextensions.k8s.io`. Delete the holders first |
| An instance a stale API server creates after the cleanup's last check | Several API servers, one of them behind by more than the cleanup: its own source has a TODO for it (`crd_finalizer.go:235`). The instance is readable only through that server, until it catches up | None is needed once it has caught up: the instance is the row above's |
| K's CRD replaced by one of another plural, or by an aggregated API server, while a process tracks the first, and the group's discovery doesn't show it yet: the API server that answers lags, or the CRD comes with a version new to the group, whose APIService isn't registered yet | Strips, for as long as that lasts, each put back by its holder's next pass: a process usually keeps one connection, so the same API server answers it at every turn. It ends when that server catches up, or has the APIService. In a namespace being deleted the first is enough for P to go | Restart every process of the kind when its plural changes, before the new CRD has holders: the old tracker's resource is fixed at `Track` (§7.3) |

*And what it doesn't cover*, where a hold stays, and `DisownPrefix` remains the
remedy:

- **a read is refused**, as when an uninstall takes the RBAC first: the list of
  holders, whose 403 says nothing of the kind, so that the CRD isn't asked, the
  GET of the CRD, or the discovery. Take the RBAC last, once the kind's holds
  are gone (§11.5);
- **discovery can't be read** for as long as it can't, as with an APIService of
  K's group, put there by hand, whose API server is gone, or without
  `Clients.Discovery`;
- **no process tracks K any more**, or the process started after the CRD went:
  `Track` needs the CRD (§7.3);
- **R1 fails**, for a protected kind that is gone too, or whose grant is;
- **K's index is lost with the CRD** (§10.6.6). While the kind is gone its
  rebuild finds that, by the same GET and veto, and writes nothing: it creates
  no index that an uninstall deleted, marks none, and isn't retried as a
  failure. When the CRD returns, the index is rebuilt from the records, those
  of instances its cleanup left behind among them (the first limit above), and
  not at each holder's next pass. But a hold that no informer showed, and no
  record names, is in no line. Give `DisownPrefix` the protected kinds then,
  not the index.

The only things that can still remove the finalizer while H's record names
P are:

- §5's live-namespace release, which H's deletion passes guard against by
  never skipping;
- Disown, which is intended;
- a kind taken for gone while H exists: the accepted limits just above;
- third parties, which drop the `written` entry through the informer
  (§10.2), within the cache's lag.

**What the proof assumes.** R1's GET, R2's Namespace GET, R2's list and the
GET of the CRD are consistent reads, and A2 is a write. **The API server tests
and removes in one step**, which it doesn't (H1 and H3, above). No decision
rests on a cache. Caches
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
  case. An A2 rejected as Deleting gives a new holder `MustNotConnect`
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

  **How a relist is seen** (revision 16). An event handler can't see one.
  client-go turns `Replaced` and `ReplacedAll` deltas into ordinary
  `OnAdd`, `OnUpdate` and `OnDelete` calls
  (`client-go@v0.37.2/tools/cache/controller.go:823-870`, `:972-1002`). And
  with `WatchListClient` on by default since client-go 1.35
  (`features/known_features.go:141-144`), a relist is usually a watch with
  `sendInitialEvents`, not a List. So the core builds its informers on its
  own ListerWatcher wrapper, which records when each list completes: when a
  List returns, or when the `k8s.io/initial-events-end` bookmark passes
  (`tools/cache/reflector.go:1061`). Then
  `t_obs = max(observed, recordSince, lastListCompleted)`. That is the reset
  of every holder's entry, in constant time. After the first list it is
  §6.3's `t_sync`.
- **A recreate behind a cached UID** (L6). P is released and removed, and
  recreated under the same name. A process other than the releaser drops
  its `written` entries for the old UID only when its informer delivers
  the old P's deletion. Until then, that process's fast path may find the
  old P, with the finalizer, in its cache, and skip A2, while Connect reads
  the new, unheld P. **The permission to skip lasts from the old P's final
  removal to that delivery**, which is watch latency. The new P is unheld
  until a pass of each holder then runs and writes, so the window is that
  latency **plus the time to the holder's next pass**, at worst the poll
  interval. It is an accepted fail-open, like a failed A2 (§8). It can't
  open while the old P is held, because the recreate fails with
  AlreadyExists until P is gone. It can't open in the releasing process,
  which drops its entries when R3 succeeds. It matters only if the new P is
  deleted inside the window.

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
  - At 2 workers and about 0.5s per pass, the backfill takes about 4
    minutes with the predicate (1,000 passes × 0.5s ÷ 2), and about 8
    without (2,000 passes). The previous revision said 8 and 16, which was
    twice too high.
  - In PerNamePoll mode a new name needs its grant before A2. Each holder's
    first pass stops at A2's 403 (about 50ms, with no Connect) and returns a
    short `RequeueAfter`, which holds no worker (§10.7.2). The provisioner
    grants the whole wave in one coalesced update, and the second pass
    completes. That gives about 1,000 × (0.05s + 0.5s) ÷ 2 ≈ 4.6 minutes
    with the predicate, and about 9 without.
  - With saturation on, local slots pace it further.
- **A release:** for M pending objects in (K, N), M GETs, up to M patches, and, for
  each turn of 64 of them (§6.2), one Namespace GET per deleting namespace and one
  paginated metadata-only list of K in N, none if R1 left nothing to decide. A
  namespace teardown with 50 Secrets lists the holders once, not 50 times, and one
  with 200, 4 times: 200 GETs, 4 lists, 200 patches; in PerName mode 4 prunes too,
  each of up to 50 lines a write, and a GET of each object whose line it took. 4
  lists are up to 0.4s of one of the two workers, below. A failed batch logs one line
  a turn. An object being deleted, in a namespace that the
  cache doesn't show being deleted or in none, is read once more, and its release begins
  after a wait below 2s, 1s on average (§6.2 R1). It is queued again then as known to be
  held, behind what is known of its key before it: behind more than a turn of that, it
  waits a turn more. Its batch is its own, with its own
  list, unless other waits of the key end while it is busy: they share the next. Behind
  a busy key it waits for that too. If it is then kept, that is reported as much later
  (§12). The list usually waits below 100ms in the API server, not at most (§6.4), with a
  release worker: the two workers then make about 20 batches a second (an estimate), and
  with more keys than that at one moment the releases come later than their waits.
  R1 began up to 0.24s after its wait's end with 40 at once of a quiet kind at a poll of
  1ms, and 0.38s at 200ms; with 200 in one namespace p50 0.59s, p90 1.2s, and 2.0s at
  the longest (3,827 R3s). So 4s in all after the grace is possible.
- **A name that no object has**, as anyone who can edit a holder's record can put
  there, costs one GET at each change of the record. **One that can't be read**, of a
  kind that can't be mapped, that the process may not get, or that no request can ask
  for, fails: it costs a read, and for an unmapped kind a discovery, at each of its
  retries too, every 5 minutes for as long as the process lives, whatever the records
  say by then (§6.2). A kind's objects share one discovery a minute while no more than
  256 such kinds are asked for; a record of 4,500 names, each of a kind of its own, is
  4,500 discoveries every 5 minutes, 15 a second, as it was without the memory, and
  nothing here bounds that: asking only for kinds that are protected is left to a later
  release. A turn with nothing to decide makes no list. 100 records
  of 4,500 made-up names each are 7,000 turns of their own key, and one turn of it
  is what another key waits for.

### 6.7 The invariant, restated so it is true

**When a holder's owner finalizer is written, every ref in its record has
either been written by A2 after the record named it, or been found absent by
a live read made after the pass's last A2 and before that finalizer write.**
That live read is the check just before `AddFinalizer` (§8, R6). A ref that
appears after it is held on a later pass, through the prober or backoff
(§10.3). Until then it is unprotected: it was created after the decision, in
a live namespace, so it is recoverable.

**"Written" isn't "held", for an instant** (H1, H3, §6.4): an A2 that succeeds on
an object whose removal has begun is lost with it, and nothing tells the pass. The
object's removal enqueues its holders, whose next passes find it Missing.

### 6.8 N processes of one kind, without leader election (CP1)

The composition controller runs without leader election (`LeaderElect`
off), and its rollouts overlap old and new pods. So every part of this
design must be safe with N processes of one kind running at once, each
reconciling any holder. Part by part:

- **Acquire** (A1, A2). A1 is guarded by the holder's resourceVersion. A2 is
  a strategic merge (built-in kinds) or a guarded merge (custom resources),
  with a fresh epoch. §6.4's proof never asks which process wrote what: a
  releaser in any process fails its test against an A2 from any process.
- **The verified sets are per process, and only ever reduce skips.** A
  process skips A2 only against its *own* `written` entry, issued after its
  *own* observation of the record (§6.3). Another process's writes never
  let it skip. N processes therefore write at least as often as one would:
  after a restart, up to N A2s per (P, K, N).
- **The holder kind's version** may differ between them (§6.2): each writes and
  lists the holders at its own. Every one of those versions must keep the record,
  or one process's release loop doesn't see another's users.
- **Release** (R0 to R3). R3's JSON-patch tests serialize any number of
  releasers. A losing releaser re-reads from R1. **Except at an object's
  removal**, which isn't one step: there two releasers of one incarnation can both
  succeed, and the second remove the next one. Each waits a random time first, so
  that they seldom meet (§6.4, H1).
- **The index** (§10.6). Every write is guarded and merges on conflict. The
  superset proof (§10.6.3) is between any acquirer and any pruner, in any
  processes. The CRD pointer is written once, after a GET that finds it
  absent, by a merge patch guarded by resourceVersion, so only one process
  wins (§7.3, "Writing the CRD").
- **Disown and the count** (§7.4). The strips are R3's patches. Every
  process counts the same cluster, live. The count fields are written by
  merge patches guarded by the CRD's resourceVersion, after a GET that read
  their previous values, and `disown-settled` only when that GET read
  `held-count` as `"0"` and the CRD unchanged since this process's first
  zero pass, its own writes of the count aside. A process that reads a
  non-zero count written by another resets its own zero counter. Any write
  of another process's starts the run over (§7.4); those of the count stop
  once every process counts the same.
- **Old pods in an overlapping rollout** that run a runtime without this
  feature don't acquire, and don't `Forget`. A holder whose owner finalizer
  an old pod removed, leaving the record, no longer counts as a user
  (§6.1). Holds such pods never made are those of §11.9.1, backfilled by
  the new pods.

**Nothing needs a single writer.** Leader election is therefore **not a
prerequisite**, and §16.3 lists no leader-election work for
unstructured-runtime. What N processes cost is duplicated work:

- every process runs the release loop, the prober and the Disown count;
- their writes conflict more often, and the losers retry.

Where a runtime has leader election, as provider-runtime's adapter does, the
adapter runs these loops only on the leader, to save that work. That is a
saving, not a safety requirement.

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

	// Next must be nil in this release: Acquire refuses a list with a ref
	// that has one, with ErrChainsNotSupported (§9). In the next release it
	// names the refs found inside this object. The tracker calls it after
	// the object is held, with the object it read, without managedFields and
	// status (§10.4). It returns an error when the object lacks what it
	// reads, never nil refs (§9.1).
	Next func(obj *unstructured.Unstructured) ([]Ref, error)

	// Guaranteed declares that the object's own controller already keeps it
	// alive while the holder exists (§9.3). It is neither recorded nor held.
	// In the next release, its Next is still followed.
	Guaranteed bool
}

// ErrChainsNotSupported is Acquired.Err when a ref has a Next (§9).
var ErrChainsNotSupported error

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
	// Discovery reads the API server's discovery documents, live: a discovery
	// client's RESTClient(). It tells whether a holder kind whose CRD is gone
	// is served by another resource (§6.4). Nil never takes a kind for gone.
	Discovery rest.Interface
	// Events records §12's events on holders, protected objects and the
	// index (k8s.io/client-go/tools/events). Nil records none. The core
	// throttles them itself.
	Events events.EventRecorder
}

type Options struct {
	Holder         schema.GroupVersionKind // the kind the caller reconciles
	OwnerFinalizers []string                // accepted owner finalizers on holders (§6.1; two during provider-runtime#46)
	Refs           RefsFunc
	Protected      []ProtectedKind          // kinds Refs and Next may name; default: Secret
	MaxDepth       int                      // levels of Next followed; default 3 (unused in this release, §9)
	Enqueue        func(holder types.NamespacedName) // the caller's queue (§10.2)
	DeletionGrace  time.Duration            // §5; default 60s
	ProbeInterval  time.Duration            // missing-ref prober; default 15s (§10.3)
	DisownInterval time.Duration            // §7.4; default 60s, longer when TimeoutBound needs it
	DisownPasses   int                      // §7.4; consecutive zero passes, default 5
	// TimeoutBound is the kind's largest reconcile or request timeout. It is
	// required: §7.3 checks (DisownPasses − 1) × DisownInterval against it
	// and LandingMargin, derives an unset DisownInterval from it (§7.4), and
	// publishes it.
	// provider-runtime passes its timeout plus grace, 90s by default.
	TimeoutBound   time.Duration
	// Enqueue, Logger and Metrics are the kind's sinks; an equal Track
	// rebinds them (§7.3). None of them may block or call Track: the rebind
	// waits for the calls in flight, which is what guarantees that no event
	// reaches an old sink once Track returns, so a sink that blocks would
	// block Track.
	// An implementation of MetricsRecorder embeds NopMetrics (§12).
	Logger         logging.Logger
	Metrics        MetricsRecorder
}
// Disown is not an option: it is the switch on the holder CRD (§7.4), so
// every process of the kind reads the same value.

type ProtectedKind struct {
	GVK   schema.GroupVersionKind
	Watch WatchMode // LabelScoped (default), PerName (§10.6) or PerNamePoll (§10.7)
	// Content keeps whole objects, not metadata, in this kind's cache, for
	// refs with a Next. For Secrets this is opt-in and holds Secret data (§10.4).
	// Don't set it in this release: nothing needs it (§9).
	Content bool
}

type SharedOptions struct {
	Namespaces     []string                  // restrict the caches (§10.5); default all
	Listers        map[schema.GroupKind]Lister // for Guaranteed refs (§9.3)
	IndexNamespace string                    // PerName and PerNamePoll (§10.6.1)
	IndexMaxLines  int                       // §10.6.4; default 2,000
}

// NewShared builds the process-wide state: informers per protected kind,
// shared by every tracker. Run it once per process. Leader election is not
// required for safety (§6.8).
func NewShared(c Clients, o SharedOptions) *Shared
func (s *Shared) Run(ctx context.Context) error

// Track adds a holder kind. It can be called before or after Run, for kinds
// discovered at runtime (§7.3). It is idempotent per holder GroupKind, and
// a second Track with equal Options rebinds Enqueue, Logger and Metrics.
// When ctx ends, a pending Track stops and returns an error wrapping
// ctx.Err().
func (s *Shared) Track(ctx context.Context, o Options) (*Tracker, error)

func (t *Tracker) Acquire(ctx context.Context, obj metav1.Object) (Acquired, error)
func (t *Tracker) BeforeEstablish(ctx context.Context, obj metav1.Object, a Acquired) error
func (t *Tracker) Forget(obj metav1.Object) (changed bool)
func (t *Tracker) Users(ctx context.Context, p Ref, namespace string) ([]types.NamespacedName, error)

// IsOwnRecordWrite reports whether the only change from old to new is the
// record or the unheld annotation, and both equal what this process last
// wrote for that holder: the holder event filter of §7.2 item 5 and §7.5.
func (t *Tracker) IsOwnRecordWrite(old, new metav1.Object) bool

// CheckRead reports a read of a protected kind that holder's record doesn't
// name (§11.9). Runtimes wrap their own clients around it.
func (t *Tracker) CheckRead(holder metav1.Object, gk schema.GroupKind, namespace, name string)

type Acquired struct {
	Held     []Ref
	// Unheld refs, each with a Reason: Missing, Deleting, NotConsented,
	// ConsentWithdrawn, ReadDenied (§11.3), or Disowned (§7.4).
	Unheld   []Unheld
	Resolved map[Ref]*unstructured.Unstructured // objects read while following Next (§9.2)
	// MustNotConnect: a ref is Deleting and unheld, and obj lacks its owner
	// finalizer, or the acquire failed for an object without one (§8).
	MustNotConnect bool
	// Degraded: the acquire failed for an established object, which proceeds
	// (fail open, §8). The error is in Err.
	Degraded bool
	Err      error
	// RetryAfter, when set, asks the caller to requeue after it, with no
	// failure backoff (IP2): the RBAC-propagation retry of §10.7.2, and a
	// failed live consent read that kept its hold (§11.3).
	RetryAfter time.Duration
	// Pending: an A2 is waiting for its grant to propagate (§10.7.2), so the
	// pass must not connect, whether or not obj is established.
	Pending bool
}

// Resolved returns the object the tracker read for ref in this pass.
func WithResolved(ctx context.Context, a Acquired) context.Context
func Resolved(ctx context.Context, ref Ref) (*unstructured.Unstructured, bool)

func FinalizerName(holder schema.GroupKind, namespace string) (string, error)

// DisownSettled reports whether a holder CRD's annotations say that Disown
// has settled under its switch as it is now: §7.4's three checks.
func DisownSettled(ann map[string]string) bool

// DisownPrefix strips a deleted holder kind's holds (§7.4).
func DisownPrefix(ctx context.Context, c Clients, o DisownPrefixOptions) (DisownReport, error)

type DisownPrefixOptions struct {
	Holder    schema.GroupKind
	Protected []schema.GroupVersionKind
	Index     *types.NamespacedName // PerName and PerNamePoll: the index to read
	Names     []Ref                 // or an explicit list of objects
	DryRun    bool                  // report only
}

// LandingMargin is how long after its context ended a write is allowed to
// land (§7.4): a margin, not a proof.
const LandingMargin = 60 * time.Second

const (
	AnnotationRefs = "credential-use.krateo.io/refs.v1"
	AnnotationUnheld = "credential-use.krateo.io/unheld"
	LabelHeld      = "credential-use.krateo.io/held"
)
```

A holder is **established** when it carries any of `OwnerFinalizers`. The tracker
decides that from the object it is given, so the caller doesn't pass it.

### 7.2 What an outside runtime must call, and when

For unstructured-runtime:

1. **At start:** `NewShared` once per process, `Track` once per holder
   kind, then `Run(ctx)`. Run it under the leader election the controller
   already uses, because the release side writes. Kinds discovered later
   can call `Track` at any time. **Reconcile only in a process whose write
   loops run** (the user's decision, 2026-11-23): they make good what a call
   leaves owing, the check of an A2 whose outcome is unknown, the re-adds and
   the release batches (§10.6.2). provider-runtime refuses the passes of any
   other process (§7.5).
2. **In every reconcile**, after reading the object and before anything
   reads a Secret or a protected object, deletion passes included:
   `a, err := t.Acquire(ctx, obj)`.
   - On a conflict, requeue.
   - If `a.MustNotConnect`, don't read credentials. Report it (§12), and
     requeue with backoff.
   - If `a.Degraded`, proceed: log once, record an event.
   - If `a.Pending`, an A2 is waiting for its grant to propagate
     (§10.7.2): don't read credentials, and requeue after `a.RetryAfter`
     with no failure backoff.
   - Otherwise, if `a.RetryAfter` is set (a failed live consent read that
     kept its hold, §11.3), proceed, and requeue after it.
   - Pass `WithResolved(ctx, a)` to whatever reads the chain. In this
     release it carries nothing, because a ref with a `Next` is refused: the
     pass's `a.Err` is `ErrChainsNotSupported`, with `a.MustNotConnect` or
     `a.Degraded` as for any other failure (§9).
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
   events whose only change is the record, with `t.IsOwnRecordWrite`, as
   provider-runtime's `r.IgnoreCredentialUseRecord()` does (M6).
6. RBAC as in §13. In PerName or PerNamePoll mode, also set
   `SharedOptions.IndexNamespace` (§10.6.1).
7. **On a stop, wait for the calls in flight** before the process exits.
8. **Set `Clients.Discovery`** to a discovery client's `RESTClient()`, as
   provider-runtime does. Without it what a kind that is gone held stays until
   `DisownPrefix` strips it (§6.4), and `Track` warns. It needs no grant beyond
   `system:discovery`.

"Find the last user" is the release loop's job, inside `Run`. `Users` is
exported for diagnostics and tests.

**A call outlives its context, within bounds** (decided 2026-11-23), so that a stop cuts no write short whose outcome nobody would
then know:

- **An A2 in flight** when its caller's context is cancelled goes on, with
  the caller's values, until the caller's own deadline, or 10s after the
  cancellation, whichever comes first. Without a deadline it is bounded by
  `TimeoutBound`. No A2 begins once the context has ended.
- **One `Acquire` or `BeforeEstablish`** returns at most 15s after its
  context ended, its last A2 and the line checks after it included, however
  many refs it holds. While the context lives, one line check takes at most
  15s.
- **A write of a prune, once begun, runs to its end**, within 15s, its check
  and its re-add included (§10.6.2); a prune whose context has ended begins
  no other.
- **Then the write loops drain** what the index holds in memory to write
  (decided 2026-11-25; §10.6.3): every live check queued, within 10s. The
  acquirers may still queue some. A manager stops the runnables that don't
  need leader election first, our informers among them, whose end ends the
  loops, and only then the controllers with the write loops' own context.
  An `Acquire` then goes on up to 15s. So the drain goes on while the write
  loops' context lives, or an acquirer call begun before it ended is in
  flight, and its 10s count from the last moment one of them was so. A
  call begun after that isn't waited for: its controller wasn't stopped
  with the loops.
- **So `RunWriteLoops` returns at most 25s after the loops ended**, when the
  informers or its context ended, whichever was first: the 15s of the
  acquirers or of the last prune's write, and the drain's 10s, whatever the
  prune took. That fits in the manager's default 30s, from which only the
  stop of our informers, well under 1s, comes before. When the controllers'
  cancel comes more than 10s after the informers' end, the cap may cut the
  drain short of a call's check, which is then logged. It is
  while the process still holds its Lease: controller-runtime v0.25.2
  cancels the leader election only once the runnables have stopped
  (`pkg/manager/internal.go`), so a manager that releases the Lease on a
  stop does so after the drain. The next leader starts up to 10s later than
  before, and no two leaders write the index at once, as long as the process
  exits when the manager's `Start` returns. A `GracefulShutdownTimeout`
  under about 25s cuts the drain short, and a lost Lease skips it: the
  manager then sets the timeout to 0 and waits for nothing, which is the
  kill case.

controller-runtime's manager waits for a stopping controller's workers, and
for the write loops, within its `GracefulShutdownTimeout`, 30s by default
for all its runnables together. Two things end a pass without that wait, as
a kill does: a pass that takes longer than the grace period, and **a lost
Lease**, after which controller-runtime v0.25.2 waits for nothing
(`pkg/manager/internal.go`). Another process's write loops may run by then.
An A2 cut short that way may have landed, or may yet, with nobody left to
check its line at once or after the landing margin: condition 2 of §10.6.3's
residual. With leader election its line stays meanwhile: the next leader's
write loops prune nothing at first (§10.6.2), so what is left is a landing
later than that. Without it, another process that has been running may prune
the line.

### 7.3 Setup: idempotent, after start, refuses to start on error

- **Idempotent per holder kind** (L8). Two Options are equal when every
  field that isn't a function or a sink is: `Holder`, the durations and
  counts, `MaxDepth`, and `OwnerFinalizers` and `Protected` as sets. A
  second `Track` with equal Options returns the existing tracker and
  rebinds its sinks, `Enqueue`, `Logger` and `Metrics`, to the new call's,
  atomically: once `Track` returns, no event reaches the old ones. With
  different Options it returns an error. `Refs` stays bound to the first
  `Track`: it is part of the kind's identity, since changing it would mean
  re-indexing every holder, and a second `Track` with another `Refs` is the
  caller's mistake, documented and not detected. This is the user's
  decision of 2026-11-18: rediscovery rebuilds a kind's controller and its
  queue (a CompositionDefinition deleted and created again), and the latest
  registration's queue must get the events. `Track`s of one kind run one at
  a time, and none holds a lock across an API call, so a slow `Track` of
  one kind never delays another's.
  The core keeps its own indexers on its own informers, so nothing registers
  a controller-runtime field index, and nothing can register one twice or
  after the cache has started.
- **After start** (C3). `Track` adds the holder informer and any protected
  informer not yet running. It starts them, and waits for their sync before
  returning. core-provider's dynamicwatch and pool can call it when a
  CompositionDefinition appears.
- **Errors.** `Track` returns an error for:
  - an invalid finalizer prefix;
  - an unmappable holder or protected kind;
  - an empty `OwnerFinalizers`;
  - `(DisownPasses − 1) × DisownInterval` below `max(2 × TimeoutBound,
    TimeoutBound + LandingMargin)`, `TimeoutBound` being the kind's largest
    reconcile or request timeout (§7.4): an explicit `DisownInterval` too
    short for it, or `DisownPasses` below 2. An unset interval is derived so
    that it suffices;
  - in PerName and PerNamePoll modes: no `IndexNamespace`; **an
    `IndexNamespace` that doesn't exist**, which fails with
    `credential-use: index namespace krateo-credential-use not found: install it (installer release) before enabling PerName`;
    an `IndexNamespace` that holders of the kind live in, or that disagrees
    with the CRD's pointer; an `IndexMaxLines` above the ceiling
    (§10.6); missing grants on the index (§10.6.7); or a watch mode for a
    protected kind that another holder kind of the same `Shared` tracks
    with another mode.

  Before `Run`, the error makes `Run` fail, so the process refuses to start
  (U2).
- **Settings every process of a kind must share are published on the holder
  CRD** (L3), in the same way as the index pointer. The first process writes
  `credential-use.krateo.io/config` once, after a GET that finds it absent
  (see "Writing the CRD" below):

  ```json
  {"deletionGrace":"60s","disownPasses":5,"disownInterval":"60s","timeoutBound":"90s"}
  ```

  `timeoutBound` is the largest reconcile or request timeout behind the
  `DisownPasses` check. **A process whose configuration disagrees refuses
  to start.** Changing a value means editing the annotation and restarting
  every process of the kind.
- **What isn't published, and why that is safe.** A rollout can run
  processes with different `Protected` kinds or a different `RefsFunc` for
  a while. That is **safe per pass**: each pass records what it computed,
  and release reads only records. But it **churns**: two versions keep
  rewriting a holder's record back and forth, and each rewrite triggers A1
  and a release batch, until the rollout finishes.
- **Writing the CRD** (revision 16). Every CRD write (this config, the index
  pointer of §10.6.1, Disown's count fields of §7.4, and a pass's removal of
  a stale `disown-settled` before its first A2, §7.4) is a GET, then a
  merge patch guarded by the CRD's resourceVersion. A conflict re-reads and
  decides again; a pass's removal that conflicts instead fails the pass,
  which is made again. An earlier revision asked for "a patch that tests the
  annotation is absent", but RFC 6902 has no test for absence: json-patch v4
  passes a `test` with `null` on a missing key only when the parent map
  exists (`gopkg.in/evanphx/json-patch.v4@v4.13.0/patch.go:677-693`), a
  library quirk, and a first count write has no previous value to test. The
  guarded merge keeps every guarantee: written once, no older count over a
  newer one, and `disown-settled` only over a `held-count` of `"0"`. It also
  conflicts with unrelated CRD writes, which cost only a retry.
  **The retries are bounded** (L4 of the review of revision 16): at most 5
  GET-and-patch attempts per write. After that the write waits for the
  loop's next tick (Disown's next pass, or the next start for the config and
  the pointer). A count write that fails 3 ticks in a row is logged as
  starved, with the CRD's name and the last conflict, and counted in
  `credentialuse_crd_write_starved_total`. A starved `held-count` is never
  read as written: `disown-settled` is only ever written over a `"0"` that
  a GET read.

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

- `Acquire` acquires nothing. It returns every ref Unheld, with the reason
  `Disowned`, the object proceeds as today, and nothing is ever
  `MustNotConnect`. `Disowned` is never written to the `unheld` annotation,
  which Disown strips with the record.
- The loop removes every finalizer with K's prefix, and its epoch, from every
  protected object, using R3's JSON patch without the user check. It
  removes K's marker label, and the held label where that was the last
  `credential-use.` finalizer.
- **Then** it strips the record from every holder of K, with a merge patch
  guarded by resourceVersion. A holder that names an object whose strip failed,
  or one of an indexed kind that couldn't be read, keeps its record, and is
  counted, until the next pass. Two aren't kept: a record of objects of a
  LabelScoped kind whose list failed, since the marker label finds them still,
  and the count is unknown meanwhile; and, with "all", one that can't be
  parsed, which names nothing. A switch for some kinds leaves a record that
  can't be parsed to its holder's next pass, which rewrites it without them: it
  may name refs of the other kinds, which nothing would hold in between (§10.2's
  batch of the namespace releases what has no user). It doesn't count it
  either, since no pass could bring the count to zero while its holder is
  paused; the objects of the kinds it covers are stripped and counted as ever.
  **The order matters in PerName mode**: Disown finds an object
  by its line or by a record, so while one may be held, a record names it. With the records
  first, a process killed between the two strips, with a prune of the same
  object's line between its removal and its re-add, left an object held that
  nothing named, and Disown settled over it (found in the model). The lines
  of the objects the records name are still added before either strip (E2).

**"Safe" is observable cluster-wide**, not only on one process's gauge.
Every pass of the Disown loop, once per `DisownInterval` (60s by default,
longer when `TimeoutBound` needs it),
makes a **live count**. In LabelScoped mode that is label-selected,
metadata-only, paginated lists of each protected kind by K's marker label,
counting finalizers with K's prefix. In PerName mode it reads the index and
makes a GET per line (§10.6). Either way it adds the holders that still
carry a record. The result goes on the CRD:

```
credential-use.krateo.io/disown: all            # the operator's switch
credential-use.krateo.io/held-count: "0"
credential-use.krateo.io/disown-settled: all    # the switch's value, as written
```

**Disown has settled when all three hold** (the user's decision, 2026-11-23),
and every reader checks all three:

1. `credential-use.krateo.io/disown` is set and not empty;
2. `credential-use.krateo.io/disown-settled` is the same string;
3. `credential-use.krateo.io/held-count` is `"0"`.

`disown-settled` alone says nothing: it may be left from another value of
the switch, or from a switch that is gone. `credentialuse.DisownSettled(ann)`
makes the three checks, for the runtime's own readers and for outside ones.
The flag holds the switch's value **as written**: `Secret,ConfigMap` and
`ConfigMap, Secret` cover the same kinds, and differ, as any whitespace,
order or duplicate does. Rewriting the switch that way starts the run over,
and the flag is written again under the new value. A `disown-settled` of
`"true"`, the form before the flag held the value, is the value of no
switch, so it reads as not settled. No release wrote it.

**Settling counts consecutive zero passes, not wall-clock time** (L4). A
process sets `disown-settled` once it has seen `DisownPasses` (**5** by
default) consecutive passes of its own count zero, over a CRD that nobody
else changed in between.

- **Setup validates** `(DisownPasses − 1) × DisownInterval ≥ max(2 ×
  TimeoutBound, TimeoutBound + LandingMargin)`, where `TimeoutBound` is the
  kind's largest reconcile or request timeout (L3) and `LandingMargin` is a
  constant, 60s, the API server's default `--request-timeout` (the user's
  decision, 2026-11-23). It is a margin, not a proof: nothing bounds from
  end to end how long after its context ended a write may still land. The
  first pass runs as soon as the switch is seen, so
  `DisownPasses` zero passes span one interval fewer (the review of PR 6).
  provider-runtime's reconcile timeout plus its grace is 90s by default, so
  the minimum is 180s, and the defaults give 4 × 60s = 240s.
- **What the span gives.** Let T be when the switch was set. A pass that
  read it live as off started before T, so its context ends by T +
  `TimeoutBound`, `BeforeEstablish` included. The first zero pass starts at
  or after T, and each next one at least an interval later: each tracker's
  clock is read after its switch, so another tracker's slow pass can't eat
  into the span. So the last zero pass starts at least `LandingMargin`
  after the last such context ended, and reads a write that landed within
  it. **A write that lands later than the margin isn't covered**: Disown may
  already have settled.
- **An unset `DisownInterval` is derived from `TimeoutBound`**:
  `max(60s, ⌈span / (DisownPasses − 1)⌉)`, with the span above, rounded up
  to a whole second, after `DisownPasses`' default (the user's decision,
  2026-11-22; the span as of 2026-11-23). The derived value is what §7.3 publishes on the CRD, so every
  process of the kind agrees on it; an interval set explicitly is validated
  as given. For example, git-provider's 4m reconcile timeout gives a
  `TimeoutBound` of 270s, so the interval is ⌈540s / 4⌉ = 135s, and Disown
  settles in 4 × 135s = 540s after the first zero pass.
- **A run is of passes over an unchanged CRD** (the user's decision,
  2026-11-23). Each pass reads the CRD's resourceVersion as it starts, and
  again as it writes its count. A CRD that changed since the pass before,
  other than by this process's own count write, makes this pass the first
  of a new run. One that changed during the pass, or couldn't be read as it
  started, counts for no run. So a switch turned off and on again between
  two passes, during which a hold may have been made, is never taken for
  one that stayed on, whatever the informer delivered: the live reads see
  the version. The cost is that **any other write of the CRD starts the run
  over too**, another process's write of the count included, which stops
  once every process counts the same.
- **A CRD that keeps changing is reported**, since Disown then never
  settles. Once runs that started over have gone on for two spans, `2 ×
  (DisownPasses − 1) × DisownInterval`, with nothing held, a Warning event
  on the CRD, `CredentialUseDisownRestarted`, and a log line name the CRD's
  newest writer at each of those restarts, the last three managers, each with
  its operation and subresource, at most once per 10 minutes per process.
  managedFields keep only the last writer of each field, so the process
  remembers them through the restarts; one that comes in after a report is
  named by the next. **Two writers under one manager name look like one**, as
  the two core-provider writers of a remote kind's hub copy may. `credentialuse_disown_restarts_total` counts every
  restart. **The remedy** is to find and stop the writer. For core-provider's CRDs:
  79 composition CRDs on krateo-057 were watched for 560s with no
  resourceVersion change (measured, with a working control); managedFields'
  newest write was about 14 hours old; its apply path returns early when
  nothing changed (read); a version bump is several writes over minutes,
  with one possibly later (read); spokes, and the hub copy of a remote kind
  (two core-provider writers), were not measured.
- **The `disown-settled` write tests that `held-count` is `"0"`**, so it
  can't land over a newer non-zero count.
- **A process that reads a non-zero count** written by another process
  resets its own zero counter. A process whose own run isn't done leaves the
  flag of one whose run is, if it is of the switch's value now.
- **A late hold that the release loop takes off** before any of Disown's
  passes sees it is counted by none, and starts no run over. It needn't: the
  run's first pass is after the switch, so its last pass starts at least
  `LandingMargin` after every pass begun before the switch has ended, and it
  reads the object by its line or its marker label, live.
- **A count that can't be made**, because the index is missing or
  unreadable, or a list fails, is written as `held-count: "unknown"`. That
  clears `disown-settled`, and is never read as zero (§10.6.5).
- **The holders' list, a LabelScoped kind's, or the index, when it can't be
  read,** keeps the count `"unknown"`, and the same event and a log line say
  which, with the failure's class and what to do about it. A lost index
  (below) has its own event, and not this one too.
- **An object in the index or a record that can't be read** keeps the count
  `"unknown"` too, since it may carry K's finalizer (the user's decision,
  2026-11-22). Under `resourceNames` RBAC that is a Secret outside the
  grant: an ensure wrote its line before an A2 that got a 403. Disown says
  so: a Warning event on the CRD, `CredentialUseDisownUnreadable`, names
  the first three and why (`team-a/Secret/nogrant: forbidden`), and how many
  more, throttled while that stays the same; a log line, with the first 50;
  and the gauge
  `credentialuse_disown_unreadable`. **The remedy:** grant `get` on the
  object, or, no sooner than `TimeoutBound` + 60s after the switch was last
  changed, when an A2 from before it has landed, confirm by hand that it
  carries no finalizer of K and add it
  to the index's `credential-use.krateo.io/vouched` annotation. Disown then
  settles, and removes the line itself. **Or vouch for a kind in a
  namespace**, with `team-a/Secret/*`, once none of its objects of that kind
  carries K's marker label, which takes one list: a tenant may name any number
  of objects the process can't read, in a spec or in a record it forges, and
  each would need a vouch of its own (the user's decision, 2026-11-24). The
  entry names its kind (decided 2026-11-25), and the event names an entry for
  each kind and namespace it can't read in. A switch widened to another kind
  finds what it can't read of that kind unvouched, so the count stays
  `"unknown"` until the operator has looked at that kind too. A bare
  `team-a/*` is malformed.
  Removing the line by hand
  alone does nothing: the tamper check restores the line of an object it
  can't read (§10.6.6). The vouch, the user's decision of
  2026-11-22, is described in §10.6.6. An object that only a record names
  gets a line before its record is stripped (below), and one that can't be
  read, so can't be stripped, keeps its holder's record too, and the count
  `"unknown"`, until it can be read or is vouched for.
- **A line this process may still owe** keeps the count `"unknown"`: while a
  live check or a re-add of a covered kind's line is pending, the landing
  margin of an A2 of unknown outcome is running (§10.6.2), or a rebuild is
  due, the index read may lack a held object's line. It looks before the
  read and after it: a re-add pending before may land too late for the read,
  and be pending no more after it.

A non-zero or unknown count clears the flag. **Downgrading the runtime,
removing the interface from the connecter, or uninstalling is safe once
the three checks above hold**, but for what lands later than the margin and,
in PerName mode, a kill inside a prune, both below. Any process can write the fields, because each one counts the same
cluster, live.

**A hold that lands later than the landing margin isn't covered.** Disown
may have settled by then, and the index's line may be gone. P then carries a
finalizer that nothing counts. The remedy is to remove it by hand: its exact
form is `credential-use.<kind>.<group>/<namespace>` (§3), and after the CRD
is gone, `DisownPrefix` with `Names` removes it.

**A kill inside a prune isn't covered either** (PerName; the user's decision,
2026-11-24). A process that is pruning P's line as the switch is set, and is
killed between the removal and its re-add, can leave a hold that Disown
settles over, if a Disown pass read the index between the prune's read of the
switch and its removal, and an A2 on P lands after that pass read P. §10.6.3
has the conditions, what the model searched, and what would close it. The
remedy is the same.

**The index's lines stay until Disown has settled** (PerName; the user's
decision, 2026-11-23). A hold that lands late has only its line for the
count to find it by. So:

- **Only a process that has settled empties the index**, after its own run
  of zeros, in a pass that writes the flag or reads it as already written
  for the same switch. Then it removes only the lines it proves free in that
  pass, after its strip: the object, read live, is gone or carries no
  finalizer of K, or is vouched for and can't be read. Each is read once
  more after the removal, and its line added back if it carries one
  (§10.6.2). A line added since the pass read the index stays, for the next
  pass.
- **No other prune takes a line meanwhile** (E1). A prune leaves out the
  lines of the kinds the switch covers while it is unsettled, as the CRD's
  cache has it, and then as a live read before its removal has it; when that
  read fails, it removes nothing. After the removal it reads the switch live
  again: lines removed under an unsettled switch go back, and so do they all
  when that read fails. A switch set after that read found it off has its
  first Disown pass after the removal, and E2 adds the lines then.

  So a removal under the switch began, with its write's first live read,
  before the switch was set, and that write's 15s run from then: they cover
  **both reads, the removal with its retries, the reads of its objects, at
  most 50, and the re-add, as calls**, however many lines the prune has
  (§10.6.2). The line is back within 15s of the switch, and the last pass of
  a run starts at least `TimeoutBound` + 60s after it. The 15s don't cover
  two things. A removal cut short may still land later, as any write
  may (the landing margin): 75s after the switch at the latest, which the
  last pass is past with a `TimeoutBound` of 15s or more, and the drop is
  then found and checked as anyone's is (§10.6.6). And a re-add that fails is
  queued: until it lands, that process's Disown counts `"unknown"`.
- **A record's refs get their lines before the record is stripped** (E2).
  A1 comes before A2, so an A2 that lands after the strip has its object's
  line there for a later pass's count, even one a prune took before the
  switch was set. An index that can't take the lines, a full one, leaves
  those records unstripped and the count `"unknown"`, with the Warning event
  `CredentialUseDisownIndexFull`, which names `SharedOptions.IndexMaxLines`
  and the holders. **Each holder's lines are taken or left whole, the smallest
  first**, in one write: a record is an annotation a tenant may write, so one
  of 2,500 made-up names stays, and keeps the count unknown, but doesn't keep
  another holder's record. A line added so is for any name a record carries,
  with or without its target's consent (§11.3), since a forged record can't be
  told from one whose consent was withdrawn; in PerName mode a line grants
  nothing, and costs one watch and one GET a pass while the switch is
  unsettled. **What the provisioner of PerNamePoll does with such a line is
  open** (§10.7, not built).
- **A dropped line of a bare object goes back** while the switch is
  unsettled (E3), and isn't reported as tampering.

**The switch turned off before it settled** leaves the lines of objects
that carry nothing. They are pruned at the next startup recheck, or with the
next release batch of the same object. In PerNamePoll mode their grants stay
as long, since the grants follow the lines (§10.7.1). **A process that
stopped between settling and emptying** leaves lines the same way; a line
whose object carries no finalizer of K never counts as a hold.

**An index lost while the switch is on** (PerName; the user's decision,
2026-11-23). Disown strips the records a rebuild would work from, so a
rebuild under the switch can't tell that it found every hold. It merges what
it finds, and leaves `credential-use.krateo.io/rebuilt` unset. An index
without that marker keeps the count `"unknown"`, with a Warning event on the
CRD, `CredentialUseIndexLost`, which names the steps:

1. **no sooner than `TimeoutBound` + 60s after the switch was last changed**,
   when an A2 of a pass from before it has landed, list the objects of the
   kinds the switch covers, **and of no other**, by K's marker label, which
   needs list rights, and remove from each every finalizer with K's prefix.
   The objects of a kind the switch doesn't cover are protected still;
2. set `credential-use.krateo.io/vouched-index` on the index ConfigMap to
   `<uid>/<kinds>`, its own `metadata.uid` and those kinds as the switch
   writes them (`Secret,KubernetesTarget.core.krateo.io`), with
   `kubectl annotate --overwrite`: a restored copy may carry another's.

The count then reads the index as complete, for those kinds. The value has
the uid so that the word holds for that ConfigMap only: a later loss, or a
restored copy, needs a new one; and the kinds, so that a switch widened to
another kind, whose objects nobody listed, needs a new one too. A kind once
vouched for stays so while the switch is narrowed, or off: its later holds
have their lines. A value without kinds vouches for none. The process never
writes it. Turning the switch off
lets a rebuild complete instead, from the records there are, and every
holder's next pass adds its own lines. It also forgets the loss: an object
whose finalizer no record names any more is then found only by its marker
label (§10.6.6).

A kind that was never tracked has nothing to lose: its first index is
complete when its rebuild is, under the switch too. Its CRD carries neither
the index pointer nor the published settings, which every `Track` writes.
Three cases follow:

- **The CRD created again, its index intact:** the index has the marker, and
  its lines are counted.
- **The CRD and the index both gone:** the new CRD can't be told from a new
  kind's, so Disown doesn't see holds left from before. `DisownPrefix` with
  `Names`, before the CRD is created again, is what finds them; after it,
  they are removed by hand.
- **A process that stopped before emptying:** above.

A kind moved from LabelScoped was tracked before. With the switch set before
its first PerName start, its new index is one without the marker, and the
count stays `"unknown"` until it is vouched for.

The rule errs on the wrong side in two cases: the second of those, and a
kind whose writes of the settings and of the pointer were both starved
(§7.3) at every start before the loss. It errs on the safe side in one: a
kind's first `Track` that fails after it published the settings, or wrote the
pointer, and before its index was rebuilt, isn't the first for the next one.
With the switch set before that, it reports a lost index that never held
anything, until it is vouched for, or the switch is off.

**A pass never holds under a stale flag** (decided 2026-11-23).
The live read of the switch before a pass's first A2 also reads
`disown-settled`. One that isn't the switch's value, left from a switch that
was removed or changed, is removed first, guarded by the version read. A
removal that fails fails the hold: no A2. A 403 names the grant it needs,
`patch` on the CRD. So a hold made now never stands under a flag that would
say nothing is held, should the old value of the switch come back.

**Removing the switch clears `held-count` and `disown-settled`**, at most
once per `DisownInterval`.

**Writing the fields** (L5, L6). Every write of these fields is a merge patch
guarded by the CRD's resourceVersion, after a GET that read the previous
values (§7.3, "Writing the CRD"). Of two processes racing, one conflicts
and re-reads, so neither overwrites a newer count with an older one. **A
CRD write makes the API server rebuild its OpenAPI**, so the fields are
written only when they change. Apart from the one-time settings and index
pointer (§7.3, §10.6) and the removal of a stale flag, these are the only
CRD writes the design makes.

**A Kind at zero replicas** has no tracker running, so nothing updates the
count. Two things cover that (L5):

- **`WithHeld` keeps the controller up while the switch is set and Disown
  hasn't settled under it**, by the three checks (§11.6). Setting the switch
  on a scaled-down Kind wakes its controller, which then performs Disown.
- **An operator can check without starting it:**
  `credentialuse.DisownPrefix` with `DryRun: true` reports, from a live
  count, what it would strip, and changes nothing.

**A pass that started before the switch** may still land an A2 after the
strip. §10.6.8, interleaving 3, shows why consecutive zero passes catch one
that lands within the margin.

**Namespaces outside `SharedOptions.Namespaces`.** Disown doesn't use the
caches. Its lists are live and cluster-wide, so it reaches every namespace,
including those the caches don't cover. Under `resourceNames` RBAC, where
cluster-wide lists aren't allowed, it works from the union of the index's
lines and the holders' records: it GETs and patches each named object. The
index is the superset of the holds (§10.6.3), and the records have to be
stripped anyway.

**Disown with no holder kind, for a deleted CRD** (L5). When the holder CRD
is gone, its objects are gone too: a CRD's deletion removes its custom
resources first. But finalizers with its prefix may remain on protected
objects. A process that still tracks the kind releases them itself, through
R2's GET of the CRD (§6.4, "The holder kind itself gone"), as long as its
grants last. `DisownPrefix` is for when none does: it was uninstalled, or
restarted, after the CRD went, or its RBAC went first. `credentialuse.DisownPrefix(ctx, clients, DisownPrefixOptions{Holder,
Protected, Index, Names, DryRun})` (§7.1) needs no holder kind and no switch. It
lists the protected kinds live by K's marker label, removes every finalizer
with the prefix `credential-use.<kind>.<group>/` and its epoch, and removes
the labels as above. In PerName mode it takes the names from the index,
whose location it can no longer read from the CRD, so it is given the index
explicitly, or a name list. It only reads the index, whose lines stay until
Disown has settled, so an unfinished Disown leaves it what it needs. It
doesn't read the vouch: an object it can't read is in the error it returns,
with the rest stripped. It refuses to run while the CRD still exists,
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

- gets the per-manager `Shared`, built once from `mgr.GetConfig()` and
  `mgr.GetRESTMapper()` with the default `SharedOptions`, and kept in a
  registry keyed by the manager, so a second `NewReconciler` on the same
  manager reuses it;
- adds it to the manager as **two runnables** (IP4, the user's decision):
  - **the informers**, which are read-only and need no leader election, as
    the manager's own cache doesn't. They start at the first of the
    runnable's `Start` and `CredentialUseSource`'s `Start`;
  - **the write loops**: the release loop, the prober, Disown and the label
    janitor. They need leader election.

  **No release batch is lost while the write loops wait for election** (M1
  of the review of revision 16), but for what a flood of names overflows (§6.2,
  "Without the write loops"). The informers start before election, under
  warmup and on standby replicas, so they see holder deletions and
  `deletionTimestamp`s before any write loop runs. Two things keep them:
  - **the (K, N) batch queue is created by `NewShared`**, before any
    informer starts. It buffers and deduplicates from the first event, within
    a bound (§6.2), and
    the grace's `AddAfter` timers go into it too. The release loop drains it
    once elected;
  - **the write loop's `Start` rescans**: it enqueues the batch of every
    (K, N) finalizer on every object in the protected stores, and in PerName
    and PerNamePoll modes of every line of the index (§10.6.5). That covers
    whatever a standby's informer saw before this process's queue existed,
    and a leader that died between `Forget` and R3. Say the leader removes
    H's owner finalizer and record and dies before R3. The standby's holder
    informer delivers H's deletion. Once elected, its rescan enqueues P's
    batch, R2 finds no record naming P, and R3 releases it.

  One leader-elected runnable would break controller-runtime's
  `EnableWarmup` (`pkg/config/controller.go:70-78`): sources then start
  before election, and the source's `WaitForSync` would time out on
  informers that hadn't started. Safety doesn't depend on the split
  (§6.8).

  **A process whose write loops haven't started reconciles nothing** (the
  user's decision, 2026-11-23). A controller with `NeedLeaderElection` set
  to false would reconcile on a standby, and what its holds owe after them,
  the check of an A2 whose outcome is unknown, the re-adds and the releases,
  would wait for an election that may never come. So until the write-loops
  runnable has started in this process, each pass of a kind with
  credential-use does nothing and returns an error that says so, and a pass
  still without them 2 minutes later (`sourceStartTimeout`) fails the manager. A
  leader's passes may start just before its write loops, and are retried;
- calls `Track`. Its `Enqueue` forwards to the queue `CredentialUseSource`
  receives in `Start`, and does nothing before then. Nothing is lost by
  that: no `Reconcile` runs before the source has started and synced, and
  the controller's own `For` source delivers every holder once at start.

**Configuring it.** Per-kind settings (protected kinds, watch mode,
`Content`, the grace and the intervals) are
`reconciler.WithCredentialUse(credentialuse.KindOptions)`. The default
protects Secrets, LabelScoped. The implicit `Shared` uses the default
`SharedOptions`. A provider that needs others (`Namespaces`,
`IndexNamespace`, `Listers`) builds its own `Shared` and passes
`reconciler.WithCredentialUseShared(s)`, which is supported in production as
well as in tests; it then fills `Clients` itself, `Discovery` too (§7.2). The
adapter sets `TimeoutBound` to its reconcile timeout
plus grace.

**The owner finalizer under a custom `WithFinalizer`.** `APIFinalizer`
gains `FinalizerName() string`, through a new optional interface,
`resource.NamedFinalizer`. A custom `resource.Finalizer` that doesn't
implement it, on a connecter that implements `CredentialRefsProvider`, is a
setup error: the runtime can't tell established holders from new ones.
`NamedFinalizer`'s contract is that `RemoveFinalizer` writes the object's
annotations together with the finalizer change, as `APIFinalizer`'s Update
does, so that `Forget` lands in the same write (§8). Where a custom one
doesn't, §6.1's exclusion still keeps the stale record harmless.
While provider-runtime#46 migrates the finalizer's name, the adapter passes
both the old and the new name in `OwnerFinalizers`, for the whole migration.

**The builder line, and the predicate, are required** (NQ3, decided). The
tracker enqueues holders when a protected object changes (§10.2), so it
needs a handle on the controller the provider builds after
`NewReconciler`. The provider writes:

```go
r := reconciler.NewReconciler(mgr, kind, reconciler.WithExternalConnecter(c), ...)
return ctrl.NewControllerManagedBy(mgr).
	Named("myresource").
	For(&v1alpha1.MyResource{}, builder.WithPredicates(r.IgnoreCredentialUseRecord())).
	WatchesRawSource(r.CredentialUseSource()).
	WithOptions(o.ForControllerRuntime()).
	Complete(r)
```

`r.IgnoreCredentialUseRecord()` drops a holder update event only if its only
change is the record or the `unheld` annotation (setting aside
`resourceVersion` and `managedFields`), **and** both equal what this process
last wrote for that holder (L8). A hand edit of the record, or another process's
write, still triggers a pass. That removes the extra pass A1 would otherwise
cause (§6.6, M6), and nothing else. **It is a method on the reconciler**
(IP3, the user's decision), not a package-level function as an earlier
revision showed: what this process last wrote is tracker state. An outside
runtime filters its holder events the same way, with `t.IsOwnRecordWrite`.

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
- **the Namespace informer failing to sync** (L3), as for the source
  check below. The consent gate and §5's namespace rows depend on it. So a
  missing `list` or `watch` on namespaces fails start with
  `credential-use: cannot sync the Namespace informer: grant get, list and watch on namespaces`.
  Outside runtimes get the same error from `Run`;
- **a tracker whose `CredentialUseSource` was never started** (M4).
  controller-runtime starts a controller's sources, and waits for the
  syncing ones, before its workers, so the first `Reconcile` proves it. If the source hasn't been started by
  then, that `Reconcile` does nothing (no Acquire, no Connect) and signals
  the runnable, whose `Start` returns
  `credential-use source for <kind> was not started: add .WatchesRawSource(r.CredentialUseSource()) to the controller`.
  If no `Reconcile` comes, the write-loop runnable also fails 2 minutes
  after leader election with the source still unstarted.

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
     Connect as today. It never gets `MustNotConnect`. An A2 rejected as
     Deleting (422) is Degraded in the same way (§11.4).
   - **A ref with a `Next`** is such an error in this release
     (`ErrChainsNotSupported`, §9), for both, before A1, and after the gate
     of what the record names (§11.3). Its event is recorded once (§9).
   - **`a.Pending` and `a.RetryAfter`** (IP2). In the RBAC-propagation
     window of §10.7.2 (`a.Pending`), the pass stops before Connect, new and
     established holders alike, writes nothing, and
     returns `RequeueAfter: a.RetryAfter` with a nil error, so no failure
     backoff is consumed and no worker sleeps. This is the one exception to
     "return the error so backoff applies". After a failed live consent read
     that kept its hold (§11.3), the pass proceeds to Connect, and requeues
     at the smaller of `a.RetryAfter` and its poll interval. `a.RetryAfter`
     backs off per (holder, ref), from 1s doubling to the poll interval
     (§11.3).
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

**Not in this release: a chain is refused** (the user's decision,
2026-11-24). The first tag holds only the objects the refs name themselves.
Reviews of the built feature found defects that only chains reach. So the
chain code stays, and stays tested, but nothing in production reaches it:
`Acquire` refuses a list in which any ref has a `Next`, with
`ErrChainsNotSupported`. The next release removes the refusal. Until then:

- **What to do.** Name each object Connect reads as a ref of its own, with a
  nil `Next`. An intermediate object may be named so, and is held, but what
  it leads to isn't. A Secret whose name is only inside another object can't
  be named, so it stays unheld, as before credential-use. `CredentialRefs`
  must leave it out, and Connect reads the intermediate object itself, as it
  does today, because `Resolved` has nothing. §11.9's checks then report that
  read, rightly: `CredentialUseCheckingReader` logs and counts it, and
  `AssertCredentialRefsCover` fails on a fixture whose Connect makes it. **So
  the helper can't be used for such a fixture in this release**: leave the
  fixture out, and test its refs directly. A list of reads to allow would be
  an exported argument that the next release makes useless.
- **What is refused.** The whole list, its direct refs with it, when any ref
  in it has a `Next`: a Secret's, a `Guaranteed` ref's (§9.3), one in another
  namespace, one of a kind the Disown switch covers, or the second of two
  refs to one object. Holding the direct refs alone would write a record
  that drops what a chain held, and would look like protection. A
  `Guaranteed` ref without a `Next` is accepted, as before.
- **Where.** In the protocol's `acquire`, before it looks at the list: before
  A1 and A2, and before any read that the list asks for. Every pass of every
  runtime goes through it, and it is the one place that follows a `Next`. A
  list that passes has no `Next`, so no pass ever finds a ref that way:
  nothing deeper is left to refuse. `BeforeEstablish` refuses the same refs,
  and a pass that was refused, so that nothing reaches A2 that way either.
  `AssertCredentialRefsCover` (§11.9) fails a connecter that names one.
- **What runs before it: the gate of what the record names** (§11.3), which
  begins every pass, whatever the pass comes to. It asks about the refs the
  record names in another namespace, never about the refs `RefsFunc` named,
  so a chain makes no call of its own. The one write it may make drops a ref
  whose target withdrew consent. **A holder whose record names no such ref, as
  a new one's, makes no call at all.** Whoever edits a holder can write its
  record, and that costs a refused pass what it costs any pass: §11.3's 16
  refs and 5s, and no more. Were the refusal first, a tenant could set the
  field that makes the provider return a `Next`, and keep another namespace's
  Secret held against its owner's withdrawal (found in the review of the
  refusal).
- **It isn't an option.** Only this repository's tests can turn chains on,
  through an unexported field. No option, environment variable or exported
  function does.

| The holder | What it sees |
|---|---|
| New (no owner finalizer), with no record that names a ref in another namespace | `MustNotConnect`. No call by the core, no Connect, no finalizer. Synced=False/ReconcileError with the error, which names the ref; Ready=NotObserved; backoff |
| New, with a record that names a ref in another namespace | The same, after the gate, which may write the drop of a withdrawn ref. It isn't established, but it counts as a user (§6.1), so without the gate it would keep that ref held against its owner |
| Established | `Degraded`. Nothing written but the gate's drop, if any: the rest of its record, its `unheld` list and its holds stay as they are; a record that can't be parsed stays so (§4). A release reads that same record, so it takes only what §5 always lets it take. On a pass whose Synced is True/ReconcileSuccess, its message says `credential-use: refs refused, not held: <the error, cut> (§9)`, for as long as it is refused; any other Synced (an Observe or Connect error, a policy report, a deferral, a paused holder) says its own outcome instead (decided 2026-11-25; §12). `CredentialUseDegraded` and a log line once (below), the counter every pass, under the reason `ChainsNotSupported`, and the gauge `credentialuse_refused_holders`. It connects as today, and `Resolved` finds nothing |
| Established and being deleted | The same, then its external Delete runs, and `Forget` removes the record in the write that removes its finalizer, which releases what it held |
| Being deleted, without an owner finalizer | provider-runtime doesn't acquire on it (§8 step 5), so nothing is refused |
| It names no `Next` again | The next pass writes the exact record, holds what it names, and releases the rest |

What an established holder loses while it is refused is upkeep, and, but for
the two cases below, no hold: a
ref that is new, or an object recreated since, isn't held, a lost finalizer
isn't written again, and the missing-ref prober forgets the holder (§10.3),
since no pass of it could hold what the prober finds. A withdrawn consent is
acted on, as in every pass (above); **that hold is one that goes and doesn't
come back while the holder is refused**, if the owner grants consent again.

**The event is recorded once per holder**, with the log line, until a pass of
it isn't degraded, or it is forgotten. That is of a refusal alone: when the
gate's write fails in a refused pass, the pass is reported as any failure,
under the event throttle, and counts `Announce` as well, since a withdrawn ref
stays held meanwhile. This state lasts until the provider or
the object changes, so the event throttle alone would let one through per
holder every 10 minutes for as long: 17 a second for 10,000 holders. A restart
records each once more. **The event doesn't last**: the API server drops it
after its `--event-ttl`, about an hour, and client-go's broadcaster queues
1,000 events and drops the rest, so after a restart with many refused holders
most of theirs are never sent. **What lasts** is Synced's message on the holder,
which a refused pass sets in every status write it makes, so it costs no write
of its own and keeps its transition time (a pass that stops at the trace marker,
or on a conflict, writes no status, and changes nothing); the gauge
`credentialuse_refused_holders`, which counts the holders whose last pass in
this process was refused, new and established, and drops one at a pass that
isn't refused, at `Forget` and at the holder's deletion; and the counter, every
pass. A new holder's message is its condition's, as for any failure. The log
line carries the ref whole, for an established holder only, and unbounded:
however long the name, and with its line breaks unless the logger escapes them.

**Disown (§7.4) is no remedy for a refused new holder**: it doesn't connect
while its refs have a `Next`, whatever the switch covers. An established one
isn't in Disown's way: the loop strips its record and the finalizers, a
refused pass writes neither back, and the switch settles. Nor does a refused
pass write them back once the switch is removed: those holds are the other
ones that go and don't come back while it is refused.

**A holder that held through a chain under an older build** is the
established row, or the new one if it hadn't got its finalizer. No release
had chains, so there is none outside a development cluster. Its record names
the intermediate object and the Secret. Both stay held until it is deleted,
or until its refs name no `Next`. A re-point of the intermediate object
isn't followed: the old Secret stays held, and the new one isn't.

The rest of this section, and §10.4, describe chains as the next release has
them.

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
				return nil, fmt.Errorf("KubernetesTarget %s has no spec.kubeconfigRef.name", kt.GetName())
			}
			return []credentialuse.Ref{credentialuse.SecretRef(kt.GetNamespace(), name)}, nil
		},
	}}, nil
}
```

The tracker follows the chain level by level, and **holds each object
before it reads it**, except a hop that §11.3's gate doesn't let it hold.
That hop is read live, without a hold, and its `Next` is still followed
(§9.4):

1. A1 for level 1, then A2 for the KubernetesTarget.
2. Read the KubernetesTarget: from its content cache, or with a live GET if
   the cache doesn't have it yet.
3. Call `Next`.
4. A1 again with the union, then A2 for the Secret.

An A1 between levels only adds to the record: it writes the chain so far
added to the record as it stands, and only the last A1 of the pass writes
exactly what the whole chain names. Otherwise a pass over a re-pointed chain
would first write a record without the Secret it held behind the
intermediate object, and a concurrent release could take that Secret's
finalizer before the pass reached it again (found building PR 4).

The record names both, and both carry the finalizer. Every function the
provider supplies is pure, and the hold-before-read ordering belongs to the
runtime. `MaxDepth` bounds the chain. A `Next` error, or a chain deeper than
`MaxDepth`, is an Acquire error: a new holder fails closed, and an
established one fails open with its record as it was (§8). **A hop with a
`Next` that a pass can't hold**, Missing or Deleting, isn't read, so what
lies behind it is unknown: that pass's record keeps every ref the record
already named, which stay held while the holder may still read through the
hop (found in the review of PR 4).

**A `Next` must return an error, never nil refs, when the object lacks what
it reads.** The object may not be populated yet, or may have lost it in a
restore. An error keeps the holder's record, and so its holds. Nil refs say
that nothing lies behind the object, so the last A1 drops the Secret from
the record while the holder still uses it. Missing is not none. And a `Next`
must not put the object's values in its error, which becomes an event and a
log: the example above doesn't wrap `err`, because `NestedString`'s error
prints the value it found.

**Refs of one object.** Of the refs of one object that a pass meets, at any
level, the first wins: a later one ends there, whatever it asks for, so a
cycle ends. That is right when the later one asks for no more than the first,
and silently not what it asked for when it asks for more. A later ref **asks
for more** than the first in two ways:

- **a hold:** it isn't `Guaranteed` (§9.3), and the first is. The object is
  neither recorded nor held;
- **a `Next`:** it has one, and the first has none. What lies behind the
  object is neither found nor held.

A `Guaranteed` ref after a held one, or a ref without a `Next` after one with
one, asks for no more. Nor does a second, different `Next`: two functions
can't be told apart, so the first is followed and the other never called.
Each later ref is compared with the first, which wins, and with no other.

The pass goes on as if the later ref weren't there. It is not an Acquire
error: a `Next` names objects from data a tenant may write, and an error would
let that data stop the holder. Each conflict is a warning in the log, naming
the holder, the object and what was asked, and counts in
`credentialuse_refs_conflicting_total{asked}`, `asked` being `hold` or `next`
(§12). **In a provider's tests it is refused**: `AssertCredentialRefsCover`
fails on a conflict (§11.9). While chains are refused (§9, above), only a hold
can conflict: a list with a `Next` is refused before any ref is met, and that
pass reports no conflict, though its list may have one.

A hold of a kind the Disown switch covers is not reported, since no ref of it
is held (§7.4). Any other hold asked for is, even without its target's consent
(§11.3): learning that would cost reads on every pass, before the A1.

**A conflict is reported once while it lasts**, per holder, object and thing
asked for. What was reported is forgotten when the holder's refs no longer
conflict so, at its `Forget`, and when it is deleted, so a holder keeps no
more than its refs ask for. Refs behind a hop the pass can't follow are
unknown, so only a pass that met all the holder's refs forgets. One of direct
refs meets them all before it writes anything. A chain that is never
whole keeps what was reported of it until the holder goes. The log has one
line for each conflict of each holder after each restart of the process, and
one each time an edit brings a conflict about.

The first is first in the order the pass meets refs: by level, and at the
first level in the order `CredentialRefs` returned them, so a conflict among
those is the same on every pass. Below it, the refs come behind the hops of
the level above: first behind the hops found unheld before the level's A2s,
for want of consent or Disowned, in their level's order; then behind those
to hold, by ID; then behind the `Guaranteed` ones, in their level's order;
and each `Next`'s refs in the order it returned them. So which of two refs
found through two hops is first can change with consent, and with it, if they
conflict, whether the object is held. A provider that names one object
through two hops gives both refs the same `Next` and the same `Guaranteed`.

**A Secret that names a Secret** is in scope the same way: the first
Secret's `Ref` has a `Next`. It needs that Secret's data, which is §10.4's
opt-in.

### 9.2 `Resolved`, and the window it doesn't close (U3)

`Resolved` is built now. Connect reads `Resolved(ctx, ref)` to get exactly
the intermediate objects that were held, not a newer cached copy. Without it,
Connect could read a re-pointed KubernetesTarget naming a Secret this pass
never held.

**It carries unheld hops too** (L1). An unheld hop (§9.4) is read live by
the tracker, through the shared 10s read, and `Resolved` returns that very
read. So Connect uses exactly what the tracker read and followed. The
effect:

- after a re-point of an unheld hop, the holder stays on the old chain for
  up to 10s, until the shared read is refreshed;
- the holder never uses a same-namespace Secret that was not held: the
  tracker follows the new hop's `Next` and holds what it names in the same
  pass, before Connect is given the new hop.

**`Resolved` alone doesn't close the window** (MED-5). If the
KubernetesTarget is re-pointed after this pass, the holder keeps connecting
through the old, still-held chain until it reconciles again. So the tracker
also **enqueues holders through the record index** when a held protected
object with a `Next`:

- changes generation, or changes through someone else's write. A change
  counts as the tracker's own only if the two versions are equal once every
  `credential-use.` label, annotation and finalizer, `resourceVersion` and
  `managedFields` are set aside (L2). For a metadata-only kind, a change to
  data can't be told apart, but it doesn't need to be: a held ref with a
  `Next` must be of a kind with `Content` (§10.4);
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

In this release a `Guaranteed` ref must have no `Next` (§9); what is said of
its `Next` here is the next release's.

A CompositionDefinition refuses Delete while compositions exist
(`ErrCompositionStillExist`). So for a composition, the CompositionDefinition
can't vanish first, and holding it would add writes and nothing else. A
provider declares this with `Ref.Guaranteed: true`. A guaranteed ref:

- is not recorded and gets no A2;
- has its `Next` followed, so the chart's `passwordRef` Secret behind it is
  still held;
- is read through a lister the caller supplies for that kind
  (`SharedOptions.Listers`), or with a live GET if there is none;
- enqueues no holder that names it only as `Guaranteed`, whatever changes in
  it, even when its kind is protected and watched because another holder
  holds the object: such a holder sees a change on its next pass;
- counts as covered for `AssertCredentialRefsCover` (§11.9).

**The fallback is flagged as unverified.** The guarantee depends on the
compositions carrying the labels the CompositionDefinition's check selects
on. Where they might be missing, the guarantee doesn't hold. `RefsFunc`
must then set `Guaranteed` only for objects that carry them, and return a
normal, held ref for the rest. Whether every composition carries those
labels is for core-provider to verify.

### 9.4 Chains that consent in part

Each hop of a chain is held or not on its own, by §11.3's gate. A chain can
therefore be partly held, and that is still full protection against the
deadlock this design fixes. That deadlock needs the holder's *own*
namespace teardown to delete something the holder's deletion reads, and
that teardown deletes only objects in the holder's namespace. Every
same-namespace hop is always held.

**A held intermediate with an unheld Secret elsewhere.** Holder H is in
namespace A. Its Configuration C is in A, and C's Secret S is in B, which
hasn't consented. C is held and S isn't. Tearing down A deletes C, which is
held and stays readable, and never deletes S, which lives in B. So H's
deletion pass can read both C and S. C's hold is not "held for no benefit":
without it, A's teardown could delete C first, and H could never find S.
The only remaining case is B being deleted while H lives, which is §11.3's
recoverable residual.

**The opposite shape: an intermediate elsewhere, with the Secret at home.**
H and S are in A, and C is in B, which hasn't consented. S is held and C
isn't. The tracker still follows C's `Next`, with a shared live read, so S
is found and held. Tearing down A deletes S, which is held, and never C. So
H's deletion reads both. The remaining case is again B being deleted while
H lives: C is gone, and H fails until C is recreated, which is
recoverable.

**The cost of an unheld hop.** It isn't labelled, so §9.2's enqueue doesn't
see it re-pointed, and it isn't in the label-scoped cache. It is read live,
and **that read is shared** (L6). Holders reconciled within one pass batch,
or within a 10s TTL, reuse it, so a Configuration shared by many holders
costs one GET per 10s, not one per holder pass. A re-point of C is then
picked up within 10s plus the holder's next pass.

**Why reusing the read is safe.** No decision to *release* reads an unheld
hop. Its read only decides which refs lie behind it, and each of those is
then gated and held with its own live checks. A read up to 10s stale can,
at worst, keep holding a ref the hop stopped naming, which is released by
the normal protocol once the record drops it. Or it can find a newly named
ref up to 10s late, which is the re-point window of §9.2 that already
exists.

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
- an object a third party left with K's marker but **not the held label**
  is outside every cache, which selects by the held label, yet
  `scaletozero` selects on the marker (§11.6). The write loops' start, and
  a Track while they run, list each protected kind live, in every
  namespace, by K's marker without the held label, and the janitor removes
  the marker (the review of #114).

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
holders cost about 5 MB. Under provider-runtime, the manager's cache also
watches the holder kind, with typed objects. The core can't use that cache
(§7.1), so the kind is watched twice. Its handler maintains `recordSince` (§6.3).

Its index maps each recorded ref to holders. The index serves:

- **R0's fast path**;
- **mapping holder events to release batches**: a create, an update whose
  record or owner finalizer changed, and a delete map to the protected
  objects in the old and the new record, and enqueue their (K, N) batch.
  **A holder that stops being a user of everything** (its record couldn't be
  parsed, and it is gone, repaired, or deleting without its owner finalizer,
  §6.1) enqueues the batch of its whole namespace: every object that carries
  K's finalizer for N, in the caches, and, where they don't cover N, in N by a
  live list (found in a review of the release's cost, 2026-11-24). What it kept
  is in no record, and an object already being deleted has no event left to
  come, so nothing would look at it again before the write loops' next start: a
  namespace's teardown waited on a holder that was gone. So does a holder that
  a relist finds gone, or replaced by another of its name, after a watch gap:
  its record may have become unparseable, or have named others, in the gap.
  - **A pass's own repair** writes all the refs it records in one A1, where they
    are all direct, as every pass's are in this release (§9): so the batch finds
    them recorded. A refused chain's pass writes no record, so one that can't be
    parsed stays, and counts, until its refs name no `Next`. **A chain that is
    followed,** by the tests only, repairs the record with the chain so far, at
    its first level's A1, and until its last one a batch may release what lies
    behind a hop. The release that follows chains must write no A1 over a record
    it can't parse until it knows the chain;
  - **Not covered**, and found by the write loops' next start, or by a holder
    that names the object:
    - a hold for N on an object of another namespace M that the caches don't
      cover (§10.5). M's own teardown doesn't help: it queues M's batch, not N's;
    - an object in a namespace that the caches cover, whose held label someone
      stripped: no label-scoped cache has it (§10.1);
    - a PerName name whose watch is forbidden, or hasn't synced: the fill reads
      the watches. Once it syncs, its add queues the object's batch;
    - a holder whose record became unparseable and was repaired inside one
      watch gap, with what it kept meanwhile; and one whose deletion no process
      saw because its kind's CRD went with it.
  - **Its cost.** The events queue it once, however big the record and however
    often it changes while it waits; what its own failures and graces requeue
    is the release loop's (§6.2). It reads every cached object of every protected
    kind, with no call; where the caches don't cover N, it lists N's held
    objects live, once per protected kind. Objects that aren't being deleted,
    with users in the cache, and those with a deleting user, cost nothing more;
    the others, a deleting object with live users among them, cost what R1 and
    R2 do. Whoever can edit a holder can cause one for each pair of writes of
    its record, or for one write and a pass of the holder, which repairs it. An
    object that a batch fails at is forgotten as failing by the next that keeps
    it by R0, as for the unparseable holder (§6.2): so toggles restart its
    backoff, and its retries go at about their rate, beside those batches;
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

**Any change to K's finalizer or epoch on a protected object** also
enqueues its (K, N) batch, in the watched modes. R0's fast path settles it
at once when users exist.

**The deletion of K's CRD** (2026-11-24) enqueues the (K, N) batch of every
finalizer of K on every object in the protected caches, the per-name ones
included, as the start's rescan does. With the CRD gone the holder informer
delivers no more deletions, so nothing else is certain to queue a hold whose
holder it missed: only a change to P itself would. The recheck of what the
caches can't show runs too, on the process whose write loops run: the objects
outside them, where `SharedOptions.Namespaces` restricts them (§10.5), and in
PerName mode those the index names (§10.6.5), whose watch may be refused. Of
the rechecks that a CRD's deletion starts, one runs at a time for a tracker,
and those asked for while it runs are one. That one cuts the run's wait short,
never its work: a run that waits to retry a part that fails, as a kind it can't
list, starts again at once with every part, and nothing counted as done, since
what a part read before the deletion may have changed; a run that doesn't wait
runs once more when it ends. No part in flight is cancelled, by choice: none
needs it, since an index prune finishes its write and its check of what it
removed under its own bound, whatever the recheck's context (§10.6.2). Without
the cut, a part that fails for good would keep the run on its backoff, and a
hold that only the recheck finds would wait for a restart. A CRD that is back before the batch keeps R0 as it was: the holder informer lists
again, within its backoff, and delivers what it missed.

**And the acquirer enqueues the batch itself** after any A2 issued from a
view it can't vouch for (L2). That is always so for a cross-namespace ref,
whose consent can change under it, and for every A2 in PerNamePoll mode,
where no watch would deliver the A2's write. Both are the backstop for an
A2 from a stale view (§11.3). The self-enqueue doesn't depend on the mode.
Because it runs after the A2 returns, the batch's consistent list (R2) runs
after the A2 too.

**On startup**, the protected informers' initial list enqueues a release
batch for every (K, N) of every held object. In PerName mode, the index
does instead (§10.6). So every hold is checked again after a restart, and a
holder delete missed while down can't strand a finalizer. **And again when
the write loops start**, after election, from the protected stores and the
index (§7.5, M1 of the review of revision 16), because the informers may
have started long before, on a standby.

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

Nothing needs `Content` in this release, which refuses a `Next` (§9). A kind
that sets it still costs the memory, and the exposure, described here.

`Next` needs the object's content, not just metadata. For a protected kind
whose refs have a `Next`, `ProtectedKind.Content` makes that kind's informer
a **dynamic (unstructured) informer**. It is scoped by the same label, and
its transform drops `managedFields` and `status`. It needs the same verbs as
§13. Memory is the size of the held intermediate objects, typically a few
per namespace.

**On Secrets, `Content` is opt-in and holds Secret data in memory**, for
held Secrets only. That is needed only for a Secret that names a Secret.
A provider that sets `Content` on Secrets takes on the memory and the
exposure. Providers whose cached client already reads all Secrets hold that
data anyway.

**A held ref with a `Next` must be of a kind with `Content`** (the user's
decision, 2026-11-21, from the review of PR 4). An earlier revision gave a
`Next` on a kind without it a metadata-only object, but a re-point of such
an object would go unseen: the metadata caches keep only `credential-use.`
keys, and a metadata change moves no generation, so nothing would enqueue
its holders (§9.2). Such a ref is an Acquire error that names the fix. A
`Guaranteed` ref isn't held, so it may have a `Next` on any kind. Every
`Next` is given the object without `managedFields` and `status`, however it
was read.

### 10.5 Namespace-restricted caches (R10)

The core builds cluster-wide informers. A caller that restricts them to
some namespaces (`SharedOptions.Namespaces`) can't cache refs outside those
namespaces. Such a ref is read live, every pass. If even the live read is
denied there (class (d) of §6.2, RBAC), Acquire returns an error: fail
closed for a new holder, fail open for an established one (§8). The doc text says so, and that such
refs need §13's cluster-scoped RBAC.

**What is restricted.** Only the protected-object informers, one per
listed namespace. The holder informers and the Namespace informer stay
cluster-wide, as §13's ClusterRole verbs on the holder kind and on
namespaces already allow: the release batches start from holder events
(§10.2), and §5's namespace rows read every namespace.

**A deletion outside the cached namespaces** reaches no informer, so
nothing enqueues the object's batch or starts its grace timer. The rule:
when a pass's A2 holds an object outside the cached namespaces and A2's
response shows it being deleted, the acquirer enqueues the object's
(K, N) batch, and R1 measures the grace from that first sight (§5, L4).
The cost is liveness only: such an object is released on the next pass of
a holder that reads it, not at once, and never earlier than the table of
§5 allows (PR 3b, 2026-11-19).

**Held objects nobody reads any more** (the user's decision of
2026-11-19: hold them, plus live rechecks). A restricted process still
holds the objects its holders name outside the cached namespaces. But a
holder deleted while the process was down, or a leader that died between a
holder's Forget and R3 (§7.5, M1), leaves an object outside the caches
that no holder pass reads and no informer delivers; the startup rescan
(§10.2) covers only the caches. If its namespace is then deleted, the
object's finalizer keeps the namespace Terminating, the failure this
design exists to prevent. Two more triggers release such objects:

- **a namespace outside the caches starting to delete**: the Namespace
  handler enqueues each tracker's (K, N) batch for it with no objects, and
  the release loop fills it with a live list of the objects there that
  carry the held label and K's finalizer for N;
- **the write loops' start, a Track while they run, and the deletion of the
  kind's CRD** (§10.2): one live list per
  protected kind, in every namespace, selected by the held label, finds the
  held objects outside the caches, and enqueues the batch of each of their
  (K, N) finalizers. A failed list is retried with the release loop's
  backoff until it succeeds: a recheck that is skipped can strand a hold.
  Only what failed is retried: each kind's lists, and each index, alone.

A hold written outside the cached namespaces while the write loops run, by
anything but a holder pass, as by hand, and that no holder's record names,
reaches no informer and no batch: it waits for the next of these triggers.
One that a record names is queued by its holder's events, as any other
(`TestAHolderKindThatReturnsNamespacedUnderARunningProcess`).

The lists only find what to recheck; R1 to R3 then read and write each
object as for any batch, so nothing is released earlier than §5 allows.
They need list on each protected kind **in every namespace**, which such
refs already need (§13's cluster-scoped RBAC): a restricted process
checks it before it starts and refuses to run without it, naming the
grant.

**The waiting gauge** counts the cached objects from the caches on every
replica (§12). Outside the caches it counts what this process's release
loop last found kept for users, so only the process that runs the write
loops reports those, and it forgets them when its write loops stop. It is
a batch's view: an object a batch fails at keeps its last count until a retry
succeeds, and an object whose finalizer a third party strips stays counted
until a batch covers it again. It is counted per object, not per
(object, N), which is exact while no hold crosses namespaces (cross-namespace
refs are unheld until consent, §11.3).

**An empty batch keeps its meaning.** (Its list is made once, and its objects then
take their turns, §6.2.) A batch added with no objects asks
for every held object of its namespace, and a later addition of objects
to the same (K, N), as a holder's event during a teardown makes, must not
turn it into a batch of those objects alone. The queue keeps a marker for
it with the batch's objects, and the release loop lists the namespace
beside them (the re-review of #114).

### 10.6 PerName mode: the index ConfigMap (M1)

Under `resourceNames` RBAC a tracker can't list a protected kind, so it
can't find what it holds by label. Without help, four things would break,
because each starts from "every object this kind holds":

- the startup recheck (§10.2);
- Disown and its count (§7.4);
- `WithHeld` (§11.6);
- the rebuild after a lost event.

**The index** is one ConfigMap per holder kind. It lists the protected
objects that kind holds:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cu-index-repo.git.krateo.io
  namespace: krateo-credential-use  # SharedOptions.IndexNamespace
data:
  entries: |
    /Secret/team-a/git-token
    core.krateo.io/KubernetesTarget/team-b/prod
```

Each line is `<group>/<kind>/<namespace>/<name>`. The group is empty for
core kinds, and so is the namespace for cluster-scoped objects. The lines
are sorted. **The index must be a superset** of what the kind holds: every
protected object that carries a finalizer with K's prefix has its line. §10.6.3
proves it.

**A line must read back as its object** (decided 2026-11-24). One whose name or
namespace has a line break, or a blank at its end, as a value copied with one
may, would be read as another line, or as more than one, and a line that names
nothing is never pruned. **And its name, and its namespace, are
at most 253 bytes each**, a DNS subdomain's, so that a few refs can't fill the
ConfigMap. So no add writes any other, and a ref to it is refused before A2, as a
line that doesn't fit is (§10.6.4): a new holder doesn't connect, and an
established one is Degraded, with an error that quotes the line's beginning and
says which it is. In LabelScoped, which has no lines, the same ref is Missing,
unless there is such an object. *The limit:* most kinds' names are DNS
subdomains, or shorter, and the API server then admits none of these; an RBAC
kind's need only be a path segment, so one with such a name can't be held in
PerName, and a hold made before the rule has no line once something drops it.

#### 10.6.1 Where it lives, and who owns it

- **The namespace is explicit:** `SharedOptions.IndexNamespace` (CP2, L6).
  It must outlive every holder of the kind, so it must be a namespace no
  holder lives in.
- **The recommended value is a dedicated namespace that no holder can be
  created in**, such as `krateo-credential-use` (the user's decision, on
  core-provider's blocker). The provider's management namespace doesn't
  qualify. installer measured krateo-057: all 83 composition instances, of
  67 kinds, are in `krateo-system`. That is structural: the Installer CR
  sets `spec.namespaces: {"krateo": "krateo-system"}`, and moving a
  composition means uninstalling and reinstalling it.
- **installer ships the namespace, in its own release:**
  - a new declared key in `.Values.namespaces`, whose schema is today
    `additionalProperties: false` with only `krateo`;
  - the Namespace annotated `helm.sh/resource-policy: keep`, so that an
    uninstall never deletes the indexes;
  - **that release lands before any controller writes an index there.**
    Otherwise the first symptom would be a namespace not found. So start
    fails clearly when the namespace is absent (§7.3).
- **Admission keeps the invariant.** A ValidatingAdmissionPolicy in the
  index namespace allows only an explicit list of writes, and denies
  everything else. It is stronger than a rule that refuses only
  composition objects. Its three exceptions come from core-provider's
  review:

  ```yaml
  apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingAdmissionPolicy
  metadata:
    name: krateo-credential-use-namespace
  spec:
    failurePolicy: Fail
    paramKind: {apiVersion: v1, kind: ConfigMap}
    matchConstraints:
      namespaceSelector:
        matchLabels: {kubernetes.io/metadata.name: krateo-credential-use}
      resourceRules:
        - apiGroups: ["*"]
          apiVersions: ["*"]
          resources: ["*"]
          scope: "Namespaced"                # never a cluster-scoped kind: see below
          operations: ["CREATE", "UPDATE"]   # never DELETE: see below
      excludeResourceRules:                  # the Namespace object itself
        - apiGroups: [""]
          apiVersions: ["*"]
          resources: ["namespaces"]
          operations: ["*"]
    variables:
      - name: kubeControllers
        expression: >-
          request.userInfo.username in [
            "system:serviceaccount:kube-system:root-ca-cert-publisher",
            "system:serviceaccount:kube-system:service-account-controller",
            "system:kube-controller-manager"]
      - name: core
        expression: request.resource.group == ""
    validations:
      - expression: >-
          (request.resource.resource == "events") ||
          (variables.core && request.resource.resource == "configmaps" &&
           object.metadata.name == "kube-root-ca.crt" && variables.kubeControllers) ||
          (variables.core && request.resource.resource == "serviceaccounts" &&
           object.metadata.name == "default" && variables.kubeControllers) ||
          (variables.core && request.resource.resource == "configmaps" &&
           object.metadata.name.startsWith("cu-index-") &&
           ("system:serviceaccounts:" + params.data.managementNamespace) in request.userInfo.groups)
        message: >-
          only cu-index-* ConfigMaps from the management namespace's service accounts,
          events, and Kubernetes' own kube-root-ca.crt and default ServiceAccount
          may be written in krateo-credential-use
        reason: Forbidden   # without it a denial is 422 Invalid (dispatcher.go:309-315)
  ---
  apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingAdmissionPolicyBinding
  metadata:
    name: krateo-credential-use-namespace
  spec:
    policyName: krateo-credential-use-namespace
    validationActions: [Deny]
    paramRef:
      name: cu-policy-params
      namespace: krateo-system             # installer's; not in the index namespace
      parameterNotFoundAction: Deny
  ---
  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: cu-policy-params
    namespace: krateo-system
  data:
    managementNamespace: krateo-system     # the policy's parameter
  ```

  **The exceptions, and why each is needed:**

  1. **Kubernetes' own writers in every namespace.** In
     kube-controller-manager, the root-CA publisher creates and reconciles
     the `kube-root-ca.crt` ConfigMap, and the service-account controller
     does the same for the `default` ServiceAccount, in every namespace.
     They are allowed for exactly those two objects, by name, and only from
     those identities. **Verified in Kubernetes v1.36.2**
     (`k8s.io/kubernetes@v1.36.2` in the module cache):
     - their clients are created as `NewClient("root-ca-cert-publisher")`
       (`cmd/kube-controller-manager/app/certificates.go:269`) and
       `NewClient("service-account-controller")`
       (`cmd/kube-controller-manager/app/core.go:618`);
     - with `UseServiceAccountCredentials` on, those are service accounts in
       `metav1.NamespaceSystem`, that is `kube-system`
       (`cmd/kube-controller-manager/app/controllermanager.go:826-831`), so
       `system:serviceaccount:kube-system:root-ca-cert-publisher` and
       `system:serviceaccount:kube-system:service-account-controller`;
     - with it off, every controller uses the controller-manager's own
       credentials (`SimpleControllerClientBuilder`, `:823`), which is why
       `system:kube-controller-manager` is allowed too;
     - the ConfigMap's name is `RootCACertConfigMapName = "kube-root-ca.crt"`
       (`pkg/controller/certificates/rootcacertpublisher/publisher.go:42`).
  2. **The policy never matches DELETE**, so the namespace can be deleted.
     Matching DELETE would refuse the namespace controller as it empties the
     namespace, and recreate the deadlock class this design removes. The
     index needs no protection from deletion by admission: it carries no
     finalizer, so deleting it can't block anything, and RBAC limits who may
     delete it. A deleted index is recreated and rebuilt (§10.6.6).
  3. **The writers are dynamic.** Composition controllers run as
     `system:serviceaccount:<management namespace>:<resource>-<version>`,
     service accounts created at runtime (68 on krateo-057), so no static
     username list can name them. The rule matches the group
     `system:serviceaccounts:<management namespace>`, which every service
     account in that namespace belongs to, ANDed with the `cu-index-` name
     prefix. The management namespace is **the policy's parameter**, a
     ConfigMap installer owns in that namespace. git-provider's fixed
     service account in `krateo-system` fits the same rule.

  **And the Namespace object itself is excluded.** For a CREATE or UPDATE
  of a Namespace, a `namespaceSelector` is matched against that Namespace's
  own labels (`k8s.io/apiserver@v0.37.2/pkg/admission/plugin/webhook/predicates/namespace/matcher.go:68-85`,
  which policy matching uses too). So without the exclusion, the policy
  would refuse installer's own updates of `krateo-credential-use`. The
  revision 12 sketch said the policy "never matches `v1/Namespace`": that
  was wrong, and is corrected here.

  **The rule's `scope: "Namespaced"` is what keeps the policy to the index
  namespace** (the user's decision, 2026-11-22). A `namespaceSelector` never
  exempts a cluster-scoped resource: the matcher returns a match for any
  request with no namespace that isn't a Namespace
  (`k8s.io/apiserver@v0.37.2/pkg/admission/plugin/webhook/predicates/namespace/matcher.go:105-112`).
  Without the scope, the policy would match every CREATE and UPDATE of
  every cluster-scoped kind in the cluster, CRDs and ClusterRoles among
  them, and deny each one, since none passes the validation. With it, a rule
  matches only a request that has a namespace and isn't for a Namespace
  (`predicates/rules/rules.go:58-72`), so only the index namespace's
  objects. PR 8's envtest found it, when a deleted copy of the policy still
  in the API server's cache denied a CRD patch. The Namespace exclusion
  stays: the scope already leaves the Namespace object out, and the
  exclusion says so explicitly.

  For core-provider, the effect is that composition.krateo.io objects are
  refused there, along with everything else not on the list.

  **Every validation sets `reason: Forbidden`**, as the sketch does. A
  ValidatingAdmissionPolicy denial with no `reason` is **422, reason
  Invalid**, not 403. The dispatcher builds it with `admission.NewForbidden`,
  then sets the reason to Invalid when the decision's is empty, and derives
  the code from it (`pkg/admission/plugin/policy/validating/dispatcher.go:309-315`,
  `policy_decision.go:73-89`, which maps Invalid to 422). With the reason
  set, an ordinary denial is a 403, and can never be confused with A2's
  Deleting outcome, which is also a 422 (§6.2). **The config-error path
  stays 422 Invalid** whatever the validations say. It is built by
  `addConfigError`, which sets no reason (`dispatcher.go:75-100`).

  **Subresources aren't matched.** `resources: ["*"]` matches a request
  only when it has no subresource. The matcher splits each rule into a
  resource and a subresource, and `"*"` has an empty subresource
  (`k8s.io/apiserver@v0.37.2/pkg/admission/plugin/webhook/predicates/rules/rules.go:106-117`).
  So `serviceaccounts/token`, for example, is outside the policy, as
  core-provider checked. Only `"*/*"` would match subresources.

  **The parameter ConfigMap, and `parameterNotFoundAction: Deny`.** Stated
  at core-provider's request; the design doesn't change:

  1. **`cu-policy-params` and the binding ship in installer's same
     release**, together with the policy and the namespace.
  2. **That ConfigMap needs the same protection as the policy.** Nobody
     may hold broad `delete` on configmaps in `krateo-system`. Whoever can
     delete it can change what the policy does.
  3. **The symptom of its absence.** With the parameters missing, the
     binding fails before any expression runs: the apiserver reports
     "no params found for policy binding with `Deny` parameterNotFoundAction"
     (`policy/generic/policy_dispatcher.go:400-401`). Under
     `failurePolicy: Fail`, that turns into a denial reading
     "failed to configure binding: …"
     (`policy/validating/dispatcher.go:75-100`). So **every** CREATE and
     UPDATE in `krateo-credential-use` is denied, including
     kube-controller-manager's `kube-root-ca.crt` and the `default`
     ServiceAccount. `cu-index-*` writes fail with **422 Invalid**, not a
     Forbidden: class (c) of §6.2. New holders fail closed and established
     ones are Degraded. **An operator could misread this as missing RBAC,
     or as a Deleting outcome.** So the tracker, and the runbook, **match
     the symptom by message, not by code**: it contains both
     `ValidatingAdmissionPolicy 'krateo-credential-use-namespace'` and
     `failed to configure binding: no params found`. The runbook's first
     step on that message is to check that `cu-policy-params` exists.

  **Why Deny, not Allow.** With `Allow`, a missing parameter would let
  every write through. Anyone able to delete one ConfigMap could then
  switch the namespace's protection off, silently. With `Deny`, losing it
  fails loudly and closed: no holder gets a hold it can't keep, and
  nothing is written where the invariant no longer holds.
- **The start check stays**, but by construction it never fires. At start
  the tracker lists holders of K in the index namespace and refuses to
  start if there are any. At acquire time, a holder there gets no hold: a
  new holder fails closed, an established one is Degraded, and the tracker
  logs it. Admission makes both unreachable, and they stay as a backstop
  for a cluster without the policy.
- **Write contention is low.** There is one ConfigMap per kind, so an index
  has at most two writers at once (old and new pods during a rollout).
  That is core-provider's observation, and it holds generally, because a
  kind has one controller.
- **The location is fixed once, on the holder CRD.** The first process of
  the kind writes `credential-use.krateo.io/index: <namespace>/<name>` on
  the CRD, once, after a GET that finds it absent, with a merge patch guarded
  by resourceVersion (§7.3, "Writing the CRD"). Every later
  process reads it. A process whose `IndexNamespace` disagrees with the
  CRD's refuses to start (§7.3), so two processes of one kind can never keep
  two indexes. Moving the index means Disown, then re-pointing the
  annotation by hand.
- **The tracker owns the ConfigMap entirely** (M2, decided). Charts don't
  ship it. At start, the tracker creates it if it is absent, and rebuilds it
  from the records (§10.6.6), so a kind moved from LabelScoped starts with a
  superset. Create races between processes are harmless: the loser gets
  AlreadyExists and reads the winner's. Its name is
  `cu-index-<lowercase kind>.<group>`.
- **Nobody else should manage it.** If a GitOps tool does anyway, for
  example because it lives in a namespace Argo CD syncs, exclude its data
  from comparison and sync:

  ```yaml
  # in the Argo CD Application
  ignoreDifferences:
    - kind: ConfigMap
      name: cu-index-repo.git.krateo.io
      namespace: krateo-credential-use
      jsonPointers: [/data]
  syncPolicy:
    syncOptions: [RespectIgnoreDifferences=true]
  ```

#### 10.6.2 Writes: Dekker on both sides

Every write is a read-modify-write guarded by the ConfigMap's
resourceVersion. On a conflict, the writer re-reads and **merges**: it
re-applies its own adds and removes to what it read, so it never overwrites
another writer's change.

- **The acquirer adds before A2, then checks after.**
  1. Before A2 for a P whose line is missing, it adds the line. Its view of
     the index is the informer's. A pass that skips A2 also adds a line the
     view lacks.
  2. After a successful A2, it **live-GETs the index**. If the line is
     missing (a concurrent prune removed it), it adds it again. A check
     that fails fails the hold, and drops the A2's `written` entry, so the
     holder's next pass writes A2 and checks again.
  3. **An A2 whose outcome is unknown** (no response, or a 5xx or 408; not a
     404, which refuses it, whatever the read that follows says, §6.2) may
     have landed, or may yet (decided 2026-11-23). It gets the
     same check at once. Once `LandingMargin` is over, the write loops read
     P live, and add its line back if P carries K's prefix. Its release batch
     is enqueued both times, since its holder may be gone. These are
     deduplicated per (P, K, N), and another such A2 starts the margin over.
     §7.2 says how long a stop lets an A2, and its check, go on.
- **The pruner removes after R3, then checks after.**
  1. After a release batch's R3s, a turn of at most 64 objects (§6.2), it
     removes, **up to 50 in one write** (L1), the lines of the objects whose K's marker label R3 removed. Each
     write has the two steps below to itself, before the next.
  2. After that write, it **live-GETs each P whose line any attempt of it
     tried to remove**. If P carries a finalizer with K's prefix again (a
     concurrent A2 re-added it), it adds the line back. That covers the
     attempts that conflicted, whose line someone else may have removed
     meanwhile, and a removal whose response was lost.
  3. **Once begun, a write's steps 1 and 2 run to their end**, within 15s,
     whatever the context does: what is queued dies with the process, so a
     stop between the removal and the re-add would lose the line. The reads
     of step 2 are one GET a line, so 50 lines make 2.5s of a client's 20
     calls a second, where a teardown's 1,000 would make 50s. They end 10s
     after the write began, and the lines of the objects they didn't come to
     are put back in the 5s that are left, a vouched object's too, which
     fails the prune: its bare objects are retried. The 10s are an end, not a
     deadline the client sees: its rate limiter refuses at once a call whose
     turn would come after a deadline, and behind a backlog every read would
     then fail as an unread object's, its line put back with no error, so that
     nothing retried the prune. Waiting its turn, a read is cut instead.
     *Its cost:* behind a backlog of 10s or more, a write of a prune holds
     its release worker the 10s before it fails, where it failed at once, and
     within its 15s in all; and a read cut while it waits keeps its place in
     the limiter, a call's turn spent for nothing unless none asked after
     it, about one a write: the reads after it are cut before they ask. A
     write that fails ends the prune, and the lines after it stay.
     *Liveness:* what the reads found
     bare before the cut stays removed, so a prune always gains, unless the
     reads before the check, or one read of it, outlast the 10s: the live
     read of the switch before the removal, the removal with its retries on
     a conflict, and the read after it share them with the check. Then no
     write of it gains: its lines linger, and none is lost, until the reads
     are faster.
- **Lines for Missing refs** (revision 16). The line is added before A2,
  so a ref whose A2 finds NotFound has a line but no hold. It is kept while
  some holder's record names the ref: PerNamePoll needs its grant for the
  prober's GET (§10.3). The release batch prunes it, under the rule above,
  once no record names the ref and the object carries no finalizer of K.
  The records are the holder cache's, so none counts while K's CRD is absent
  from its informer, as in R0 (§6.2): a ghost's would keep its line for good.
  The recheck that the CRD's deletion starts prunes such lines (§10.2), those
  of refs that are Missing too. A prune rests on no record for safety, as
  below.
- **The startup recheck's prune** follows the same rule. It prunes the lines
  whose objects carry no finalizer with K's prefix, and that no record names,
  then live-GETs those objects, and adds back any that now do.
- **Disown keeps the lines of the kinds it covers until it has settled**
  (§7.4). Every prune above leaves them out, by the CRD's cache and then by a
  live read of the switch, without which it removes nothing; and puts back
  what it removed if a second live read, after its removal, finds the switch
  set and unsettled, or fails.
  Disown's own emptying, once settled, follows the pruner's rule too.
- **A re-add that fails** after a prune is retried by the write loops until
  it lands.
- **A prune leaves the line of a hold that may land yet** (the user's
  decision, 2026-11-24), though nothing names its object any more, and a read
  finds it bare:
  1. **One the process before sent.** For `TimeoutBound` + `LandingMargin`
     after a process's write loops start, and for a kind tracked while they
     run after its `Track`, its prunes remove nothing. With leader election
     the sender has ended, or lost its Lease, before they start. Disown's own
     emptying isn't kept back: it follows a run of zeros as long as that.
  2. **One this process sent.** No prune, Disown's emptying included, removes
     the line of an object for which this process has an A2 in flight, from
     before its line is ensured until that line is checked, or one of unknown
     outcome, until the landing margin is over. It looks before the removal,
     and after it puts back the line of one begun meanwhile.

  What it leaves is pruned later, under the rules above: the write loops read
  each object live once the wait is over, 10s for an A2 in flight. One whose
  read fails is read again 30s later; one the authorizer refuses keeps its
  line until a startup recheck. One of a kind the Disown switch covers is
  dropped from what is left, and is the emptying's, once Disown has settled
  (§7.4). What is left dies with the process, and the next startup recheck
  finds it.

  *The cost:* after every start of the write loops, a leader change included,
  the lines of what was released linger, 150s at provider-runtime's defaults
  (a `TimeoutBound` of 90s). Meanwhile they count towards the cap (§10.6.4),
  and each has its watch. Once they are due the write loops read each with a
  GET, in one tick: after a restart over 2,000 bare lines that is about 100s
  at a client's 20 calls a second, and the index's other ticks, of every kind,
  wait behind it, the live checks of dropped lines and the late checks among
  them. By argument alone 70s would do with leader election,
  since no A2 begins once its context has ended, one in flight goes on for at
  most 10s (§7.2), and the landing margin is 60s; it isn't modelled. The 150s
  also cover a pass that isn't cut short, but not a process that is paused past
  its Lease and goes on later.

#### 10.6.3 Why the index stays a superset

The reviewer's interleaving:

1. R2 sees no users of P.
2. H2, a new holder, announces, and sees P's line present, so it doesn't add
   it.
3. R3 removes the finalizer.
4. H2's A2 re-adds the finalizer (P is not being deleted).
5. The prune removes the line.

Now P carries the finalizer and has no line. Without the checks after the
writes, the index has stopped being a superset.

With them, let:

- tW be the time H2's A2 lands;
- tI be the time of H2's index GET after it, so tI > tW;
- tP be the time the prune lands;
- tQ be the time of the pruner's GET of P after it, so tQ > tP.

There are two cases:

- **tP < tI.** H2's GET sees the line missing, and adds it back.
- **tP > tI.** Then tQ > tP > tI > tW, so the pruner's GET sees the
  finalizer H2's A2 added, and adds the line back.

Both GETs are consistent reads, so in every ordering at least one side sees
the other. If both add the line back, the merge makes that one line. The
same argument covers the startup recheck's prune against a concurrent A2.
An add that fails (an error, a full index) stops before A2, so a finalizer
is never written without its line having been written first, or re-written
by the holder's next pass. That last case is an acquirer whose process dies
between A2 and its post-check: on restart its `written` set is empty, so the
holder's next pass adds the line before A2 again (L2).

**The residual is negligible.** The case above needs the holder never to be
reconciled again: a holder that never got its owner finalizer, deleted
while every process of the kind is down, after a prune had removed its line
and before any post-check ran. Its object then keeps the finalizer with no
line. The marker label still finds it (§10.6.6). A pruner killed between its
removal and its re-add leaves the same: P held, recorded, and without its
line, until the holder's next pass adds it, or another process whose index
watch showed the line checks its drop (§10.6.6). So does a re-add that
failed, and was queued in memory, as behind a backlog of the client's rate
limiter past what is left of the write's 15s, when the process is killed
before the write loops retry it. The startup recheck doesn't find it: it
reads only the lines there are. The holder's next pass does, since even a
skip ensures the line. **A stop drains** what is queued (decided 2026-11-25):
as the write loops end, after their last prune, they make every live check
the index holds in memory, whatever its backoff, again while some fail, the
pause between rounds doubling from 200ms, within 10s, the bound an A2 in
flight is given after a stop (§7.2). Those are the re-adds a prune couldn't
write, the checks an acquirer queued when its line check failed, the checks
of lines someone else dropped, and the late checks of A2s whose margin is
over, their batches queued too. The drain waits for the stop of the write
loops' own context, and for the acquirer calls begun before it, which may
queue more, and its 10s count from then (§7.2). A round's failures don't
count towards a check's backoff. What the drain leaves is logged, with how
many, and is the killed case: the checks left undone, the calls still in
flight at its cap, and the late holds whose margin isn't over, which aren't
checked early (condition 2 below); the prunes put off are counted beside
them. When the write loops end and the process goes on, as when a consumer
stops them alone while its informers run, the index's informer may queue
the check of a drop after the drain: the next write loops take it. A kill stays the
accepted case (2026-11-24). With
Disown turned on meanwhile it is the third residual, below.

**A hold that lands when nobody looks any more is the other residual** (the
user's decision, 2026-11-23). The argument above needs both checks to run, the
acquirer's after its A2 has landed. With a pruner that makes its own, an A2
leaves P with the finalizer and without a line only when all three of these
hold together:

1. **The sender makes no check after the A2**: its response is lost, or the
   sender is killed between the response and the check; or the check ran and
   failed, or its add did, as on a full index, so that the line is queued in
   memory only, and the sender ends before the write loops retry it.
2. **The late check doesn't see the landing** (§10.6.2): the sender ends
   within `LandingMargin`, killed, or by a stop whose grace period ends, or the
   write lands later than `LandingMargin`.
3. **A prune takes P's line before the landing**, because no record in the
   pruner's holder cache names P, and neither rule of §10.6.2 for a hold that
   may land yet keeps it. Two things are left that it can be:
   - **a landing later than the rules wait for**, whoever prunes: later than
     `LandingMargin` after its outcome was unknown, for the sender's own prune;
     or than `TimeoutBound` + `LandingMargin` after the write loops of its
     successor started;
   - **without leader election, the prune of another process** whose write
     loops have run for longer than that, and so have not just started.

The late check can come later than the margin, behind the write loops' other
work, as the deferred prunes after a restart (§10.6.2). That widens 2 from a
sender that ends within the margin to one that ends before its late check. It
doesn't widen 3: until the late check, the hold's entry keeps the line from
this process's own prunes, its successor prunes nothing for its quiet time,
and an A2 that lands within the margin has landed by then, so a release batch
after it reads P held. What can still take the line is what 3 names.

With one of them off, the line is there:

- **without 1**, the check sees the line missing and adds it back, or the
  pruner's check after it sees the finalizer: the two cases above;
- **without 2**, the sender, alive, reads P after the landing and adds its
  line back, and its release batch releases P if its holder is gone;
- **without 3**, nothing takes the line: a prune leaves the line of an
  object that a record in its holder cache names, and that of a hold that
  may land yet (§10.6.2).

**The remedy** is to remove the finalizer by hand. Its exact form is
`credential-use.<kind>.<group>/<namespace>` (§3); the marker label finds the
object (§10.6.6); and once the CRD is gone, `DisownPrefix` with `Names`
removes it.

**What the model shows** (`TestModelLateHold`). It searches one acquirer, one
release batch of P in another process, with its prune, the holder's deletion,
the late landing, and the late check with its batch, through the code of the
acquirer, the release batch and the index. With all three conditions, in four
ways (lost and killed within the margin, lost and landing past it, killed
before the check, lost and killed before it), it reaches P held without its
line. With any one off, in seven cases, it reaches none.

- **Its bound** (under `CREDENTIALUSE_MODEL_DEEP`; CI's is in §14): every
  schedule with at most three preemptions, and two in
  the three cases where the late check and its batch race a holder that goes;
  the control of the sender's own prune, below, has one.
  Every search finished, within 2,000,000 schedules (the largest, 967,369, at
  5a72b68). That, and a5340fe, ae2012d and 7fd7022 here and in §14, are heads of
  this change's own branch before its rebase: the history doesn't have them.
- **What it is given:** whether the landing is within the margin, so that the
  late check comes after it, or not; and, in the cases without 3, that the
  pruner sees a record naming P.
- **What each clean case rests on:** in the cases without 1 and 2 the line
  was taken before the landing in some schedules, and the check put it back.
  In the four without 3 **no schedule has the line taken at all**: they show
  that a line nothing takes is there, and nothing more.
- **What it leaves out:** a grace period that ends is a kill there. One
  holder, one object, one batch. A check that fails, and is queued, isn't
  searched: what is queued dies with the process, so by reading one whose
  sender ends before the retry is a kill before the check.
- **The pruner's rules for a hold that may land yet** have cases of their own,
  searched at 5a72b68 within the same bounds, the sender's own at a5340fe. Four
  reach none with all three conditions and a landing within the margin. No
  schedule of theirs has the line taken before the landing, and those that have
  a prune leave it are:
  - the sender's successor's, kept back at first: 58,436 of 66,163 with the
    sender ended within the margin, 41,603 of 46,530 with it killed before its
    check;
  - the sender's own: 68,225 of 75,952 with it ended within the margin, and
    159,536 of 385,294 with it alive.

  Four still reach it, and **none is a residual of the rules' own**. Two are
  their controls, a successor that prunes at once, which the code doesn't do:
  33,444 of 93,215 schedules, and 27,709 of 71,700, in those two ways. The other
  two are the margin's: a successor kept back, with the landing past the
  margin, 36,694 of 104,931; and the sender's own prune, with the sender alive
  and the landing past the margin, 18,990 of 54,796 with one preemption. That
  is the 60s margin, accepted on 2026-11-23 (the user's decision), and what it
  leaves. Another
  process's prune is the cases above, which follow neither rule. The model has
  no durations: "kept back" is until the landing, or past the margin not.

**A kill inside a prune, as Disown is turned on**, is the third (the user's
decision, 2026-11-24: accepted, and documented here). Here the pruner doesn't
make its check. Disown can settle with P held when all of these come
together:

1. A process prunes P's line. It read P bare, and the prune's live read of
   the switch, before its removal, found it off.
2. **Between that read and the removal**, the switch is set, and a Disown
   pass, of any process, reads the index with P's line still in it. It adds
   no line (E2, §7.4). It finds P bare, and strips the record that names it.
   The strip may come later.
3. An A2 on P, of a pass that read the switch as off, lands after that
   Disown pass has read P. If its check runs, it runs before the removal and
   sees the line.
4. The pruning process is killed after its removal has landed, and before
   its re-add has.

P then carries the finalizer, and neither a line nor a record names it, so
no count finds it. One kill is enough. The sender may live, and have had its
answer. The holder may still be there. No holder cache need be behind. What
makes it narrow is 2: the gap is between two calls of one write of a prune,
the live read of the switch and the removal that follows it, milliseconds in
practice and 10s at most (§10.6.2); and the kill must come within 15s of that
read.

**Another process of the kind makes it narrower than that**, and than the
model has it, which has no watch on the index. One whose index watch had
shown P's line sees it dropped by a write that isn't its own, and its write
loops, a standby's once it is elected, read P, find it held, and add the line
back; while that check is owed its Disown counts `"unknown"` (§7.4, §10.6.6).
So with such a process alive the path also needs its watch to be behind for a
whole run of zeros. This is by reading the code, not from a search.

- **The remedy** is the one above: the finalizer comes off by hand, the
  marker label finds the object, and once the CRD is gone `DisownPrefix`
  with `Names` removes it.
- **What the model shows** (`TestModelKillInAPrune`). With one Disown
  process, the one that dies or another, and with both, it reaches the path:
  with the kill at the second read of the switch or at the re-add, with the
  pruner's holder cache behind or fresh, and with the sender alive. It
  doesn't reach it with no kill, or one before the removal or after the
  prune; with the switch set outside that gap, or the index read outside it;
  or with the A2 landing before the pass. The last holds every process's
  passes until the A2 has landed, and checks that itself.
- **Its bound:** with one Disown process, every schedule with at most two
  preemptions; with both, at most one (searched at ae2012d, the landing before
  the pass in 352,060 schedules). **It takes two things as given**, as
  §14 states them: the landing margin, and a prune's bound. The dying
  process runs one Disown pass, and the kill comes at one of the prune's own
  calls.
- **What would close it:** a wait. No pass strips a record before 15s (a
  prune's bound) after its process's first pass under the switch. Every prune
  that read the switch as off began before any process's first pass under
  it, so has ended, or died, by then, and the line E2 then adds stays. In the
  model no schedule reaches the path with it, within the same bound. It
  delays the settling by one `DisownInterval`. **It changes nothing in the
  index's format, so it can be added later.** It holds back the records a
  pass strips, not one that goes with its holder, deleted in the same time:
  P is then found by its line only, as in the second residual.

Stripping the records before the objects would widen it: the kill could then
also fall between a pass's two strips, with the A2 landed before the pass.
§7.4's order is why it can't.

In LabelScoped mode there is no line to lose: the marker label is in the same
write as the finalizer.

**Disown's strip and the API server's two steps** (§6.4). When the strip of
an object being deleted, or `DisownPrefix`'s, removes its last finalizer, the
API server stores no update: it deletes the object in a second step, with no
precondition from the patch, so the patch's test of the resourceVersion
doesn't reach that delete. The strip is then as exposed as R3 is. An A2 that
lands between the steps is lost with the object. And two such writes, as
every process's Disown makes of the same objects, can both succeed, the
second deleting an object created again under the name meanwhile. This was
read in `k8s.io/apiserver` v0.37.2
(`pkg/registry/generic/registry/store.go:658-680`, `:847`, `:869-870`;
`pkg/endpoints/handlers/patch.go:711`), and not probed for the strip. The
index isn't exposed: its superset is of finalizers that are there, and an
object created again carries none until an A2, which adds its line first.

#### 10.6.4 Size, and when it fills (L1, L2)

- **The cap is in lines:** `SharedOptions.IndexMaxLines`, 2,000 by default.
  At about 400 bytes a line, which is the worst case (a 253-character name,
  a 63-character namespace, a long group), 2,000 lines fit in a
  ConfigMap's 1 MiB. PerName is for hundreds of names, not thousands.
- **The hard ceiling** is 17,000 lines, about what fits of typical size
  (60 bytes) in 1 MiB. It is a constant, and setup rejects a cap above it. Every write also checks the byte
  size, so lines longer than typical meet the 1 MiB limit before the cap.
- **At 80% of the cap**, the tracker records a Warning event on the index
  ConfigMap and sets `credentialuse_index_fill_ratio`.
- **When it is full**, adding a line fails, so A2 doesn't run. A new holder
  fails closed, and an established holder is Degraded (§8).
- **The remedy, documented:** move the kind to LabelScoped (§13), or raise
  the cap within the ceiling.

#### 10.6.5 Who reads it, and what a missing index means

- **The startup recheck** reads the index, GETs each named object, and
  enqueues a release batch for every (K, N) finalizer it finds. A line whose
  object the authorizer refuses to read (an RBAC 403, class (d) of §6.2) is
  kept, logged and counted
  (`credentialuse_index_read_denied_total`) once per object, and never
  retried: no retry can read it before RBAC changes. Any other failed read
  makes the recheck retry with backoff, reading again only what failed. A
  line an operator vouched for
  (§10.6.6) is read like any other: while it can't be read it is skipped,
  neither reported nor retried, and once readable and held its batch is
  enqueued.
- **Disown** reads it to find what to strip and what to count (§7.4).
- **`WithHeld`** reads it, and stops at the first line whose object still
  carries K's prefix (§11.6).

**A kind taken out of PerName, or uninstalled, keeps its lines** (decided
2026-11-26: the behaviour stays). The tracker prunes only the lines of its
PerName kinds (`prunable`), so the lines of a kind moved to LabelScoped, or
of one whose CRD is gone, stay. `scaletozero.WithHeldIndex` still GETs them:
once the RBAC for those names is revoked (a 403), or the kind is no longer
served (no match), the GET fails, and `WithHeldIndex` returns the error,
which keeps the controller up. *The remedy* is a hand edit of the index.
First make sure that kind's holds are found another way, by passing it to
`WithHeld` once it is LabelScoped, or that its objects are gone with its CRD.
Then remove its lines:

```
kubectl -n <IndexNamespace> edit configmap cu-index-<lowercase holder kind>.<holder group>
# in data.entries, delete every line that starts with <group>/<Kind>/,
# the moved or uninstalled protected kind; keep every other line
```

The tracker doesn't treat the drop as tampering, since the lines aren't of a
PerName kind, and doesn't add them back.

**A missing or unreadable index is an error, never "no holds"** (L4).
`WithHeld` returns the error, so `scaletozero` keeps the controller up.
Disown writes `held-count: "unknown"`, which is not `"0"`, so it never
settles. The tracker keeps retrying, and recreates the index if it is
missing (§10.6.6). Disown counts an index without the `rebuilt` marker as
`"unknown"` too, until an operator vouches for it, or the switch is off
(§7.4).

#### 10.6.6 Tampering, deletion, and the rebuild (M2, L5)

The tracker watches the ConfigMap by name (or polls it in PerNamePoll mode,
§10.7). **A change that drops lines this process didn't remove** is checked
line by line, with one live GET each. **A drop is this process's own only
when the version that shows it is the one a successful write of its own
returned**, whichever of the event and the response comes first. Anything
else is checked: a drop seen while a removal of the same line conflicts, or
has an unknown outcome, may be someone else's (decided 2026-11-23). So is a drop
the per-object watch shows released. That cache can lag a hold, and the process
that made the drop can die before its own check after it, so L5's exception for a
visible R3 is withdrawn (the user's decision, 2026-11-23). A drop delivered while
its object's check runs is checked again.

**Where the checks run** (IP4: the informers only read, and every write is
in the write loops). The dropped-line checks, their restores and the
rebuild below run in the write loops: on the leader, or on every process
when there is no leader election (§6.8). The index informer, which runs on
standbys too, only buffers what it sees, the dropped lines and the
deletions, in a queue the write loops drain, so a standby loses nothing
before its election. Every other dropped line therefore costs one GET per
process that runs the write loops: N GETs per dropped line only with N
processes and no leader election. That cost is accepted. A dropped line whose object no longer carries K's prefix
needs nothing: another process pruned it. While a Disown switch that covers
its kind is unsettled, it goes back (§7.4, E3). A dropped line whose object
still carries the prefix is **tampering**, or a lost race. The tracker adds
those lines back, records a Warning event and counts
`credentialuse_index_tampered_total`.

**A durable marker closes the crash window** (the user's decision,
2026-11-22). The index carries `credential-use.krateo.io/rebuilt` only once
a rebuild from the records has completed: the rebuild sets it in the same
guarded write as its last merge, and every create leaves it absent,
an acquirer's or `Track`'s alike. `Track` and the startup recheck rebuild,
by merging, an index without it. So a process that dies between creating
the index and rebuilding it, or an acquirer that recreated a deleted index
while no write loops ran, leaves no hold without its line past the next
start.

**The marker's value is `<uid>/<kinds>`**: the ConfigMap's own
`metadata.uid`, so a rebuild that creates the index sets it in a second write;
and the PerName kinds the rebuild was of, as the Disown switch lists them.

- **A copy restored from a backup**, by a backup tool or by `kubectl apply`,
  gets another uid, and may lack the lines of the holds made since: its marker
  is another ConfigMap's, so it isn't marked, and it is rebuilt, or is a lost
  one under Disown (§7.4).
- **A kind moved to PerName while the index is another kind's** (decided
  2026-11-24) is in no marker: its holds from before have no lines.
  `Track` and the startup recheck rebuild an index whose marker isn't of
  exactly its kinds; under a switch that covers the moved kind it is a lost
  one for that kind, and not for the others. A kind moved away is taken out of
  the marker by that rebuild, under the switch too, so that moved back it
  needs one again.
- **A copy applied or replaced over the live index**, as `kubectl replace`
  or `apply` of a backup's contents onto the ConfigMap that still exists,
  keeps its uid and its marker, and may lack the lines of holds made since. A
  process whose index watch had those lines sees them dropped, and checks
  each (above). One that hadn't, as when no process of the kind ran
  meanwhile, is left with the restored copy's case in place, and the marker
  doesn't show it: a hold without its line, which the marker label still
  finds (§10.6.3's remedy).
- **An index whose marker is `"true"`**, the value before 2026-11-24, isn't
  marked, and is rebuilt at the next start. No tag has written one.

**A rebuild under Disown doesn't set the marker** (decided 2026-11-23), unless the kind was never tracked before: the records it works
from may be stripped. It reads the switch after its list of the holders, so
one that is off then had stripped none of them. A change of the switch makes
the rebuild of an index without the marker due. `vouched-index` (§7.4) is
the operator's word in its place, for Disown's count only.

**An operator's vouch** (the user's decision, 2026-11-22). The index's
`credential-use.krateo.io/vouched` annotation lists, comma-separated and
written as the unheld annotation names refs (`team-a/Secret/nogrant`), the
objects an operator has checked by hand to carry no finalizer of K. An entry
`team-a/Secret/*` is every object of that kind in that namespace (the user's
decision, 2026-11-24, for a namespace; of its kind, decided 2026-11-25); there
is none for every namespace, nor for every kind of one, and a bare `team-a/*`
is malformed. Malformed entries are ignored, with a warning once per value;
so is an entry of a kind the index has no lines of, as `team-a/secret/*` or
`team-a/Secret.v1/*`, which vouches for nothing. While a vouched object
can't be read, Disown's count and its strip skip it, the startup recheck
skips it after its read fails, the tamper check doesn't restore its
dropped line, a prune may remove its line, and a rebuild doesn't add it. Every other unreadable
line keeps the superset's rule: restored, and counted unknown. A vouched
object that can be read and carries K's prefix is treated as any other,
counted and kept, and the wrong vouch is logged, when it was vouched for by
name: a kind's vouch in a namespace says nothing of what can be read there.
The process never writes the annotation, and its own writes keep it. **A vouch
stays until it is removed**, and a later hold of an object that then can't be
read, its grant revoked, isn't in the index's superset: remove a kind's vouch
once Disown has settled. A read that a prune's bound cut short is no read: the
vouch isn't of it, and its line goes back (§10.6.2).

**If the ConfigMap is deleted**, the tracker recreates it and rebuilds it
from the records. It lists the holders of K, takes the union of their
records, GETs each named object, and keeps those that carry a finalizer with
K's prefix. The rebuild writes by **merging** into what is there, re-reading
and merging again on a conflict (L5), so concurrent adds by other acquirers
are never lost.

**The residual.** An object whose last holder's record was already gone
(after `Forget`), but whose R3 hadn't landed when the index was deleted, is
in no record, so the rebuild can't find it. Its finalizer stays until
someone removes it. It isn't lost: K's marker label is still on it, so an
operator with list rights finds it with
`kubectl get secrets -A -l credential-use.krateo.io/k-<hash>=true`, and
`DisownPrefix` with an explicit name list strips it.

#### 10.6.7 RBAC for the index

- `get`, `update` and `patch` on the one ConfigMap, by `resourceNames`;
- `watch` on it, through a `metadata.name` field-selected watch (§13). In
  PerNamePoll mode, no watch is needed;
- **`create` on ConfigMaps in the index namespace.** It can't be scoped by
  name: a create request carries no name in its URL, so the request has no
  name for a `resourceNames` rule to match. That is broader than the rest,
  and it is confined to that one namespace.

These grants are bound in the index namespace. That is not the unsafe
namespaced Role of §13: the index namespace holds no holders (§10.6.1), so
no teardown of a managed namespace can remove them.

#### 10.6.8 The reviewer's three earlier interleavings, resolved

1. **A restart between `Forget` and R3.** H's last pass writes `Forget`
   with its owner-finalizer removal, and H is gone. The process restarts
   before R3, so the in-memory release batch is lost. P still carries the
   finalizer, and its line is still in the index, because lines are pruned
   only after R3. The startup recheck reads the index, finds the finalizer,
   and enqueues the batch. R2 finds no users, so R3 releases it.
   (LabelScoped mode reaches the same end through the protected informer's
   initial list.)
2. **A never-established holder deleted while the process was down.** H's
   pass held P (A1, the index line, A2), then Connect failed, so H never got
   its owner finalizer. H is deleted while no process runs. It has no
   finalizer, so it vanishes at once, and no event reaches anyone. On
   restart, P's line is in the index, so the recheck enqueues its batch. R2
   finds no record naming P, and R3 releases it.
3. **A1 before the Disown switch.** A pass that began before the switch was
   set runs A1, then the switch is set, the Disown loop strips P's finalizer
   and H's record, and then the pass's A2 lands and re-adds the
   finalizer. Two things close this:
   - every Acquire reads the switch again just before each A2, and a pass
     that sees it set stops and returns its refs unheld;
   - a pass already past that check, whose A2 lands after the strip, is
     caught by the Disown loop's next pass. The live count, from the index
     in PerName mode, sees the finalizer, so the loop strips it again, and
     the non-zero count resets the settling. The index's line is there for
     it, since Disown keeps the lines until it has settled (§7.4).

   Disown is settled only after `DisownPasses` consecutive zero passes of one
   process, the last of which starts at least `LandingMargin` after every
   pass begun before the switch has ended (§7.4). So such a pass can't land
   a write after it is settled, unless the write lands later than the
   margin.

### 10.7 PerNamePoll mode: runtime-provisioned RBAC, no watches

A provider that grants its controller access per name **at runtime** can't
use PerName's watches without extra privilege. Kubernetes' escalation
prevention lets a subject grant only permissions it already holds, unless it
has `escalate`. To grant a named `watch` on any Secret, the provisioner
would need `watch`, and so `list`, on all Secrets. That is exactly the grant
oasgen removed in #510, and oasgen also rejects relying on `escalate`.

So **`Watch: PerNamePoll`** uses no watches at all on protected kinds, only
GETs and patches. The provisioner holds `get` and `patch` on Secrets
cluster-wide (plus the same on any other protected kind), and grants the
controller named `get` and `patch`.

**The credential-use ClusterRole is dedicated** (M1):

- only the provisioner creates and writes it;
- no chart ships it;
- it has no `aggregationRule`. An aggregated role's rules are rewritten by
  the aggregation controller, which would undo the provisioner's grants;
- it carries only the tracker's `get` and `patch` for holds. **The
  controller's own read grants for Connect come from the provider's
  separate provisioning**, and must cover every name in the index. A line
  is written only for a ref Connect reads, so they do, but for the lines
  Disown adds (§7.4, E2), which may be of names they don't cover.

If a GitOps tool manages the ClusterRole anyway, exclude its rules:

```yaml
ignoreDifferences:
  - group: rbac.authorization.k8s.io
    kind: ClusterRole
    name: credential-use.rest-dynamic-controller
    jsonPointers: [/rules]
syncPolicy:
  syncOptions: [RespectIgnoreDifferences=true]
```

**"As today" still holds under this split.** Disown empties the index once
it has settled, so the provisioner then revokes the credential-use grants.
Until then the lines stay (§7.4), and with them the grants its strip and its
count need. With the switch off again before it settled, the lines and the
grants of objects that carry nothing stay until the next startup recheck.
Connect keeps working
through the provider's own read grants. A Degraded holder proceeds to a
Connect that doesn't depend on the credential-use ClusterRole at all. And
a 403 window in that ClusterRole (§10.7.1) affects only the tracker's GETs
and patches, never Connect.

#### 10.7.1 The grants are a pure function of the index (M1)

The provisioner doesn't take grant requests. It **reconciles** the
ClusterRole's `resourceNames` so that they cover the index's names, continuously.

- **Its inputs:**
  - the index ConfigMap of **every kind the ClusterRole serves** (L1).
    These are **watched by name**, which is the primary signal (L3);
  - the ClusterRole it manages, **kept in a cache** by a watch on its name,
    and written only with resourceVersion-guarded updates. A
    ClusterRoleBinding binds it to the controller;
  - **a slow fallback poll**, every `ProvisionResync` (5 minutes by
    default), which re-reads every index and the ClusterRole live, in case
    an event was lost.
- **Its loop**, on every index or ClusterRole event, and on each fallback
  poll:
  1. The desired grants are the **union** of the names in every served
     index's lines, per protected kind.
  2. **Grant** every desired name the ClusterRole lacks, in **one** update
     (coalesced, L6), guarded by resourceVersion, merging on conflict.
  3. For each granted name no line names any more, **revoke only after a
     check**:
     - live-GET, with its own cluster-wide `get`, every object of that name
       it last saw in any served index. If any carries a finalizer with
       **any served kind's prefix**, keep the name (L1);
     - otherwise, re-read **every** served index live, after those GETs. If
       no line in any of them names it still, add it to the revoke set.
  4. Remove the revoke set in one guarded update.
- **Steady-state read cost** (L3): two watches per served kind and one for
  the ClusterRole, no GETs, and one live read of each index and the
  ClusterRole per fallback poll. GETs happen only when a name is about to
  be revoked.

**The order this gives, end to end.** To hold a new name, the acquirer adds
the index line, then waits for the grant (§10.7.2), then runs A2. To drop
one, R3, then the prune, then the provisioner's revoke. **An A2 never runs
before its grant.** The earlier revision's "RBAC, then the index line" is
replaced: the grant now follows from the line, which comes first. An A2 that
runs without its grant fails with an **RBAC 403**, class (d) of §6.2. That
looks like a credential problem, but it is an ordering or provisioning
defect, and it is reported as an Acquire error, never as the outcome
Deleting, which is a 422 with a `metadata.finalizers` cause (§6.2).

**Proof against the reviewer's interleaving:**

1. The prune removes P's line.
2. The provisioner GETs P and sees no finalizer.
3. H2's A2, which still had the grant, re-adds the finalizer.
4. The provisioner re-reads the index, finds no line, and revokes the name.
5. H2's post-check (§10.6.2) finds the line missing and re-adds it.

Now P carries the finalizer and has its line, but the grant is gone. **The
hold isn't lost**: revoking a permission never touches P's finalizer, and
the index is a superset again (§10.6.3). The next provisioner cycle reads
the line and grants the name again. **The residual is one provisioner
reaction of 403** on that name: the watch delivering H2's re-added line,
and one guarded update. That is typically under a second, and at worst one
fallback poll (5 minutes) if the event is lost. During it, the tracker's A2,
R3 and Disown patches on P fail and retry. Connect isn't affected, because
it reads through the provider's own grants (M1). Ordered the other way, with H2's A2
landing before the provisioner's GET, the GET sees the finalizer and the
name isn't revoked at all.

**Name-level grants are cluster-wide** (L4). A ClusterRole's
`resourceNames` match a name in every namespace. So granting `git-token`
lets the controller **`get` and `patch`** every Secret named `git-token`, in
any namespace. Binding per namespace with RoleBindings would narrow that,
but RoleBindings die with their namespace (§13), so the design accepts the
breadth and states it. The tracker still patches only objects it holds, and
**the cross-namespace gate (§11.3)** decides which those are. So the breadth
is a capability the controller has, not a reference a tenant can make it
act on.

#### 10.7.2 A 403 from A2 is retried briefly (L4)

RBAC propagates to each API server through that server's own informer, so a
grant that one server already applies can still be denied by another for a
moment. **An RBAC 403 on A2 (class (d) of §6.2, never (b) or (e)) is retried
without sleeping in a worker** (L2). The
pass stops before Connect, for a new holder and for an established one alike,
and sets `Acquired.RetryAfter` to 0.5s, then 1s, 2s, 4s (tracked per
holder and ref in memory), which the caller returns as `RequeueAfter` with no
failure backoff (IP2, §8 step 5). Once about 8s in all have passed, the 403 is
reported as an Acquire error, the ordering or provisioning defect. For a new
holder, the pass then fails closed and requeues with backoff. **For an established holder it is
Degraded, and that is harmless**: Degraded is today's behaviour for one
pass. Nothing is removed, the record already names P, so every releaser
counts the holder, and the next pass writes A2 once the grant has
propagated.

#### 10.7.3 What replaces the watches

- **Held objects are polled** with GETs, with the prober's backoff (§10.3):
  from 15s, doubling up to 5 minutes, and back to 15s whenever the object
  changes. The index is polled the same way.
- **Holder events still arrive** through the holder informer, so releases
  after a holder's deletion stay prompt. Only changes to *protected* objects
  are seen by polling: a finalizer stripped, a `deletionTimestamp`, a
  recreate, a re-pointed chain.
- **The `written` skip needs a live GET** (L1 of the previous pass). Without a watch, a stripped
  finalizer or a recreate isn't delivered to drop the entry. So in
  PerNamePoll, A2 may skip only after a live GET of P confirms the UID in
  the `written` entry and the finalizer. The steady state then costs one GET
  per held ref per pass, instead of nothing. That is still cheaper than the
  write it saves.

**Latency and cost, against PerName.**

- **Latency.** A change to a held object is seen within the poll interval:
  up to 5 minutes for an object that has been stable, not within watch
  latency. The windows of §6.5 and §9.2 stretch by that much, except where
  a pass's own GET (L1) sees the change first. The grace still runs from
  local first sight (§5), so a late sighting delays a release but never
  shortens the grace.
- **Cost.**
  - Polling: one GET per held object per poll interval, so under 2 GETs/s
    for 500 held objects polled at 5 minutes, and about 33/s right after
    changes, at 15s;
  - the skip check: one GET per held ref per pass;
  - **the ClusterRole** (L6). At most one update per provisioner cycle,
    whatever the number of names. A 2,000-name rule is about 60 KB at a
    typical 30 bytes per name, and up to about 500 KB with 253-character
    names. Each update re-sends the whole object, and every API server's
    RBAC informer receives it. Authorizing a request that uses the rule
    scans the names linearly
    (`k8s.io/kubernetes@v1.36.2/pkg/apis/rbac/v1/evaluation_helpers.go:80-92`).
    At 2,000 names that is microseconds per request.

  PerName instead holds one watch per object and skips without a GET.

**The honest cost.** The provisioner can read any Secret whose name it
knows. It can't list or watch Secrets, but `get` on all Secrets is still a
broad read, and the controller's named grants reach every namespace.

**The alternative.** Protect only refs whose names are known when the RBAC
is generated, such as oasgen's configuration-level Secrets, with static
`resourceNames` in PerName mode. Per-object Secrets resolved from paths are
then not protected, and the deadlock remains possible for them. That is
documented, not hidden.

## 11. Lifecycle edges

### 11.1 A ref that changes on update

The next pass computes the new set. A1 writes it, adding new refs and
dropping old ones, and A2 holds the new ones. Dropping a ref is always safe,
because the holder will never read it again. The holder's update event maps
the dropped refs to a release batch. In the next release, a ref reached
through `Next` changes the same way when an intermediate changes (§9.2). In
this one, an update that adds a ref with a `Next` is refused whole (§9): the
record, and so the holds, stay those of the refs before it, but for what
§11.3's gate drops.

### 11.2 A Secret that doesn't exist yet

A2 gets NotFound, so the ref is Unheld/Missing. The record keeps it. Connect
runs and usually fails, as it does today, and the holder gets no owner
finalizer. When the Secret appears, the prober or the holder's next pass
holds it, and `BeforeEstablish` re-checks it before any owner finalizer is
written. A provider that tolerates a missing optional Secret is unaffected.

### 11.3 A ref in another namespace: held only with the target's consent (M2)

**Same-namespace refs are always held.** A ref whose object lives in a
namespace other than the holder's is held **only if the target consents**
(the user's decision).

**Consent.** The target object, or its Namespace, carries:

```
credential-use.krateo.io/allow-holders-from: "team-a,team-b"
```

The value is a comma-separated list of holder namespaces. **Consent is the
union of the object's list and its Namespace's list** (L3, the user's
decision). A holder namespace named in either is allowed. An object's list
can add to what its Namespace grants, but **can't narrow it**. The
implication for owners: to restrict one Secret, don't grant at the
Namespace level. Grant on each object that should be shared.

**`*` is allowed on the target object, and not on a Namespace.** On an
object, `*` is one owner's deliberate choice about one object: a platform
Secret in `krateo-system` that every tenant is meant to use is the real
case for it. On a Namespace, `*` would open every object in it, present and
future, to pinning by every tenant, and to grants for them under
PerNamePoll. That is never one decision about one Secret, so a Namespace
must list namespaces by name. A `*` on a Namespace is ignored, with a
Warning event on it.

**The check.** It is a live read, made for each cross-namespace ref
**before** the index line, the grant and A2:

- a GET of the Namespace's metadata, which the tracker can always make
  (§13);
- a GET of the target object's metadata. In PerNamePoll mode it is made
  with the provider's own read grant for that object, which Connect needs
  anyway (§13). If that GET is denied, Connect can't read the object either,
  and the ref is treated as not held.

**A ref without consent is not held.** The gate runs before A1, so the ref
isn't in the record. It gets no index line (but from Disown, of a record
someone forged, §7.4), no grant and no A2, though its
`Next` is still followed (§9.4). It is listed in the holder's `unheld`
annotation as `NotConsented` (below). Connect proceeds **exactly as today**,
with a Warning event, `CredentialRefNotHeld`, naming the ref and the missing
consent, and the metric `credentialuse_refs_not_held_total`. This is a
fail-open with no regression, not an Acquire error: nothing that worked
before stops working.

An unheld ref's consent is re-checked live on the prober's backoff (§10.3),
not on every pass, because the target isn't labelled and so isn't in the
cache. When consent appears, the ref is held on the next pass.

**Why the original deadlock doesn't apply to a ref that isn't held.** The
deadlock in §1 needs the holder's *own* namespace teardown to delete the
Secret first. That teardown deletes only objects in the holder's namespace,
so it never deletes a Secret in another one. What remains is the reverse
case: **the Secret's own namespace is deleted while the holder lives.** The
holder then fails Connect, and if it is deleted, it waits. That is
recoverable. Once that namespace is fully gone, it and the Secret can be
recreated. Alternatively, the holder's owner can set deletion-policy
`orphan`, or the target's owner can consent, and the holder is protected
from its next pass.

**Consent withdrawn: the hold is released** (the user's decision). The
target owner's consent is authoritative. When **neither** the object's nor
its Namespace's `allow-holders-from` names a holder's namespace any longer
(the union, above), that holder's hold is dropped:

- **from the record.** The holder's next pass writes A1 without the ref;
- **from the index and the grant**, through the normal path: R3, then the
  prune, then the provisioner's revoke (§10.6.2, §10.7.1);
- **and the finalizer** goes through the normal release protocol, with §5's
  grace and live re-checks. Holders of the same kind in the same namespace
  all lose consent together, so the finalizer for (K, N) goes once the last
  of them has dropped the ref.

**How long that takes.** The change enqueues the target's holders where a cache
or the Namespace informer delivers it, and otherwise each holder's next poll
sees it. One that records more than 16 refs outside the caches
(`SharedOptions.Namespaces`, §10.5) reads 16 of them a pass, those it names and
those it doesn't alike, so a withdrawal of one it doesn't name may take it up to
⌈m/16⌉ poll intervals for m of them, and longer where passes read fewer, and
then §5's grace. A holder that names the ref reads it on every pass. Whoever edits the holder sets m: 4,000 forged
refs that were there first make it about 250 poll intervals, over four hours at
one minute; and at most about 4,000, under three days, if they are of a
protected kind that no cache covers and whose reads hang. A holder whose passes
fail is paced by its controller's error backoff instead.

**The gate runs on every pass** (M1), for every held cross-namespace ref,
not only before A2:

- **On a pass that skips A2** (§6.3), consent is read from the cache. The
  label-scoped informer keeps `credential-use.*` annotations on the target
  (§10.1), and the Namespace informer covers the Namespace. In PerNamePoll
  mode it is read from the live GET that the skip already makes (§10.7.3).
- **On a pass that runs A2**, consent is read live, before the A2, as it is
  before the line, the grant and the A2 of every new hold. **The live read is
  mandatory in the adding direction.**
- **A held ref is dropped only on a positive, live-confirmed withdrawal**
  (M1). When the cache suggests a withdrawal, or **can't answer** (a source
  not yet synced, or the object or its Namespace absent from the cache),
  the pass makes **one live read of the object and of its Namespace**
  before any drop:
  - if neither list names the holder's namespace, the pass drops the ref in
    A1;
  - if either does, it keeps the hold. That is the case of a stale cache,
    or of a source still syncing after a restart;
  - **if the live read fails, it keeps the hold and requeues**: the ref
    stays held, so the pass proceeds to Connect, and `Acquired.RetryAfter`
    asks for the requeue (§8 step 5, IP2). **The requeue backs off** (M2 of
    the review of revision 16), per (holder, ref), in memory: 1s, then 2s,
    4s and so on, doubling up to the holder's poll interval (1 minute by
    default in provider-runtime), and back to 1s after a live read that
    succeeds. So a persistent failure, such as an overloaded API server,
    costs each cross-namespace holder at most one extra reconcile per poll
    interval once the backoff has grown, not one per second. There are two exceptions:
    - **NotFound means the target is gone, not a failed read** (L1). If the
      object, or its Namespace, is NotFound, the ref is treated as
      **Missing** (§6.2). There is nothing left to hold, and nothing is
      requeued for it. The record keeps the ref, as for any Missing ref,
      and the prober watches for it to return (§10.3);
    - **a persistent 403 on the object read** (L2, PerNamePoll mode, where
      the read uses the provider's own read grant) means the provider no
      longer lets the controller read that object, and Connect can't
      either. This matches the adding direction, where a denied read means
      not held. After **5 consecutive 403s, spanning at least 2 minutes**,
      the pass drops the ref in A1. It records a `CredentialRefReadDenied`
      Warning event on the holder, counts
      `credentialuse_refs_read_denied_total`, and lists the ref in `unheld`
      with the reason `ReadDenied`. Until the bound, it keeps the hold and
      requeues. The two minutes let a grant still propagating, or briefly
      reprovisioned, recover first (§10.7.2).

  On a pass that runs A2 the consent read is live already, so that read
  serves.

**Every recorded ref is gated, and first** (found in a review of the chain
code, 2026-11-24). A record keeps refs that its pass doesn't name: behind
a hop it can't follow (§9.1), and when the pass fails (§8). The gate of the
refs that a pass names never reaches those, so a withdrawal didn't release
them, for as long as the holder stayed so; and with the target's namespace
being deleted, its teardown waited on a holder that no longer named the
target, and the owner's recovery, withdrawing consent, did nothing. So every
pass **begins** by gating each ref in another namespace that the record names,
before its refs are named and whatever it then comes to, a `Refs` error, a
refused chain (§9) and a holder in the index namespace included:

- **a cached yes serves**, so it costs no call in the steady state. That holds
  on a deleting pass too, for the refs the pass doesn't name: their withdrawal
  is then seen once the informer delivers it, as on any pass that skips its
  A2. Those it names are still read live before their A2. Anything but a
  cached yes is read live, within the bound below, and the pass uses that
  answer for a ref it then names, rather than read again: it is the live read
  of the same pass that the A2 needs;
- **what it drops goes in one A1** at once, which a pass that fails afterwards
  has then written. It conflicts as any A1 does, and the pass that wrote it
  skips no A2 (§6.3): the holder's version is new. A withdrawal is reported
  once that A1 is written, with the dropped ref in `Acquired.Unheld` as
  `ConsentWithdrawn`, once, whether or not the pass names it. A pass with
  nothing to drop writes nothing;
- **an A1 refused for its size is written again without the unheld list.** An
  object's annotations are capped at 256 KiB together, and the ref's unheld
  entry is about as long as its record entry. A holder whose other annotations
  fill the rest would otherwise keep the hold, with a 422 on every pass.
  Without the list the patch only shrinks them. The withdrawal is then not
  listed, only reported. A pass that names such a ref still fails at its last
  A1, whose list doesn't fit either, until there is room, but the hold is
  released;
- **an unreadable consent keeps the ref**, with `RetryAfter` on a failing pass
  too, and one that is gone stays, as any Missing ref. Nothing is dropped once
  the pass's context has ended;
- **it asks nothing** about refs in the holder's own namespace, which need no
  consent; about cluster-scoped objects, which have no Namespace to read; about
  kinds that aren't protected; about kinds that the Disown switch covers, by
  the informer, with no read of the CRD (§7.4); or for a record it can't parse,
  which names nothing. Only the target's owner, or whoever edits the target's
  Namespace, can make a holder lose a hold by it, on that target, after a live
  read.

**What it may cost.** Whoever can edit a holder can write its record, with
about 4,000 refs of their choosing. So what the gate reads is bounded:

- a ref whose **cache covers it, has synced, and lacks it** is kept, and not
  read: it isn't held, or is gone. An unsynced cache lacks nothing. A ref that
  the pass names is read live in any case, as before. **What that misses**, and
  so a withdrawal of it isn't seen until a pass follows the whole chain, or
  the cause ends:
  - **LabelScoped, in a namespace that its caches cover:** an object with its
    finalizer but without its held label, which someone else stripped. No cache
    has it, and only an A2, which a ref that only the record keeps doesn't get,
    restores the label. The owner can set it back,
    `credential-use.krateo.io/held: "true"`: the informer then delivers the
    object, and each holder's next pass reads it. Outside the covered
    namespaces (§10.5) the ref is read live, labelled or not;
  - **PerName:** a hold without an index line, and so without a watch. The
    watch doesn't depend on the label (§10.6). No prune takes the line of an
    object that a record names (§10.6.2), and the write loops put back one
    that someone else drops, or the whole index, as long as they can read the
    object (§10.6.6); the ref is gated again once its watch has synced;
  - **a kind no longer in `Protected`** while records still name it: it isn't
    asked about at all. Disown it first (§7.4), or protect it again;
- the rest are read live, **at most 16 a pass**, so at most 16 GETs of objects
  and 16 of Namespaces, each Namespace once (a name that can't exist, §6.2, is
  one of the 16, and has no GET of its own), **and within 5s together**, or half
  of what the pass's context has left, if that is sooner: a deadline of the
  gate's own, which a client's rate limit isn't told: a read that has no turn
  yet waits for it, and the deadline cuts the wait, where client-go's limiter
  would refuse it at once. So a pass whose client is short of turns takes its
  worker up to those 5s. A read that hangs, as of a kind whose conversion webhook
  does, takes neither the pass nor the A1 of what the others found. **But it
  takes up to those 5s of the passes of that holder that meet it**, of every one
  with 16 refs to read or fewer, for as long as it hangs and the record names it.
  Whoever can edit a holder can have it so, where reads of a protected kind
  hang: with many made-up names, if no cache covers the kind there; or with one
  existing object that a cache has without consent, which is read live too, and
  costs the 5s as well. They can't make one hang;
- those that have waited the longest go first, always, in memory: a ref starts
  to wait when a pass first asks about it, and again when it is read. So a ref
  is read **within ⌈m/16⌉ passes that read their 16**, where m is how many refs,
  itself included, that first pass asks about, whatever is added to the record
  later: fresh refs, or ones that leave it and return, go behind it. A restart,
  or a new leader, forgets how long each has waited, so the wait starts over
  there: with m of 17 to 32 it costs one more pass. A read counts once it is
  answered, a failure included. One that the deadline cuts keeps its place, so
  it is first in the next pass, with the whole deadline; if that isn't enough for
  it, it goes behind the others. So every pass that reads anything moves at
  least one ref behind the others, or drops it: a rotation of k refs whose reads
  hang costs at most 2k passes, and never more than m in all. A withdrawn ref
  whose A1 wasn't written is first again. A pass reads the refs it names itself
  where the gate didn't, as it always did, under its own context: one of them
  that hangs takes the pass, as on any pass;
- **a client short of turns**, as under a rate limit with busy workers, cuts the
  pass: one that answers k of its refs moves those k, but for withdrawn ones,
  which stay first until their A1 lands, and the next goes on from there. A ref
  takes two turns, or one if its Namespace was read in that pass, which the first
  one's never was. A wait that is cut costs the client one turn, if somebody
  waits behind it. **What remains:** while a turn takes more than half of the
  gate's time, 2.5s of 5s, no ref is read, and a withdrawn one stays held. From
  the context, a read that waits for a turn looks like one that hangs, so the
  first in line goes behind the others when the deadline cuts it: under a load
  that keeps time with the passes, with every other one ending before its first
  answer, some refs may be so moved each time, and never read. And what a
  client's own limiter or transport, other than client-go's, refuses at once, by
  its place in the pass, counts as read;
- **for what a pass asked about and doesn't know**, refs it couldn't read or
  didn't get to, `RetryAfter` asks for the next, with one backoff a holder, the
  gate's own: 1s, doubling up to the poll interval, back to 1s once a pass knows
  all. So one ref that never can be read keeps the others' retry at the poll
  interval. The refs that the pass names keep their own, per (holder, ref); one
  that a holder stops naming while it fails stays in memory until the holder
  goes;
- **what it remembers** of a holder is of the refs that its record names now,
  and goes with the holder;
- another holder's pass costs what it did. The reads of one holder's record
  use the same client as the others', though, and at its rate.

**The release loop reads the names too, and those bounds aren't its.** A change
of a record queues one batch, of every name in the old and the new one (§10.2),
in every watch mode, and R0 settles only names that the caches have. So the
process that runs the write loops makes a GET of each of the others, one after
the other, where the caches cover them too, but none of a name that can't exist
(§6.2). It grows with the record, 4,000 for a change that puts 4,000 made-up
names there, and 8,000 for one that replaces them all. The changes that come
while a batch waits cost one batch together, of every name of all of them. 4,000
GETs in a row, at about 2ms each (against an
envtest, 2026-11-24), hold one of the two workers for 8s, and for longer where
the client's rate limit is lower; beside R0's own work, which grows with the
square of the names.

**That is once only where each GET finds its object, or that there is none, and
nothing asks for it again.** What a batch fails at is read again, and only that
(§6.2): each object after 1s, doubling up to 5 minutes, for as long as it fails,
and whatever the record says by then. A batch fails at an object's GET, and
whoever writes the record can name what always fails: a kind that can't be
mapped, or an object outside a `resourceNames` grant. So m such names cost m
tries every 5 minutes, beside those of the batches that changes queue. It fails
too at the Namespace of an object that is being deleted, an epoch's restore, R3,
or an object that kept changing under 5 releases, each for that object; at the
holders' list, for all that it was to decide; at the index's prune, for those
whose lines it was to prune; and at the list of what a whole namespace holds, for
that list. One that waits for its grace, or whose epoch was restored, is looked
at again alone. A restart forgets them, and reads the names that the records have
then. A standby's queue, though, keeps every name of every change until it is
elected.

It was so before the gate, and how much a batch takes in one turn, and what a
holder's data can keep in memory, is the release loop's to bound, apart from
this.

**The gate adds to it:** an A1 that drops refs is a change of the record. Its
batch has the names of the one holder's record as it was, which the new one's
are among, and whatever else waits for that (K, N). So dropping one ref of m
costs up to m GETs, which any drop did, and the gate may drop them over many
passes. All the refs that it asks about share a pass's 16 reads, those that the
pass names and those it doesn't, withdrawn or not: of m, w of them withdrawn, the
drops cost at most w such batches, as each drops one or more, and in passes that
read their 16 at most ⌈m/16⌉, and one for what a pass drops among the refs it
names itself. Each of the w is an object that exists, and that a cache has
without consent, or that no cache covers. Outside the caches, 4,000 names of
which one in each 16 that a pass reads is an existing Secret without consent cost
up to 250 batches and 970,000 GETs, over at least 250 passes that don't follow
the whole chain; and more batches, of one drop each, where passes read fewer.
Where a cache has the objects, R0 settles those still in the record that aren't
being deleted, and a batch reads those dropped since the last one, and the
made-up names, which no cache has: 250 cached without consent and 3,750 made-up
cost up to 16 batches of about 3,766.

**What remains.** Where no cache covers the target (outside
`SharedOptions.Namespaces`, §10.5), there is no cached yes, so an honest
holder's recorded refs there cost up to those GETs on every pass. They are the
reads that it makes anyway for the refs it names. A ref that is gone costs
the gate none where a cache covers it, and one GET a pass, with its Namespace's, where
none does, until a pass that follows the whole chain drops it from the record.
And a forged record still tells its author whether a target exists where no
cache covers it, or is held where one does: the ref of one that exists without
consent is dropped, and of one that doesn't is kept. The unheld list answers
that for any ref the holder may name already (`NotConsented`, or nothing), and
the gate answers it for no more than 16 refs a pass.

**A paused holder keeps its holds, whatever the target's owner does.** The gate
is part of a pass, and a paused holder has none (§9.2): its recorded refs stay
held, with their consent withdrawn. So are those of a holder that no controller
reconciles. That is a limit (the user's decision, 2026-11-24). **The way out:**

- **unpause the holder.** Its next pass drops the ref. Deleting it isn't enough
  while it is paused: its pass returns before its deletion, so it stays until it
  is unpaused, unless it has no owner finalizer;
- **or remove the finalizer by hand**, `credential-use.<kind>.<group>/<holder
  namespace>` (§3), from the target. Nothing puts it back while its consent is
  withdrawn;
- **or the Disown switch** (§7.4), for the whole holder kind; or, once the kind's
  CRD is deleted, `DisownPrefix` with `Names`, which refuses while it exists.

Nor is anything dropped while a holder is edited faster than a pass takes, since
every A1 then conflicts, or while the API server refuses the A1 for a cause other
than its size (a 422 from a schema or a webhook), or while every pass's context
ends before its A1. The gate leaves that half of what the context has. Nor is
anything dropped from a record that can't be parsed while every pass ends
before its last A1 (§4): it names nothing to ask about, and only that A1 writes
a valid one. That is no wider than a paused holder.

**A ref that only the record keeps, recreated without the annotation, is
dropped.** A two-step apply (the object, then its annotation) of a target that
was deleted is, between the two, an object whose owner doesn't consent. If it
is read then, the gate drops the ref, as it would one that the pass names. But
no pass names this one, so it isn't held again when the annotation comes: only
once the hop is back, by the pass that follows it to the ref. Give a recreated
target its annotation in the same write, or the consent on its Namespace.

`BeforeEstablish` and the prober don't change: a ref the gate dropped isn't
Missing, and one it kept is neither probed for nor held by them, as before.

**Why a drop needs that confirmation.** Releasing a hold is the direction
that can expose a holder. A stale or unsynced cache is most likely exactly
when it matters most: just after a restart during a namespace teardown, or
just after a new hold, before the informer has delivered the object. Acting
on the cache then could release a hold whose consent never changed. With
the live confirmation, a hold is released only when the target's owner has
really withdrawn consent. A cache that is wrong in the other direction
(showing consent that was withdrawn) only delays the drop until the live
read of the next pass that runs A2, or until the informer catches up.

**How a withdrawal reaches the holders.**

- **A change to the target's annotation** is delivered by the label-scoped
  informer, and enqueues the target's holders through the index.
- **A change to a Namespace** is delivered by a metadata-only informer on
  Namespaces. Its transform keeps the name, UID, resourceVersion,
  `deletionTimestamp` and `credential-use.*` annotations. **Its handler
  compares the transformed old and new objects** (L5), and acts only on a
  change to a `credential-use.*` annotation or to the `deletionTimestamp`.
  On either, it enqueues the holders of every held object in that
  namespace, and on a new `deletionTimestamp`, those objects' release
  batches too (§5).
- **In PerNamePoll mode**, the target isn't watched. Its annotation is seen
  by the next pass's skip GET, by the held-object poll (at most 5 minutes),
  and by any pass that runs A2. A Namespace isn't a protected kind, so its
  informer runs in PerNamePoll too.

A `CredentialConsentWithdrawn` Warning event is recorded on the target and
on each affected holder, and `credentialuse_consent_withdrawn_total` is
counted. The holder's event names the target, the namespace whose consent
was withdrawn, and that the hold was released:

```
hold on team-b/Secret/db-creds released: holders from team-a are no longer allowed (allow-holders-from on Namespace team-b)
```

**A durable record of what isn't held.** An event expires in about an hour,
but a Secret may be removed days after its consent was withdrawn. The holder
would then see a plain not-found, which looks the same as a rotation gap. So
the runtime keeps a second annotation on the holder, next to the record:

```
credential-use.krateo.io/unheld: [{"ref":"team-b/Secret/db-creds","reason":"ConsentWithdrawn","since":"2026-11-08T17:02:11Z"}]
```

- **Reasons:** `NotConsented` (never held, because consent was missing),
  `ConsentWithdrawn` (held, then released), and `ReadDenied` (held, then
  released after a persistent 403 on the consent read). `since` is when the ref
  entered that state, and it is kept while the state lasts.
- **The same write.** It is written in the A1 merge patch, together with
  the record, whenever either changes, so it costs no extra write. The
  holder event filter treats it like the record (§7.5).
- **It is bounded.** It holds at most 16 entries. **When it has to trim,
  it keeps `ConsentWithdrawn` and `ReadDenied` entries ahead of
  `NotConsented` ones** (L4), because a released hold is the cause a later
  not-found needs explained.
  Within each reason, the most recent come first. Beyond 16, the last entry
  is `{"ref":"…","reason":"More","count":<n>,"unmet":<m>}`. `unmet` is how many
  of the `n` are released holds that the passes don't name (below), left out
  when it is 0. A pass counts the refs it names each time, and can't count
  those, so it carries that number over. With refs of at most about 350
  characters, it stays under 6 KB.
- **An entry goes** when the ref is held again, or when the holder no longer
  names it. A released hold's entry, `ConsentWithdrawn` or `ReadDenied`, stays
  while the holder's passes can't follow its whole chain, or fail before their
  last A1, though none names the ref: the holder may still read through to it,
  and the entry explains what it then finds. It goes with the first pass that
  follows the whole chain and doesn't name it. That pass's last A1 is what
  removes it, so a pass whose refs lead nowhere further, which writes its one
  A1 before its A2s, removes it even if an A2 then fails. `unmet` goes with
  it. It may count too many for as long: a pass that names refs which its
  gate's A1 left out of the list takes them out of the count in its last A1,
  and one that fails before that doesn't.
- **It is the runtime's.** Like the record, it shouldn't be edited. An edit
  is overwritten by the next A1.

**Reading it next to a not-found** (operators and providers). When a holder
fails with a not-found for a credential, look at
`credential-use.krateo.io/unheld`:

- **an entry for that object with `ConsentWithdrawn`**: its owner withdrew
  consent at `since`, the hold was released, and the object was then free
  to be deleted. Recover inside the holder's own live namespace, by
  re-pointing or with deletion-policy `orphan`;
- **an entry with `NotConsented`**: it was never protected (§11.3);
- **no entry**: the ref was held, or it is in the holder's namespace. The
  not-found is then a rotation gap, or §5.2's residual.

provider-runtime adds a pointer to that annotation to the failure message
(§12), so the cause is visible without knowing the annotation exists.

**The holder then proceeds as for any unheld ref**: Connect works as today,
with no hold. If the Secret later goes, the holder's owner recovers inside
the holder's own live namespace, with deletion-policy `orphan` or a
re-point.

**Why the drop can't race an A2 into a stranded or unindexed finalizer.**
Three rules close it:

1. **Every A2 of a cross-namespace ref is preceded by a live consent read in
   the same pass**, as is every new hold (line, grant, A2). A pass that runs
   A2 after the withdrawal sees it, writes no A2, and drops the ref instead.
2. **A pass that skips A2 still checks consent**, from the cache or from the
   skip's live GET, and drops the ref only after a live read confirms the
   withdrawal. A skip writes nothing to P, so it can't re-add a finalizer.
3. **After every A2 of a cross-namespace ref, and after every A2 in
   PerNamePoll mode, the acquirer enqueues the (K, N) release batch
   itself** (§10.2). That covers an A2 still in flight. Say process Y's pass
   read consent live just before the withdrawal, and its A2 lands after a
   release has already removed the finalizer. Y's self-enqueued batch runs
   after the A2. Its consistent list (R2) finds no record naming P, because
   the dropping pass's A1 has removed it, and releases P by §5. In the
   watched modes, the A2's write also arrives as an informer event, which
   enqueues the same batch.

So no finalizer is left without a record and a pending release check. A2
still runs only after its index line (§10.6.3), so none is left without a
line either.

**Chains.** Each hop is held or not on its own, by the same gate. A hop
outside the *holder's* namespace needs consent for the holder's namespace,
whatever namespace the previous hop was in, because the hold and its
finalizer belong to the holder (§3). **An unheld hop doesn't stop the
chain.** Its `Next` is still followed, so a same-namespace Secret behind a
cross-namespace intermediate is still held. Because the unheld hop isn't
labelled, it isn't in the label-scoped cache, so it is read live, and the
read is shared for up to 10s (§9.4). §9.4 works through both shapes.

**The provisioner never grants a ref that isn't held.** A ref without
consent never gets an index line, and the provisioner grants only what the
index names (§10.7.1). Only the tracker's identity can write the index,
besides cluster administrators (§10.6.7).

**What this closes.**

- **Tenant-chosen grants.** Without the gate, a tenant could name any
  Secret in another namespace, and under PerNamePoll the provisioner would
  grant the controller `get` and `patch` on it *because* of that reference.
  Now a grant follows only a reference the target has consented to.
- **Pinning.** Without the gate, a tenant could put a finalizer on a Secret
  in a namespace it doesn't control, delaying that Secret's deletion and
  holding that namespace's teardown open. Now only consenting targets can
  be pinned, and only by the namespaces they list.

A cross-namespace user that *is* held keeps a Secret in a terminating
namespace alive until that user goes, as before. That namespace stays
Terminating until then, and the waiting event names the user.

### 11.4 A Secret already being deleted when an object first references it

If it carries this (K, N)'s finalizer, A2 holds it. The epoch moves and the
holder counts, so §5 keeps the Secret while its namespace is terminating or
a user is deleting. Otherwise it is released as §5 says. If it lacks the
finalizer, A2 is rejected (Deleting):

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
> and wait until its `credential-use.krateo.io/held-count` is `0` and its
> `credential-use.krateo.io/disown-settled` is the same string as its
> `credential-use.krateo.io/disown`. Check all three: the switch set and not
> empty, the flag equal to it, and the count `0`. If the count stays
> `unknown`, or the flag doesn't come, the CRD's Warning events say why and
> what to do (§7.4). After an out-of-order
> uninstall, remove the objects' own finalizers first. Then, if the CRD is
> gone, run the `DisownPrefix` Job. Otherwise remove by hand the
> `credential-use.<kind>.<group>/<namespace>` finalizers, the annotation with
> the same key, the kind's `credential-use.krateo.io/k-<hash>` label, and the
> `credential-use.krateo.io/held` label once no `credential-use.` finalizer
> is left.
>
> If the CRDs are deleted while the provider runs, with the objects gone, it
> releases what they held by itself. Keep it, and its RBAC, until nothing
> carries the kind's finalizer any more: list each protected kind, live and in
> every namespace, by the label `credential-use.krateo.io/k-<hash>`, or, in
> PerName mode, read each object its index names; or run
> `DisownPrefix` with `DryRun`. Its metrics don't tell: they count what the
> process caches.

The last paragraph is the release of a kind that is gone (§6.4, 2026-11-24). It
is the way out of a CRD deleted first, not the way to uninstall: Disown's count
is live, cluster-wide and settles, and it runs while the holders' own
finalizers can still be handled.

core-provider#313 is the same class: a controller that runs in the
namespace being deleted dies before it can release anything. This design
doesn't fix that. A provider must not run in a namespace whose objects it
manages, and the RBAC that keeps it working must not live there either
(§13).

### 11.6 Scale to zero (R11)

At 0 replicas, nothing releases. So **scale-to-zero also counts, as
instances, protected objects that carry a finalizer with the Kind's
prefix**. A new option asks for it,
`scaletozero.WithHeld(protected ...schema.GroupVersionKind)`.

**The change is additive** (IP5, the user's decision). `Exists` and
`Reconcile` keep their signatures and behaviour. Two functions are added,
and take the options:

- `ReconcileWithOptions(ctx, c, deploy, gvk, sel, lastSeen, holdDown, active, opts ...Option)`.
  `Reconcile` calls it with no options. It takes the RESTMapper it needs
  for the holder CRD's name from `c.RESTMapper()`;
- `ExistsWithOptions(ctx, r, gvk, sel, opts ...Option)`, for owners that ask
  outside a reconcile, as core-provider's `Undeploy` does (§16.3). A
  `client.Reader` carries no RESTMapper, so with `WithHeld` it also needs
  `WithRESTMapper(m)`, and fails without it.

With `WithHeld`, if
the instance list finds nothing, the check lists each protected kind
metadata-only, selected by **K's marker label** (§10.1), with `Limit: 1`. It
stops at the first object whose finalizers include the prefix
`credential-use.<kind>.<group>/`, and pages on only past a hash collision
(L3). A stale hold then keeps the controller running until it releases it,
after which the Kind can scale down.

In PerName mode, `WithHeld` reads the index instead, through
`WithHeldIndex(index types.NamespacedName)`, and GETs lines until one still
carries K's prefix (§10.6). In both modes, **a set Disown switch that hasn't
settled counts as an instance**, by §7.4's three checks
(`credentialuse.DisownSettled`), so the controller runs until Disown has
finished. A `disown-settled` that isn't the switch's value, `"true"`
included, hasn't settled it.

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
     silently protect nothing (§16.2). In this release it fails on a ref
     with a `Next`, before it connects (§9). In the next, given the kind's
     `KindOptions`, it follows refs as the tracker does when it holds them
     all: to the kind's `MaxDepth`, in the tracker's order, and with the
     object as the tracker gives it to a `Next` and to `Resolved`, without
     `managedFields` and `status` (`credentialuse.NextObject`). It fails on
     what would stop a
     holder, a chain that is too deep, a held ref with a `Next` of a kind
     without `Content`, or a `Next`'s error; and on refs of one object that
     conflict (§9.1), which would not. (TODO, for the release with chains:
     the helper takes a `Next`'s version from `o.Protected`);
   - **at run time:** `reconciler.CredentialUseCheckingReader(reader, ...)`
     wraps the provider's controller-runtime client. A read of a protected
     kind absent from the object's record logs once per object and counts in
     `credentialuse_unrecorded_reads_total`. The core can't wrap a
     controller-runtime client (§7.1), so it exposes `t.CheckRead`, and
     outside runtimes wrap their own clients around it.
3. **`RefsFunc` or `Next` fails:** fail closed for a new holder, open for an
   established one (§8). Release never calls them. A `Next` is never called
   in this release: a list that has one is refused in the same way (§9).
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
| A holder with refs that aren't held (§11.3) | A durable, runtime-owned list of them, with reasons, on the holder; when a credential read fails, a pointer to it in the failure message | annotation `credential-use.krateo.io/unheld`; the Synced suffix below |

In a terminating namespace, events are rejected (`git-provider-rlgov.log`
line 7), so there the log and the gauge are the signal. The message for a
new holder that must not connect is
`credential <kind> <ns>/<name> is being deleted and can't be held; not connecting until it is replaced or the reference changes`.
No new condition type is added (U4).

**The pointer to unheld refs, in the failure message.** When a pass fails in
Connect or Observe, so that Synced=False/ReconcileError, **and** the holder's
`unheld` list is non-empty, provider-runtime appends exactly:

```
; credential-use: <n> ref<s> not held (<reason> since <since><more>), see annotation credential-use.krateo.io/unheld
```

- `<n>` is the number of entries, counting those folded into `More`, and
  `<s>` is `s` when `<n>` is not 1;
- `<reason>` and `<since>` are those of the most recent entry, with `<since>`
  in RFC 3339 UTC;
- `<more>` is empty when `<n>` is 1, and `, +<n-1> more` otherwise.

For example:
`; credential-use: 1 ref not held (ConsentWithdrawn since 2026-11-08T17:02:11Z), see annotation credential-use.krateo.io/unheld`.

**Its length is at most 160 characters.** At most 142 is reachable, with
`<n>` capped at 9999 and reasons of at most 16 characters. It is added
**only** on a failing pass with a non-empty list. It is never added to a
successful pass, and never to a failure that isn't in Connect or Observe.
It is message text, not a new condition (U4).

**The one exception: a refused chain** (decided 2026-11-25). While an
established holder's refs are refused (§9), on a pass whose Synced is
True/ReconcileSuccess its message is exactly
`credential-use: refs refused, not held: <the refusal's error> (§9)`, on one
line: the error quotes and cuts the ref. Any other Synced (an Observe or
Connect error, a policy report, a deferral, a paused holder) says its own
outcome instead. The message is set in every status write the pass makes, so it
costs no write of its own, and keeps the condition's transition time while it
doesn't change; a pass that writes no status (it stops at the trace marker, or
on an Acquire or a status conflict) changes nothing. The first pass that isn't refused,
a failed one included, takes it off. A refusal lasts until the provider or
the object changes, and its event doesn't (§9), so the object itself says it.
A new holder's Synced is False/ReconcileError with the error, as for any
failure. Outside runtimes, in neutral terms: report a refused chain durably on
the object, not only as an event.

**Outside runtimes**, in neutral terms: when a pass fails because the
object's credentials or the objects that lead to them can't be read, and
the object's `unheld` annotation is non-empty, add to the failure you
report: the number of unheld refs, the most recent reason and its time, and
the annotation's key.

**The held gauge counts finalizers, not labels** (L4).
`credentialuse_held_objects{holder_kind}` is the number of (protected
object, finalizer) pairs with the kind's prefix in the process's cache. A
labelled object whose finalizers are gone doesn't count, and the janitor in
§10.1 removes its label. The cluster-wide, live count for Disown is
separate, and is written on the CRD (§7.4).

**Names dropped without the write loops** (§6.2) are said at the loops' start, and at the
end of a Track of their kind: one warning for each holder kind, with how many
namespaces' were dropped, whether every namespace's were, and what is kept, before the
list that makes up for them, and one for each try of it that fails. Nothing says it
while the process waits.

**The metrics**, all labelled `holder_kind`:
`credentialuse_held_objects` and `credentialuse_waiting_objects` (gauges),
`credentialuse_index_fill_ratio` (gauge, PerName and PerNamePoll),
`credentialuse_refused_holders` (gauge, the holders whose last pass in this
process was refused for a chain, §9; decided 2026-11-25),
`credentialuse_degraded_total{reason}` (every fail-open of §8, which earlier
revisions counted without naming; a refused chain has a reason of its own,
`ChainsNotSupported`, §9), `credentialuse_refs_not_held_total`,
`credentialuse_refs_conflicting_total{asked}` (refs that asked for more than
the first ref of the same object, which wins, §9.1; `asked="next"` can't occur
while chains are refused, §9),
`credentialuse_consent_withdrawn_total`, `credentialuse_refs_read_denied_total`,
`credentialuse_index_tampered_total`,
`credentialuse_unrecorded_reads_total` (§11.9),
`credentialuse_admission_denied_total` (class (e) of §6.2),
`credentialuse_release_rejected_total` (a release patch that failed against
an unchanged object, §6.2), `credentialuse_disown_unreadable` (gauge, the
objects Disown's last pass couldn't read, §7.4),
`credentialuse_disown_restarts_total` (Disown's runs of zero passes that
started over because the CRD changed, §7.4),
`credentialuse_index_read_denied_total` (objects the index names that a check
couldn't read for a 403, once each, §10.6.5) and
`credentialuse_crd_write_starved_total`
(§7.3). The core defines them as a
`MetricsRecorder` interface, and `pkg/telemetry` implements it with
OpenTelemetry. **The interface grows with the metrics** (it gained
`DisownRestarts` on 2026-11-23), so an implementation outside this module,
such as core-provider's adapter, must embed `credentialuse.NopMetrics`.
Otherwise one passed as `Options.Metrics` stops compiling at the next
method, and one passed to provider-runtime's `WithMetrics`, which looks for
the interface by a type assertion, silently records none of these metrics.

**The events on the holder CRD** are Disown's, all Warnings:
`CredentialUseDisownUnreadable`, `CredentialUseDisownIndexFull`,
`CredentialUseIndexLost` and `CredentialUseDisownRestarted` (§7.4).

**An informer that can't list or watch is logged**, a warning on a tracker's
sink or a klog error, since what it caches then goes stale in silence (the CRD and RBAC probes, 2026-11-24). The core
sets its own watch error handler, to fail a start on a 403, which takes
client-go's log away, so it logs instead: once per informer and cause, the
error's class, however often the reflector retries, and again after the
informer has watched again. A list alone isn't that: with `watch` refused, the
reflector lists at every retry.

- **The holder, CRD and index informers** log on the tracker's sink.
- **The per-name informers** (§13 shape 1) log there too: a 403 once per
  name, since each name needs its own grant; anything else once per protected
  kind, until all of the kind's watch again, since what one meets they all do.
- **The Namespace and protected-kind informers** are shared by every tracker
  and have no sink: they log as client-go does, through klog.

A watch that expired or ended, and what fails as the process stops, are
routine, and aren't logged. Before an informer's first sync a 403 is the
start's error, and isn't logged twice; a per-name informer has no start that
could fail, and logs it. An informer that stops, as a failed `Track`'s or a
per-name one whose line went, is forgotten: the next one logs its own.

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
| the same | `list`, `watch` | the label-scoped informer; one list of every object's metadata, with no selector, after names were dropped without the write loops (§6.2) |
| namespaces | `get`, `list`, `watch` | §5's live `deletionTimestamp` check; the metadata-only informer that sees consent on Namespaces (§11.3) |
| the holder kind | `get`, `list`, `watch`, `patch`, `update` | the holder informer, R2's consistent list, A1 (patch), and `Forget` in the finalizer-removal Update and on its own (update) (L8) |
| customresourcedefinitions, by `resourceNames` (the holder CRDs) | `get`, `list`, `watch`, `patch` | the Disown switch, read through a field-selected watch; R2's GET of a CRD that is gone (§6.4); the one-time settings and index pointer, the count fields, written only on change, and the removal of a stale `disown-settled` before a hold, without which that hold fails (§6.4, §7.3, §7.4, §10.6) |
| configmaps, by `resourceNames` (PerName and PerNamePoll: the kind's index) | `get`, `update`, `patch`, and `watch` in PerName mode | §10.6 |
| configmaps, in the index namespace (`krateo-credential-use`) | `create` (it can't be scoped by name) | the tracker creates the index (§10.6.7). The namespace itself is installer's; providers never create it, and admission limits what may be written there (§10.6.1) |
| the non-resource URLs `/apis/*` | `get` | the discovery of a holder kind's group, once its CRD is gone (§6.4). `system:discovery` grants it by default |
| events | `create`, `patch` | §12 |

`update`, `create` and `delete` on protected kinds are not needed: every
write is a patch. The grants are by resource, not by version. A patch of a
protected kind goes through the mapper's version, and a write of a holder through
its declared one. **Before adopting, check the conversions of each custom
resource they patch** (§6.2): through one that loses something, a metadata patch
drops fields, and a hold may be undone, or never released.

**After a protected kind's CRD is created again with another scope**, restart the
providers that track it. Until then their new holders of its objects may connect
without a hold, and in PerName holds may be counted as gone (§6.2: a limit the
user accepted, 2026-11-24).

**A new privilege for providers with static CRDs** (L6). git-provider ships
its CRDs in its chart, and has never needed to write them. It now needs
`patch` on its own CRDs, by name. The writes are rare: the Disown fields
when they change, a stale flag's removal, and the settings and the PerName
index pointer once. A CRD write makes the
API server rebuild its OpenAPI, which is why nothing else writes the
CRD.

**An uninstall takes these grants last** (2026-11-24). Holds that outlive
their holders are released by the running provider, after its CRD has gone
too (§6.4), and it needs every grant above for it, the CRD's `get` included:
a name in `resourceNames` grants it whether or not the CRD exists. The order,
and how to tell that nothing is held any more, are §11.5's.
`credentialuse_held_objects` doesn't tell: it counts (object, finalizer) pairs
in this process's caches, and so no object outside `SharedOptions.Namespaces`,
and none whose per-name watch is refused.

**LabelScoped exposes every Secret.** Its informer needs `list` and
`watch` on Secrets cluster-wide, with no name restriction. The tracker's
transform keeps only metadata in memory, but the grant itself lets the
provider read every Secret's data. That is the grant oasgen removed in
#510. Use it only where the provider already holds it.

**By name, three shapes.**

1. **Static, by name (`Watch: PerName`).** Where refs are known when the
   RBAC is generated, as core-provider generates RBAC per composition and
   knows the passwordRef Secret, the ClusterRole lists them in
   `resourceNames`. `resourceNames` does authorize `list` and `watch`, but
   only for a request with a `metadata.name` field selector: the API server
   takes the request's name from that selector and nowhere else
   (`k8s.io/apiserver@v0.37.2/pkg/endpoints/request/requestinfo.go:245-250`).
   So PerName makes one field-selected watch per held object, plus GETs.
   That fits small held sets, and needs the index (§10.6). The watches
   follow the index's lines. They cover every namespace, so
   `SharedOptions.Namespaces` doesn't restrict them, and no list of the kind
   is ever made: §10.5's live lists and the marker sweep leave a PerName
   kind out, and the prober uses GETs only. Every holder kind of one
   `Shared` must agree on a protected kind's watch mode.
2. **Provisioned at runtime, by name (`Watch: PerNamePoll`, §10.7).** A
   provider whose names are only known at runtime grants each one in a
   ClusterRole's `resourceNames`, bound by a ClusterRoleBinding. **The grants
   are a pure function of the index**: a provisioner reconciles them from
   the index lines, and revokes a name only after a live GET shows no
   finalizer and a later read shows no line (§10.7.1). The order is the index
   line, then the grant, then A2, and the reverse after the prune. An A2
   never runs before its grant: one that does fails with a 403 that looks
   like a credential problem, and is retried briefly while RBAC propagates
   (§10.7.2). The provisioner holds `get` and `patch`
   on the protected kinds cluster-wide, and grants only those, named, so
   there are no watches.
3. **Unrestricted (`Watch: LabelScoped`).** Refs that can't be known in
   advance and aren't provisioned, such as git-provider's user-supplied
   Secrets, need the verbs on all objects of the kind.

**Escalation.** Kubernetes lets a subject grant only permissions it holds,
unless it has `escalate` on roles (and `bind` to bind roles it doesn't
hold). A provider that holds `*` on `rbac.authorization.k8s.io` has
`escalate` and `bind` implicitly. **The design never depends on that**: the
PerNamePoll provisioner grants only `get` and `patch`, which it holds.

Doc text for consumers:

> **This release holds direct refs only.** Every ref your `CredentialRefs`
> (or `RefsFunc`) returns must have a nil `Next`: name each Secret, and each
> other object Connect reads, as a ref of its own. If any ref has a `Next`,
> the whole list is refused with `credentialuse.ErrChainsNotSupported`: a new
> object doesn't connect and reports `Synced=False`; an existing one keeps
> working, with `Synced=True` whose message says
> `credential-use: refs refused, not held: …` for as long as it lasts, one
> `CredentialUseDegraded` event, which expires, and the gauge
> `credentialuse_refused_holders`; it keeps what it already holds, but
> **nothing new is held, its direct refs included**. On an upgrade
> to credential-use, an existing object has no record yet, so it holds
> nothing. Disown doesn't let a refused new object connect. A Secret whose
> name is only inside another object can't be named, so leave it out: it
> isn't protected, as before. Read that other object in Connect as you do
> today; `credentialuse.Resolved` returns nothing. A later release follows
> `Next`, and will accept it. Run `reconcilertest.AssertCredentialRefsCover`
> in your tests: it fails on a `Next`, and on a Secret that Connect reads and
> the refs don't name, so it can't be used for a fixture whose Connect reads
> a Secret through another object.
>
> If your connecter implements `CredentialRefsProvider` (or your runtime
> calls `credentialuse`), grant, in a ClusterRole bound with a
> ClusterRoleBinding: `get`, `list`, `watch` and `patch` on `secrets` and on
> every other kind your refs name; `get` on `namespaces`; `patch` and
> `update` on your own kinds; `get`, `list`, `watch` and `patch` on your
> kinds' CustomResourceDefinitions, by name. Use `resourceNames` only when
> you know every Secret name when you generate the RBAC (`Watch: PerName`),
> or when you provision each name at runtime (`Watch: PerNamePoll`); both
> need the index ConfigMap's grants (§10.6.7). With PerNamePoll, the
> credential-use ClusterRole is created and written only by your provisioner.
> Don't ship it in a chart, and don't give it an `aggregationRule`. Connect's
> own read grants come from your separate provisioning. Don't use a
> namespaced Role for these permissions: it is deleted with its namespace,
> and objects there then can't finish deleting. Discovery (`get` on `/apis/*`)
> is granted by `system:discovery`, unless your cluster has removed its binding.
>
> **Refs across namespaces.** Holders hold a Secret in another namespace
> only if that Secret, or its Namespace, carries
> `credential-use.krateo.io/allow-holders-from` listing the holder's
> namespace. `*` is accepted on the Secret only. Without it, the reference
> still works as before, but isn't protected, and the holder gets a
> `CredentialRefNotHeld` event. Removing a namespace from the annotation
> later releases those holders' holds through the normal protocol, with a
> `CredentialConsentWithdrawn` event. It needs a pass of each holder, so not
> of a paused one. With restricted caches (`SharedOptions.Namespaces`), a
> holder that records more than 16 refs outside them, m of them, may take up
> to m/16 poll intervals, rounded up, for one it no longer names.

## 14. Tests, and what the reviewer should attack

**Envtest: namespace teardown** (modelled on
`pkg/scaletozero/envtest_test.go`, with its own `TestMain` and
`requireAPIServer`). It runs twice. In `pkg/credentialuse`, a minimal
reconcile loop that follows §7.2 drives the core. In
`pkg/credentialuse/e2e`, a test-only package, so that the core's tests don't
depend on `pkg/reconciler`, a manager runs the real reconciler for
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
  teardown; a re-pointed Target enqueues its holders. These, and every other
  test of a chain, set the tests' switch (§9).
- A chain is refused as production builds a tracker (§9): no call at all for
  a record that names no ref in another namespace, for a new, an established
  and a deleting holder, wherever in the list the `Next` is; a recorded ref
  elsewhere is gated all the same, and its withdrawal releases it, with the
  refused chain among the cases of §11.3's gate; a holder keeps what it held and can finish its deletion; a list
  without a `Next` is the whole chain; no production code sets the switch.
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
- **Disown under PerName** (§7.4): the lines stay until it has settled, and
  a hold that lands late, with its response arriving or lost, is found by
  its line; only a process whose own run is done empties the index, and only
  lines it proves free; no prune takes a line meanwhile, whichever side of
  its removal the switch is set on, nor with the switch unread; a full index
  leaves the records unstripped; two processes settle apart.
- **The residual of §10.6.3, in the model:** with its three conditions a hold
  without its line is reached, and with any one off none is, in schedules of
  at most three preemptions, or two where stated there. That is all "safe with
  one off" says.
- **Disown over the index, in the model:** the switch set, turned off and on
  again, widened, and narrowed and widened again; the A2 answered, lost, and
  lost with its sender killed; the holder there or gone; with a pruner whose
  holder cache and CRD cache are behind. Two things are taken as given: **the
  landing margin**, that a hold whose pass read the switch before the run's
  first pass has landed, or failed, by its last; and **a prune's bound**, that
  a prune begun before the run's first pass has ended by then, its re-add
  included. A control per rule checks that the model sees what it is for, and
  stops at the first schedule that does.
- **What the index's and Disown's models are not.** Those with a Disown pass
  have a copy of its steps, made by hand: what they run of the code is the
  index, the acquirer, the release batch and the count's decision, and a
  change of `disownPass` has to be made in the copies too. Only the drops'
  model has the watch on the index, whose dropped lines a live process checks.
  A prune here has
  one line, so one write. The late check, a prune and Disown meet with no
  kill only in "Disown over the index", and there in the cases with a lost
  response and a sender that lives; two of the four with the holder gone aren't
  exhaustive at 2, below.
- **The bounds of the index's and Disown's models.** CI searches every
  schedule with one preemption less than `CREDENTIALUSE_MODEL_DEEP=1` does,
  which is the bound §10.6.3 states: the flag, 2 with two processes and 3 with
  one (DEEP 3); the late hold, 2, and 1 with the late check and a holder that
  goes (DEEP 3 and 2), the cases of the pruner's rules
  included, but 1 in both for the control of the sender's own prune; Disown over
  the index, 1 (DEEP 2); the kill inside a
  prune, 2 for the wait that isn't built and 1 for the other clean cases (DEEP
  2), and with two Disown processes 1 for the wait, the rest under DEEP only,
  at 1. A search that reaches 1,000,000 schedules stops, and says that it
  isn't exhaustive: under DEEP, two cases of Disown over the index are at 2,
  "lost, holder gone" with the switch set, and "lost, holder gone" with it
  off and on (every schedule with 1, and the first 1,000,000 with 2). The
  kill's six clean cases with two Disown processes were searched whole, at 1.
  **A search with one
  preemption is no evidence for a case that needs two**: in "off and on" and
  "narrowed and widened", and in the controls "a run over a changed CRD" and
  "a pruner whose CRD cache is behind", the violation or the landing needs
  one or two, so only DEEP covers what those controls guard.
- **What they cost**, measured at 7fd7022 with CI's bounds on one VM size
  (n2-standard-4), not in CI: the kill 436s to 457s over two runs (465s at
  5a72b68), Disown
  over the index 222s, the late hold 76s (180s at a5340fe, of which 95s are the
  cases of the pruner's rules, new since), the flag 46s, the drops 2s. Every
  model together took 3,970s, 66 minutes, of which these new ones are about
  13; the older ones alone, main's at 2beee26, took 3,144s there, 52 minutes.
  In CI's last green runs, without the new ones, the model step took 2,134s to
  2,320s; its time with them is not measured yet. Under DEEP on that VM size:
  the late hold 1,000s (1,486s at 5a72b68, of which 438s are those cases; the
  control of the sender's own prune, added since, 31s at a5340fe), the
  flag 509s, Disown over the index 5,716s, and the
  kill's clean cases with one Disown process 626s; at ae2012d, every case of the
  kill with two Disown processes 1,783s.
- **A prune leaves the line of a hold that may land yet** (§10.6.2): no prune
  but Disown's emptying for a while after the write loops start, and for a
  kind tracked while they run; none of an A2 of this process in flight, or of
  unknown outcome, one begun during the removal included; what is left is
  pruned once it is due, unless it is held, named or unreadable by then, or of
  a kind the switch covers, which is left to the emptying; the
  index is told of an A2 before its line is ensured, and of its end after its
  check, in a pass and before establishing.
- **The flag** (§7.4): a switch turned off and on again while nothing runs
  isn't settled by the old flag; no A2 under a stale flag, its removal
  refused included; a run starts over when the CRD changes between its
  passes or during one; the report of a CRD that keeps changing, its metric
  and its throttle.
- **A lost index under Disown** (§7.4): the count stays `"unknown"`, through
  a rebuild too, until the index is vouched for by its uid; another
  ConfigMap's uid doesn't count; a kind never tracked before settles.
- **An A2 of unknown outcome** (§10.6.2): checked at once and after the
  margin, its batch enqueued both times; a stop lets it, and a prune, run to
  their bounds and no further (§7.2).
- **A process whose write loops haven't started** does nothing in a pass,
  and fails the manager later (§7.5).
- A holder informer relist resets `recordSince`, and the next passes write
  (L2). The relist is forced with a 410 both on the watch-list path and on
  the List path it falls back to.
- `IgnoreCredentialUseRecord` passes a record change it didn't write (L8).
- **The index stays a superset** (§10.6.3). The reviewer's interleaving runs
  as a test, with interceptors holding each step: R2 sees no users, H2
  sees the line, R3, H2's A2, then the prune. It is run in both orders of
  H2's index GET and the prune, and asserts the line is back either way.
  The same is checked for the startup recheck's prune against an A2.
- The tracker creates a missing index at start; a hand edit that drops a
  held line is restored, with an event and the tampering metric; a
  conflicting rebuild merges with a concurrent add (L5); a missing or
  unreadable index keeps `WithHeld` true and the count `"unknown"` (L4).
- The 80% warning and the full index: a new holder fails closed and an
  established one is Degraded (L2). Setup rejects a cap above the
  ceiling, an index namespace that holds holders, and a short
  `(DisownPasses − 1) × DisownInterval`, an explicit interval or a single
  pass (L3); an unset interval is derived long enough (§7.4).
- **N processes without leader election** (§6.8): three `Shared` instances
  of one kind, none leader-elected, run the stress test, and Disown and its
  count settle correctly, including a process that reads another's non-zero
  count.
- **PerNamePoll** (§10.7): no watch is opened on a protected kind; holds
  under RBAC provisioned in order; an A2 run before its RBAC fails with a
  403 that is reported as an Acquire error, not as Deleting; a failed
  provisioning fails closed for a new holder and is Degraded for an
  established one.
- **Error classification** (§6.2): each of the five classes is produced
  for real in envtest and classified correctly:
  - (a) an A2 on an object being deleted without the finalizer;
  - (b) a write the namespace policy denies, which is a 403 because of
    `reason: Forbidden`;
  - (c) a write with `cu-policy-params` deleted, a **422 that must not be
    read as Deleting**, because it has no `metadata.finalizers` cause;
  - (d) an A2 without RBAC, **with the authorizer's exact message pinned**,
    for a namespaced and a cluster-scoped object;
  - (e) NamespaceLifecycle's 403 on a create in a terminating namespace.

  Only (a) yields Deleting. (b) to (e) are Acquire errors, failing closed
  for a new holder and Degraded for an established one, and only (d) is
  retried as RBAC propagation. A policy with no `reason` is also tested,
  to pin that it yields 422. So is R3's failed `test` op, the 422 with the
  generic message and no causes, and a `remove` at a moved index.
- The Deleting outcome is recognised by its 422 Invalid with a Forbidden
  cause on `metadata.finalizers`, and never by a 403.
- **The provisioner** (§10.7.1): grants follow the index, coalesced to one
  ClusterRole update per cycle; the reviewer's interleaving (prune, a GET
  with no finalizer, A2 before revoke, the post-check re-add) ends with the
  name re-granted within one cycle, and the finalizer never lost; no
  revoke while the object carries K's prefix.
- A 403 on A2 is retried for about 8s, then reported as an Acquire error
  (L4). In PerNamePoll, the `written` skip happens only after a live GET
  confirms the UID and the finalizer (L1).
- The published settings: a second process with a different
  `deletionGrace` refuses to start (L3). A rollout with two `RefsFunc`
  versions stays safe and is measured for churn. Both owner finalizer
  names are accepted during provider-runtime#46.
- Tamper checks GET every dropped line once, except those of a version this
  process's own write returned; a removal's conflict or lost response, and a
  drop during its check, lose no line (§10.6.6).
- **Cross-namespace consent** (§11.3): a ref without consent gets no line,
  no grant and no A2, and Connect proceeds with `CredentialRefNotHeld`;
  consent by object, by namespace, and `*` on an object; a `*` on a
  Namespace is ignored; withdrawal releases the hold through the normal
  protocol, seen by watch and by the PerNamePoll poll, and a concurrent A2
  that lands after the release is caught by the batch its own write
  enqueues; both partly consenting chain shapes of §9.4, held hop by hop,
  with the chain followed past an unheld hop; the provisioner never grants an
  unheld ref; the Secret's own namespace deleted while an unheld holder
  lives, and its recovery.
- The provisioner serves two kinds from one ClusterRole: the union of
  grants, and revocation checked against both prefixes after re-reading
  both indexes (L1). It is watch-driven with a fallback poll (L3). A 403
  on A2 requeues without holding a worker (L2).
- Disown and Degraded leave Connect working under the split ClusterRoles
  (M1).
- **The gate on every pass** (M1):
  - an established holder that skips A2 sees a withdrawal in its cache,
    confirms it with a live read, drops the ref in A1, and the hold is
    released. The same in PerNamePoll mode, through the skip's live GET;
  - a cached withdrawal the live read contradicts drops nothing, and a
    failed live read keeps the hold and requeues;
  - **a restart with Namespace-level consent while the target's namespace
    is terminating:** the Namespace informer hasn't synced, the live read
    shows consent, the hold survives, and the teardown completes with the
    holder protected;
  - **a skipping pass right after a new hold, before the informer delivers
    P:** the cache can't answer, the live read shows consent, and the hold
    stays;
  - a deleting holder always reads live.
- `Resolved` carries unheld hops too. After a re-point of an unheld hop,
  Connect uses the old chain for up to 10s, and never an unheld
  same-namespace Secret (L1).
- **The dedicated index namespace:** start fails clearly when it is
  absent. With the policy absent, the start check refuses a holder of K
  there. With the ValidatingAdmissionPolicy installed (envtest, with the
  policy, its binding and its parameter ConfigMap applied):
  - writes of `kube-root-ca.crt` by
    `system:serviceaccount:kube-system:root-ca-cert-publisher`, and of the
    `default` ServiceAccount by
    `system:serviceaccount:kube-system:service-account-controller`, are
    allowed, and so are both as `system:kube-controller-manager`. The same
    names written by any other identity are denied;
  - **deleting the namespace completes**: the policy matches no DELETE, and
    the test's namespace controller empties and finalizes it;
  - a dynamic composition-controller service account,
    `system:serviceaccount:krateo-system:widgets-v1-2-0`, created during
    the test, writes its `cu-index-*` ConfigMap;
  - **a composition object is refused**, and so is a ConfigMap not named
    `cu-index-*`, or one written from outside the management namespace;
  - installer's update of the `krateo-credential-use` Namespace object is
    allowed, which tests the exclusion;
  - events are allowed.
- A NotFound on the live confirmation makes the ref Missing, with no
  requeue loop. Five 403s over 2 minutes in PerNamePoll drop the ref, with
  the event, the metric and a `ReadDenied` entry. Start fails without
  list or watch on namespaces.
- **Failover** (M1 of the review of revision 16): two processes, one
  leader. The leader removes a holder's owner finalizer and record and
  stops before R3. The standby's informers see the holder go before it is
  elected; once elected, its rescan releases P. The same with a grace timer
  that was pending when the leader stopped.
- A persistent consent-read failure under API-server overload (an
  interceptor fails every consent GET): each cross-namespace holder keeps
  its hold, and its requeues back off from 1s to the poll interval, then
  return to 1s after one read succeeds (M2 of the review of revision 16).
- An R3 that fails against an unchanged resourceVersion is reported, with
  the event and the metric, and isn't retried as contention (L1 of the
  review of revision 16).
- R0 stops for a deleting P with a deleting user in the cache (L1). In
  PerNamePoll, an A2 enqueues its own release batch (L2). Consent is the
  union of the two lists, and an object's list can't narrow its
  Namespace's (L3). Trimming keeps `ConsentWithdrawn` first (L4). The
  Namespace handler ignores changes that aren't to `credential-use.*` or
  the `deletionTimestamp` (L5). A shared unheld hop is read once per 10s
  (L6).
- **The unheld record and its pointer:** a ConsentWithdrawn ref plus a
  not-found Connect error produce Synced=False/ReconcileError whose message
  ends with the exact suffix, under 160 characters. A successful pass, and
  an empty list, add nothing. The annotation is written in the same patch
  as the record, never in a write of its own. It caps at 16 entries with a
  `More` entry. The holder event names target, namespace and release.
- A foreground-deleted holder that owns its Secret completes (§11.8).
- Setup after start, and `Track` twice with equal and with different
  Options.
- **A holder kind that is gone** (§6.4): the premises (a CRD stays while an
  instance has a finalizer; what its list and its GET then answer); a standby
  whose holder and CRD went before it is elected releases the hold, in each
  mode, with a mapper that remembers the kind, one that forgot it and
  controller-runtime's; a refused GET of the CRD keeps the hold until it is
  granted; the kind that returns before R3 keeps its new holder's; a ghost,
  left by a connection that was down, is released when the CRD informer sees the
  CRD gone; a kind that goes and returns is served as before; a CRD replaced by
  one of another plural keeps the new holders' holds, without a fight between
  the two processes; and, on the wire, the order of the reads, and that the GET
  of the CRD carries no resourceVersion.
- `scaletozero.ExistsWithOptions` and `ReconcileWithOptions` with `WithHeld`,
  and `Exists` and `Reconcile` unchanged without options, stopping at the first object with
  K's marker label, and paging past a forced hash collision.
- `Watch: PerName` against `resourceNames` RBAC, through an impersonated
  ServiceAccount. envtest's API server already runs
  `--authorization-mode=RBAC`
  (`controller-runtime@v0.25.2/pkg/internal/testing/controlplane/apiserver.go:303`).

**Unit tests for the races (§6)** run on an in-memory API model: a
linearizable store with resourceVersions, `ValidateNoNewFinalizers`,
strategic-merge and JSON-patch semantics, a namespace phase, and a lagging
cache per actor. It removes an object in one step, which the API server doesn't
(H1 and H3, §6.4), and has no wait before a release, which only moves it. They
enumerate every schedule of:

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

Beside it, with the same protocol code and no caches, a holder kind whose CRD is
gone (§6.4): a releaser's two batches, the first on an API server whose list is
a 404 whether or not the CRD is there, against the CRD's return, a new holder,
its establishment, P's deletion and its own, on every schedule of three
preemptions. Two unsafe rules, the CRD read before R1 and the list alone, are
its controls: it must find each one's violation.

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
processes would. The run is recorded, and audited at its end (below). At the
end, every namespace tears down, and no object is left with a
`credential-use.` finalizer and no users.

**What the stress test shows.** `TestStress` runs direct refs in four of its
five sub-tests (Secrets in the holder's namespace, with two and with three
processes and with every hold outside the caches; and Secrets in another
namespace whose consent toggles), and chains in the fifth. It used to fail
only when a deleting holder's read failed, and it counted every such failure.
That could tell neither of two things: a wrong release of an object that
nobody was reading then, which the test's own DELETE finished off; nor a
failure that the API server had caused (H3 below). Now:

- **What it has.** Its own watches have every version of every Widget, Target,
  Secret and Namespace, from a list at the start to an object made after the
  last write, so a watch that missed something fails the run. resourceVersions
  order them all, since envtest has one etcd. It sees every call the processes
  make through their metadata clients, with its answer: for R2's list, the
  revision of what it answered. It has the answer to each of its own DELETEs.
  It knows which pass a call belongs to, and for a release, which goroutine,
  since a batch runs on one. It has its own reading of records, users and
  consent.
- **Every release is judged at the moment it lands**, whether the object is
  being deleted or not, and whatever becomes of it. R1 is the batch's last read
  of the object before the patch, and R2's whole list, and the Namespace's read,
  must come between the two. The patch must apply to exactly what R1 read: the
  same incarnation, no A2 since. And §5's table
  must allow it, for the users (§6.1) the watch had at R2's revision, or whose
  A2 on that incarnation had landed when the release did: §6.4 says the second
  are among the first, so a list read too early is caught. A user that had
  announced, and whose A2 hadn't landed, isn't one: §6.4's third case. A
  deletion of a holder or of the namespace after R2's reads is §6.4's accepted
  window. The grace is measured from when the test began the DELETE, so one
  shortened by less than the DELETE, the informer and the queue take isn't
  seen.
- **Every finalizer that goes, goes by a release, and every object that goes,
  by a release of that incarnation or by a DELETE that named it.**
- **What a pass reports is checked:** a deletion pass holds each ref with an
  A2 of its own. A live one may skip, by §6.3 and by what its own informers had
  delivered, which the test's hooks see: the holder's version it works from,
  last; an A2 of its process that began after the first delivery of a record
  that names the ref; and nothing since that drops it, delivered before the
  pass began. That the informers deliver, within 5s of the test's watch, is an
  audit of its own (`LAG`), which may also say that the machine is starved: it
  prints the longest that the test itself stood still. No A2 comes before the
  record names its object. Each reason why a ref isn't held, and each acquire
  that stops or leaves the holder Degraded, has an answer of the API server's
  behind it, the last one the pass had: a call that its own context ended has
  none.
- **Reads that must be live are:** a GET with a resourceVersion is no R1, and
  no read of a Namespace or a consent. And none answers less than the watch
  had delivered before it began. A NotFound of what the watch had delivered is
  right if the incarnation went, and nothing that began after the answer was
  made at a revision below its removal's: the watch may deliver the removal, and
  an A2 before it, after the answer.
- **Consent** (§11.3): an A2 of another namespace's object follows, in its
  pass, live reads of the Namespace and of the object, the last of which
  consent; and a skip, a consent its caches may have had.
- **One worker a key** (§10.2), **and the labels** that go with the finalizers
  (§3).
- **Each A2 writes an epoch its finalizer never had on that object**
  (`EPOCH-REUSED`), so that R3's test tells the holds apart. One that wrote
  nothing, answered with a version another write made, is no bound of anything,
  since its revision is older than it; and its hold counts from its answer on, so
  that a release that takes it from under its user is judged as one over an A2
  that landed.
- **A record write drops no ref the holder still names,** except one in
  another namespace whose consent that pass read, live, as refused. And a pass
  that went through every ref leaves exactly the refs of the version it read,
  less those without consent.
- **The owner finalizer** (§6.7): it is added by a pass that held each ref of
  the record, or read it absent after its last A2.
- **What must happen in every run does.** Canaries make it, beside the
  goroutines and within the run's 8s. An object with one live user is deleted,
  and goes after its grace. One in a namespace that was deleted first is kept
  past it, until its user goes; and so is another namespace's. One whose user
  begins to be deleted just before R2's list, after the grace, with R0 past, is
  kept until the user goes; and that a batch did list that user after the grace,
  by its own process's first sight of the deletion, is a floor. A hold goes when
  its holder names another object. And a hold of another namespace's object goes
  with its consent, though its holder still names it. And a run with too few
  releases, holds, skips, establishments, or consenting reads, fails. Those
  that go by the number of passes fall with it, so a defect that makes fewer
  passes lowers them too.
- **A deletion pass reads what it has just held** (I1). It is a deletion pass
  if the holder was being deleted when the pass read it. One that began live
  isn't counted there, and is in every other audit. A failed read is exempt
  only as H3, on proof: a DELETE of the test's was answered with a Status that
  names the very incarnation this pass's A2 held; that incarnation carried the
  finalizer when it went, and was never marked as being deleted; the DELETE
  began before the hold it went with could be seen; no release of that name
  that succeeded overlaps that hold, and none has an unknown outcome. Nothing is
  exempt by the error's text, or by how the holder is when it is read again.
  A DELETE whose answer is lost names no incarnation, so it exempts nothing,
  and fails the run by itself.
- **On a failure it prints** what it found, and the timeline around it, by
  revision: versions, calls, with what each list had, passes, releases and
  DELETEs. Each lost A2 that it counts and doesn't fail on (H1, below) is logged
  the same way, as a `NOTE`, to be read by hand.

Each audit is tested on made-up histories, without an API server, and against
mutants of the protocol by `hack/revert-check-credential-use-stress.py`, which
counts the iterations each takes to be found, by the classes it names. Before
the canaries, at 79c6868, in the plain sub-test of 8s on 4 CPUs, 13 of 16
mutants were found in the first or second iteration every time (R3 without the
epoch test, for one, in 3 of 4 iterations). The three others: after the grace,
deleting users not counted, 3 reds in 9 iterations; the same in R2 alone, 2 in
21; and **a release that ignores a terminating namespace, §1's own teardown, 0
in 30.** Two canaries make the three happen in every run. The mutants of today
are to run in the plain sub-test, in the cross one, and once each
(`REVERT_REPEATS=1`) with every hold outside the caches; no output of theirs is
on file yet.

**What a green run of 300 iterations says**, as
`hack/count-credential-use-stress.py` counts one: a defect that these audits
can see, at envtest's timings, shows in under about 1% of iterations (95%). It
says no more. It says nothing about stale holders (the driver reads them live);
more than one ref per holder; lost responses, 5xx, restarts, failover, clock
skew, or two API servers; a holder recreated under its name; liveness beyond the
canaries; chains, PerName, Disown, or cluster-scoped kinds; events, or the
unheld annotation; or a real cluster's rates of H1 and H3. Of the target's
namespaces of cross-namespace holds, only the canary's is deleted.

An iteration is excused only if every error line of it is an audit's of the API
server's own H1, two findings at most, of a class whose rate is **stated** for
its sub-test, with the commit it was measured at, which nothing compares with
the one that ran. **None is stated now, so an iteration with H1 is red.** H1's wait changes the rates: one measured before
it would hide a defect that loses the wait, which brings the old rate back. They
are to be measured on top of it, every one read by hand, and again when the wait,
the audits or the machine change. A stated rate is itself a count, and is
compared as one: a run is over it if, were the rates the same, so many of the two
runs' total would fall in it with a probability under 2.5%. Short runs of one
sub-test are added up first. What that can't see is a defect of H1's shape that
adds less: were 5 in 207 stated, up to 19 in 300 would pass, and were 0 in 207,
up to 7; and one that loses the wait is seen only if it raises the rate by more.

Before the wait, at 79c6868, in the plain sub-test on 4 CPUs, unloaded, the
recreate was seen in 5 of 207 iterations, 2.4% [0.8, 5.6], and the lost A2 in
none, under 1.5%. The audits of that commit had no class for it, and called all
five `UNEXPLAINED-REMOVAL`. Each was read by hand: two processes' releases of one
incarnation answered within 5ms of each other, the test's create between, and
the recreated Secret gone one or two revisions after it was created. They
haven't been judged again by today's rule, whose near end is when the create
began, and whose far end is bounded. So they state nothing.

**What it can't show:**

- **H3, the API server's, not ours.** A plain DELETE reads an object without
  finalizers and removes it in a second step, over a first A2 that succeeded
  in between. No hold can stop it, and it is as if the DELETE had come first.
  A probe of the chained sub-test, most of it under CPU load, saw held objects
  that had never been marked as being deleted go 58 times in 472 iterations of
  8s, and 5 of those failed a deletion pass's read. It didn't have the DELETEs'
  answers, so it took them for H3 by elimination; the stress has them. It
  counts both, and fails on neither.
- **H1, the same gap in a release.** The API server tests R3 and removes the
  object in two steps. An A2 that lands between them succeeds, and goes with
  the object. It was seen in 25 of 10,625 removals in that probe, with no
  failed read. The stress counts it, by the version that R3's answer says its
  tests ran on; a deletion pass's read that fails for it fails the run, under
  its own name (`H1-LOST-A2`), on proof: the pass's A2 made the last version,
  every release that took it had tested an older one, and no release of that
  incarnation has an unknown outcome. The
  other half of H1, where a second release of one incarnation removes the
  next, wasn't seen there. The stress has seen it: two processes' releases of a
  Secret answered, one removed it, the test's recreate landed, and the other's
  removed the recreated one at the next revision. It fails the run, under its
  own name (`H1-RECREATE`), only where both releases answered, of two
  processes, one of them after the create began; no other write of ours on the
  name overlaps the recreated Secret's life, and none, nor a DELETE, may have
  landed; and nothing that began after the late release had ended was answered
  as of before the removal. An A2 whose answer has its finalizer may overlap;
  and so may a third process's release that, too, was answered as the removal
  of the incarnation before: it is another candidate for the late one. Where,
  within 200ms of the late release's end, neither the removal was delivered nor
  anything answered as of after it, it isn't bounded: it fails the run as
  `H1-RECREATE-UNBOUNDED`, which nothing excuses. One process's two releases of
  one incarnation are a defect of ours, whenever they were.
- **The chained case.** A pass that followed an older version of a Target may
  drop from the record a Secret that the holder now needs (H2, open). The
  stress counts such drops and doesn't fail on them: every release after one
  is right by the records. Chains aren't in the first release.
- **A wrong release that its own races hide.** A defect whose window is a few
  milliseconds is found at the rate at which the stress's operations fall in
  it, so one green run says little of it.
- **`BeforeEstablish` left out:** a holder here has one ref, so its A2's own
  404 is confirmed by the read that §6.7 asks for.
- **A deleting user outside the caches:** that canary doesn't run there, where
  only a pass of its holder would see the object being deleted.
- **What the mutants weren't run on.** With three processes, and chained, the
  same audits run, and what they would miss there isn't measured.
- **`LAG`'s blind spots:** a run's last 5s; the Namespaces' feed; a handler that
  returns just after the test's hook; a holder's versions within 5s of its end
  (how many are judged is counted, with a floor); and, of a protected object that
  a relist found gone, its versions in the 5s before that.
  A machine that stands still for 5s fails it, and it can't tell.
- **A stale NotFound** of an incarnation that did go soon after, in a moment when
  nothing else was written between.
- **The index** (§10.6), Disown, a restart and a lost response: other tests',
  and the model's.
- **A real cluster's rates.** envtest has one etcd, which doesn't sync its
  writes, and no latency to it.

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
  its workers before a source, if any controller-runtime path does;
- "this CRD is gone" against "this GroupKind is gone" (§6.4): another plural,
  an aggregated API server, a discovery that lags, fails in part, or is cached
  somewhere; and whether anything can make the two processes' fight a loop again;
- R0, R1's shortcut and the index's prune while the CRD is absent from its
  informer (§6.2): whether they can do more than cost live reads.

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

**15.9 A released mark, on objects being deleted** (H1, §6.4): the release patch
would leave the finalizer and set a mark that makes every A2 fail, and a second
patch would remove the finalizer. It closes the lost A2 and the recreate, at one
more write per such release and 130 to 150 lines in the release protocol, with a
backstop for a process that stops between the two. Not taken (decided
2026-11-24): the random wait makes the recreate rarer for 60 lines, and lost A2s
aren't a notable share of the releases, which was the condition to take it: 0.24%
under stress, with no violation. It is worth another look if a real cluster's
window is well past 5ms, or if kinds without leader election commonly have held
Secrets deleted and created again at once.

**15.10 A longer wait, and other ways to spread two processes' releases** (H1, §6.4).

- *A wait below 10s:* 3 of 1,000 releases overlapped (95% 0.06 to 0.87%), against 43 and
  44 of 1,000 at 2s, with 40 at once of a quiet kind, in two versions of the test: too
  few to say how many times fewer. With more at once it should near five times (10s
  over 2s), since the lists then wait for the same progress notifications all the time;
  that isn't measured. The wait alone would take 5s on average and up to 10s, not 1s and
  up to 2s. Not taken: the wait stays below 2s, as built (decided 2026-11-24, after
  the measurements).
- *A random delay between R2's list and R3* spreads what the list aligned. It widens
  every window that begins at R2's reads, the unrecoverable one among them, to make a
  recoverable case rarer, and holds a worker. Not built or measured.
- *An order of each process's own for a batch's R3s* was built, measured and taken out:
  with 200 Secrets in one namespace 18 and 12 of 3,000 releases overlapped with it, in
  two runs, and 14 and 11 with the wait alone.
- *A list that doesn't wait:* no one request makes one, on an etcd whose watches can
  report progress. A list without a resourceVersion is then served from the watch cache,
  with a limit or without (v0.37.2 `storage/cacher/delegator/interface.go`), and one with
  a resourceVersion isn't a consistent read.

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

**How exposed it is** (oasgen's correction, §1). Its Delete tolerates a
failed Secret read, and is exposed only where a resolved value is needed to
build the delete request, such as a path parameter, when `BuildCallConfig`
returns nil. Protection matters for those kinds; for the rest, a missing
Secret doesn't block deletion today.

rest-dynamic-controller adopts through unstructured-runtime's integration
(§7.2), not `CredentialRefsProvider`. Its `RefsFunc` is the collector it is
writing for #510:

- **Per-object Secrets** resolved from paths in the object's own spec come
  straight from `RefsFunc`.
- **The Configuration-level Secret** is a chain: a ref to the Configuration,
  with a `Next` that names its Secret. The Configuration is held too. **The
  first tag refuses chains (§9)**: until the next release, this Secret is
  unheld, unless the collector can name it without reading the
  Configuration through the API.
- **`RefsFunc` needs the RestDefinition** that describes the paths to be
  loaded and readable. It is called only on acquire. **Release never calls
  it**, and must not, or the deadlock comes back one level out: a teardown
  that deletes the RestDefinition first would leave nothing able to
  release. When the RestDefinition can't be read, `RefsFunc` errors. A new
  holder fails closed, an established one fails open with its record (§8).
- **A mistyped or unmatched path resolves to not found**, so it silently
  protects nothing. `AssertCredentialRefsCover` (§11.9) fails on it. The
  collector's tests should run it for every RestDefinition fixture.
- **RBAC, as things stand.** Today oasgen's provider mints
  `resourceNames`-scoped Secret Roles that are authorized only through
  `escalate`, which comes from a `*` on `rbac.authorization.k8s.io`. Its
  ClusterRole doesn't mention Secrets at all. oasgen-provider#526 tracks
  that posture. In oasgen's session, the user decided that #510 proceeds on
  the existing mechanism, and the posture change happens separately in
  #526. This design never relies on `escalate` (§13), so oasgen has two
  options, and **hasn't committed to either**:
  1. **PerNamePoll** (§10.7), with a provisioner that holds cluster-wide
     `secrets: [get, patch]`, a new grant for oasgen. Its cluster-scoped
     builder can target a ClusterRole and ClusterRoleBinding, as oasgen has
     confirmed. The credential-use ClusterRole is dedicated (§10.7), and
     Connect's read grants stay with oasgen's own provisioning, which must
     cover the index. The cost is that the provisioner can read any Secret
     whose name it knows.
  2. **Protect only names known when the RBAC is generated**, such as the
     Configuration-level Secrets, with `Watch: PerName`. Per-object Secrets
     resolved from paths then stay unprotected, and the deadlock remains
     possible for them.
- **What oasgen measured, and only that.**
  - At the single call site that reads them, per-CR `secretRef` Secrets
    are read from the CR's own namespace
    (`EnsureSecretRole(..., mg.GetNamespace(), ...)`). So those are
    same-namespace refs, which are held by default.
  - Auth `SecretKeySelector`s carry their own namespace, and the collector
    groups them by it (`authsecretrbac.go:131`). So they can be
    cross-namespace, and are held only where the target consents (§11.3).
  - **Not checked:** whether any other Secret read in oasgen-provider or
    rest-dynamic-controller crosses namespaces.
  - Where these refs are cross-namespace, the partial coverage follows from
    the opt-in by design. An unheld cross-namespace Secret isn't exposed to
    the deadlock, because the CR's namespace teardown never deletes it
    (§9.4). The remedy, where protection is wanted anyway, is an
    `allow-holders-from` annotation on the target, and that is the target
    owner's decision.

### 16.3 unstructured-runtime (core-provider)

It has the same deadlock (§1, and core-provider#402 for the chart
`passwordRef` in the composition controller's Delete). It integrates the
core as §7.2 says, using `Track` after start for kinds discovered at
runtime (§7.3). It builds its own `Clients`, so it sets `Clients.Discovery` too.

**Its holder kind is the CompositionDefinition** (the user's decision,
2026-11-23), on the hub, LabelScoped. Compositions aren't its holders: what
follows about them as holders is kept for a later adoption, and isn't planned.
§7.2 stands: other outside runtimes use it.

- **The first tag holds direct refs only (§9).** What a CompositionDefinition
  names itself, such as its own chart `passwordRef`, can be held with it.
  The two items below need the next release.
- A remote CompositionDefinition names its KubernetesTarget with a `Next`
  to the kubeconfig Secret (§9.1).
- Compositions name their CompositionDefinition as `Guaranteed`, with a
  `Next` to the chart `passwordRef` Secret (§9.3). The label fallback
  there is unverified.
- RBAC is by name where generated (§13), and **its lifetime must outlast
  the holds** (see below).
- With per-version controllers, the release list covers the whole
  GroupKind (§3), and each controller's A1 goes through its own version.
- **The conversions** of its CRDs, if they serve several versions, must keep
  what §6.2 says: a protected kind's every field, label and annotation; the
  holder kind's labels and annotations, and its fields for Disown's strip.
  Whether they do is for core-provider to check.
- **Its composition delete is a Helm uninstall**, which deletes Secrets, and
  a chart's own Namespace, before custom resources. It doesn't wait for one
  kind to be gone before deleting the next (§5.1, verified), so the grace
  covers it. core-provider needs no change to its uninstall order.

**When.** core-provider is on provider-runtime v1.7.2. It adopts after
#395 (the v1.11/v1.12 bump), which is queued behind #313, #402 and #399
(CP3a).

**No leader election is needed** (CP1). The composition controller runs
without it, and its rollouts overlap pods. §6.8 shows the design is safe
that way, so there is no new unstructured-runtime work for it.

**The index namespace** (CP2, core-provider's blocker). In PerName mode the
index needs a namespace no holder can live in (§10.6.1). Compositions can be
in any namespace, because each composition controller watches
cluster-wide, and on krateo-057 they are all in `krateo-system`. So neither
the definition's namespace nor the management namespace qualifies. The
index goes in installer's dedicated `krateo-credential-use` namespace
instead:

- the namespace ships in installer's own release, before any composition
  controller enables PerName (§10.6.1);
- the ValidatingAdmissionPolicy there refuses composition.krateo.io
  objects, and everything else not on its allowlist (§10.6.1). Composition
  controllers' runtime service accounts are matched by the
  `system:serviceaccounts:krateo-system` group with the `cu-index-` prefix,
  so new controllers need no policy change;
- **core-provider generates the composition controller's RBAC** for
  configmaps in that namespace: `get`, `create`, `update`, `patch`, `watch`.

This no longer depends on core-provider#313, which concerns the controller
dying with its own namespace (§11.5). #313's Relocating mode doesn't change
the cases above (CP5).

**Deletion passes still acquire** (CP4), even after #402's fix (1) stops
Delete from reading `passwordRef`: Connect and other reads on the delete
path still need their holds.

**Generated RBAC must outlast the holds.** core-provider confirmed this at
main `6cb7d0e`, in `go/core-provider/internal/tools/deploy/deploy.go`.
`Undeploy`:

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
  error, while a check with `WithHeld(...)` (or `WithHeldIndex` in PerName
  mode) finds a hold under the kind's prefix that this definition's RBAC is
  the only grant for. The `!SkipCRD` path checks with
  `scaletozero.ExistsWithOptions`, which counts the instances first. The
  `SkipCRD` path, which skips the instance guard because a surviving
  definition keeps the kind's instances, checks with
  `scaletozero.HeldWithOptions`, which counts only the holds and the switch
  (decided 2026-11-26). Both pass
  `WithHeldFilter(func(id credentialuse.ID) bool { return !coveredBySurvivorOf(holderGVK.GroupKind(), id) })`
  (decided 2026-11-26), so that only those holds count; the check still stops
  at the first that does. A call checks one holder kind, so the filter
  captures it. The filter doesn't apply to the Disown switch, which isn't the
  hold of a name: while it hasn't settled, `Undeploy` refuses.
- **"The only grant" is computed, not read** (CP3b). It is the name's
  absence from the union of the `resourceNames` that core-provider would
  generate for every *other* live definition of the GroupKind, computed
  from those definitions' specs as its generator computes them. It is never
  read from RBAC objects, which can lag or be edited.
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
| A refused chain says so on the object: Synced=True's message, and the gauge `credentialuse_refused_holders`, the one exception to "never on a successful pass" (decided 2026-11-25) | §9, §12 |
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

**The §10.6 pass, core-provider's review, and oasgen's** (the decisions on
`587aa71`):

| Item | Resolved in |
|---|---|
| M1 The index is a true superset: Dekker on both sides with consistent reads, the startup prune included, proved on the reviewer's interleaving | §10.6.2, §10.6.3 |
| M2 The tracker owns the index and creates it; `create` can't be scoped by name; tampering detected, merged back, with an event and a metric; an Argo CD `ignoreDifferences` note | §10.6.1, §10.6.6, §10.6.7, §13 |
| L1 Prunes coalesced, up to 50 lines in one write; a default cap of 2,000 lines; 17,000 only as the hard ceiling | §10.6.2, §10.6.4 |
| L2 A warning at 80%; at full, fail closed or Degraded; the remedy | §10.6.4 |
| L3 `DisownPasses` 5; the validated span; `disown-settled` tests `held-count`; the zero counter resets on another's non-zero count | §7.4, §7.3 |
| L4 A missing or unreadable index is an error, never "no holds" | §10.6.5, §7.4 |
| L5 The rebuild merges with concurrent adds | §10.6.6 |
| L6, CP2 `SharedOptions.IndexNamespace`; no holders there; checked at start and at acquire; composition-controller adoption follows core-provider#313 | §10.6.1, §7.1, §7.3, §16.3 |
| L7 | covered by M2 |
| CP1 N processes without leader election: safe part by part; nothing needs a single writer, so no leader-election work | §6.8, §16.3 |
| CP3a Timeline: after #395, behind #313, #402 and #399 | §16.3 |
| CP3b "The only grant": the union of `resourceNames` computed from live definitions' specs | §16.3 |
| CP4 Deletion passes still acquire after #402's fix (1) | §16.3 |
| CP5 #313's Relocating mode cited; no change to the cases | §16.3 |
| oasgen: `resourceNames` authorizes list and watch only with a `metadata.name` field selector, cited | §13 |
| oasgen: runtime-provisioned RBAC by name, `Watch: PerNamePoll`; the mandatory order; failed provisioning; latency and cost against PerName; the honest cost; the narrower alternative; `*` on RBAC implies `escalate` and `bind`, never relied on | §10.7, §13, §16.2 |
| The Deleting outcome is a 422 Invalid, not a 403, so an RBAC denial can't be mistaken for it | §2, §6.2, §10.7 |

**The PerNamePoll RBAC pass** (the decisions on `5eea950`):

| Item | Resolved in |
|---|---|
| M1 Grants as a pure function of the index; the provisioner's inputs and loop; revoke only after a live GET with no finalizer and a later read with no line; the proof on the reviewer's interleaving; the residual of one provisioner cycle of 403; the order is now line, then grant, then A2 | §10.7.1, §13 |
| L1 The `written` skip in PerNamePoll needs a live GET of the UID and the finalizer | §10.7.3 |
| L2 §10.6.3's sentence amended; the negligible residual | §10.6.3 |
| L3 `DeletionGrace`, `DisownPasses`, `DisownInterval` and the timeout bound published on the CRD, write-once, start fails on disagreement; a mixed `Protected` or `RefsFunc` is safe per pass but churns; `OwnerFinalizers` is a set during provider-runtime#46 | §7.3, §7.1, §6.1 |
| L4 A 403 on A2 is retried for about 8s; why Degraded is harmless | §10.7.2 |
| L5 Tamper checks skip lines with a visible R3 (withdrawn 2026-11-23: every drop but this process's own is read); the N× GET cost accepted | §10.6.6 |
| L6 ClusterRole updates coalesced per cycle; the cost of a 2,000-name rule, with the linear match cited | §10.7.1, §10.7.3 |

**Revision 7** (cross-namespace consent and the provisioner):

| Item | Resolved in |
|---|---|
| M2 Cross-namespace refs held only with the target's consent (`allow-holders-from`, by object or namespace; `*` on objects only); a live check before line, grant and A2; not held means fail-open as today with `CredentialRefNotHeld`; withdrawal keeps the hold until the reference ends; chains gated per hop; the provisioner never grants an unheld ref; why tenant-chosen grants and pinning are closed; why the deadlock doesn't apply to an unheld ref, and the remaining recoverable case | §11.3, §5, §13, §16.2 |
| M1 The credential-use ClusterRole is dedicated (provisioner-only, no chart, no `aggregationRule`); Connect's read grants are separate and cover the index; an Argo CD note; Disown and Degraded still as today | §10.7, §13 |
| L1 One ClusterRole for several kinds: the union of indexes; revocation checks every served prefix after re-reading every index | §10.7.1 |
| L2 A 403 on A2 returns a short `RequeueAfter`, never a sleep; the backfill figures recomputed, and the old ones corrected | §10.7.2, §6.6 |
| L3 The provisioner is watch-driven, caches the ClusterRole, and falls back to a slow poll; its steady-state read cost | §10.7.1 |
| L4 The widening says `get` and `patch`, and points to the cross-namespace gate | §10.7.1 |
| oasgen-provider#526 cited; both adoption options as they stand; oasgen not committed to either | §16.2 |
| Chains that consent in part: each hop gated on its own; why that is full protection; the held-intermediate and the intermediate-elsewhere shapes; an unheld hop no longer stops the chain | §9.4, §11.3 |
| oasgen's coverage as measured: per-CR Secrets same-namespace, always held; Configuration-level Secrets held only with consent; partial by design, and the remedy | §16.2 |

**Revision 8:**

| Item | Resolved in |
|---|---|
| Withdrawn consent releases the hold (the user's decision, replacing revision 7's "keep until the reference ends"): dropped from the record, the index line and the grant; the finalizer through the normal protocol; seen by watch on the target and on Namespaces, and by the poll in PerNamePoll; events on target and holders and a metric; the holder then proceeds as unheld | §11.3, §13 |
| The proof that the drop can't race an A2 into a stranded or unindexed finalizer: a live consent read before every cross-namespace A2, and a release batch enqueued by every change to K's finalizer or epoch, which also covers A2s from any stale view of a record | §11.3, §10.2 |
| oasgen's coverage claim narrowed to what was measured, with what wasn't checked stated | §16.2 |
| The holder-side event names the target, the withdrawing namespace and the release; a durable, bounded `credential-use.krateo.io/unheld` annotation (`NotConsented`, `ConsentWithdrawn`) written in the same patch as the record; how to read it next to a not-found | §11.3, §7.1, §7.5 |
| A pointer to the `unheld` annotation appended to Synced=False on a failing Connect or Observe: the exact suffix, its length bound, when it's added, neutral guidance for outside runtimes, and a test | §12, §14 |

**Revision 9:**

| Item | Resolved in |
|---|---|
| M1 The consent gate on every pass for every held cross-namespace ref: cached (or the PerNamePoll skip's live GET) on skipping passes, live before every A2 and every new hold; a withdrawal drops the ref in A1; why a stale cached drop is safe; the rule-1 contradiction fixed; a test | §11.3, §14 |
| L1 R0 stops for a deleting P with a deleting user in the cache; the teardown cost before and after | §6.2 R0 |
| L2 The acquirer enqueues its own release batch after any A2 from a view it can't vouch for: every cross-namespace A2, and every A2 in PerNamePoll | §10.2, §11.3 |
| L3 Consent is the union of the object's and the Namespace's lists; an object can't narrow its Namespace; the advice to owners | §11.3 |
| L4 Trimming keeps `ConsentWithdrawn` ahead of `NotConsented` | §11.3 |
| L5 The Namespace handler compares transformed objects, and acts only on `credential-use.*` annotations or the `deletionTimestamp` | §11.3 |
| L6 A shared unheld hop's live read is reused per pass batch or 10s TTL; why that is safe | §9.4 |

**Revision 10:**

| Item | Resolved in |
|---|---|
| M1 A held cross-namespace ref is dropped only on a positive, live-confirmed withdrawal: a live read of the object and its Namespace when the cache suggests a withdrawal or can't answer; drop only if neither list names the holder's namespace; keep and requeue if the read fails; the "stale cached drop is safe" argument replaced; the restart-during-teardown and just-after-a-new-hold tests | §11.3, §14 |
| L1 `Resolved` carries unheld hops, so Connect uses exactly what the tracker read: up to 10s on the old chain after a re-point, and never an unheld same-namespace Secret | §9.2, §14 |

**Revision 11** (Low items after the reviewer confirmed revision 10 clean):

| Item | Resolved in |
|---|---|
| L1 A NotFound on the live confirmation means the target is gone: Missing, not a failed read, so no endless requeue | §11.3 |
| L2 A persistent 403 on the object read in PerNamePoll: dropped after 5 consecutive 403s over at least 2 minutes, with an event, a metric and a `ReadDenied` entry | §11.3 |
| L3 The Namespace informer's sync gates start, like the source check; a missing list or watch on namespaces fails start with a clear error | §7.5 |

**Revision 12** (crediting core-provider and oasgen):

| Item | Resolved in |
|---|---|
| core-provider's BLOCKER: the index namespace. The user chose a dedicated namespace no holder can be created in (`krateo-credential-use`), as installer measured on krateo-057: 83 compositions of 67 kinds, all in `krateo-system`, structurally. installer ships it in its own release, with a new `.Values.namespaces` key and `helm.sh/resource-policy: keep`, before any index is written. Start fails clearly when it is absent. A ValidatingAdmissionPolicy allows only `cu-index-*` ConfigMaps from providers' service accounts (and their events). core-provider generates the configmap RBAC there. The start check stays, and never fires by construction. At most two writers per index | §10.6.1, §7.3, §13, §16.3 |
| core-provider: the claim that compositions live in the definition's namespace removed; they can be anywhere, because each composition controller watches cluster-wide | §16.3 |
| oasgen's correction: rest-dynamic-controller isn't a second known instance. Its Delete tolerates a failed Secret read (`restResources.go:1692`, in Delete at `:1574`), and is exposed only where a resolved value builds the delete request (`BuildCallConfig` returning nil). core-provider#402 stays the known second instance | §1, §16.2 |

**Revision 13** (crediting core-provider, who found three breakages in the policy sketch):

| Item | Resolved in |
|---|---|
| Kubernetes' own per-namespace writers: `kube-root-ca.crt` and the `default` ServiceAccount, allowed by name and by identity, with the identities verified in Kubernetes v1.36.2. `system:kube-controller-manager` added for clusters without `UseServiceAccountCredentials` | §10.6.1 |
| The policy matches CREATE and UPDATE only, never DELETE, so the namespace can be deleted; the index is protected by its finalizer-free design and RBAC, not by admission | §10.6.1 |
| Dynamic writers: match the group `system:serviceaccounts:<management namespace>` ANDed with the `cu-index-` prefix, with the management namespace as the policy's parameter; git-provider fits the same rule | §10.6.1 |
| Our own correction: a `namespaceSelector` matches a Namespace object's own CREATE and UPDATE, so the Namespace is now excluded explicitly; revision 12's "never matches `v1/Namespace`" was wrong | §10.6.1 |
| Tests: Kubernetes' writers allowed, the namespace deletion completes, a dynamic composition-controller service account writes its index, a composition object is refused | §14 |

**Revision 14** (crediting core-provider, who checked §10.6.1 and §16.3 and asked for these to be stated):

| Item | Resolved in |
|---|---|
| The parameter ConfigMap and the binding ship in installer's same release | §10.6.1 |
| The parameter ConfigMap needs the policy's protection: no broad configmap `delete` in `krateo-system` | §10.6.1 |
| The symptom of its absence, verified in apiserver v0.37.2: every write in the namespace is denied before any expression runs; `cu-index-*` writes fail with a Forbidden naming the binding (new holders fail closed, established ones are Degraded); it can be misread as missing RBAC | §10.6.1 |
| Why `Deny` stays: `Allow` would let one ConfigMap deletion switch the protection off | §10.6.1 |
| core-provider's check that `resources: ["*"]` doesn't match subresources such as `serviceaccounts/token`, verified in the matcher | §10.6.1 |

**Revision 15** (crediting core-provider, who found that a policy denial with no `reason` is a 422):

| Item | Resolved in |
|---|---|
| A ValidatingAdmissionPolicy denial without `reason` is 422 Invalid (`dispatcher.go:309-315`, `policy_decision.go`), so the sketch sets `reason: Forbidden` and its denials are 403; the config-error path stays 422 whatever the validations say | §10.6.1 |
| Error classification by code, reason, message and causes, never `IsForbidden` or `IsInvalid` alone: (a) Deleting, (b) policy denial, (c) policy config error, (d) RBAC; only (a) is Deleting; the PerNamePoll RBAC retry is for (d) only; every spot that relied on "Forbidden" fixed | §6.2, §10.5, §10.7.1 |
| The missing-params symptom corrected to 422 Invalid, and matched by message in the tracker and the runbook | §10.6.1 |
| Tests for each class, including a missing-params 422 that must not be read as Deleting | §14 |
| Correcting revision 14's report: the "Forbidden naming the binding" there was wrong; it is a 422 | §10.6.1 |
| **Our own finding, recorded:** the backfill figures in §6.6 were twice too high in revisions 3 to 6 (8 and 16 minutes). Revision 7 corrected them to 4 and 8 | §6.6 |

**Revision 16** (the implementation plan,
[plan-credential-use-implementation](./plan-credential-use-implementation.md)).
The plan found 23 places where the text was ambiguous or wrong once coded.
The user decided the five that change an API or a rule:

| Item | Resolved in |
|---|---|
| IP1 RBAC 403s are recognised positively, by the authorizer's own message, whose exact text an envtest pins; every other 403 is an admission denial, an Acquire error at once, never retried as RBAC propagation | §6.2 (classes (d) and (e)), §10.7.2, §14 |
| IP2 `Acquired.RetryAfter`: the RBAC-propagation window stops the pass before Connect for new and established holders and requeues with no backoff; a failed live consent read keeps its hold, proceeds and requeues | §7.1, §8 step 5, §10.7.2, §11.3 |
| IP3 The predicate is a method, `r.IgnoreCredentialUseRecord()`, which changes the builder line; the core's `t.IsOwnRecordWrite` for outside runtimes | §7.1, §7.2, §7.5 |
| IP4 Two runnables: the informers without leader election, the write loops with it, so `EnableWarmup` works | §7.5 |
| IP5 scale-to-zero is additive: `ReconcileWithOptions` and `ExistsWithOptions` take `WithHeld` and `WithHeldIndex`; `Reconcile` and `Exists` are unchanged, and `Reconcile` calls `ReconcileWithOptions` with no options. `ExistsWithOptions` also takes `WithRESTMapper` with `WithHeld`, because a reader has no RESTMapper | §11.6, §14, §16.3 |

The other items, which stay within the design:

| Item (plan §5) | Resolved in |
|---|---|
| A1 A policy's text is a cause message and a substring of the status message, not a prefix (`dispatcher.go:300-317`) | §6.2 |
| A3 R3's failed `test` op is a 422 with the generic message and no causes (`patch.go:423-426`); JSON patches are classified apart, and "state changed" goes back to R1, at most 5 times per batch | §6.2 |
| A4 No "test that it is absent": every CRD write is a GET, then a merge patch guarded by resourceVersion | §6.8, §7.3, §7.4, §10.6.1 |
| A6 `Options.TimeoutBound`, required | §7.1, §7.5 |
| A7 `Clients.Events`; the metrics named, including `credentialuse_degraded_total` and `credentialuse_unrecorded_reads_total`; the otel implementation in `pkg/telemetry` | §7.1, §12 |
| A8 `DisownPrefix` takes `DisownPrefixOptions`, with `Index`, `Names` and `DryRun` | §7.1, §7.4 |
| A10 `CheckingReader` is `reconciler.CredentialUseCheckingReader`; the core exposes `t.CheckRead` | §7.1, §11.9 |
| A11 Configuring it: `WithCredentialUse(KindOptions)`; `WithCredentialUseShared(s)` for non-default `SharedOptions`, in production too; a per-manager registry | §7.5 |
| A13 Disown's unheld refs have the reason `Disowned`, never written to `unheld` | §7.1, §7.4 |
| A14 Index lines for Missing refs are kept while a record names the ref, and pruned by the batch after | §10.6.2 |
| A15 Disown under `resourceNames` RBAC works from the index's lines and the records | §7.4 |
| A17 `NamedFinalizer`'s contract: `RemoveFinalizer` writes the annotations with the finalizer change; §6.1 covers a custom one that doesn't | §7.5 |
| A18 A1 and the custom-resource A2 build the optimistic-lock merge patch in the core, on client-go only | §6.2 |
| A19 The index ceiling is the constant 17,000 lines | §10.6.4 |
| A20 A relist is seen through the core's own ListerWatcher wrapper, on List's return or the initial-events-end bookmark, because handlers can't see one and watch-list is on by default | §6.5 |
| A21 The reconciler-driven envtests live in the test-only `pkg/credentialuse/e2e` | §14 |
| A22 The adapter's `Enqueue` forwards to the source's queue once it starts | §7.5 |
| A23 The holder kind is watched twice under provider-runtime, so its memory is doubled | §10.2 |

A2, A5, A9, A12 and A16 are IP1 to IP5 above.

**The independent review of revision 16** found nothing High: each
amendment keeps the merged design's safety proofs. Its findings:

| Item | Resolved in |
|---|---|
| M1 Release batches lost before the write loops run: the informers start before election, so the startup recheck, holder deletes seen on standby and grace timers could be lost, stranding a finalizer after a failover between `Forget` and R3. The batch queue is created in `NewShared` and buffers from informer start, and the write loops' `Start` rescans the protected stores and the index; a failover test | §7.5, §10.2, §14 |
| M2 The consent-read `RetryAfter` had no backoff: exponential per (holder, ref), from 1s doubling to the poll interval, reset on success; an overload test | §11.3, §8 step 5, §14 |
| L1 An R3 422 against an unchanged resourceVersion is an error with an event and a metric, not contention; each retry rebuilds the patch, which tests every value it removes | §6.2, §12, §14 |
| L2 A1 copies the whole returned `ObjectMeta`, and re-reads the object before the next write when the generation or labels changed | §6.2 A1 |
| L3 Class (e) is counted; the RBAC message format is the same from apiserver v0.31.0 to v0.37.2; Krateo's minimum Kubernetes is 1.36 (the user's decision), and CI pins the message on 1.36.2 and 1.37.0 | §6.2, §12, §18 |
| L4 Guarded CRD writes are bounded to 5 attempts, and a starved count write is logged and counted | §7.3, §12 |

**Revision 17** (the independent review of the protocol's code, PR 2,
provider-runtime#59). It found one High and four Mediums, each reproduced by
a test or a model schedule before the fix:

| Item | Resolved in |
|---|---|
| H1 The A2 skip could rest on an observation of an earlier run of the record: a lost A1 response in one process, another process's remove and re-add with a failed A2, or a typed cache ahead of the core informer let a pass report P held with no finalizer. A skip now needs the informer to have delivered exactly the version the pass works from, naming P; a delivered version that ends P's run drops `observed` too | §6.3, §6.4 |
| M1 The model forgot a hold using the stale record after a lost A1: its instrumentation now reads the live record | §14 (the model) |
| M2 CI searched one fault or one preemption, never both: it now searches both for the scenarios with a second process, including a second acquiring process for one holder, and varies the random walk's seed | §14 (the model) |
| M3 A crash in the model skipped the actor's next wait, losing the deletion phase: a crash now restarts every actor of the process, and the model asserts each deletion phase ran | §14 (the model) |
| M4 R2 read the clock for the grace after the list, so a namespace deletion inside the grace, after the Namespace GET, could be released over: the clock is read before R2's first read, and the model requires every namespace deletion before a release to come after the grace | §6.2 R2, §6.4 |
| Lows: the handler maps keyed by UID; first sights pruned once P is released or gone; the grace kept on the local monotonic clock against wall-clock steps; R3 drops only its own (P, N) entry | §6.2 R3, §6.3, §5 |

**The re-review of revision 17** (#59 at c0d5e61) found nothing High or
Medium. Its points:

| Item | Resolved in |
|---|---|
| The skip proof holds as of the delivered version *rv*; the equal-lag stale pass (a process whose typed cache and informer are equally behind) is an accepted window, like L6, pinned by a test | §6.4 |
| The exact-version rule rests on PR 3 recording `lastList` no later than a relist's versions reach the holder handler | the plan, PR 3 |
| L3's width is about M round trips for M R3 patches in a row, not "a few milliseconds" | §6.4 |

**Revision 18** (2026-11-23), after a flaky envtest showed that the index
could lose a held object's line, and Disown settle with a finalizer left.
A drop was taken for this process's own by the line it named, before its
write had landed. So a removal by someone else, a conflict, or a lost
response could swallow a drop nobody then checked. What was decided, the
user's decisions being named as theirs in those sections:

| Item | Resolved in |
|---|---|
| A drop is this process's own only in the version a write of its own returned; every other is read once; L5's skip of a visible R3 withdrawn | §10.6.6 |
| Every line an attempt of a prune tried to remove is read after it; a prune, once begun, runs to its end | §10.6.2 |
| E: the lines stay until Disown has settled; a process empties the index only after its own run of zeros, and only of lines it proves free; E1 no other prune takes a line meanwhile; E2 a record's refs get their lines before it is stripped, and a full index leaves the record; E3 a dropped line of a bare object goes back | §7.4, §10.6.2, §10.7 |
| G: the count is `"unknown"` while this process may owe a line | §7.4 |
| An index lost under Disown: no marker from a rebuild under the switch; `"unknown"` until `vouched-index` names the ConfigMap's uid and the kinds; three cases and two limits | §7.4, §10.6.5, §10.6.6 |
| The landing margin: `LandingMargin`, 60s, a margin and not a proof; the span is `max(2 × TimeoutBound, TimeoutBound + LandingMargin)`; a later landing isn't covered | §7.3, §7.4, §10.6.8 |
| F: an A2 of unknown outcome is checked at once and after the margin, with its batch; a stop doesn't cut an A2 short; every bound stated | §7.2, §10.6.2 |
| The residual: a hold without its line needs three conditions together, shown in the model within its bound; the remedy | §10.6.3 |
| A kill inside a prune, as Disown is turned on: accepted and documented (2026-11-24), with the wait that would close it, which can be added later | §10.6.3, §7.4 |
| A prune leaves the line of a hold that may land yet (2026-11-24): none for `TimeoutBound` + `LandingMargin` after the write loops start, and none of an A2 of its own process; what is left of the residual is a late landing, and another process without leader election; its cost, 150s at the defaults | §10.6.2, §10.6.3, §7.2 |
| `disown-settled` is the switch's value as written; a reader makes three checks, through `DisownSettled`; no A2 under a stale flag | §7.3, §7.4, §11.5, §11.6 |
| A pass strips the objects before the records, and a record only once every object it names is stripped | §7.4 |
| A run of zeros is of passes over a CRD nobody else changed; a CRD that keeps changing is reported, with an event and a metric | §7.4, §12 |
| A process whose write loops haven't started refuses its passes | §7.2, §7.5 |
| An operator's vouch of a kind in a namespace, `team-a/Secret/*` (for a namespace 2026-11-24; of its kind, decided 2026-11-25): a bare `team-a/*` is malformed, and a switch widened to another kind finds it unvouched | §7.4, §10.6.6 |
| A stop drains the live checks the index holds in memory, within 10s, before the write loops end (decided 2026-11-25); a kill stays the accepted case | §7.2, §10.6.3 |

## 18. Open questions for the user

**Decided since the last revision**: NQ1 (a 60s grace, §5), NQ2 (no cap,
§5.2), NQ3 (the builder line, §7.5) and NQ4 (provider-runtime#46). In
revision 16, IP1 to IP5 (§17).

**Decided after the review of revision 16:** Krateo's minimum Kubernetes
is **1.36**. CI runs the premise envtests, the exact RBAC message among
them, on 1.36.2 and 1.37.0 (§6.2).

**Decided on 2026-11-24:** the first tag supports direct refs only. A ref
that uses a chain gets a clear error, and chains come in the next release
(§9). U1 and C2 (§17) therefore ship there, not in the first tag.

**Decided on 2026-11-25:** an established holder whose chain is refused says so
on Synced=True's message, and a gauge counts such holders (§9, §12).

Nothing is open.

Nothing is open. No question needs the user.
