---
type: Architecture
title: provider-runtime — credential-use finalizer
description: Design, not built, for holding the Secrets (and the objects that lead to them) that a managed resource reads, for as long as losing them would be unrecoverable, so a namespace deletion can't wedge on a managed resource whose credentials were deleted first.
resource: github.com/krateo-platformops/provider-runtime
tags: [design, finalizer, deletion, credentials, reconciler]
timestamp: 2026-11-09T00:00:00Z
---

# Credential-use finalizer

**Status:** design, revision 17. Nothing is built yet; the build follows
[the implementation plan](./plan-credential-use-implementation.md), eleven
stacked PRs that ship as v1.13.0. The
user chose the approach: a finalizer on each Secret an object uses, the
analog of Crossplane's ProviderConfigUsage. The user's decisions are
binding here: U1 to U7 on the first review, NQ1 to NQ4 on the second, and
IP1 to IP5 on the implementation plan. Revision 16 amends the text where
the plan found it ambiguous once coded (§17, "Revision 16"). Revision 17
amends the skip rule and the grace's clock after the independent review of
the protocol's code (§17, "Revision 17").
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

**The table covers held refs.** A ref in another namespace is held only with
the target's consent (§11.3). One that isn't held isn't covered here, and
the original deadlock doesn't apply to it: the holder's namespace teardown
never deletes a Secret in another namespace.

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
    - **Missing**: NotFound;
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
**the state changed**: the releaser goes back to R1, at most 5 times per
batch, and then requeues the batch with backoff. Each retry rebuilds the
patch from R1's new read. A 422 with a policy's text
is (c) there too, and anything else is an error for the batch.

**A "state changed" that didn't change is an error** (L1 of the review of
revision 16). If R1's fresh GET returns the very resourceVersion the failed
patch was built from, nothing changed, so the 422 isn't contention: the
patch didn't match the state it was built from, which is a defect. The
releaser doesn't retry it. It logs it, records a Warning event on P
(`CredentialReleaseRejected`), counts `credentialuse_release_rejected_total`,
and requeues the batch with backoff. The same holds for Disown's strips and
the janitor's patch.

**The releaser** works on one holder kind K and one namespace N at a time,
for the set of protected objects pending there (R7). For each pending P:

- **R0. Fast path.** Ask the holder informer's index for users of P in N,
  leaving out the holders §6.1 excludes. Stop if either:
  - P is not being deleted, and a user exists. This settles the usual case,
    where one of several users leaves;
  - P is being deleted, and the cache shows a **deleting** user (L1). §5
    keeps P in every row while a user is deleting.

  A stale cache can only keep a hold longer, which is always safe. The
  release then runs on the event that shows the user gone.

  **The teardown cost**, for a namespace with M held Secrets and H holders
  deleting:
  - *before*, every holder event re-ran the whole batch: up to H × (M GETs,
    one Namespace GET and one list);
  - *after*, those batches stop at R0, and one full batch (M GETs, one
    Namespace GET, one list) runs once the cache shows the last deleting
    user gone.
- **R1. Get P live.** Note its resourceVersion, the index *i* of the
  finalizer in `metadata.finalizers`, the epoch *e*, and its
  `deletionTimestamp`.
  - If the finalizer is absent, P is done.
  - If the epoch is absent (only a third party could cause that), set a
    fresh one and requeue.
  - If P is being deleted in a namespace that was live when last seen, has
    users by R0, and is still inside the grace, requeue the batch at the
    end of the grace (§5).
- **R2. The last reads, after every R1 in the batch.** First read the
  clock: whether each P's grace is over is decided on that reading, taken
  before any of R2's reads (revision 17, §6.4). For each P being
  deleted, GET its Namespace live and read its `deletionTimestamp`. Then
  make **one** live, paginated, metadata-only list of K in N
  (`PartialObjectMetadataList`, no resourceVersion, so it is a consistent
  read), with no label or version selector. For each P, compute its users
  and its deleting users (§6.1), and apply §5's table.
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
- If it lands after tR, the release won. This needs tD_H inside the
  releaser's few milliseconds between the list and the patch, and after the
  grace. A2 then reports Deleting, H proceeds Degraded, and H fails until P
  is recreated. Its namespace is live, so this is recoverable, and it is
  part of §5.2's residual.

The namespace's deletion is handled the same way. Its `deletionTimestamp`
is read live after the grace, as the last step before R3. A namespace
deletion that starts after that read deletes the holders, and their
deletion passes write, which breaks an R3 that hasn't landed yet.

**The grace is measured before R2's reads** (M4 of the review of the
protocol's code, revision 17). R2 reads the clock first, and decides
whether the grace is over on that reading. So the Namespace GET comes after
the grace's end, and a namespace deletion it doesn't see began after the
grace too. Revision 16's code read the clock after the list: a namespace
deletion that began inside the grace, after the GET, was then released over
as if it had begun after the grace. A test reproduces it, and the model
checks every release against it.

**That leaves an unrecoverable window, about M round trips wide for M R3
patches in a row** (L3). Say
the namespace's deletion begins between R2's Namespace GET and R3's patch,
after the grace, while no user is deleting. R3 then lands first, because
the namespace controller takes far longer than that to discover and delete
the holders. P goes, the namespace is terminating so P can't be recreated,
and the holders wedge exactly as in §1. It needs a Secret deleted in a live
namespace, then the grace running out with no user deleting, then the
namespace deleted inside that window. Nothing in this design closes
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

The only things that can still remove the finalizer while H's record names
P are:

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
- **Release** (R0 to R3). R3's JSON-patch tests serialize any number of
  releasers. A losing releaser re-reads from R1.
- **The index** (§10.6). Every write is guarded and merges on conflict. The
  superset proof (§10.6.3) is between any acquirer and any pruner, in any
  processes. The CRD pointer is written once, after a GET that finds it
  absent, by a merge patch guarded by resourceVersion, so only one process
  wins (§7.3, "Writing the CRD").
- **Disown and the count** (§7.4). The strips are R3's patches. Every
  process counts the same cluster, live. The count fields are written by
  merge patches guarded by the CRD's resourceVersion, after a GET that read
  their previous values, and `disown-settled` only when that GET read
  `held-count` as `"0"`. A process that reads a non-zero count written
  by another resets its own zero counter.
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
	MaxDepth       int                      // levels of Next followed; default 3
	Enqueue        func(holder types.NamespacedName) // the caller's queue (§10.2)
	DeletionGrace  time.Duration            // §5; default 60s
	ProbeInterval  time.Duration            // missing-ref prober; default 15s (§10.3)
	DisownInterval time.Duration            // §7.4; default 60s
	DisownPasses   int                      // §7.4; consecutive zero passes, default 5
	// TimeoutBound is the kind's largest reconcile or request timeout. It is
	// required: §7.3 checks DisownPasses × DisownInterval against twice it,
	// and publishes it. provider-runtime passes its timeout plus grace, 90s.
	TimeoutBound   time.Duration
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
// discovered at runtime (§7.3). It is idempotent per holder GroupKind.
func (s *Shared) Track(o Options) (*Tracker, error)

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

// DisownPrefix strips a deleted holder kind's holds (§7.4).
func DisownPrefix(ctx context.Context, c Clients, o DisownPrefixOptions) (DisownReport, error)

type DisownPrefixOptions struct {
	Holder    schema.GroupKind
	Protected []schema.GroupVersionKind
	Index     *types.NamespacedName // PerName and PerNamePoll: the index to read
	Names     []Ref                 // or an explicit list of objects
	DryRun    bool                  // report only
}

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
   can call `Track` at any time.
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
   events whose only change is the record, with `t.IsOwnRecordWrite`, as
   provider-runtime's `r.IgnoreCredentialUseRecord()` does (M6).
6. RBAC as in §13. In PerName or PerNamePoll mode, also set
   `SharedOptions.IndexNamespace` (§10.6.1).

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
- **Errors.** `Track` returns an error for:
  - an invalid finalizer prefix;
  - an unmappable holder or protected kind;
  - an empty `OwnerFinalizers`;
  - `DisownPasses × DisownInterval` below twice the kind's largest
    reconcile or request timeout (§7.4);
  - in PerName and PerNamePoll modes: no `IndexNamespace`; **an
    `IndexNamespace` that doesn't exist**, which fails with
    `credential-use: index namespace krateo-credential-use not found: install it (installer release) before enabling PerName`;
    an `IndexNamespace` that holders of the kind live in, or that disagrees
    with the CRD's pointer; or an `IndexMaxLines` above the ceiling
    (§10.6).

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
  pointer of §10.6.1, and Disown's count fields of §7.4) is a GET, then a
  merge patch guarded by the CRD's resourceVersion. A conflict re-reads and
  decides again. An earlier revision asked for "a patch that tests the
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
process sets `disown-settled` once it has seen `DisownPasses` (**5** by
default) consecutive passes count zero.

- **Setup validates** `DisownPasses × DisownInterval ≥ 2 ×` the kind's
  largest reconcile or request timeout (L3). provider-runtime's reconcile
  timeout plus its grace is 90s by default, so the minimum is 180s, and the
  defaults give 300s.
- **The `disown-settled` write tests that `held-count` is `"0"`**, so it
  can't land over a newer non-zero count.
- **A process that reads a non-zero count** written by another process
  resets its own zero counter.
- **A count that can't be made**, because the index is missing or
  unreadable, or a list fails, is written as `held-count: "unknown"`. That
  clears `disown-settled`, and is never read as zero (§10.6.5).

A non-zero count clears the flag. **Downgrading the runtime, removing the interface from the connecter,
or uninstalling is safe once `held-count` is `0` and `disown-settled` is
`true`.** Any process can write the fields, because each one counts the
same cluster, live.

**Writing the fields** (L5, L6). Every write of these fields is a merge patch
guarded by the CRD's resourceVersion, after a GET that read the previous
values (§7.3, "Writing the CRD"). Of two processes racing, one conflicts
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
strip. §10.6.8, interleaving 3, shows why consecutive zero passes catch it.

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
objects. `credentialuse.DisownPrefix(ctx, clients, DisownPrefixOptions{Holder,
Protected, Index, Names, DryRun})` (§7.1) needs no holder kind and no switch. It
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
  of the review of revision 16). The informers start before election, under
  warmup and on standby replicas, so they see holder deletions and
  `deletionTimestamp`s before any write loop runs. Two things keep them:
  - **the (K, N) batch queue is created by `NewShared`**, before any
    informer starts. It buffers and deduplicates from the first event, and
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
  (§6.8);
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
well as in tests. The adapter sets `TimeoutBound` to its reconcile timeout
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
before it reads it**, except a hop that §11.3's gate doesn't let it hold.
That hop is read live, without a hold, and its `Next` is still followed
(§9.4):

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

**Any change to K's finalizer or epoch on a protected object** also
enqueues its (K, N) batch, in the watched modes. R0's fast path settles it
at once when users exist.

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
denied there (class (d) of §6.2, RBAC), Acquire returns an error: fail
closed for a new holder, fail open for an established one (§8). The doc text says so, and that such
refs need §13's cluster-scoped RBAC.

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
  ship it. At start, the tracker creates it if it is absent. Create races
  between processes are harmless: the loser gets AlreadyExists and reads the
  winner's.
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
  1. Before A2 for a P whose line is missing, it adds the line.
  2. After a successful A2, it **live-GETs the index**. If the line is
     missing (a concurrent prune removed it), it adds it again.
- **The pruner removes after R3, then checks after.**
  1. After a release batch's R3s, it removes, **in one write per batch**
     (L1), the lines of the objects whose K's marker label R3 removed.
  2. After that write succeeds, it **live-GETs each pruned P**. If P carries
     a finalizer with K's prefix again (a concurrent A2 re-added it), it
     adds the line back.
- **Lines for Missing refs** (revision 16). The line is added before A2,
  so a ref whose A2 finds NotFound has a line but no hold. It is kept while
  some holder's record names the ref: PerNamePoll needs its grant for the
  prober's GET (§10.3). The release batch prunes it, under the rule above,
  once no record names the ref and the object carries no finalizer of K.
- **The startup recheck's prune** follows the same rule. It prunes the lines
  whose objects carry no finalizer with K's prefix, then live-GETs those
  objects, and adds back any that now do.

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
line. The marker label still finds it (§10.6.6).

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
  enqueues a release batch for every (K, N) finalizer it finds.
- **Disown** reads it to find what to strip and what to count (§7.4).
- **`WithHeld`** reads it, and stops at the first line whose object still
  carries K's prefix (§11.6).

**A missing or unreadable index is an error, never "no holds"** (L4).
`WithHeld` returns the error, so `scaletozero` keeps the controller up.
Disown writes `held-count: "unknown"`, which is not `"0"`, so it never
settles. The tracker keeps retrying, and recreates the index if it is
missing (§10.6.6).

#### 10.6.6 Tampering, deletion, and the rebuild (M2, L5)

The tracker watches the ConfigMap by name (or polls it in PerNamePoll mode,
§10.7). **A change that drops lines this process didn't remove** is checked
line by line, except for **lines removed alongside a successful R3 the
tracker can see**. In PerName mode, the per-object watch has already
delivered that P lost K's last finalizer. Those need no check (L5). Every
other dropped line costs a GET, in every process: with N processes, N GETs
per dropped line. That cost is accepted. A dropped line whose object no longer carries K's prefix
needs nothing: another process pruned it. A dropped line whose object
still carries the prefix is **tampering**, or a lost race. The tracker adds
those lines back, records a Warning event and counts
`credentialuse_index_tampered_total`.

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
   set runs A1, then the switch is set, the Disown loop strips H's record
   and P's finalizer, and then the pass's A2 lands and re-adds the
   finalizer. Two things close this:
   - every Acquire reads the switch again just before each A2, and a pass
     that sees it set stops and returns its refs unheld;
   - a pass already past that check, whose A2 lands after the strip, is
     caught by the Disown loop's next pass. The live count, from the index
     in PerName mode, sees the finalizer, so the loop strips it again, and
     the non-zero count resets the settling.

   Disown is settled only after `DisownPasses` consecutive zero passes,
   spanning at least twice the longest reconcile (§7.4). So no pass that
   started before the switch can still land a write after it is settled.

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
  is written only for a ref Connect reads, so they do.

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

**"As today" still holds under this split.** Disown empties the index, so
the provisioner revokes the credential-use grants. Connect keeps working
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
the dropped refs to a release batch. A ref reached through `Next` changes
the same way when an intermediate changes (§9.2).

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
isn't in the record. It gets no index line, no grant and no A2, though its
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
  is `{"ref":"…","reason":"More","count":<n>}`. With refs of at most about 350
  characters, it stays under 6 KB.
- **An entry goes** when the ref is held again, or when the holder no longer
  names it.
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
   - **at run time:** `reconciler.CredentialUseCheckingReader(reader, ...)`
     wraps the provider's controller-runtime client. A read of a protected
     kind absent from the object's record logs once per object and counts in
     `credentialuse_unrecorded_reads_total`. The core can't wrap a
     controller-runtime client (§7.1), so it exposes `t.CheckRead`, and
     outside runtimes wrap their own clients around it.
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

**The metrics**, all labelled `holder_kind`:
`credentialuse_held_objects` and `credentialuse_waiting_objects` (gauges),
`credentialuse_index_fill_ratio` (gauge, PerName and PerNamePoll),
`credentialuse_degraded_total{reason}` (every fail-open of §8, which earlier
revisions counted without naming), `credentialuse_refs_not_held_total`,
`credentialuse_consent_withdrawn_total`, `credentialuse_refs_read_denied_total`,
`credentialuse_index_tampered_total`,
`credentialuse_unrecorded_reads_total` (§11.9),
`credentialuse_admission_denied_total` (class (e) of §6.2),
`credentialuse_release_rejected_total` (a release patch that failed against
an unchanged object, §6.2) and `credentialuse_crd_write_starved_total`
(§7.3). The core defines them as a
`MetricsRecorder` interface, and `pkg/telemetry` implements it with
OpenTelemetry.

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
| namespaces | `get`, `list`, `watch` | §5's live `deletionTimestamp` check; the metadata-only informer that sees consent on Namespaces (§11.3) |
| the holder kind | `get`, `list`, `watch`, `patch`, `update` | the holder informer, R2's consistent list, A1 (patch), and `Forget` in the finalizer-removal Update and on its own (update) (L8) |
| customresourcedefinitions, by `resourceNames` (the holder CRDs) | `get`, `list`, `watch`, `patch` | the Disown switch, read through a field-selected watch; the count fields and the one-time index pointer, written only on change (§7.4, §10.6) |
| configmaps, by `resourceNames` (PerName and PerNamePoll: the kind's index) | `get`, `update`, `patch`, and `watch` in PerName mode | §10.6 |
| configmaps, in the index namespace (`krateo-credential-use`) | `create` (it can't be scoped by name) | the tracker creates the index (§10.6.7). The namespace itself is installer's; providers never create it, and admission limits what may be written there (§10.6.1) |
| events | `create`, `patch` | §12 |

`update`, `create` and `delete` on protected kinds are not needed: every
write is a patch.

**A new privilege for providers with static CRDs** (L6). git-provider ships
its CRDs in its chart, and has never needed to write them. It now needs
`patch` on its own CRDs, by name. The writes are rare: the Disown fields
when they change, and the PerName index pointer once. A CRD write makes the
API server rebuild its OpenAPI, which is why nothing else writes the
CRD.

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
   That fits small held sets, and needs the index (§10.6).
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
> and objects there then can't finish deleting.
>
> **Refs across namespaces.** Holders hold a Secret in another namespace
> only if that Secret, or its Namespace, carries
> `credential-use.krateo.io/allow-holders-from` listing the holder's
> namespace. `*` is accepted on the Secret only. Without it, the reference
> still works as before, but isn't protected, and the holder gets a
> `CredentialRefNotHeld` event. Removing a namespace from the annotation
> later releases those holders' holds through the normal protocol, with a
> `CredentialConsentWithdrawn` event.

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
  `DisownPasses × DisownInterval` (L3).
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
- Tamper checks skip lines whose R3 was seen, and GET the rest (L5).
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
  error, while `scaletozero.ExistsWithOptions(... WithHeld(...))` (or `WithHeldIndex`
  in PerName mode) finds a hold under the kind's prefix that this
  definition's RBAC is the only grant for.
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
| L1 Prunes coalesced to one write per batch; a default cap of 2,000 lines; 17,000 only as the hard ceiling | §10.6.2, §10.6.4 |
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
| L5 Tamper checks skip lines with a visible R3; the N× GET cost accepted | §10.6.6 |
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

## 18. Open questions for the user

**Decided since the last revision**: NQ1 (a 60s grace, §5), NQ2 (no cap,
§5.2), NQ3 (the builder line, §7.5) and NQ4 (provider-runtime#46). In
revision 16, IP1 to IP5 (§17).

**Decided after the review of revision 16:** Krateo's minimum Kubernetes
is **1.36**. CI runs the premise envtests, the exact RBAC message among
them, on 1.36.2 and 1.37.0 (§6.2).

Nothing is open.

Nothing is open. No question needs the user.
