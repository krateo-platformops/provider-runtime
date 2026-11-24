// Package credentialuse holds the Secrets, and the objects that lead to them,
// that a managed resource reads to reach its external system, for as long as
// losing them would be unrecoverable. A namespace deletion then can't wedge
// on a managed resource whose credentials were deleted first.
//
// The design is docs/design-credential-use-finalizer.md, and the build
// follows docs/plan-credential-use-implementation.md. Section numbers (§) in
// this package's comments are the design's.
//
// # What is here so far
//
// This is the vocabulary the protocol is built from (plan, PR 1):
//
//   - Ref and ID, the refs a holder reads, and the record that stores them on
//     the holder (§4);
//   - FinalizerName, the marker label and the epoch: the keys a hold writes on
//     the protected object (§3, §10.1);
//   - Classify and ClassifyJSONPatch, which read an API error by its code,
//     reason, message and causes, never by IsForbidden or IsInvalid alone
//     (§6.2);
//   - the patch builders: an acquiring write (A2), a release (R3) and the
//     label janitor's patch (§6.2, §10.1);
//   - Decide and the user predicates: §5's table and §6.1's rules, as pure
//     functions.
//
// On top of it (plan, PRs 2, 3a and 3b):
//
//   - the protocol: A1 and A2, BeforeEstablish and Forget, and R0 to R3,
//     over a narrow server interface and the verified sets (§6);
//   - the client-go runtime: NewShared and Track, which build the core's
//     own metadata informers on a ListerWatcher that records each list's
//     completion (§6.5 L2), publish a kind's shared settings on its CRD
//     (§7.3), and queue release batches from the informers' events (§10.2);
//   - the write loops, RunWriteLoops (§7.5): the release loop that drains
//     that queue after a rescan, the missing-ref prober (§10.3) and the
//     label janitor (§10.1), with §12's waiting gauge and events; and
//     caches restricted to some namespaces (§10.5).
//
// PerName mode (plan, PR 8) is for resourceNames RBAC, where a protected kind
// can't be listed: each holder kind keeps an index ConfigMap of what it may
// hold in SharedOptions.IndexNamespace, kept a superset of its holds by
// Dekker on both sides, and watches each object the index names by name
// (§10.6, §13 shape 1). Disown and DisownPrefix read it (§7.4).
//
// provider-runtime's reconciler is its first user (plan, PR 7): see
// reconciler.CredentialRefsProvider. For a runtime's adapter the package
// adds KindOptions, Tracker.HasSynced, IsOwnRecordWrite, CheckRead,
// SummarizeUnheld, WithResolvedObjects, NextObject and Ref.AsksMoreThan
// (§7.5, §9.1, §11.9, §12).
//
// # Before adopting
//
// A CRD of a protected kind that serves several versions must convert between
// them without losing a field, a label or an annotation: a hold is metadata,
// written through one of them. With the conversion strategy None, that means
// equal schemas. Otherwise a hold drops fields of the object, or is never
// released. The holder kind's must keep labels and annotations, or its record is
// lost, and fields, for Disown (§6.2).
//
// # Dependencies
//
// The package depends on client-go and apimachinery only, never on
// controller-runtime: runtimes without a manager use it too (§7.1). A test
// checks that.
package credentialuse
