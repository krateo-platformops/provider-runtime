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
// On top of it (plan, PRs 2 and 3a):
//
//   - the protocol: A1 and A2, BeforeEstablish and Forget, and R0 to R3,
//     over a narrow server interface and the verified sets (§6);
//   - the client-go runtime: NewShared and Track, which build the core's
//     own metadata informers on a ListerWatcher that records each list's
//     completion (§6.5 L2), publish a kind's shared settings on its CRD
//     (§7.3), and queue release batches from the informers' events (§10.2).
//     The write loops that drain that queue come with PR 3b.
//
// Nothing in provider-runtime uses this package yet.
//
// # Dependencies
//
// The package depends on client-go and apimachinery only, never on
// controller-runtime: runtimes without a manager use it too (§7.1). A test
// checks that.
package credentialuse
