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
// Nothing here talks to an API server, and nothing in provider-runtime uses
// this package yet.
//
// # Dependencies
//
// The package depends on client-go and apimachinery only, never on
// controller-runtime: runtimes without a manager use it too (§7.1). A test
// checks that.
package credentialuse
