package credentialuse

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

// The patches are applied here with the libraries the API server applies
// them with (k8s.io/apiserver@v0.37.2 pkg/endpoints/handlers/patch.go):
// json-patch v4 for JSON and merge patches, and strategicpatch for strategic
// merge patches. premises_envtest_test.go sends them to a real API server.

var otherGK = schema.GroupKind{Group: "core.krateo.io", Kind: "CompositionDefinition"}

// asSet compares finalizer lists as sets: a strategic merge puts the
// patch's entries first, and the order carries no meaning.
var asSet = cmpopts.SortSlices(func(a, b string) bool { return a < b })

const (
	fRepoA  = "credential-use.repo.git.krateo.io/a"
	fRepoB  = "credential-use.repo.git.krateo.io/b"
	fOtherA = "credential-use.compositiondefinition.core.krateo.io/a"
)

func secretJSON(t *testing.T, m metav1.ObjectMeta) []byte {
	t.Helper()
	b, err := json.Marshal(&corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: m})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func apply(t *testing.T, doc []byte, p Patch) ([]byte, error) {
	t.Helper()
	switch p.Type {
	case types.StrategicMergePatchType:
		return strategicpatch.StrategicMergePatch(doc, p.Data, &corev1.Secret{})
	case types.MergePatchType:
		return jsonpatch.MergePatch(doc, p.Data)
	case types.JSONPatchType:
		ops, err := jsonpatch.DecodePatch(p.Data)
		if err != nil {
			t.Fatalf("the JSON patch doesn't decode: %v\n%s", err, p.Data)
		}
		return ops.Apply(doc)
	}
	t.Fatalf("unknown patch type %q", p.Type)
	return nil, nil
}

func metaOf(t *testing.T, doc []byte) metav1.ObjectMeta {
	t.Helper()
	var s corev1.Secret
	if err := json.Unmarshal(doc, &s); err != nil {
		t.Fatal(err)
	}
	return s.ObjectMeta
}

func TestAcquireStrategicPatch(t *testing.T) {
	p, err := AcquireStrategicPatch(repoGK, "a", "e1")
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != types.StrategicMergePatchType {
		t.Fatalf("type = %q, want a strategic merge patch", p.Type)
	}
	// It keeps a third party's finalizer, labels and annotations, and
	// another kind's finalizer (§6.2: it can't drop anyone else's).
	in := metav1.ObjectMeta{
		Name: "s", Namespace: "a", ResourceVersion: "7",
		Finalizers:  []string{"example.com/other", fOtherA},
		Labels:      map[string]string{"app": "x", LabelHeld: "true", MarkerLabel(otherGK): "true"},
		Annotations: map[string]string{"note": "n", fOtherA: "e0"},
	}
	out, err := apply(t, secretJSON(t, in), p)
	if err != nil {
		t.Fatal(err)
	}
	got := metaOf(t, out)
	if diff := cmp.Diff([]string{"example.com/other", fOtherA, fRepoA}, got.Finalizers, asSet); diff != "" {
		t.Errorf("finalizers (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"note": "n", fOtherA: "e0", fRepoA: "e1"}, got.Annotations); diff != "" {
		t.Errorf("annotations (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"app": "x", LabelHeld: "true", MarkerLabel(otherGK): "true", MarkerLabel(repoGK): "true"}, got.Labels); diff != "" {
		t.Errorf("labels (-want +got):\n%s", diff)
	}
	if got.ResourceVersion != "7" {
		t.Error("the strategic merge patch carries a resourceVersion: it must have no precondition")
	}

	// Again, on an object that already has it: no duplicate, and the epoch
	// moves (§6.2: held, and the epoch still moves).
	p2, _ := AcquireStrategicPatch(repoGK, "a", "e2")
	out2, err := apply(t, out, p2)
	if err != nil {
		t.Fatal(err)
	}
	got2 := metaOf(t, out2)
	if diff := cmp.Diff([]string{"example.com/other", fOtherA, fRepoA}, got2.Finalizers, asSet); diff != "" {
		t.Errorf("finalizers after a second write (-want +got):\n%s", diff)
	}
	if got2.Annotations[fRepoA] != "e2" {
		t.Errorf("epoch = %q, want e2", got2.Annotations[fRepoA])
	}
}

func TestAcquireGuardedPatch(t *testing.T) {
	in := metav1.ObjectMeta{
		Name: "t", Namespace: "a", ResourceVersion: "42",
		Finalizers:  []string{"example.com/other", fOtherA},
		Labels:      map[string]string{"app": "x"},
		Annotations: map[string]string{"note": "n"},
	}
	p, err := AcquireGuardedPatch(&in, repoGK, "a", "e1")
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != types.MergePatchType {
		t.Fatalf("type = %q, want a merge patch", p.Type)
	}
	var body struct {
		Metadata struct {
			ResourceVersion string   `json:"resourceVersion"`
			Finalizers      []string `json:"finalizers"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(p.Data, &body); err != nil {
		t.Fatal(err)
	}
	if body.Metadata.ResourceVersion != "42" {
		t.Errorf("resourceVersion = %q, want the one read: the merge patch must be guarded", body.Metadata.ResourceVersion)
	}
	out, err := apply(t, secretJSON(t, in), p)
	if err != nil {
		t.Fatal(err)
	}
	got := metaOf(t, out)
	// A merge patch replaces the list, so the patch must carry everyone's.
	if diff := cmp.Diff([]string{"example.com/other", fOtherA, fRepoA}, got.Finalizers); diff != "" {
		t.Errorf("finalizers (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"note": "n", fRepoA: "e1"}, got.Annotations); diff != "" {
		t.Errorf("annotations (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"app": "x", LabelHeld: "true", MarkerLabel(repoGK): "true"}, got.Labels); diff != "" {
		t.Errorf("labels (-want +got):\n%s", diff)
	}

	// Already held: not added twice.
	held := metav1.ObjectMeta{Name: "t", ResourceVersion: "43", Finalizers: []string{fRepoA}}
	p2, _ := AcquireGuardedPatch(&held, repoGK, "a", "e2")
	out2, _ := apply(t, secretJSON(t, held), p2)
	if diff := cmp.Diff([]string{fRepoA}, metaOf(t, out2).Finalizers); diff != "" {
		t.Errorf("finalizers when already held (-want +got):\n%s", diff)
	}

	if _, err := AcquireGuardedPatch(&metav1.ObjectMeta{Name: "t"}, repoGK, "a", "e"); err == nil {
		t.Error("a guarded patch without a resourceVersion was built")
	}
}

func TestAcquirePatchesNeedAnEpochAndAValidName(t *testing.T) {
	if _, err := AcquireStrategicPatch(repoGK, "a", ""); err == nil {
		t.Error("AcquireStrategicPatch accepted an empty epoch")
	}
	if _, err := AcquireGuardedPatch(&metav1.ObjectMeta{ResourceVersion: "1"}, repoGK, "a", ""); err == nil {
		t.Error("AcquireGuardedPatch accepted an empty epoch")
	}
	if _, err := AcquireStrategicPatch(schema.GroupKind{Kind: "Pod"}, "a", "e"); err == nil {
		t.Error("AcquireStrategicPatch accepted a holder kind with no group")
	}
}

func TestEscapePointer(t *testing.T) {
	cases := map[string]string{
		"credential-use.repo.git.krateo.io/a": "credential-use.repo.git.krateo.io~1a",
		"a~b/c":                               "a~0b~1c",
		"~1":                                  "~01",
		"plain":                               "plain",
	}
	for in, want := range cases {
		if got := escapePointer(in); got != want {
			t.Errorf("escapePointer(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReleasePatch(t *testing.T) {
	marker, otherMarker := MarkerLabel(repoGK), MarkerLabel(otherGK)
	cases := map[string]struct {
		in       metav1.ObjectMeta
		want     metav1.ObjectMeta
		fullTest bool
	}{
		"K's last and the last of any kind: both labels go": {
			in: metav1.ObjectMeta{
				Finalizers:  []string{"example.com/other", fRepoA},
				Labels:      map[string]string{"app": "x", LabelHeld: "true", marker: "true"},
				Annotations: map[string]string{"note": "n", fRepoA: "e1"},
			},
			want: metav1.ObjectMeta{
				Finalizers:  []string{"example.com/other"},
				Labels:      map[string]string{"app": "x"},
				Annotations: map[string]string{"note": "n"},
			},
			fullTest: true,
		},
		"another namespace of K still holds: both labels stay": {
			in: metav1.ObjectMeta{
				Finalizers:  []string{fRepoA, fRepoB},
				Labels:      map[string]string{LabelHeld: "true", marker: "true"},
				Annotations: map[string]string{fRepoA: "e1", fRepoB: "e2"},
			},
			want: metav1.ObjectMeta{
				Finalizers:  []string{fRepoB},
				Labels:      map[string]string{LabelHeld: "true", marker: "true"},
				Annotations: map[string]string{fRepoB: "e2"},
			},
		},
		"another kind still holds: K's marker goes, the held label stays": {
			in: metav1.ObjectMeta{
				Finalizers:  []string{fOtherA, fRepoA},
				Labels:      map[string]string{LabelHeld: "true", marker: "true", otherMarker: "true"},
				Annotations: map[string]string{fRepoA: "e1", fOtherA: "e0"},
			},
			want: metav1.ObjectMeta{
				Finalizers:  []string{fOtherA},
				Labels:      map[string]string{LabelHeld: "true", otherMarker: "true"},
				Annotations: map[string]string{fOtherA: "e0"},
			},
			fullTest: true,
		},
		"labels already stripped: nothing to remove, no full test": {
			in: metav1.ObjectMeta{
				Finalizers:  []string{fRepoA},
				Annotations: map[string]string{fRepoA: "e1"},
			},
			want: metav1.ObjectMeta{},
		},
		// A third party emptied the labels' values: they are tested as read,
		// empty, and still removed.
		"labels with empty values": {
			in: metav1.ObjectMeta{
				Finalizers:  []string{fRepoA},
				Labels:      map[string]string{LabelHeld: "", marker: ""},
				Annotations: map[string]string{fRepoA: "e1"},
			},
			want:     metav1.ObjectMeta{},
			fullTest: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tc.in.Name, tc.in.ResourceVersion = "s", "1"
			p, err := ReleasePatch(&tc.in, repoGK, "a")
			if err != nil {
				t.Fatal(err)
			}
			if p.Type != types.JSONPatchType {
				t.Fatalf("type = %q, want a JSON patch", p.Type)
			}
			if got := strings.Contains(string(p.Data), `"path":"/metadata/finalizers","value"`); got != tc.fullTest {
				t.Errorf("tests the whole finalizer array = %v, want %v\n%s", got, tc.fullTest, p.Data)
			}
			out, err := apply(t, secretJSON(t, tc.in), p)
			if err != nil {
				t.Fatalf("the release didn't apply to the state it was built from: %v\n%s", err, p.Data)
			}
			got := metaOf(t, out)
			tc.want.Name, tc.want.ResourceVersion = "s", "1"
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("after the release (-want +got):\n%s", diff)
			}
		})
	}
}

// A release must fail, whole, against any state but the one it was built
// from in the ways that matter (§6.4): a moved epoch, a moved index, or, when
// it removes a label, any change to the finalizer array.
func TestReleasePatchFailsOnAChangedState(t *testing.T) {
	read := metav1.ObjectMeta{
		Name: "s", ResourceVersion: "1",
		Finalizers:  []string{"example.com/other", fRepoA},
		Labels:      map[string]string{LabelHeld: "true", MarkerLabel(repoGK): "true"},
		Annotations: map[string]string{fRepoA: "e1"},
	}
	p, err := ReleasePatch(&read, repoGK, "a")
	if err != nil {
		t.Fatal(err)
	}
	change := func(f func(m *metav1.ObjectMeta)) metav1.ObjectMeta {
		m := *read.DeepCopy()
		f(&m)
		return m
	}
	cases := map[string]metav1.ObjectMeta{
		// An acquiring write landed after R1: a fresh epoch (§6.4, tG < tW < tR).
		"a new epoch": change(func(m *metav1.ObjectMeta) { m.Annotations[fRepoA] = "e2" }),
		// Another releaser removed the finalizer, and an A2 re-added it at
		// another index.
		"re-added at another index": change(func(m *metav1.ObjectMeta) { m.Finalizers = []string{fRepoA, "example.com/other"} }),
		// A finalizer before it went, so index i now names something else.
		"the index moved, out of range": change(func(m *metav1.ObjectMeta) { m.Finalizers = []string{fRepoA} }),
		// Another namespace of K acquired: the marker label must stay.
		"another namespace of K acquired": change(func(m *metav1.ObjectMeta) {
			m.Finalizers = append(m.Finalizers, fRepoB)
			m.Annotations[fRepoB] = "x"
		}),
		// Another kind acquired: the held label must stay.
		"another kind acquired": change(func(m *metav1.ObjectMeta) {
			m.Finalizers = append(m.Finalizers, fOtherA)
			m.Annotations[fOtherA] = "x"
		}),
		"already released": change(func(m *metav1.ObjectMeta) {
			m.Finalizers = []string{"example.com/other"}
			delete(m.Annotations, fRepoA)
		}),
		// Every value it removes is tested (L1 of the review of revision 16).
		"K's marker label changed": change(func(m *metav1.ObjectMeta) { m.Labels[MarkerLabel(repoGK)] = "edited" }),
		"the held label changed":   change(func(m *metav1.ObjectMeta) { m.Labels[LabelHeld] = "edited" }),
	}
	for name, current := range cases {
		t.Run(name, func(t *testing.T) {
			doc := secretJSON(t, current)
			out, err := apply(t, doc, p)
			if err == nil {
				t.Fatalf("the release applied to a changed state:\n%s", metaOf(t, out).Finalizers)
			}
		})
	}
}

// The index test is what keeps a release from removing someone else's
// finalizer after an index shift that leaves the epoch alone. Here no label
// is removed (another namespace of K still holds), so there is no full-array
// test to catch it: a finalizer before F goes, F moves down, and index i now
// names the other namespace's hold.
func TestReleasePatchIndexShiftWithoutALabelRemoval(t *testing.T) {
	read := metav1.ObjectMeta{
		Name: "s", ResourceVersion: "1",
		Finalizers:  []string{"example.com/other", fRepoA, fRepoB},
		Labels:      map[string]string{LabelHeld: "true", MarkerLabel(repoGK): "true"},
		Annotations: map[string]string{fRepoA: "e1", fRepoB: "e2"},
	}
	p, err := ReleasePatch(&read, repoGK, "a")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(p.Data), `"path":"/metadata/finalizers","value"`) {
		t.Fatalf("the case needs no full-array test, but the patch has one:\n%s", p.Data)
	}
	current := *read.DeepCopy()
	current.Finalizers = []string{fRepoA, fRepoB}
	out, err := apply(t, secretJSON(t, current), p)
	if err == nil {
		t.Fatalf("the release applied after an index shift, leaving finalizers %v", metaOf(t, out).Finalizers)
	}
}

func TestReleasePatchErrors(t *testing.T) {
	if _, err := ReleasePatch(&metav1.ObjectMeta{Finalizers: []string{fRepoB}}, repoGK, "a"); !errors.Is(err, ErrNotHeld) {
		t.Errorf("without the finalizer: %v, want ErrNotHeld", err)
	}
	if _, err := ReleasePatch(&metav1.ObjectMeta{Finalizers: []string{fRepoA}}, repoGK, "a"); !errors.Is(err, ErrNoEpoch) {
		t.Errorf("without the epoch: %v, want ErrNoEpoch", err)
	}
	if _, err := ReleasePatch(&metav1.ObjectMeta{Finalizers: []string{fRepoA}, Annotations: map[string]string{fRepoA: ""}}, repoGK, "a"); !errors.Is(err, ErrNoEpoch) {
		t.Errorf("with an empty epoch: %v, want ErrNoEpoch", err)
	}
}

func TestRestoreEpochPatch(t *testing.T) {
	cases := map[string]metav1.ObjectMeta{
		"no annotations at all": {Name: "s", ResourceVersion: "5", Finalizers: []string{fRepoA}},
		"others' annotations":   {Name: "s", ResourceVersion: "5", Finalizers: []string{fRepoA}, Annotations: map[string]string{"note": "n"}},
		"an emptied epoch":      {Name: "s", ResourceVersion: "5", Finalizers: []string{fRepoA}, Annotations: map[string]string{fRepoA: ""}},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			p, err := RestoreEpochPatch(&in, repoGK, "a", "e9")
			if err != nil {
				t.Fatal(err)
			}
			out, err := apply(t, secretJSON(t, in), p)
			if err != nil {
				t.Fatalf("didn't apply: %v\n%s", err, p.Data)
			}
			got := metaOf(t, out)
			if got.Annotations[fRepoA] != "e9" {
				t.Errorf("epoch = %q, want e9", got.Annotations[fRepoA])
			}
			if in.Annotations["note"] != "" && got.Annotations["note"] != "n" {
				t.Error("another annotation was lost")
			}
			// Any write in between breaks it.
			moved := *in.DeepCopy()
			moved.ResourceVersion = "6"
			if _, err := apply(t, secretJSON(t, moved), p); err == nil {
				t.Error("it applied to a later resourceVersion")
			}
		})
	}
	if _, err := RestoreEpochPatch(&metav1.ObjectMeta{ResourceVersion: "1"}, repoGK, "a", "e"); !errors.Is(err, ErrNotHeld) {
		t.Errorf("without the finalizer: %v, want ErrNotHeld", err)
	}
}

func TestJanitorPatch(t *testing.T) {
	marker, otherMarker := MarkerLabel(repoGK), MarkerLabel(otherGK)
	cases := map[string]struct {
		in         metav1.ObjectMeta
		wantLabels map[string]string
		nothing    bool
	}{
		"stripped of every finalizer": {
			in:         metav1.ObjectMeta{Labels: map[string]string{"app": "x", LabelHeld: "true", marker: "true"}},
			wantLabels: map[string]string{"app": "x"},
		},
		"another kind still holds": {
			in:         metav1.ObjectMeta{Finalizers: []string{fOtherA}, Labels: map[string]string{LabelHeld: "true", marker: "true", otherMarker: "true"}},
			wantLabels: map[string]string{LabelHeld: "true", otherMarker: "true"},
		},
		"K still holds": {
			in:      metav1.ObjectMeta{Finalizers: []string{fRepoB}, Labels: map[string]string{LabelHeld: "true", marker: "true"}},
			nothing: true,
		},
		"no labels": {
			in:      metav1.ObjectMeta{Labels: map[string]string{"app": "x"}},
			nothing: true,
		},
		// Only the held label: K's janitor removes it, as no credential-use
		// finalizer is left.
		"only the held label": {
			in:         metav1.ObjectMeta{Labels: map[string]string{LabelHeld: "true"}},
			wantLabels: nil,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tc.in.Name, tc.in.ResourceVersion = "s", "3"
			p, err := JanitorPatch(&tc.in, repoGK)
			if tc.nothing {
				if !errors.Is(err, ErrNothingToClean) {
					t.Fatalf("JanitorPatch = %s, %v; want ErrNothingToClean", p.Data, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			out, err := apply(t, secretJSON(t, tc.in), p)
			if err != nil {
				t.Fatalf("didn't apply: %v\n%s", err, p.Data)
			}
			if diff := cmp.Diff(tc.wantLabels, metaOf(t, out).Labels, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("labels (-want +got):\n%s", diff)
			}
			// A new acquiring write in between breaks it.
			moved := *tc.in.DeepCopy()
			moved.ResourceVersion = "4"
			moved.Finalizers = append(moved.Finalizers, fRepoA)
			if _, err := apply(t, secretJSON(t, moved), p); err == nil {
				t.Error("the janitor applied over a new acquiring write")
			}
		})
	}
}
