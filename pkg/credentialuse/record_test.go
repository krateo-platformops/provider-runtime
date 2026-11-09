package credentialuse

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func holderWith(annotations map[string]string) *metav1.ObjectMeta {
	return &metav1.ObjectMeta{Name: "h", Namespace: "ns", Annotations: annotations}
}

func TestSecretRef(t *testing.T) {
	r := SecretRef("team-a", "git-token")
	want := ID{Group: "", Kind: "Secret", Namespace: "team-a", Name: "git-token"}
	if diff := cmp.Diff(want, r.ID()); diff != "" {
		t.Errorf("SecretRef ID (-want +got):\n%s", diff)
	}
	if r.GroupKind != (schema.GroupKind{Kind: "Secret"}) {
		t.Errorf("SecretRef GroupKind = %v", r.GroupKind)
	}
}

func TestIDString(t *testing.T) {
	cases := map[string]struct {
		id   ID
		want string
	}{
		"a core kind":           {ID{Kind: "Secret", Namespace: "team-a", Name: "git-token"}, "/Secret/team-a/git-token"},
		"a grouped kind":        {ID{Group: "core.krateo.io", Kind: "KubernetesTarget", Namespace: "team-b", Name: "prod"}, "core.krateo.io/KubernetesTarget/team-b/prod"},
		"a cluster-scoped kind": {ID{Group: "g.io", Kind: "Thing", Name: "x"}, "g.io/Thing//x"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.id.String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSortedIDsSortsAndDeduplicates(t *testing.T) {
	a := ID{Kind: "Secret", Namespace: "a", Name: "x"}
	b := ID{Kind: "Secret", Namespace: "b", Name: "x"}
	c := ID{Group: "g.io", Kind: "Target", Namespace: "a", Name: "x"}
	in := []ID{c, b, a, b, a}
	got := SortedIDs(in)
	if diff := cmp.Diff([]ID{a, b, c}, got); diff != "" {
		t.Errorf("SortedIDs (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]ID{c, b, a, b, a}, in); diff != "" {
		t.Errorf("SortedIDs modified its input (-want +got):\n%s", diff)
	}
}

func TestEncodeRecord(t *testing.T) {
	probe := ID{Kind: "Secret", Namespace: "rlgov-probe", Name: "probe-dummy-creds"}
	cases := map[string]struct {
		ids     []ID
		want    string
		wantErr bool
	}{
		// The probe's Repo named one Secret twice (§16.1): one ref.
		"the design's example, named twice": {
			ids:  []ID{probe, probe},
			want: `[{"group":"","kind":"Secret","namespace":"rlgov-probe","name":"probe-dummy-creds"}]`,
		},
		"sorted": {
			ids:  []ID{{Kind: "Secret", Namespace: "b", Name: "x"}, {Kind: "Secret", Namespace: "a", Name: "x"}},
			want: `[{"group":"","kind":"Secret","namespace":"a","name":"x"},{"group":"","kind":"Secret","namespace":"b","name":"x"}]`,
		},
		"no refs":      {ids: nil, want: `[]`},
		"no kind":      {ids: []ID{{Namespace: "a", Name: "x"}}, wantErr: true},
		"no name":      {ids: []ID{{Kind: "Secret", Namespace: "a"}}, wantErr: true},
		"no namespace": {ids: []ID{{Group: "g.io", Kind: "Thing", Name: "x"}}, want: `[{"group":"g.io","kind":"Thing","namespace":"","name":"x"}]`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := EncodeRecord(tc.ids)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("EncodeRecord(%v) = %q, want an error", tc.ids, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("EncodeRecord: %v", err)
			}
			if got != tc.want {
				t.Errorf("EncodeRecord = %s\nwant           %s", got, tc.want)
			}
		})
	}
}

func TestReadRecord(t *testing.T) {
	x := ID{Kind: "Secret", Namespace: "ns", Name: "x"}
	y := ID{Kind: "Secret", Namespace: "ns", Name: "y"}
	cases := map[string]struct {
		annotations map[string]string
		want        Record
	}{
		"absent":             {nil, Record{}},
		"empty array":        {map[string]string{AnnotationRefs: `[]`}, Record{Present: true, IDs: []ID{}}},
		"unsorted, repeated": {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","name":"y"},{"group":"","kind":"Secret","namespace":"ns","name":"x"},{"group":"","kind":"Secret","namespace":"ns","name":"x"}]`}, Record{Present: true, IDs: []ID{x, y}}},
		// Every one of these fails safe: the holder counts as a user (§4).
		"empty value":           {map[string]string{AnnotationRefs: ``}, Record{Present: true, Unparseable: true}},
		"not JSON":              {map[string]string{AnnotationRefs: `oops`}, Record{Present: true, Unparseable: true}},
		"null":                  {map[string]string{AnnotationRefs: `null`}, Record{Present: true, Unparseable: true}},
		"an object":             {map[string]string{AnnotationRefs: `{"kind":"Secret"}`}, Record{Present: true, Unparseable: true}},
		"an unknown field":      {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","name":"x","key":"token"}]`}, Record{Present: true, Unparseable: true}},
		"an entry with no name": {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","name":""}]`}, Record{Present: true, Unparseable: true}},
		"trailing data":         {map[string]string{AnnotationRefs: `[] []`}, Record{Present: true, Unparseable: true}},
		// M1 of the review of #53: a lenient decoder reads each of these as
		// another object, and the holder then doesn't count as a user.
		"a missing namespace":               {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","name":"creds"}]`}, Record{Present: true, Unparseable: true}},
		"a null namespace":                  {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":null,"name":"creds"}]`}, Record{Present: true, Unparseable: true}},
		"a missing group":                   {map[string]string{AnnotationRefs: `[{"kind":"Target","namespace":"ns","name":"t"}]`}, Record{Present: true, Unparseable: true}},
		"a null group":                      {map[string]string{AnnotationRefs: `[{"group":null,"kind":"Target","namespace":"ns","name":"t"}]`}, Record{Present: true, Unparseable: true}},
		"a key in another case":             {map[string]string{AnnotationRefs: `[{"Group":"","kind":"Secret","namespace":"ns","name":"x"}]`}, Record{Present: true, Unparseable: true}},
		"a duplicate key":                   {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","namespace":"other","name":"x"}]`}, Record{Present: true, Unparseable: true}},
		"a trailing ]":                      {map[string]string{AnnotationRefs: `[]]`}, Record{Present: true, Unparseable: true}},
		"a trailing }":                      {map[string]string{AnnotationRefs: `[]}`}, Record{Present: true, Unparseable: true}},
		"a number":                          {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","name":1}]`}, Record{Present: true, Unparseable: true}},
		"a nested array":                    {map[string]string{AnnotationRefs: `[[{"group":"","kind":"Secret","namespace":"ns","name":"x"}]]`}, Record{Present: true, Unparseable: true}},
		"an unclosed entry":                 {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","name":"x"`}, Record{Present: true, Unparseable: true}},
		"an unclosed array":                 {map[string]string{AnnotationRefs: `[`}, Record{Present: true, Unparseable: true}},
		"an array left open after an entry": {map[string]string{AnnotationRefs: `[{"group":"","kind":"Secret","namespace":"ns","name":"x"}`}, Record{Present: true, Unparseable: true}},
		"an explicit empty namespace (cluster-scoped)": {map[string]string{AnnotationRefs: `[{"group":"g.io","kind":"Thing","namespace":"","name":"x"}]`}, Record{Present: true, IDs: []ID{{Group: "g.io", Kind: "Thing", Name: "x"}}}},
		"spacing is fine": {map[string]string{AnnotationRefs: " [ { \"name\" : \"x\" , \"group\":\"\", \"namespace\":\"ns\", \"kind\":\"Secret\" } ] \n"}, Record{Present: true, IDs: []ID{x}}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ReadRecord(holderWith(tc.annotations))
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ReadRecord (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRecordRoundTrip(t *testing.T) {
	ids := []ID{{Kind: "Secret", Namespace: "b", Name: "x"}, {Group: "g.io", Kind: "T", Namespace: "a", Name: "y"}}
	v, err := EncodeRecord(ids)
	if err != nil {
		t.Fatal(err)
	}
	got := ReadRecord(holderWith(map[string]string{AnnotationRefs: v}))
	if diff := cmp.Diff(Record{Present: true, IDs: SortedIDs(ids)}, got); diff != "" {
		t.Errorf("round trip (-want +got):\n%s", diff)
	}
}

func TestRecordNames(t *testing.T) {
	x := ID{Kind: "Secret", Namespace: "ns", Name: "x"}
	y := ID{Kind: "Secret", Namespace: "ns", Name: "y"}
	if !(Record{Present: true, IDs: []ID{x}}).Names(x) {
		t.Error("a record doesn't name its own ref")
	}
	if (Record{Present: true, IDs: []ID{x}}).Names(y) {
		t.Error("a record names a ref it doesn't hold")
	}
	if (Record{}).Names(x) {
		t.Error("an absent record names a ref")
	}
	if !(Record{Present: true, Unparseable: true}).Names(y) {
		t.Error("an unparseable record doesn't name every ref (§4: it fails safe)")
	}
	// A Record built by hand needn't be sorted (L7 of the review of #53).
	a := ID{Kind: "Secret", Namespace: "ns", Name: "a"}
	unsorted := Record{Present: true, IDs: []ID{y, x, a}}
	for _, id := range []ID{a, x, y} {
		if !unsorted.Names(id) {
			t.Errorf("an unsorted record doesn't name %s", id)
		}
	}
}
