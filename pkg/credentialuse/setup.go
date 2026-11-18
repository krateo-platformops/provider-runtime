package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// crdGVR is the CustomResourceDefinition resource.
var crdGVR = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

// maxCRDWriteAttempts bounds the GET-and-patch attempts of one CRD write
// (§7.3, L4 of the review of revision 16).
const maxCRDWriteAttempts = 5

// published are the settings every process of a holder kind must share,
// published on its CRD as credential-use.krateo.io/config (§7.3, L3).
type published struct {
	DeletionGrace  string `json:"deletionGrace"`
	DisownPasses   int    `json:"disownPasses"`
	DisownInterval string `json:"disownInterval"`
	TimeoutBound   string `json:"timeoutBound"`
}

// formatDuration writes d as the design's examples do: "60s", and Go's form
// for anything finer than a second.
func formatDuration(d time.Duration) string {
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return d.String()
}

func publishedOf(o Options) published {
	return published{
		DeletionGrace:  formatDuration(o.DeletionGrace),
		DisownPasses:   o.DisownPasses,
		DisownInterval: formatDuration(o.DisownInterval),
		TimeoutBound:   formatDuration(o.TimeoutBound),
	}
}

// disagreement returns how got, read off the CRD, differs from want, or "".
// Durations are compared as durations, so "60s" and "1m0s" agree.
func disagreement(want, got published) string {
	var diffs []string
	durations := []struct {
		name      string
		want, got string
	}{
		{"deletionGrace", want.DeletionGrace, got.DeletionGrace},
		{"disownInterval", want.DisownInterval, got.DisownInterval},
		{"timeoutBound", want.TimeoutBound, got.TimeoutBound},
	}
	for _, d := range durations {
		w, _ := time.ParseDuration(d.want)
		g, err := time.ParseDuration(d.got)
		if err != nil || g != w {
			diffs = append(diffs, fmt.Sprintf("%s %q here, %q published", d.name, d.want, d.got))
		}
	}
	if want.DisownPasses != got.DisownPasses {
		diffs = append(diffs, fmt.Sprintf("disownPasses %d here, %d published", want.DisownPasses, got.DisownPasses))
	}
	if len(diffs) == 0 {
		return ""
	}
	return fmt.Sprint(diffs)
}

// publishConfig publishes o's shared settings on the holder kind's CRD once,
// or checks them against what is published there (§7.3). The first process
// writes credential-use.krateo.io/config after a GET that finds it absent,
// with a merge patch guarded by the CRD's resourceVersion; a conflict reads
// and decides again, at most maxCRDWriteAttempts times. After the last
// conflict a read-only GET still checks what another process may have
// published meanwhile (G3 of the review of #109); if the annotation is
// still absent, the write waits for the next start, logged and counted. A
// process whose settings disagree refuses to start: changing a value means
// editing the annotation and restarting every process of the kind.
func (s *Shared) publishConfig(ctx context.Context, o Options) error {
	gk := o.Holder.GroupKind()
	m, err := s.c.Mapper.RESTMapping(gk, o.Holder.Version)
	if err != nil {
		return err
	}
	if s.c.Dynamic == nil {
		return errors.New("credential-use: Clients.Dynamic is required")
	}
	name := m.Resource.Resource + "." + m.Resource.Group
	crds := s.c.Dynamic.Resource(crdGVR)
	want := publishedOf(o)
	value, err := json.Marshal(want)
	if err != nil {
		return err
	}
	// check reads the CRD; done reports whether it carries the
	// annotation, which then agrees with want, or check fails.
	check := func() (crd *unstructured.Unstructured, done bool, err error) {
		crd, err = crds.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, false, fmt.Errorf("credential-use: reading the CRD of %s to publish its settings (%s): %w", gk, AnnotationConfig, err)
		}
		cur, ok := crd.GetAnnotations()[AnnotationConfig]
		if !ok {
			return crd, false, nil
		}
		var got published
		if err := json.Unmarshal([]byte(cur), &got); err != nil {
			return nil, false, fmt.Errorf("credential-use: %s on CRD %s can't be read: %w", AnnotationConfig, name, err)
		}
		if d := disagreement(want, got); d != "" {
			return nil, false, fmt.Errorf("credential-use: this process's settings for %s disagree with those published on CRD %s (%s): %s; "+
				"edit the annotation and restart every process of the kind to change them", gk, name, AnnotationConfig, d)
		}
		return crd, true, nil
	}
	var last error
	for range maxCRDWriteAttempts {
		crd, done, err := check()
		if err != nil || done {
			return err
		}
		patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"resourceVersion": crd.GetResourceVersion(),
			"annotations":     map[string]string{AnnotationConfig: string(value)},
		}})
		if err != nil {
			return err
		}
		_, err = crds.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
		if err == nil {
			return nil
		}
		if Classify(err) != ClassConflict {
			return fmt.Errorf("credential-use: publishing the settings of %s on CRD %s: %w", gk, name, err)
		}
		last = err
	}
	if _, done, err := check(); err != nil || done {
		return err
	}
	o.Metrics.CRDWriteStarved(kindLabel(gk))
	o.Logger.Warn("credential-use: publishing the settings conflicted every time; they will be written at the next start",
		"crd", name, "attempts", maxCRDWriteAttempts, "lastConflict", last.Error())
	return nil
}
