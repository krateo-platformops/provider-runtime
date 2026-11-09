#!/bin/bash
# Usage: setup.sh
#
# Prepares the cluster in env.local for runs: installs the Greeting CRD from
# examples/greeting-controller/crd.yaml and waits until it is established. It is
# idempotent, and every block script calls it first, so a new cluster can't skip
# it: the re-measure's first calibration attempt failed at setup because the new
# cluster had no CRD. Every kubectl uses this directory's kubeconfig and the
# context from env.local. Exits 2 if the CRD can't be installed.
cd "$(dirname "$0")" || exit 1
# shellcheck disable=SC1091
. ./env.local
export USE_GKE_GCLOUD_AUTH_PLUGIN=True
k() { kubectl --kubeconfig "$PWD/kubeconfig" --context "$RLGOV_CONTEXT" "$@"; }
root=$(git rev-parse --show-toplevel)
k apply -f "$root/examples/greeting-controller/crd.yaml" >/dev/null || { echo "SETUP-FAILED: apply the Greeting CRD"; exit 2; }
k wait --for condition=established --timeout=60s crd/greetings.examples.krateo.io >/dev/null || { echo "SETUP-FAILED: the Greeting CRD is not established"; exit 2; }
echo "setup: Greeting CRD established on $RLGOV_CONTEXT"
