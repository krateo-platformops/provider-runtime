#!/bin/bash
# Usage: cleanup-run.sh <run-name>. Removes what a run left on the cluster, in
# the order that doesn't stick: the controller, then the finalizers, then the
# namespace and the binding. Safe to run on a run that is already gone.
cd "$(dirname "$0")" || exit 1
# shellcheck disable=SC1091
. ./env.local
export USE_GKE_GCLOUD_AUTH_PLUGIN=True
KC="$PWD/kubeconfig"
ns=$1
k() { kubectl --kubeconfig "$KC" --context "$RLGOV_CONTEXT" "$@"; }
k -n "$ns" delete deploy measure --ignore-not-found --wait=true >/dev/null
k -n "$ns" delete greetings --all --wait=false >/dev/null
k -n "$ns" get greetings -o name | xargs -n20 kubectl --kubeconfig "$KC" --context "$RLGOV_CONTEXT" -n "$ns" patch --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null
# These two say whether it worked: a failed step above leaves its work to them.
k delete namespace "$ns" --ignore-not-found --wait=false >/dev/null || exit 1
k delete clusterrolebinding "rlgov-$ns" --ignore-not-found >/dev/null || exit 1
echo "cleaned $ns"
