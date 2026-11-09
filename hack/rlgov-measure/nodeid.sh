#!/bin/bash
# Usage: nodeid.sh [label]. Appends the node's name, boot ID and kubelet version
# to results/node-identity.txt. A change between two lines means a repair or an
# upgrade happened: any run between them is discarded and reported.
cd "$(dirname "$0")" || exit 1
# shellcheck disable=SC1091
. ./env.local
export USE_GKE_GCLOUD_AUTH_PLUGIN=True
mkdir -p results
line=$(kubectl --kubeconfig "$PWD/kubeconfig" --context "$RLGOV_CONTEXT" get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.status.nodeInfo.bootID} {.status.nodeInfo.kubeletVersion} {.status.nodeInfo.kernelVersion}{"\n"}{end}')
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ${1:-check} $line" | tee -a results/node-identity.txt
