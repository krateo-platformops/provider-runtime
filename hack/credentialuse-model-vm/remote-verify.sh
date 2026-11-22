#!/bin/bash
# Runs a branch's pre-push checks on a disposable VM, from a git bundle, as
# remote-run-bundle.sh does: the whole repo as CI's build job runs it (envtest
# assets set, -race, every package), then, if that passes, a revert-check
# script. The VM gets no GitHub credentials: the source is a bundle copied to
# $HOME, cloned, and checked out at the commit given.
#
# Outputs, in ~/out: ci.log and ci.time (/usr/bin/time -v), revert.txt and
# revert.time, a header with the machine and the times. However it ends, the
# script writes ~/out/DONE with the exit status, packs ~/out into
# ~/out.tar.gz, waits GRACE seconds (180 by default) for the copy-back, and
# powers the VM off.
#
# Usage: remote-verify.sh <go sha256> <bundle> <commit> <revert script> [envtest version]
# e.g.   remote-verify.sh ccfe7ae8...c5 cred-6.bundle 04e5760 hack/revert-check-credential-use-6.py 1.37.0
set -uo pipefail
want=$1 bundle=$2 commit=$3 revert=$4 k8s=${5:-1.37.0}
mkdir -p "$HOME/out"
finish() {
  rc=$?
  echo "exit $rc at $(date -u '+%Y-%m-%d %H:%M:%S UTC')" > "$HOME/out/DONE"
  uptime >> "$HOME/out/DONE"
  tar -C "$HOME" -czf "$HOME/out.tar.gz" out
  sync
  sleep "${GRACE:-180}"
  sudo poweroff
}
trap finish EXIT
set -e
cd "$HOME"
if [ ! -x /usr/local/go/bin/go ]; then
  curl -fsSLO https://go.dev/dl/go1.26.10.linux-amd64.tar.gz
  echo "$want  go1.26.10.linux-amd64.tar.gz" | sha256sum -c -
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.26.10.linux-amd64.tar.gz
fi
# GCE's Debian 12 image has no /usr/bin/time and no C compiler; git and
# python3 for the clone and the revert check, gcc for -race, which needs cgo
# (the first run exited 2: "-race requires cgo").
if [ ! -x /usr/bin/time ] || ! command -v git > /dev/null || ! command -v python3 > /dev/null || ! command -v gcc > /dev/null; then
  sudo apt-get update -qq && sudo apt-get install -y -qq time git python3 gcc libc6-dev > /dev/null
fi
export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH GOTOOLCHAIN=local CGO_ENABLED=1
dir="$HOME/src-${commit:0:7}"
rm -rf "$dir"
git clone -q --no-checkout "$HOME/$bundle" "$dir"
git -C "$dir" bundle verify "$HOME/$bundle" > "$HOME/out/bundle.verify" 2>&1
git -C "$dir" checkout -q --detach "$commit"
git -C "$dir" log -1 --format='%H %s' > "$HOME/out/commit"
cd "$dir"
# setup-envtest at the version this repo's developers use; it downloads the
# envtest binaries for linux.
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.0.0-20260125163108-a19ec76a3c5d
KUBEBUILDER_ASSETS=$(setup-envtest use -p path "$k8s")
test -n "$KUBEBUILDER_ASSETS"
export KUBEBUILDER_ASSETS
{
  echo "source bundle $bundle at $(git rev-parse HEAD), machine $(curl -fsS -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/machine-type | sed 's|.*/||'), $(nproc) vCPU"
  echo "envtest $k8s at $KUBEBUILDER_ASSETS"
  echo "ci start $(date -u '+%Y-%m-%d %H:%M:%S UTC'); $(uptime)"
} > "$HOME/out/header"
set +e
/usr/bin/time -v go test -race -count=1 -skip '^TestModel' ./... > "$HOME/out/ci.log" 2> "$HOME/out/ci.time"
rc=$?
echo "ci end $(date -u '+%Y-%m-%d %H:%M:%S UTC'); go test exit $rc; $(uptime)" >> "$HOME/out/header"
[ $rc -ne 0 ] && exit $rc
echo "revert start $(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> "$HOME/out/header"
/usr/bin/time -v python3 "$revert" > "$HOME/out/revert.txt" 2> "$HOME/out/revert.time"
rc=$?
echo "revert end $(date -u '+%Y-%m-%d %H:%M:%S UTC'); exit $rc; $(uptime)" >> "$HOME/out/header"
git status --short > "$HOME/out/tree-after"
exit $rc
