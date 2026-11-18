#!/bin/bash
# remote-run.sh, from a git bundle instead of a git archive tar, and with one
# or more go test runs in sequence on the same VM. The VM gets no GitHub
# credentials: the source is a bundle copied to $HOME, cloned, and checked
# out at the commit given (which must be in the bundle).
#
# Each run streams -v output to ~/out/<name>.log, with /usr/bin/time -v in
# ~/out/<name>.time and a header in ~/out/<name>.header. A run starts only if
# the one before it exited 0 (stopAfterFail=1), so that a failed first run
# is reported, not followed. However it ends, the script writes ~/out/DONE
# with the exit status, packs ~/out into ~/out.tar.gz, and powers the VM off.
#
# Usage: remote-run-bundle.sh <go sha256> <bundle> <commit> \
#          -- <name> <timeout> <-run pattern> [VAR=value ...] [-- <name> ...]
set -uo pipefail
want=$1 bundle=$2 commit=$3; shift 3
mkdir -p "$HOME/out"
finish() {
  rc=$?
  echo "exit $rc at $(date -u '+%Y-%m-%d %H:%M:%S UTC')" > "$HOME/out/DONE"
  uptime >> "$HOME/out/DONE"
  tar -C "$HOME" -czf "$HOME/out.tar.gz" out
  sync
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
if [ ! -x /usr/bin/time ] || ! command -v git > /dev/null; then
  sudo apt-get update -qq && sudo apt-get install -y -qq time git > /dev/null
fi
export PATH=/usr/local/go/bin:$PATH GOTOOLCHAIN=local
dir="$HOME/src-${commit:0:7}"
# git bundle verify needs a repository, so it runs in the clone (the first
# attempt, outside one, exited 1).
git clone -q --no-checkout "$HOME/$bundle" "$dir"
git -C "$dir" bundle verify "$HOME/$bundle" > "$HOME/out/bundle.verify" 2>&1
git -C "$dir" checkout -q --detach "$commit"
git -C "$dir" log -1 --format='%H %s' > "$HOME/out/commit"
cd "$dir"
go vet ./pkg/credentialuse/... 2> "$HOME/out/vet"
set +e
[ "${1:-}" = "--" ] && shift
while [ $# -gt 0 ]; do
  name=$1 timeout=$2 pattern=$3; shift 3
  vars=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do vars+=("$1"); shift; done
  [ "${1:-}" = "--" ] && shift
  {
    echo "source bundle $bundle at $(git rev-parse HEAD), machine $(curl -fsS -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/machine-type | sed 's|.*/||'), $(nproc) vCPU"
    echo "start $(date -u '+%Y-%m-%d %H:%M:%S UTC'); $(uptime)"
    echo "env: ${vars[*]}"
    echo "run: $pattern, timeout $timeout"
  } > "$HOME/out/$name.header"
  env "${vars[@]}" /usr/bin/time -v go test -count=1 -v -timeout "$timeout" -run "$pattern" ./pkg/credentialuse/ \
    > "$HOME/out/$name.log" 2> "$HOME/out/$name.time"
  rc=$?
  echo "end $(date -u '+%Y-%m-%d %H:%M:%S UTC'); $(uptime); go test exit $rc" >> "$HOME/out/$name.header"
  [ $rc -ne 0 ] && exit $rc
done
exit 0
