#!/bin/bash
# Runs one model measurement on a disposable VM, as hack/case-freeze's
# scripts do: installs go1.26.10 from the tarball checked against the sha256
# given, unpacks the source archive (a `git archive` tar in $HOME), and runs
# go test with the given environment and -run pattern, streaming -v output
# to ~/out/<name>.log with /usr/bin/time -v in ~/out/<name>.time.
#
# However it ends, it writes ~/out/DONE (with the exit status), packs ~/out
# into ~/out.tar.gz on the boot disk, and powers the VM off, so an idle VM
# stops billing for compute at once. The outputs are copied back from the
# disk, and the VM is then deleted.
#
# Usage: remote-run.sh <go sha256> <src.tar> <name> <timeout> <-run pattern> [VAR=value ...]
# e.g.   remote-run.sh ccfe7ae8...c5 src-487543c.tar size 180m '^TestModelStatelessSize$' CREDENTIALUSE_MODEL_SIZE=1
set -uo pipefail
want=$1 tarf=$2 name=$3 timeout=$4 pattern=$5; shift 5
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
export PATH=/usr/local/go/bin:$PATH GOTOOLCHAIN=local
dir="$HOME/src-${tarf%.tar}"
mkdir -p "$dir" && tar -C "$dir" -xf "$HOME/$tarf"
cd "$dir"
{
  echo "source $tarf, machine $(curl -fsS -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/machine-type | sed 's|.*/||'), $(nproc) vCPU"
  echo "start $(date -u '+%Y-%m-%d %H:%M:%S UTC'); $(uptime)"
  echo "env: $*"
} > "$HOME/out/$name.header"
go vet ./pkg/credentialuse/... 2> "$HOME/out/$name.vet"
set +e
env "$@" /usr/bin/time -v go test -count=1 -v -timeout "$timeout" -run "$pattern" ./pkg/credentialuse/ \
  > "$HOME/out/$name.log" 2> "$HOME/out/$name.time"
rc=$?
echo "end $(date -u '+%Y-%m-%d %H:%M:%S UTC'); $(uptime); go test exit $rc" >> "$HOME/out/$name.header"
exit $rc
