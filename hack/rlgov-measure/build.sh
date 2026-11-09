#!/bin/bash
# Usage: build.sh new|ctrl [tag] [--local]
#
# Builds the harness for linux/amd64 against this worktree (new) or v1.10.0,
# d0632f1 (ctrl), and makes an image with crane: no Docker daemon. The base,
# alpine:3.20, is pulled for linux/amd64.
#
# By default it pushes the image. The registry path comes from env.local.
# crane authenticates through gcloud's credential helper, set in a gitignored
# .docker here, so no token passes through this script and ~/.docker is not
# touched.
#
# With --local it pushes nothing and needs no env.local or credentials: the
# image is written to images/measure-<tag>.tar (gitignored), and its record in
# results/image-<tag>.txt says so. A cluster run needs the image pushed to a
# registry the cluster can pull from, which is set up with the cluster.
set -euo pipefail
cd "$(dirname "$0")"
kind=$1; tag=${2:-$kind}; local=${3:-}
[ "$tag" = --local ] && { tag=$kind; local=--local; }
if [ "$local" != --local ]; then
  # shellcheck disable=SC1091
  . ./env.local
fi
root=$(git rev-parse --show-toplevel)
gofmt -l . | grep -v ctrl-src && exit 1
export CGO_ENABLED=0 GOOS=linux GOARCH=amd64
if [ "$kind" = ctrl ]; then
  # ctrl/go.mod and ctrl/go.sum are committed and pin v1.10.0's own
  # dependencies (controller-runtime v0.23.3, apimachinery v0.35.3). They are
  # not derived from go.mod: once the harness follows main to a newer
  # controller-runtime, the control would be built against that, and would no
  # longer be the code users run.
  mkdir -p ctrl-src
  git -C "$root" archive --format=tar -o "$PWD/ctrl.tar" d0632f1
  tar -xf ctrl.tar -C ctrl-src
  go build -tags ctrl -modfile=ctrl/go.mod -o measure .
else
  go build -o measure .
fi
unset CGO_ENABLED GOOS GOARCH
tar -cf "layer-$kind.tar" measure
commit=$(git -C "$root" rev-parse HEAD)
[ "$kind" = ctrl ] && commit="d0632f1 (harness at $commit)"
mkdir -p results
if [ "$local" = --local ]; then
  mkdir -p images
  out="images/measure-$tag.tar"
  .gobin/crane mutate --platform linux/amd64 alpine:3.20 --append "layer-$kind.tar" \
    --entrypoint /measure --user 65532:65532 -t "measure:$tag" -o "$out" >/dev/null
  rm -f "layer-$kind.tar"
  echo "$tag $commit $(.gobin/crane digest --tarball "$out") local tarball $out, not pushed" > "results/image-$tag.txt"
else
  export DOCKER_CONFIG="$PWD/.docker"
  mkdir -p "$DOCKER_CONFIG"
  command -v docker-credential-gcloud >/dev/null || { echo "docker-credential-gcloud is not on PATH" >&2; exit 1; }
  printf '{"credHelpers": {"%s": "gcloud"}}\n' "${RLGOV_IMAGE_REPO%%/*}" > "$DOCKER_CONFIG/config.json"
  base=$(.gobin/crane append --platform linux/amd64 -b alpine:3.20 -f "layer-$kind.tar" -t "$RLGOV_IMAGE_REPO/measure:$tag-base")
  .gobin/crane mutate "$base" --entrypoint /measure --user 65532:65532 -t "$RLGOV_IMAGE_REPO/measure:$tag"
  rm -f "layer-$kind.tar"
  echo "$tag $commit $(.gobin/crane digest "$RLGOV_IMAGE_REPO/measure:$tag")" > "results/image-$tag.txt"
fi
echo "built $tag from $(git -C "$root" rev-parse --short HEAD) $(git -C "$root" status --short pkg | wc -l | tr -d ' ') dirty"
