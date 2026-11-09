#!/bin/bash
# Usage: push.sh <tag>
#
# Pushes images/measure-<tag>.tar, built by build.sh --local, to the registry in
# env.local as measure:<tag>, unchanged, then checks that the pushed digest is
# the one results/image-<tag>.txt recorded at build time. Rebuilding instead
# would embed the current commit in the binary and give another digest.
# crane authenticates through gcloud's credential helper, set in a gitignored
# .docker here, so no token passes through this script and ~/.docker is not
# touched.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
. ./env.local
tag=${1:?usage: push.sh <tag>}
tarball="images/measure-$tag.tar"
record="results/image-$tag.txt"
[ -s "$tarball" ] || { echo "no $tarball" >&2; exit 1; }
want=$(awk '{for (i = 1; i <= NF; i++) if ($i ~ /^sha256:/) {print $i; exit}}' "$record")
[ -n "$want" ] || { echo "no digest in $record" >&2; exit 1; }
[ "$(.gobin/crane digest --tarball "$tarball")" = "$want" ] || { echo "$tarball is not the image $record recorded" >&2; exit 1; }
export DOCKER_CONFIG="$PWD/.docker"
mkdir -p "$DOCKER_CONFIG"
command -v docker-credential-gcloud >/dev/null || { echo "docker-credential-gcloud is not on PATH" >&2; exit 1; }
printf '{"credHelpers": {"%s": "gcloud"}}\n' "${RLGOV_IMAGE_REPO%%/*}" > "$DOCKER_CONFIG/config.json"
ref="$RLGOV_IMAGE_REPO/measure:$tag"
.gobin/crane push "$tarball" "$ref" >/dev/null
got=$(.gobin/crane digest "$ref")
if [ "$got" != "$want" ]; then
  echo "PUSH-MISMATCH $ref is $got, recorded $want" >&2
  exit 1
fi
echo "pushed $ref $got (as recorded)"
