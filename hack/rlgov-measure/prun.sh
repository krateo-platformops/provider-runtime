#!/bin/bash
# Usage: prun.sh <name> <mode: ramp|burst|cpu|curve|overhead> <seconds> <image-tag> KEY=VALUE...
#
# One run in its own namespace (named after the run), one at a time. Every
# kubectl goes through k(), so it uses this directory's kubeconfig and the
# context from env.local, never the shared ~/.kube/config. env.local is written
# by the cluster setup and is gitignored.
#
# ramp:  Greetings added 2 every 5s, 16 MiB held 1s per Observe, poll 500ms, 256Mi.
# burst: 64 Greetings exist before the controller starts, 16 MiB held 300ms, 256Mi.
# cpu:   64 Greetings, 250m; each Observe spends 20ms of CPU, measured on its
#        thread, then waits 400ms as a call to an external API would. The CPU
#        block re-run's workload, approved 2026-11-08 (remeasure-plan.txt). The
#        first block's LOAD_CPU_MS=3000 counted sha256 rounds, about 0.8s of CPU
#        a call on GKE, and with no wait one call in flight saturated 250m.
# curve: 64 Greetings, 250m, no governor (NO_GOVERNOR=true), GOMAXPROCS 2; each
#        Observe spends 3000ms of CPU measured on its thread, then waits 28.5s:
#        the long drain's plant at a fixed MaxConcurrentReconciles, given as
#        WORKERS=n (results/curve-settings.txt). No liveness probe.
#
# Each restart's cause is recorded as it happens, because lastState keeps only
# the latest. The node's load comes from the run's STAT lines.
#
# A run the driver couldn't watch is discarded, not read as a result: the log
# keeps its data and gets a "# DISCARDED" line with the reason. Causes: macOS
# slept or dark-woke during the run (pmset), the wall clock jumped between two
# polls, the cluster stopped answering, or a Greeting batch failed to apply. It
# exits 0 for those, so a block moves on and the run still counts as run.
#
# A failed setup step exits 2 at once, without retrying: nothing was measured,
# and whatever broke it will break the next run too, so a block must stop there.
cd "$(dirname "$0")" || exit 1
# shellcheck disable=SC1091
. ./env.local
KC="$PWD/kubeconfig"
export USE_GKE_GCLOUD_AUTH_PLUGIN=True
k() { kubectl --kubeconfig "$KC" --context "$RLGOV_CONTEXT" "$@"; }
R=results
mkdir -p "$R"
name=$1; mode=$2; secs=$3; tag=$4; shift 4
ns=$name
discard="$R/$name.discard"
: > "$discard"
note() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1" >> "$discard"; }
# setup_failed <why>: nothing was measured, so record it, tidy up and stop.
setup_failed() {
  echo "# run=$name mode=$mode image=$tag seconds=$secs started=$started" > "$R/$name.log"
  echo "# DISCARDED setup failed: $1" >> "$R/$name.log"
  rm -f "$discard" "$R/$name.stream" "$R/$name.causes"
  bash ./cleanup-run.sh "$ns" || echo "CLEANUP-FAILED $ns"
  echo "SETUP-FAILED $name: $1"
  exit 2
}
case $mode in
  ramp)  cpu=2; mem=256Mi; load="WORKERS=16 LOAD_ALLOC_MIB=16 LOAD_HOLD_MS=1000 POLL_MS=500" ;;
  burst) cpu=2; mem=256Mi; load="WORKERS=16 LOAD_ALLOC_MIB=16 LOAD_HOLD_MS=300 POLL_MS=1000" ;;
  cpu)   cpu=250m; mem=512Mi; load="WORKERS=16 LOAD_CPU_MS=20 LOAD_HOLD_MS=400 POLL_MS=1000" ;;
  curve) cpu=250m; mem=512Mi; load="LOAD_CPU_MS=3000 LOAD_HOLD_MS=28500 POLL_MS=1000 NO_GOVERNOR=true GOMAXPROCS=2" ;;
  overhead) cpu=1; mem=128Mi; load="SAMPLER_OVERHEAD=true" ;;
esac
started=$(date +%Y-%m-%dT%H:%M:%S)
window_start=$(date '+%Y-%m-%d %H:%M:%S')
k create namespace "$ns" >/dev/null || setup_failed "create namespace"
k -n "$ns" create serviceaccount measure >/dev/null || setup_failed "create serviceaccount"
k create clusterrolebinding "rlgov-$ns" --clusterrole=cluster-admin --serviceaccount="$ns:measure" >/dev/null || setup_failed "create clusterrolebinding"
greetings() { # first last
  python3 -c "
for j in range($1, $2 + 1):
    print('---\napiVersion: examples.krateo.io/v1\nkind: Greeting\nmetadata: {name: g%02d, namespace: $ns}\nspec: {message: hello}' % j)" | k apply -f - >/dev/null
}
if [ "$mode" = burst ] || [ "$mode" = cpu ] || [ "$mode" = curve ]; then
  greetings 0 63 || setup_failed "apply the backlog"
fi
envs=""
for kv in $load WATCH_NAMESPACE=$ns "$@"; do
  key=${kv%%=*}; v=${kv#*=}
  envs+="            - {name: $key, value: \"$v\"}"$'\n'
done
probe='          livenessProbe:
            httpGet: {path: /healthz, port: 8081}
            periodSeconds: 2
            timeoutSeconds: 1
            failureThreshold: 3'
# RLGOV_PROBE=incident-provider is the probe that provider ships: its chart sets
# no period, timeout or failure, so Kubernetes' defaults are spelled out here. The
# default above is harsher than anything a consumer ships.
if [ "${RLGOV_PROBE:-}" = incident-provider ]; then
  probe='          livenessProbe:
            httpGet: {path: /healthz, port: 8081}
            periodSeconds: 10
            timeoutSeconds: 1
            failureThreshold: 3'
fi
# Overhead mode serves no health endpoint, so the probe would kill it; the curve
# measures the plant, and a probe killed by throttling would cut runs short.
[ "$mode" = overhead ] && probe=''
[ "$mode" = curve ] && probe=''
cat <<EOF | k apply -f - >/dev/null
apiVersion: apps/v1
kind: Deployment
metadata: {name: measure, namespace: $ns}
spec:
  replicas: 1
  selector: {matchLabels: {app: measure}}
  template:
    metadata: {labels: {app: measure}}
    spec:
      serviceAccountName: measure
      containers:
        - name: measure
          image: $RLGOV_IMAGE_REPO/measure:$tag
          imagePullPolicy: IfNotPresent
          env:
$envs
          resources:
            requests: {cpu: "$cpu", memory: "$mem"}
            limits: {cpu: "$cpu", memory: "$mem"}
$probe
EOF
[ "${PIPESTATUS[1]}" = 0 ] || setup_failed "apply the deployment"
# A crash-looping controller can time this out. That is a result, not a failed
# setup, so it is only recorded. No pod at all is a failed setup.
rollout=ok
k -n "$ns" rollout status deploy/measure --timeout=180s >/dev/null 2>&1 || rollout=timed-out
pod=$(k -n "$ns" get pod -l app=measure -o jsonpath='{.items[0].metadata.name}')
[ -n "$pod" ] || setup_failed "no pod"
: > "$R/$name.stream"
: > "$R/$name.causes"
( while true; do k -n "$ns" logs -f "$pod" 2>/dev/null | grep --line-buffered -E '^(STAT|OVERHEAD|LOG)' >> "$R/$name.stream"; sleep 0.5; done ) &
streamer=$!
# One line per termination: restartCount, reason, exit code, finished time. A
# restart can't be missed at 1s: the back-off is 10s or more.
# It is also the heartbeat: a jump in the wall clock between two polls means the
# driver stopped, and repeated failures mean it lost the cluster.
( last=$(date +%s); fails=0
  while true; do
    if k --request-timeout=10s -n "$ns" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount} {.status.containerStatuses[0].lastState.terminated.reason} {.status.containerStatuses[0].lastState.terminated.exitCode} {.status.containerStatuses[0].lastState.terminated.finishedAt}{"\n"}' 2>/dev/null >> "$R/$name.causes"; then
      fails=0
    else
      fails=$((fails+1))
      [ "$fails" = 5 ] && note "the cluster did not answer 5 polls in a row"
    fi
    now=$(date +%s)
    [ $((now-last)) -gt 30 ] && note "the driver stalled for $((now-last))s between two polls"
    last=$now
    sleep 1
  done ) &
watcher=$!
ramp=""
if [ "$mode" = ramp ]; then
  ( sleep 15; for i in $(seq 0 2 62); do greetings "$i" $((i+1)) || note "the Greeting batch from $i failed to apply"; sleep 5; done ) &
  ramp=$!
fi
sleep "$secs"
kill $streamer $watcher $ramp 2>/dev/null; pkill -P $streamer 2>/dev/null
# Timestamps in pmset's log are local, in this same format.
window_end=$(date '+%Y-%m-%d %H:%M:%S')
slept=$(pmset -g log | awk -v a="$window_start" -v b="$window_end" '/Entering Sleep|DarkWake from/ { t=$1" "$2; if (t>=a && t<=b) print t }' | tr '\n' ',')
[ -n "$slept" ] && note "macOS slept or dark-woke during the run: $slept"
{
  echo "# run=$name mode=$mode image=$tag cpu=$cpu mem=$mem env=$* seconds=$secs started=$started ended=$(date +%Y-%m-%dT%H:%M:%S)"
  sed 's/^/# DISCARDED /' "$discard"
  echo "# rollout=$rollout"
  echo "# probe=$(k -n "$ns" get deploy measure -o jsonpath='{.spec.template.spec.containers[0].livenessProbe}')"
  echo "# driver_load_end=$(uptime)"
  awk '!seen[$0]++' "$R/$name.stream"
  echo "# restarts=$(k -n "$ns" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
  echo "# lastTerminated=$(k -n "$ns" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}')"
  echo "# terminations:"
  awk 'NF>=2 && !seen[$0]++ {print "# term " $0}' "$R/$name.causes"
  echo "# events:"
  k -n "$ns" get events --field-selector involvedObject.name="$pod" -o custom-columns=REASON:.reason,COUNT:.count,MSG:.message --no-headers 2>/dev/null | grep -v -E 'Scheduled|Pulled|Created|Started' || true
} > "$R/$name.log"
state=""
[ -s "$discard" ] && state=" DISCARDED"
rm -f "$R/$name.stream" "$R/$name.causes" "$discard"
bash ./cleanup-run.sh "$ns" || echo "CLEANUP-FAILED $ns"
echo "done $name:$state $(grep '# restarts' "$R/$name.log") $(grep '# lastTerminated' "$R/$name.log")"
