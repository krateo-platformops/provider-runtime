#!/bin/bash
# Local smoke of the gated queue with the example controller, no cluster: a
# local kube-apiserver and etcd from envtest, the example with
# --saturation-governor, and 12 Greetings. It checks that every Greeting becomes
# Ready, that none was written ReconcileDeferred, and what the governor logged.
# Everything it starts runs on a kubeconfig of its own in hack/.smoke, and stops
# at the end. Writes its report to stdout.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 1
S=hack/.smoke
rm -rf "$S"
mkdir -p "$S"
KC="$PWD/$S/kubeconfig"
export KUBEBUILDER_ASSETS
KUBEBUILDER_ASSETS=$("$HOME/go/bin/setup-envtest" use -i -p path 1.35.0) || { echo "no envtest binaries"; exit 1; }
k() { kubectl --kubeconfig "$KC" --context envtest "$@"; }

go build -tags smoke -o "$S/envtest-up" ./hack/envtest-up || exit 1
go build -o "$S/greeting-controller" ./examples/greeting-controller || exit 1
"$S/envtest-up" --out "$KC" > "$S/envtest.log" 2>&1 &
envpid=$!
for _ in $(seq 1 120); do grep -q ENVTEST-READY "$S/envtest.log" && break; sleep 0.5; done
grep -q ENVTEST-READY "$S/envtest.log" || { echo "envtest did not start"; cat "$S/envtest.log"; kill $envpid; exit 1; }

KUBECONFIG="$KC" "$S/greeting-controller" --saturation-governor > "$S/controller.log" 2>&1 &
ctrlpid=$!
python3 -c "
for i in range(12):
    print('---\napiVersion: examples.krateo.io/v1\nkind: Greeting\nmetadata: {name: g%02d, namespace: default}\nspec: {message: hello}' % i)" | k apply -f - > /dev/null
sleep 30

echo "# greetings: name ready synced-reason"
k get greetings -o jsonpath='{range .items[*]}{.metadata.name} {.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Synced")].reason}{"\n"}{end}'
echo "# ready: $(k get greetings -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c True) of 12"
echo "# ReconcileDeferred: $(k get greetings -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Synced")].reason}{"\n"}{end}' | grep -c ReconcileDeferred)"
echo "# controller still running: $(kill -0 $ctrlpid 2>/dev/null && echo yes || echo no)"
echo "# governor lines, and every ERROR or WARN line, from the controller's log:"
echo "# error and warn lines: $(grep -cE '"level":"(ERROR|WARN)"' "$S/controller.log")"
grep -E 'Saturation|saturat|local capacity|Another Governor|More reconcilers|"level":"(ERROR|WARN)"' "$S/controller.log" | cut -c1-300 | head -20

kill $ctrlpid 2>/dev/null
wait $ctrlpid 2>/dev/null
kill $envpid 2>/dev/null
wait $envpid 2>/dev/null
rm -rf "$S"
echo "# stopped"
