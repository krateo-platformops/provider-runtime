"""Revert check for docs/design-local-slot-before-pop.md: remove each behaviour of
the gated queue in turn and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills: each stops the package's remaining tests,
so it says nothing about them. The baseline must pass, or the run is invalid.
Each file is restored after its mutant, and the end checks the tree is as it
started.

Usage: python3 hack/revert-check-local-slot.py > hack/revert-check-local-slot.txt
"""
import hashlib, os, re, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
Q = "pkg/ratelimit/queue.go"
GOV = "pkg/ratelimit/governor.go"
REC = "pkg/reconciler/ratelimit.go"
OPT = "pkg/controller/options.go"
PKGS = ["./pkg/ratelimit/", "./pkg/controller/", "./pkg/reconciler/", "./pkg/telemetry/"]

MUTANTS = [
    # The gated queue.
    ("a slot is granted with no item to hand out", Q, "q.idle == 0 || q.free-q.granted <= 0", "q.idle == 0 || q.free-q.granted < 0"),
    ("a slot is granted with no idle worker", Q, "q.idle == 0 || q.free-q.granted <= 0", "q.free-q.granted <= 0"),
    ("a shut queue is granted slots", Q, "if q.shutdown || q.idle == 0", "if q.idle == 0"),
    ("Done gives no slot back", Q, "\tq.mu.Unlock()\n\tq.gov.releaseLocal()\n}\n\n// tryGrant", "\tq.mu.Unlock()\n}\n\n// tryGrant"),
    ("Done for an unknown key gives a slot back", Q, "\tif _, ok := q.processing[key]; !ok {\n\t\tq.mu.Unlock()\n\t\treturn\n\t}\n\tq.inner.Done(key)", "\tq.inner.Done(key)"),
    ("Done calls inner outside the lock (the review's first race)", Q,
     "func (q *gatedQueue[T]) Done(key T) {\n\tq.mu.Lock()\n\tif _, ok := q.processing[key]; !ok {\n\t\tq.mu.Unlock()\n\t\treturn\n\t}\n\tq.inner.Done(key)\n",
     "func (q *gatedQueue[T]) Done(key T) {\n\tq.inner.Done(key)\n\tq.mu.Lock()\n\tif _, ok := q.processing[key]; !ok {\n\t\tq.mu.Unlock()\n\t\treturn\n\t}\n"),
    ("the pop calls inner outside the lock (the review's second race)", Q,
     "\tkey, priority, shutdown := q.inner.GetWithPriority()\n\tif shutdown {\n\t\treportBrokenBooks",
     "\tq.mu.Unlock()\n\tkey, priority, shutdown := q.inner.GetWithPriority()\n\tq.mu.Lock()\n\tif shutdown {\n\t\treportBrokenBooks"),
    ("a pop the record disagrees with goes unreported", Q,
     "\t\treportBrokenBooks(q.gov.log, \"the inner queue handed out a key the record didn't hold as free\",\n\t\t\t\"key\", fmt.Sprint(key), \"ready\", isReady, \"processing\", isProcessing)\n",
     "\t\t_ = fmt.Sprint(key)\n"),
    ("a key re-added while processing never comes back", Q, "\tdelete(q.processing, key)\n\tif _, ok := q.ready[key]; ok {\n\t\tq.free++\n\t}\n", "\tdelete(q.processing, key)\n"),
    ("a re-add while processing counts as available", Q, "\tq.ready[key] = struct{}{}\n\tif _, busy := q.processing[key]; busy {\n\t\treturn false\n\t}\n", "\tq.ready[key] = struct{}{}\n"),
    ("an idle worker doesn't ask for a slot", Q, "\tq.idle++\n\tq.mu.Unlock()\n\tq.gov.dispatch()\n", "\tq.idle++\n\tq.mu.Unlock()\n"),
    ("an add doesn't ask for a slot", Q, "\tq.mu.Unlock()\n\tif more {\n\t\tq.gov.dispatch()\n\t}\n}\n\n// addReadyLocked", "\tq.mu.Unlock()\n\t_ = more\n}\n\n// addReadyLocked"),
    ("a delayed key's priority is lost when added now", Q, "\t\tpriority = max(priority, w.priority)\n\t\tw.timer.Stop()", "\t\tw.timer.Stop()"),
    ("a delayed add of a ready key delays it", Q, "\tif _, ok := q.ready[key]; ok {\n\t\tq.inner.AddWithOpts(priorityqueue.AddOpts{Priority: &priority}, key)\n\t\treturn\n\t}\n\tat := ", "\tat := "),
    ("a delay is never shortened", Q, "\tif at.Before(w.at) {\n\t\tw.timer.Stop()", "\tif false {\n\t\tw.timer.Stop()"),
    ("a delayed key's higher priority is dropped", Q, "\tw.priority = max(w.priority, priority)\n\tif at.Before", "\tif at.Before"),
    ("keys due together come due one by one", Q, "\t\tif wi.at.After(now) && wi != w {", "\t\tif wi != w {"),
    ("due leaves the moved keys' timers running", Q, "\t\tif wi != w {\n\t\t\twi.timer.Stop()\n\t\t}\n", ""),
    ("a timer callback waits for the queue's lock under the clock's", Q, "func() { go q.due(key, w, gen) }", "func() { q.due(key, w, gen) }"),
    ("a late timer runs a rescheduled entry", Q, "q.waiting[key] != w || w.gen != gen", "q.waiting[key] != w"),
    ("rate-limited adds ignore the limiter", Q, "\t\tif o.RateLimited {\n\t\t\tif rl := q.rateLimiter.When(key); after == 0 || rl < after {\n\t\t\t\tafter = rl\n\t\t\t}\n\t\t}\n", ""),
    ("shutdown doesn't wake waiting workers", Q, "\tq.inner.ShutDown()\n\tq.cond.Broadcast()\n", "\tq.inner.ShutDown()\n"),
    ("shutdown keeps a granted slot", Q, "\t\tif release {\n\t\t\tq.gov.releaseLocal()\n\t\t}\n\t\treturn zero, 0, true", "\t\t_ = release\n\t\treturn zero, 0, true"),
    # The governor.
    ("grants go to the same queue first", GOV, "\t\t\t\tg.nextQueue = (idx + 1) % n\n", ""),
    ("grants exceed capacity", GOV, "for g.localInFlight < g.local.Load().capacity {", "for g.localInFlight <= g.local.Load().capacity {"),
    ("a released slot isn't granted on", GOV, "\t\tg.localInFlight--\n\t}\n\tg.dispatchLocked()\n}", "\t\tg.localInFlight--\n\t}\n}"),
    ("a release with none in flight is clamped in silence", GOV, "\t\treportBrokenBooks(g.log, \"a local slot was released with none in flight\")\n", ""),
    ("a capacity rise isn't granted", GOV, "\t}\n\tg.dispatchLocked()\n\tg.mu.Unlock()\n\tif next.capacity < cur.capacity {", "\t}\n\tg.mu.Unlock()\n\tif next.capacity < cur.capacity {"),
    ("no warning for a second sampler", GOV, "\tif runningSamplers.Add(1) > 1 {", "\tif runningSamplers.Add(1) > 1000 {"),
    ("no error for an ungated reconciler", GOV, "\tif reconcilers > gated {", "\tif reconcilers > gated+1000 {"),
    ("calls take a local slot again", GOV, "\tfor _, st := range states {\n\t\tst.inFlight++\n\t}\n\treturn nil\n}", "\tfor _, st := range states {\n\t\tst.inFlight++\n\t}\n\tg.localInFlight++\n\treturn nil\n}"),
    # The reconciler and the controller options.
    ("a keyless call goes to the governor (expected equivalent: AcquireAll gates nothing without keys)", REC,
     "\tif !slices.ContainsFunc(keys, func(key string) bool { return key != \"\" }) {", "\tif false && !slices.ContainsFunc(keys, func(key string) bool { return key != \"\" }) {"),
    ("AcquireAll without keys asks the governor (expected equivalent: acquire with no keys takes nothing)", GOV,
     "\tif len(keys) == 0 {\n\t\treturn func() {}, nil\n\t}\n\td = g.acquire(keys)", "\td = g.acquire(keys)"),
    ("ForControllerRuntime doesn't gate", OPT, "\tgated := g != nil && g.SaturationEnabled()", "\tgated := false && g != nil"),
    ("ForControllerRuntime doesn't register the controller", OPT, "\tif gated {\n\t\tg.RegisterGatedController()\n\t}\n", ""),
    ("the recorder isn't applied to a gated queue", OPT, "\t\tif o.QueueWaitRecorder == nil {\n\t\t\treturn queue\n\t\t}\n", "\t\tif true {\n\t\t\treturn queue\n\t\t}\n"),
    ("saturation with UsePriorityQueue=false is accepted", OPT, "\tif rl.Saturation.Enabled && o.UsePriorityQueue != nil && !*o.UsePriorityQueue {", "\tif false {"),
]


def go_test():
    return subprocess.run(["go", "test", "-count=1", "-timeout", "180s", *PKGS], cwd=ROOT, capture_output=True, text=True, timeout=900)


def digest():
    h = hashlib.sha256()
    for f in sorted({m[1] for m in MUTANTS}):
        h.update(open(os.path.join(ROOT, f), "rb").read())
    return h.hexdigest()


def classify(r):
    out = r.stdout + r.stderr
    if r.returncode == 0:
        return "SURVIVED", ""
    if "build failed" in out or "[setup failed]" in out:
        return "BUILD", ""
    if "panic: test timed out" in out:
        m = re.search(r"running tests:\n((?:\t\t.*\n)+)", out)
        return "TIMEOUT", (m.group(1).strip().replace("\n", " ").replace("\t", "") if m else "")[:160]
    if re.search(r"^panic:|\[recovered\]", out, re.M):
        m = re.search(r"panic: .*", out)
        return "PANIC", m.group(0)[:90] if m else ""
    return "KILLED", ",".join(sorted(set(re.findall(r"^\s*--- FAIL: (\S+)", out, re.M))))[:160]


start = digest()
base = go_test()
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print("baseline passed\n")

counts = {}
for name, path, old, new in MUTANTS:
    full = os.path.join(ROOT, path)
    src = open(full).read()
    if src.count(old) != 1:
        print(f"NOT-APPLIED ({src.count(old)} matches)  {name}", flush=True)
        counts["NOT-APPLIED"] = counts.get("NOT-APPLIED", 0) + 1
        continue
    try:
        open(full, "w").write(src.replace(old, new))
        state, detail = classify(go_test())
    finally:
        open(full, "w").write(src)
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {len(MUTANTS)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
