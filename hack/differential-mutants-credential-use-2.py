"""The model's three-way differential test under the revert-check mutants it
must agree on (docs/credential-use-model-reduction.md §7 and §10.5): the
stateless unreduced search, the stateful unreduced one and the stateful
reduced one, on every small scenario at {0,1} and {1,0}, with the sampled
merge audit on every prune.

Under each mutant the three searches must agree, scenario by scenario, on
their terminal abstract states and on their violation messages, and between
them find at least one violation. The baseline (no mutant) runs first and
must pass.

Each source file is restored after its mutant, and the end checks the tree
is as it started.

Usage:
  python3 hack/differential-mutants-credential-use-2.py > hack/differential-mutants-credential-use-2.txt

Each part can also run alone, so that the parts run side by side, each in its
own clone: name them, "baseline" or a mutant's number from 1:
  python3 hack/differential-mutants-credential-use-2.py baseline
  python3 hack/differential-mutants-credential-use-2.py 2

DIFF_TIMEOUT_S is go test's timeout (default a day), DIFF_PARALLEL its
-parallel (default GOMAXPROCS), and DIFF_LOG_DIR, if set, keeps each part's
full go test output there.
"""
import hashlib, os, re, signal, subprocess, sys, time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["acquire.go", "release.go", "verified.go", "patch.go"]

# The four of §7, as hack/revert-check-credential-use-2.py writes them (R3's
# array test is in patch.go, which that check doesn't mutate).
MUTANTS = [
    ("H1's skip without the exact version: a skip needs no delivered version", "verified.go",
     [("\tif v.delivered[h] != hRV {\n", "\tif false {\n")]),
    ("M4's late clock: R2 measures the grace after its reads", "release.go", [
        ("\t\tnow := p.clock.Now()\n\t\tnsDeleting := map[string]bool{}\n", "\t\tnsDeleting := map[string]bool{}\n"),
        ("\t\t\tbreak\n\t\t}\n\n\t\t// Decide, then R3.\n", "\t\t\tbreak\n\t\t}\n\t\tnow := p.clock.Now()\n\n\t\t// Decide, then R3.\n")]),
    ("a deletion pass may skip A2", "acquire.go",
     [("\t\tif !deleting && !added[id] {\n", "\t\tif (deleting || !deleting) && !added[id] {\n")]),
    ("R3 without the array test", "patch.go",
     [("\t\t{Op: \"test\", Path: \"/metadata/finalizers\", Value: finalizers},\n", "")]),
]

TEST = "^TestModelStatefulAgreesWithStateless$"


TIMEOUT = int(os.environ.get("DIFF_TIMEOUT_S", "86400"))
PARALLEL = os.environ.get("DIFF_PARALLEL", "")
LOG_DIR = os.environ.get("DIFF_LOG_DIR", "")


def uptime():
    return subprocess.run(["uptime"], capture_output=True, text=True).stdout.strip()


def go_test(part):
    env = dict(os.environ)
    env.pop("CREDENTIALUSE_MODEL_AUDITSTOP", None)
    args = ["go", "test", "-count=1", "-v", "-timeout", f"{TIMEOUT}s", "-run", TEST]
    if PARALLEL:
        args += ["-parallel", PARALLEL]
    start = time.time()
    print(f"  load at the start: {uptime()}", flush=True)
    r = subprocess.run(args + ["./" + PKG], cwd=ROOT, env=env, capture_output=True, text=True, timeout=TIMEOUT + 300)
    print(f"  load at the end:   {uptime()}", flush=True)
    out = r.stdout + r.stderr
    if LOG_DIR:
        os.makedirs(LOG_DIR, exist_ok=True)
        open(os.path.join(LOG_DIR, f"differential-{part}.log"), "w").write(out)
    return r.returncode, out, time.time() - start


def digest():
    h = hashlib.sha256()
    for f in FILES:
        h.update(open(os.path.join(ROOT, PKG, f), "rb").read())
    return h.hexdigest()


SUMMARY = re.compile(r"stateful_test\.go:\d+: stateless: (\d+) schedules, (\d+) terminal states, (\d+) violations; "
                     r"stateful: (\d+) runs, (\d+) states, (\d+) pruned, (\d+) terminal states, (\d+) violations, (\d+) audits, (\d+) divergences; "
                     r"reduced: (\d+) runs, (\d+) states, (\d+) pruned, (\d+) terminal states, (\d+) violations, (\d+) audits, (\d+) divergences")
CAPACITY = re.compile(r"capacity used \(§10\.2\): at most (\d+) calls between jumps \(K = \d+\), (\d+) jumps chosen or forced, smallest tick (\S+)")


def split(out):
    """go test -v's output, per subtest: its status and its log lines. Logs
    stream under a "=== NAME/CONT/RUN" header naming the test they belong to;
    statuses come as "--- PASS/FAIL: name (secs)"."""
    logs, status, cur = {}, {}, None
    for line in out.splitlines():
        m = re.match(r"^=== (?:RUN|CONT|NAME|PAUSE)\s+(\S+)", line)
        if m:
            cur = m.group(1)
            continue
        m = re.match(r"^\s*--- (PASS|FAIL|SKIP): (\S+) \(", line)
        if m:
            status[m.group(2)] = m.group(1)
            cur = None
            continue
        if cur and line.startswith("    "):
            logs.setdefault(cur, []).append(line.strip())
    return logs, status


def report(out):
    """Per subtest: its result and its three searches' counts; the violation
    messages found; whether every subtest logged its summary."""
    logs, status = split(out)
    lines, msgs, complete = [], set(), True
    for name in sorted(n for n in set(logs) | set(status) if "/" in n):
        body = "\n".join(logs.get(name, []))
        st = status.get(name, "NONE")
        m = SUMMARY.search(body)
        c = CAPACITY.search(body)
        short = name.split("/", 1)[1]
        if m:
            g = list(map(int, m.groups()))
            lines.append(f"  {st} {short}\n"
                         f"       stateless {g[0]} schedules, {g[1]} terminal, {g[2]} violations; "
                         f"stateful {g[3]} runs, {g[4]} states, {g[6]} terminal, {g[7]} violations, {g[8]} audits, {g[9]} divergences; "
                         f"reduced {g[10]} runs, {g[11]} states, {g[13]} terminal, {g[14]} violations, {g[15]} audits, {g[16]} divergences"
                         + (f"\n       capacity: {c.group(1)} calls between jumps, {c.group(2)} jumps, smallest tick {c.group(3)}" if c else ""))
        else:
            complete = False
            lines.append(f"  {st} {short} (no summary: it stopped early)")
        if st != "PASS":
            for l in logs.get(name, []):
                if re.search(r"differ|didn't finish|harness failed|merge audit|panic|only in", l):
                    lines.append("       " + l[:300])
        msgs |= set(re.findall(r"violation: (.*)", body))
    if re.search(r"^panic:", out, re.M):
        complete = False
        lines.append("  PANIC " + re.search(r"^panic: .*", out, re.M).group(0)[:300])
    return lines, sorted(msgs), complete


start = digest()
restore = None


def on_signal(signum, _frame):
    if restore:
        open(restore[0], "w").write(restore[1])
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

parts = sys.argv[1:] or ["baseline"] + [str(i + 1) for i in range(len(MUTANTS))]
print(f"commit {subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], cwd=ROOT, capture_output=True, text=True).stdout.strip()}, parts {' '.join(parts)}", flush=True)

if "baseline" in parts:
    code, out, secs = go_test("baseline")
    lines, msgs, complete = report(out)
    print(f"BASELINE  {'passed' if code == 0 and complete else 'FAILED'} in {secs/60:.1f} min")
    print("\n".join(lines))
    for m in msgs:
        print(f"       message: {m[:240]}")
    if code != 0 or not complete:
        print("BASELINE FAILED: the run is invalid\n" + out[-3000:])
        sys.exit(1)
    print(flush=True)

results = {}
for i, (name, f, edits) in enumerate(MUTANTS):
    if str(i + 1) not in parts:
        continue
    full = os.path.join(ROOT, PKG, f)
    src = open(full).read()
    bad = [old for old, _ in edits if src.count(old) != 1]
    if bad:
        print(f"NOT-APPLIED {name}", flush=True)
        results["NOT-APPLIED"] = results.get("NOT-APPLIED", 0) + 1
        continue
    mutated = src
    for old, new in edits:
        mutated = mutated.replace(old, new)
    restore = (full, src)
    try:
        open(full, "w").write(mutated)
        code, out, secs = go_test(str(i + 1))
    finally:
        open(full, "w").write(src)
        restore = None
    lines, msgs, complete = report(out)
    if "build failed" in out:
        state = "BUILD"
    elif code != 0 or not complete:
        state = "DISAGREE"
    elif not msgs:
        state = "NO-VIOLATION"
    else:
        state = "AGREE"
    results[state] = results.get(state, 0) + 1
    print(f"{state:12} {name} ({secs/60:.1f} min)")
    print("\n".join(lines))
    for m in msgs:
        print(f"       message: {m[:240]}")
    print(flush=True)

if results:
    print(" ".join(f"{k}={v}" for k, v in sorted(results.items())) + f" of {sum(results.values())} run here")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
