"""Revert check for the version of pkg/credentialuse's calls on the holder kind
(§6.2 of docs/design-credential-use-finalizer.md): break each call, and the
rule's limit to the holder kind, in turn and see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD (didn't compile), PANIC (a test panicked) and TIMEOUT (go
test's own deadline) are not kills. The baseline must pass, or the run is
invalid. Each source file is restored after its mutant, and the end checks the
tree is as it started.

Each mutant runs the tests named for it in KILLERS, and the baseline runs
them all. They are unit tests.

Usage:
  python3 hack/revert-check-credential-use-holder-version.py > hack/revert-check-credential-use-holder-version.txt

REVERT_TIMEOUT_S sets go test's timeout per run (default 900). REVERT_ONLY, a
regular expression, runs only the mutants whose names it matches.
"""
import hashlib, os, re, signal, subprocess, sys, time

sys.stdout.reconfigure(line_buffering=True)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PKG = "pkg/credentialuse/"
FILES = ["server.go", "shared.go", "tracker.go", "disown.go", "index.go", "uncached.go"]

# (name, file, [(old, new), ...]): every old must occur exactly once in file.
MUTANTS = [
    ("the holder kind goes through the preferred version", "server.go",
     [("\tif gk == s.holder.GroupKind() && s.holder.Version != \"\" {", "\tif false {")]),
    ("every kind goes through the holder's version", "server.go",
     [("\tif gk == s.holder.GroupKind() && s.holder.Version != \"\" {", "\tif s.holder.Version != \"\" {")]),
    ("a server knows no holder kind", "shared.go",
     [("clientServer{md: s.c.Metadata, mapper: s.c.Mapper, holder: holder}", "clientServer{md: s.c.Metadata, mapper: s.c.Mapper}")]),
    ("a tracker's own calls go through the preferred version", "tracker.go",
     [("s.newServer(o.Holder)", "s.newServer(gk.WithVersion(\"\"))")]),
    ("Disown lists the holders at the preferred version", "disown.go",
     [("s.listHolders(ctx, t)", "s.liveList(ctx, t.gk, \"\", \"\")")]),
    ("Disown strips the records at the preferred version", "disown.go",
     [("srv := s.newServer(t.o.Holder)", "srv := s.newServer(t.gk.WithVersion(\"\"))")]),
    ("the index lists the holders at the preferred version", "index.go",
     [("kinds, store, t.proto.srv)", "kinds, store, s.newServer(t.gk.WithVersion(\"\")))")]),
    ("an empty holder version isn't a declared one", "shared.go",
     [("\to.Holder = m.GroupVersionKind\n", "\t_ = m\n")]),
    ("the holder informer reads the mapper's version", "tracker.go",
     [("\tm, err := s.c.Mapper.RESTMapping(gk, o.Holder.Version)\n", "\tm, err := s.c.Mapper.RESTMapping(gk)\n")]),
]

KILLERS = {
    "the holder kind goes through the preferred version": "TestA1UsesTheHoldersDeclaredVersion|TestR2ListsTheHoldersDeclaredVersion|TestHolderCallsVersions",
    "every kind goes through the holder's version": "TestHolderCallsVersions",
    "a server knows no holder kind": "TestA1UsesTheHoldersDeclaredVersion|TestR2ListsTheHoldersDeclaredVersion|TestDisownUsesTheHoldersDeclaredVersion",
    "a tracker's own calls go through the preferred version": "TestA1UsesTheHoldersDeclaredVersion|TestR2ListsTheHoldersDeclaredVersion",
    "Disown lists the holders at the preferred version": "TestDisownUsesTheHoldersDeclaredVersion",
    "Disown strips the records at the preferred version": "TestDisownUsesTheHoldersDeclaredVersion",
    "the index lists the holders at the preferred version": "TestHolderCallsVersions",
    "an empty holder version isn't a declared one": "TestAnEmptyHolderVersionIsTheOneTrackMaps",
    "the holder informer reads the mapper's version": "TestHolderInformerUsesTheDeclaredVersion",
}

ALL = sorted({t for ts in KILLERS.values() for t in ts.split("|")})


def anchored(tests):
    return "^(" + tests + ")$"


class Run:
    def __init__(self, returncode, stdout, stderr):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


TIMEOUT = int(os.environ.get("REVERT_TIMEOUT_S", "900"))


def go_test(tests):
    r = subprocess.run(["go", "test", "-count=1", "-timeout", f"{TIMEOUT}s", "-run", anchored(tests), "./" + PKG],
                       cwd=ROOT, env=dict(os.environ), capture_output=True, text=True, timeout=TIMEOUT + 300)
    return Run(r.returncode, r.stdout, r.stderr)


def digest():
    h = hashlib.sha256()
    for f in FILES:
        h.update(open(os.path.join(ROOT, PKG, f), "rb").read())
    return h.hexdigest()


def killers(out):
    failed = set(re.findall(r"^\s*--- FAIL: (\S+)", out, re.M))
    return sorted(t for t in failed if not any(o.startswith(t + "/") for o in failed))


def classify(r):
    out = r.stdout + r.stderr
    if r.returncode == 0:
        return "SURVIVED", ""
    if "build failed" in out or "[setup failed]" in out:
        return "BUILD", ""
    if "panic: test timed out" in out:
        return "TIMEOUT", ""
    if re.search(r"^panic:|\[recovered\]", out, re.M):
        m = re.search(r"panic: .*", out)
        return "PANIC", m.group(0)[:90] if m else ""
    return "KILLED", "killed in: " + ", ".join(killers(out))


def utc():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def commit():
    return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()


start = digest()
missing = [name for name, _, _ in MUTANTS if name not in KILLERS]
if missing:
    sys.exit(f"no killing tests named for {missing}")
base = go_test("|".join(ALL))
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + (base.stdout + base.stderr)[-3000:])
    sys.exit(1)
print(f"baseline passed, commit {commit()}\n")

restore = None


def on_signal(signum, _frame):
    if restore:
        open(restore[0], "w").write(restore[1])
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)

ONLY = os.environ.get("REVERT_ONLY")
if ONLY:
    print(f"only the mutants matching {ONLY!r}\n")
counts = {}
run = 0
for num, (name, f, edits) in enumerate(MUTANTS, 1):
    if ONLY and not re.search(ONLY, name):
        continue
    run += 1
    full = os.path.join(ROOT, PKG, f)
    src = open(full).read()
    bad = [old for old, _ in edits if src.count(old) != 1]
    if bad:
        print(f"NOT-APPLIED ({[src.count(b) for b in bad]} matches)  {name}", flush=True)
        counts["NOT-APPLIED"] = counts.get("NOT-APPLIED", 0) + 1
        continue
    mutated = src
    for old, new in edits:
        mutated = mutated.replace(old, new)
    restore = (full, src)
    began = utc()
    try:
        open(full, "w").write(mutated)
        state, detail = classify(go_test(KILLERS[name]))
    finally:
        open(full, "w").write(src)
        restore = None
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}\n          ran: {KILLERS[name]}\n          mutant {num}: {began} to {utc()}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {run}" + (f" (of {len(MUTANTS)})" if ONLY else ""))
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
