"""Revert check for design §11 items 14 and 15: remove each behaviour in turn and
see whether a test notices.

A mutant counts as killed only if it compiled, the tests ran, and one failed by
assertion. BUILD means it didn't compile, which is not a kill. PANIC means a test
panicked, which is not counted as a kill either: a panic stops the package's
remaining tests, so it says nothing about them. The baseline must pass, or the
whole run is invalid. Each file is restored from memory after its mutant, and
the check at the end confirms the tree is back as it started.
"""
import hashlib, os, re, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SAT = "pkg/ratelimit/saturation.go"
GOV = "pkg/ratelimit/governor.go"
PKGS = ["./pkg/ratelimit/", "./pkg/telemetry/"]

MUTANTS = [
    # Item 14: the limit.
    ("Start doesn't apply the limit", GOV, "\tg.sat.applyGoMemoryLimit()\n", ""),
    ("limit set with the memory input off", SAT,
     "func (s *saturation) applyGoMemoryLimit() {\n\tif !s.opts.ExperimentalMemory {\n\t\treturn\n\t}\n",
     "func (s *saturation) applyGoMemoryLimit() {\n"),
    ("GOMEMLIMIT ignored", SAT, 'if v := s.getenv("GOMEMLIMIT"); v != "" {', 'if v := s.getenv("GOMEMLIMIT"); v == "\\x00" {'),
    ("the program's own limit ignored", SAT, "if cur := s.setMemoryLimit(-1); cur != math.MaxInt64 {", "if cur := s.setMemoryLimit(-1); cur == -2 {"),
    ("option ignored", SAT, "if v := s.opts.ExperimentalGoMemoryLimit; v > 0 {", "if v := s.opts.ExperimentalGoMemoryLimit; v < 0 {"),
    ("limit ratio 0.9", SAT, "DefaultGoMemoryLimitRatio = 0.80", "DefaultGoMemoryLimitRatio = 0.90"),
    ("limit is memory.max itself", SAT, "v := int64(float64(limit) * DefaultGoMemoryLimitRatio)", "v := int64(limit)"),
    ("limit from memory.max not logged", SAT,
     's.log.Info("Go memory limit set from memory.max", "bytes", v, "memoryMax", limit, "ratio", DefaultGoMemoryLimitRatio)\n', ""),
    ("limit set though memory.max isn't real", SAT,
     '\tif why != "" {\n\t\ts.log.Info("Go memory limit not set", "reason", why)\n\t\treturn\n\t}\n', '\t_ = why\n'),
    ("negative limit accepted", SAT, "case o.ExperimentalGoMemoryLimit < 0:", "case o.ExperimentalGoMemoryLimit < -1<<62:"),
    # Item 15: the headroom.
    ("headroom ratio default 0.10", SAT, "DefaultGoMemoryHeadroomRatio = 0.15", "DefaultGoMemoryHeadroomRatio = 0.10"),
    ("headroom ratio default not applied", SAT,
     "\tif o.ExperimentalGoMemoryHeadroomRatio == 0 {\n\t\to.ExperimentalGoMemoryHeadroomRatio = DefaultGoMemoryHeadroomRatio\n\t}\n", ""),
    ("ratio exclusive", SAT, "r.enabled && r.percent <= s.opts.ExperimentalGoMemoryHeadroomRatio*100", "r.enabled && r.percent < s.opts.ExperimentalGoMemoryHeadroomRatio*100"),
    ("headroom never cuts", SAT,
     'causes = append(causes, "Go memory headroom "+formatPercent(r.percent)+" of limit")', '_ = r'),
    ("headroom has a hard level", SAT,
     'causes = append(causes, "Go memory headroom "+formatPercent(r.percent)+" of limit")',
     'causes = append(causes, "Go memory headroom "+formatPercent(r.percent)+" of limit")\n\t\thard = true'),
    ("cause doesn't name the headroom", SAT, '"Go memory headroom "+formatPercent', '"memory "+formatPercent'),
    ("memory.stat still read with headroom", SAT, "s.reader.read(s.opts.ExperimentalMemory && !headroom.enabled)", "s.reader.read(s.opts.ExperimentalMemory)"),
    ("memory.stat never read", SAT, "s.reader.read(s.opts.ExperimentalMemory && !headroom.enabled)", "s.reader.read(false)"),
    ("working set not replaced", SAT,
     '\tif headroom.enabled {\n\t\tsmp.workingSet = disabled("the Go memory headroom replaces it while a Go memory limit is set")\n\t}\n', ""),
    ("headroom read with the input off", SAT, "\tif s.opts.ExperimentalMemory {\n\t\theadroom = goHeadroom(s.goMemory())\n\t}\n", "\theadroom = goHeadroom(s.goMemory())\n"),
    ("an input nobody asked for is logged", SAT, 'if r.enabled || r.disabledBecause == "" || s.loggedDisabled[in] {', "if r.enabled || s.loggedDisabled[in] {"),
    ("used counts garbage", SAT, "return goMemoryStats{limit: limit, used: live + notHeap}", "_ = notHeap\n\t_ = live\n\treturn goMemoryStats{limit: limit, used: total - released}"),
    ("used leaves out the non-heap memory", SAT, "return goMemoryStats{limit: limit, used: live + notHeap}", "_ = notHeap\n\treturn goMemoryStats{limit: limit, used: live}"),
    ("unknown metric reaches Uint64", SAT, "if r.samples[i].Value.Kind() != metrics.KindUint64 {", "if false {"),
    ("read allocates", SAT, "var v [7]uint64\n", "v := append(make([]uint64, 0, 7), make([]uint64, 7)...)\n\tkeep = v\n"),
    ("no limit reads as a limit", SAT, "case m.limit >= math.MaxInt64:", "case m.limit == 1:"),
    ("unreported limit reads as a limit", SAT, "case m.limit == 0:", "case m.limit == 1:"),
    ("past the limit underflows", SAT, "case m.used >= m.limit:", "case m.limit == 2:"),
    ("ratio above 1 accepted", SAT, "o.ExperimentalGoMemoryHeadroomRatio < 0 || o.ExperimentalGoMemoryHeadroomRatio > 1", "o.ExperimentalGoMemoryHeadroomRatio < 0"),
    ("ratio below 0 accepted", SAT, "o.ExperimentalGoMemoryHeadroomRatio < 0 || o.ExperimentalGoMemoryHeadroomRatio > 1", "o.ExperimentalGoMemoryHeadroomRatio > 1"),
]


def go_test():
    return subprocess.run(["go", "test", "-count=1", *PKGS], cwd=ROOT, capture_output=True, text=True, timeout=300)


def digest():
    h = hashlib.sha256()
    for f in (SAT, GOV):
        h.update(open(os.path.join(ROOT, f), "rb").read())
    return h.hexdigest()


def classify(r):
    out = r.stdout + r.stderr
    if r.returncode == 0:
        return "SURVIVED", ""
    if "build failed" in out or "[setup failed]" in out:
        return "BUILD", out.strip().splitlines()[1][:90] if len(out.strip().splitlines()) > 1 else ""
    if re.search(r"^panic:|^--- FAIL.*\n.*panic:|\[recovered\]", out, re.M):
        return "PANIC", (re.search(r"^panic: .*", out, re.M) or re.search(r"panic: .*", out)).group(0)[:90]
    return "KILLED", ",".join(sorted(set(re.findall(r"^--- FAIL: (\S+)", out, re.M))))[:110]


start = digest()
base = go_test()
if base.returncode != 0:
    print("BASELINE FAILED: the run is invalid\n" + base.stdout[-2000:])
    sys.exit(1)
print("baseline passed\n")

counts = {}
for name, path, old, new in MUTANTS:
    full = os.path.join(ROOT, path)
    src = open(full).read()
    if src.count(old) != 1:
        print(f"NOT-APPLIED ({src.count(old)} matches)  {name}")
        counts["NOT-APPLIED"] = counts.get("NOT-APPLIED", 0) + 1
        continue
    mutated = src.replace(old, new)
    if "keep = v" in new:
        mutated += "\nvar keep []uint64\n"
    try:
        open(full, "w").write(mutated)
        state, detail = classify(go_test())
    finally:
        open(full, "w").write(src)
    counts[state] = counts.get(state, 0) + 1
    print(f"{state:9} {name}\n          {detail}", flush=True)

print("\n" + " ".join(f"{k}={v}" for k, v in sorted(counts.items())) + f" of {len(MUTANTS)}")
print("tree restored" if digest() == start else "TREE DIFFERS FROM THE START")
