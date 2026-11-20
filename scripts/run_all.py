"""Rebuild the main table from scratch and rerun every measurement in order,
writing results/stepN.txt. Steps 3-6 build on the main table left by the
step before, so they only make sense in this order.

    python scripts/run_all.py            # all steps
    python scripts/run_all.py 4 5        # from step 4 to step 5 (the table must be in the right state)
"""

import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STEPS = {
    1: ["load_all.py"],
    2: ["concurrency.py"],
    3: ["evolution.py", "query_overhead.py"],
    4: ["maintenance.py"],
    5: ["wap.py"],
    6: ["analytics.py", "ducklake.py"],
}
NOISE = ("Warning", "warn(", "self.delete", "retrying")


def main(argv):
    first, last = (int(argv[0]), int(argv[-1])) if argv else (1, max(STEPS))
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for step in range(first, last + 1):
        lines = []
        for script in STEPS[step]:
            path = os.path.join(ROOT, "scripts", script)
            if not os.path.exists(path):
                continue
            t = time.perf_counter()
            print(f"== step {step}: {script}", flush=True)
            p = subprocess.run([sys.executable, path], cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8")
            out = [x for x in p.stdout.splitlines() if not any(n in x for n in NOISE)]
            print("\n".join(out))
            if p.returncode != 0:
                print(p.stderr[-3000:])
                return p.returncode
            lines += out
            print(f"   ({time.perf_counter() - t:.0f} s)", flush=True)
        with open(os.path.join(ROOT, "results", f"step{step}.txt"), "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
