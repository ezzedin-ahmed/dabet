#!/usr/bin/env python3
"""Fail if any direct kgo.NewClient call omits the shared transport security.

A direct client bypasses pkg/kafkax and therefore carries no TLS or SASL. That
works against a plaintext local broker and fails only against a managed one,
which is the worst possible place to discover it. Each call site must splice in
SecurityConfig.Options() -- conventionally a `secOpts` slice.

The call's extent is found by balancing parentheses from `kgo.NewClient(`, so a
long option list cannot slip past a fixed window.
"""
import pathlib, sys

def call_text(lines, i):
    """Return the source of the kgo.NewClient call starting on line i."""
    depth, out = 0, []
    started = False
    for line in lines[i:]:
        out.append(line)
        for ch in line:
            if ch == "(":
                depth += 1
                started = True
            elif ch == ")":
                depth -= 1
        if started and depth <= 0:
            break
    return "\n".join(out)

bad = []
for f in sorted(pathlib.Path("services").rglob("*.go")):
    if f.name.endswith("_test.go"):
        continue
    lines = f.read_text().splitlines()
    for i, line in enumerate(lines):
        if "kgo.NewClient(" in line and "secOpts" not in call_text(lines, i):
            bad.append(f"{f}:{i+1}")

if bad:
    print("unsecured kgo.NewClient (no security options spliced in):")
    for b in bad:
        print("  " + b)
    sys.exit(1)
print(f"kafka clients all carry transport security")
