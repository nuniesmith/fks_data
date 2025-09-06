#!/usr/bin/env python
from __future__ import annotations
import os, sys, xml.etree.ElementTree as ET, argparse
from pathlib import Path

def find_file(explicit: str | None):
    for name in ([explicit] if explicit else []) + ["coverage-combined.xml", "coverage.xml"]:
        if name and Path(name).is_file():
            return Path(name)
    return None

def parse_rate(p: Path) -> float:
    root = ET.parse(p).getroot()
    r = root.get('line-rate')
    if r is None:
        raise RuntimeError('line-rate missing')
    return float(r) * 100

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--percent-file', default=None)
    args = parser.parse_args()
    xml = find_file(os.getenv('COVERAGE_FILE'))
    if not xml:
        print('COVERAGE: no xml (skip)')
        return 0
    try:
        pct = parse_rate(xml)
    except Exception as e:
        print(f'COVERAGE: parse error: {e}')
        return 0
    t_raw = os.getenv('COVERAGE_FAIL_UNDER')
    try:
        thresh = float(t_raw) if t_raw else None
    except ValueError:
        print(f'COVERAGE: bad threshold {t_raw!r}')
        thresh = None
    if args.percent_file:
        try:
            with open(args.percent_file, 'w') as f:
                f.write(f'{pct:.2f}\n')
        except OSError as e:
            print(f'COVERAGE: write error {e}')
    summary_path = os.getenv('GITHUB_STEP_SUMMARY')
    if summary_path:
        try:  # pragma: no cover
            with open(summary_path, 'a') as f:
                f.write(f"\n### Coverage\n\nObserved: {pct:.2f}%\n")
        except OSError:
            pass
    hard_fail = os.getenv('COVERAGE_HARD_FAIL') == '1'
    if thresh is None:
        print(f'COVERAGE: observed {pct:.2f}% (soft)')
        return 0
    if pct + 1e-9 < thresh:
        if hard_fail:
            print(f'COVERAGE: {pct:.2f}% below {thresh:.2f}% (FAIL)')
            return 1
        print(f'COVERAGE: {pct:.2f}% below {thresh:.2f}% (soft fail)')
        return 0
    print(f'COVERAGE: {pct:.2f}% meets {thresh:.2f}%')
    return 0

if __name__ == '__main__':
    sys.exit(main())
