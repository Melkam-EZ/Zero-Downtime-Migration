"""python -m probe --url http://127.0.0.1:8080 --verify-dsn postgresql://... [--duration 120] [--stop-file .state/stop]"""
import argparse
import json
import pathlib
import sys
import time

from .probe import Prober, format_report


def main(argv=None):
    p = argparse.ArgumentParser(prog="probe")
    p.add_argument("--url", required=True)
    p.add_argument("--rps", type=float, default=40)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--duration", type=float, default=0, help="seconds; 0 = run until the stop file appears or Ctrl-C")
    p.add_argument("--stop-file", default=".state/stop")
    p.add_argument("--verify-dsn", help="authoritative database to check acknowledged writes against")
    p.add_argument("--report", default="probe-report.json")
    p.add_argument("--events", default=".state/events.jsonl")
    args = p.parse_args(argv)
    stop = pathlib.Path(args.stop_file)
    stop.parent.mkdir(parents=True, exist_ok=True)
    stop.unlink(missing_ok=True)
    pr = Prober(args.url, rps=args.rps, workers=args.workers)
    pr.start()
    print(f"probe running at ~{args.rps:g} requests/s; create {stop} or press Ctrl-C to finish", flush=True)
    end = time.time() + args.duration if args.duration else None
    try:
        while not stop.exists() and (end is None or time.time() < end):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    pr.stop()
    integrity = pr.verify(args.verify_dsn) if args.verify_dsn else None
    ev = []
    ep = pathlib.Path(args.events)
    if ep.exists():
        ev = [json.loads(line) for line in ep.read_text().splitlines() if line.strip()]
    rep = pr.report(integrity, ev)
    pathlib.Path(args.report).write_text(json.dumps(rep, indent=2))
    print(format_report(rep))
    return 0 if rep["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
