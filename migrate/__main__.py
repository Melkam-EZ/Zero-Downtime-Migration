"""python -m migrate <preflight|replicate|canary|cutover|rollback|finalize|status|reset|run>"""
import argparse
import json
import sys

from .config import ConfigError, from_env
from .plan import Abort, Migration


def main(argv=None):
    p = argparse.ArgumentParser(prog="migrate")
    p.add_argument("cmd", choices=["preflight", "replicate", "canary", "cutover", "rollback", "finalize", "status", "reset", "run"])
    p.add_argument("--force", action="store_true", help="reset: allow after cutover (destroys the new database's data)")
    args = p.parse_args(argv)
    try:
        cfg = from_env()
    except ConfigError as exc:
        print("config error:", exc, file=sys.stderr)
        return 2
    mig = Migration(cfg)
    try:
        if args.cmd == "preflight":
            mig.preflight()
        elif args.cmd == "replicate":
            mig.replicate()
        elif args.cmd == "canary":
            mig.canary()
        elif args.cmd == "cutover":
            mig.cutover()
        elif args.cmd == "rollback":
            mig.rollback()
        elif args.cmd == "finalize":
            mig.finalize()
        elif args.cmd == "reset":
            mig.reset(force=args.force)
        elif args.cmd == "status":
            print(json.dumps(mig.status(), indent=2, default=str))
        elif args.cmd == "run":
            mig.preflight(); mig.replicate(); mig.canary(); mig.cutover()
            print("\nCutover done. Watch for a while, then: python -m migrate finalize  (or rollback)")
    except Abort as exc:
        print("\nSTOPPED:", exc, file=sys.stderr)
        return 1
    finally:
        mig.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
