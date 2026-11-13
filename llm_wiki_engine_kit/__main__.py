"""`python -m llm_wiki_engine_kit serve --store <dir> --cache <dir> --factory <module:function> [--unjailed]`.

It runs any converter as a daemon, for an engine author's own trials and for the kit's tests. `--read` grants the jail
one more read-only tree, such as the directory that holds the factory's module.
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

from . import serve


def _lazy(spec: str):
    """The factory `module:function`, imported only when the inner process calls it."""
    module, _, name = spec.partition(":")
    if not module or not name:
        raise SystemExit(f"--factory {spec!r} is not module:function")

    def factory(daemon: dict):
        return getattr(importlib.import_module(module), name)(daemon)

    return factory


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(prog="python -m llm_wiki_engine_kit")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("serve", help="answer one engine's requests")
    run.add_argument("--store", required=True, help="<state>/engine-requests/<engine>")
    run.add_argument("--cache", required=True, help="<cache>/engines/<engine>; the scratch is its tmp/")
    run.add_argument("--factory", required=True, help="module:function that builds the converter")
    run.add_argument("--threads", type=int, default=1)
    run.add_argument("--read", action="append", default=[], help="one more read-only tree for the jail")
    run.add_argument("--unjailed", action="store_true", help="skip the jail")
    run.add_argument("--inner", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    return serve(Path(args.store).name, _lazy(args.factory), threads=args.threads, reads=args.read, argv=argv)


if __name__ == "__main__":
    sys.exit(main())
