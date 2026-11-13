"""Turn a convert function into a daemon that serves one engine's folder of the request store.

The machine-config contract owns the store (`<state>/engine-requests/<engine>/<key>/`). An engine passes `serve` a
factory. The factory builds the converter, and it runs only in the inner process, inside the jail:

    def make_converter(daemon: dict) -> Callable[[Path, Path, dict], dict | None]: ...
    serve(engine="docling", factory=make_converter, threads=1, reads=[...], env={...})

`daemon` holds `engine`, `store`, `scratch` and `threads`. The converter gets the body's path, the path to write the
markdown to, and the request. It answers None for a converted file, or `{"reason": <reason>, "traits": [...]}`.

Started as the outer process, `serve` re-execs itself under `nono` and imports nothing of the engine. The inner process
knows itself by `INNER_ENV`, never by its argv, so an engine that hands `serve` its own `argv` still stops at one level.
Every name in a request folder is the host's or this daemon's, so a read refuses a link and a write goes to a `.tmp-`
name first.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Callable, Iterable, Mapping

REQUEST, RESULT, OUTPUT = "request.json", "result.json", "output.md"
TMP_OUTPUT, TMP_RESULT, TMP_ATTEMPTS = ".tmp-output.md", ".tmp-result.json", ".tmp-attempts"
# A folder whose convert killed the inner process this many times answers `unreadable` at the next start.
MAX_ATTEMPTS = 2
REQUEST_CAP = 64 * 1024
SCAN_SECONDS = 2.0
RECYCLE_AFTER = 50
# The inner process's exit when it asks the outer one for a fresh inner process.
RECYCLE_EXIT = 75
NONO_ENV = "LLM_WIKI_NONO"
INNER_ENV = "LLM_WIKI_ENGINE_KIT_INNER"
# The outer process's wait before it starts an inner process again after one that crashed: doubled per crash.
BACKOFF_SECONDS, BACKOFF_CEILING = 1.0, 30.0
UNREADABLE = {"reason": "unreadable", "traits": []}
_BODY = re.compile(r"body(\.[A-Za-z0-9]+)*")
_READ = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_WRITE = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC

Converter = Callable[[Path, Path, dict], "dict | None"]
Factory = Callable[[dict], Converter]


def _say(message: str) -> None:
    print(f"engine-kit: {message}", file=sys.stderr, flush=True)


# --- one request ----------------------------------------------------------------


def read_request(folder: Path) -> dict | None:
    """The folder's `request.json`, or None where it is a link, not a regular file, over the cap or off its shape."""
    try:
        fd = os.open(folder / REQUEST, _READ)
    except OSError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return None
        data = os.read(fd, REQUEST_CAP + 1)
    except OSError:
        return None
    finally:
        os.close(fd)
    if len(data) > REQUEST_CAP:
        return None
    try:
        request = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(request, dict) or not isinstance(request.get("body"), str):
        return None
    if not _BODY.fullmatch(request["body"]) or not isinstance(request.get("options", {}), dict):
        return None
    return request


def _write_new(fd: int, name: str, data: bytes) -> None:
    """`name` in the folder `fd`, made fresh: a leftover from a dead daemon, a link included, is removed first."""
    try:
        os.unlink(name, dir_fd=fd)
    except FileNotFoundError:
        pass
    out = os.open(name, _WRITE, 0o644, dir_fd=fd)
    try:
        os.write(out, data)
        os.fsync(out)
    finally:
        os.close(out)


def _sync(fd: int, name: str) -> None:
    held = os.open(name, _READ, dir_fd=fd)
    try:
        os.fsync(held)
    finally:
        os.close(held)


def _publish(fd: int, src: str, dst: str) -> None:
    """Rename `src` to `dst`, then sync the folder, so a power loss never leaves `result.json` beside a lost output."""
    os.replace(src, dst, src_dir_fd=fd, dst_dir_fd=fd)
    os.fsync(fd)


def _attempts(fd: int) -> int:
    try:
        held = os.open(TMP_ATTEMPTS, _READ, dir_fd=fd)
    except FileNotFoundError:
        return 0
    try:
        raw = os.read(held, 32)
    finally:
        os.close(held)
    try:
        return int(raw.decode("ascii").strip())
    except (UnicodeDecodeError, ValueError):
        return MAX_ATTEMPTS


def _regular(fd: int, name: str) -> bool:
    try:
        return stat.S_ISREG(os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode)
    except FileNotFoundError:
        return False


def _shape(answer) -> dict:
    """A converter's failure answer as `result.json` holds it; anything else is the converter's bug."""
    if not isinstance(answer, dict) or not isinstance(answer.get("reason"), str):
        raise TypeError(f"a converter answers None or a dict with a reason, not {answer!r}")
    traits = answer.get("traits", [])
    if not isinstance(traits, (list, tuple)) or not all(isinstance(one, str) for one in traits):
        raise TypeError(f"traits must be a list of words, not {traits!r}")
    return {"reason": answer["reason"], "traits": list(traits)}


def answer(folder: Path, request: dict, converter: Converter) -> bool:
    """Convert one request and answer it, `output.md` first and `result.json` last, each through a `.tmp-` name.

    True where the converter raised: the inner process then recycles. Each attempt is counted in `.tmp-attempts`
    before the convert, so a convert that kills the process is tried `MAX_ATTEMPTS` times, then answered `unreadable`.
    The body and `request.json` are never written.
    """
    fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        try:
            os.unlink(TMP_OUTPUT, dir_fd=fd)
        except FileNotFoundError:
            pass
        failed = False
        tried = _attempts(fd)
        if tried >= MAX_ATTEMPTS:
            _say(f"{folder.name}: {tried} attempts ended the process; answering unreadable")
            result = UNREADABLE
        elif not _regular(fd, request["body"]):
            result = UNREADABLE
        else:
            _write_new(fd, TMP_ATTEMPTS, str(tried + 1).encode("ascii"))
            try:
                got = converter(folder / request["body"], folder / TMP_OUTPUT, request)
                result = None if got is None else _shape(got)
            except Exception as exc:
                _say(f"{folder.name}: the converter raised {type(exc).__name__}: {exc}")
                result, failed = UNREADABLE, True
        if result is None:
            if _regular(fd, TMP_OUTPUT):
                _sync(fd, TMP_OUTPUT)
                _publish(fd, TMP_OUTPUT, OUTPUT)
                result = {"reason": None, "traits": []}
            else:
                result = UNREADABLE
        _write_new(fd, TMP_RESULT, (json.dumps(result) + "\n").encode("utf-8"))
        _publish(fd, TMP_RESULT, RESULT)
        try:
            os.unlink(TMP_ATTEMPTS, dir_fd=fd)
        except FileNotFoundError:
            pass
    finally:
        os.close(fd)
    _say(f"{folder.name}: answered reason={result['reason']}")
    return failed


# --- finding work ---------------------------------------------------------------


def _absent(folder: Path, name: str) -> bool:
    try:
        os.lstat(folder / name)
    except FileNotFoundError:
        return True
    return False


def pending(store: Path, busy: set[str]) -> list[Path]:
    """Every folder in `store` that has a `request.json`, no `result.json` and is not in `busy`, by name."""
    try:
        entries = sorted(os.scandir(store), key=lambda entry: entry.name)
    except OSError:
        return []
    found = []
    for entry in entries:
        if entry.name.startswith(".") or entry.name in busy or not entry.is_dir(follow_symlinks=False):
            continue
        folder = Path(entry.path)
        if not _absent(folder, REQUEST) and _absent(folder, RESULT):
            found.append(folder)
    return found


# --- the inner process ----------------------------------------------------------


def _work(folder: Path, converter: Converter) -> bool | None:
    """One folder's answer; None where its request does not read, or the folder went away mid-answer (a host prune)."""
    request = read_request(folder)
    if request is None:
        return None
    try:
        return answer(folder, request, converter)
    except OSError as exc:
        _say(f"{folder.name}: skipped, the folder did not take the answer: {exc}")
        return None


def _inner(engine: str, factory: Factory, threads: int, store: Path, scratch: Path) -> int:
    """Answer requests until a converter raises, `RECYCLE_AFTER` files are done, or the outer process is gone."""
    parent = os.getppid()
    converter = factory({"engine": engine, "store": store, "scratch": scratch, "threads": threads})
    done, failed = 0, False
    running: dict[Future, str] = {}
    skipped: set[str] = set()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        while True:
            if os.getppid() != parent:
                return 0
            stopping = failed or done >= RECYCLE_AFTER
            if stopping and not running:
                return RECYCLE_EXIT
            if not stopping:
                room = min(threads, RECYCLE_AFTER - done) - len(running)
                for folder in pending(store, set(running.values()) | skipped)[: max(room, 0)]:
                    running[pool.submit(_work, folder, converter)] = folder.name
            if not running:
                skipped.clear()
                time.sleep(SCAN_SECONDS)
                continue
            finished, _ = wait(running, timeout=SCAN_SECONDS, return_when=FIRST_COMPLETED)
            for future in finished:
                name = running.pop(future)
                outcome = future.result()
                if outcome is None:
                    # A request that does not read waits for the next idle scan, not the next free worker.
                    skipped.add(name)
                    continue
                done += 1
                failed = failed or outcome


def _apply_env(scratch: Path, env: Mapping[str, str]) -> None:
    """The inner process's environment, set before the factory imports anything of the engine."""
    os.environ.update(
        {
            "HOME": str(scratch),
            "TMPDIR": str(scratch),
            "HF_HUB_OFFLINE": "1",
            "DO_NOT_TRACK": "1",
            "PYTHONNOUSERSITE": "1",
            **env,
        }
    )


# --- the outer process ----------------------------------------------------------


def _nono() -> str | None:
    named = os.environ.get(NONO_ENV, "").strip()
    if named:
        return named
    return shutil.which("nono")


def _wipe(scratch: Path) -> None:
    if scratch.is_symlink():
        scratch.unlink()
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)


def _inner_argv() -> list[str]:
    """This same program again. `-P` keeps the cwd and the script's directory off the inner `sys.path`, and `-s` the
    user's site directory, so nothing a converter wrote can be imported by the next inner process."""
    return [sys.executable, "-P", "-s", *sys.orig_argv[1:]]


def _jail_argv(nono: str, store: Path, scratch: Path, reads: list[Path]) -> list[str]:
    argv = [nono, "run", "-s", "--block-net"]
    for path in reads:
        argv += ["-r", str(path)]
    argv += ["-a", str(store), "-a", str(scratch), "--"]
    pythonpath = _carried_pythonpath(reads)
    if pythonpath:
        # nono drops PYTHONPATH from the jail's environment, so a checkout's kit would not import inside.
        argv += [sys.executable, "-P", "-s", "-c", _CARRY_PYTHONPATH, pythonpath]
    return argv + _inner_argv()


_CARRY_PYTHONPATH = "import os, sys; os.environ['PYTHONPATH'] = sys.argv[1]; os.execv(sys.argv[2], sys.argv[2:])"


def _carried_pythonpath(grants: Iterable[Path]) -> str:
    """The `PYTHONPATH` entries that are absolute and inside a tree the jail already reads; no entry adds a grant."""
    roots = [Path(one).resolve() for one in grants]
    kept = []
    for one in os.environ.get("PYTHONPATH", "").split(os.pathsep):
        if not one or not os.path.isabs(one):
            continue
        path = Path(one).resolve()
        if any(path == root or root in path.parents for root in roots):
            kept.append(str(path))
    return os.pathsep.join(kept)


def _reads(extra: Iterable[str | os.PathLike]) -> list[Path]:
    """The interpreter's own trees, the kit's, and the engine's: each once, each that exists."""
    found: list[Path] = []
    for raw in (sys.prefix, sys.base_prefix, Path(__file__).resolve().parents[1], *extra):
        path = Path(raw).resolve()
        if path.exists() and path not in found:
            found.append(path)
    return found


def _outer(store: Path, scratch: Path, reads: Iterable[str | os.PathLike], unjailed: bool) -> int:
    """Start inner processes one after another until a signal; a crashed one is started again after a backoff.

    The scratch is wiped before each start, and the inner process runs in the interpreter's prefix, a tree the jail
    reads and cannot write.
    """
    if unjailed:
        argv = _inner_argv()
    else:
        nono = _nono()
        if not nono:
            _say(f"no nono on PATH, and {NONO_ENV} names none: the daemon will not run unjailed")
            return 2
        argv = _jail_argv(nono, store, scratch, _reads(reads))
    cwd = Path(sys.prefix).resolve()
    env = {**os.environ, INNER_ENV: "1"}
    backoff = BACKOFF_SECONDS
    child: list[subprocess.Popen] = []
    stopping: list[int] = []

    def stop(signum, _frame) -> None:
        stopping.append(signum)
        if child and child[-1].poll() is None:
            child[-1].send_signal(signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        _wipe(scratch)
        child.append(subprocess.Popen(argv, cwd=cwd, env=env))
        code = child[-1].wait()
        child.clear()
        if stopping:
            return 0
        if code == RECYCLE_EXIT:
            backoff = BACKOFF_SECONDS
            continue
        _say(f"the inner process exited {code}; starting another in {backoff:.0f}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_CEILING)
    return 0


def serve(
    engine: str,
    factory: Factory,
    threads: int = 1,
    reads: Iterable[str | os.PathLike] = (),
    env: Mapping[str, str] | None = None,
    argv: list[str] | None = None,
) -> int:
    """Run the daemon for `engine` and answer its exit code; `argv` carries `--store`, `--cache`, `--unjailed`."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--store", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--unjailed", action="store_true")
    args, _rest = parser.parse_known_args(sys.argv[1:] if argv is None else argv)
    store = Path(args.store).absolute()
    scratch = Path(args.cache).absolute() / "tmp"
    if os.environ.pop(INNER_ENV, "") == "1":
        _apply_env(scratch, env or {})
        return _inner(engine, factory, max(1, threads), store, scratch)
    store.mkdir(parents=True, exist_ok=True)
    return _outer(store, scratch, reads, args.unjailed)
