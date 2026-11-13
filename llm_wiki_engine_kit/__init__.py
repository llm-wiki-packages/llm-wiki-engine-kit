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
import fcntl
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
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
BACKOFF_SECONDS, BACKOFF_CEILING = 0.5, 30.0
# This many inner processes in a row that end with no file answered stop the daemon, so its service reads failed.
CRASH_LIMIT = 5
# A convert's time, unless the request's options carry `timeout_seconds`; past it the outer process kills the inner.
TIMEOUT_SECONDS = 1800.0
# The scratch's own folder for what the inner process tells the outer one: a marker per running convert, whose
# mtime is its start and whose text its timeout, and `answered` once a file is answered.
# `outer.lock` is held by the outer process while the inner one runs: a free lock means the outer process is gone.
KIT_DIR, ANSWERED, CONVERTING, OUTER_LOCK = ".kit", "answered", "converting-", "outer.lock"
_POLL_SECONDS = 0.25
# A stop the inner process has not finished in this long ends with a SIGKILL to its group.
STOP_ESCALATE_SECONDS = 10.0
_MARKER_CAP = 32
UNREADABLE = {"reason": "unreadable", "traits": []}
_BODY = re.compile(r"body(\.[A-Za-z0-9]+)*")
_READ = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_WRITE = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC

Converter = Callable[[Path, Path, dict], "dict | None"]
Factory = Callable[[dict], Converter]


@dataclass(frozen=True)
class Timings:
    """The daemon's waits, in seconds. The defaults are the service's; a test passes sub-second ones."""

    scan: float = SCAN_SECONDS
    poll: float = _POLL_SECONDS
    backoff: float = BACKOFF_SECONDS
    backoff_ceiling: float = BACKOFF_CEILING
    stop_escalate: float = STOP_ESCALATE_SECONDS


# The inner process's running converts, `name -> (folder, attempts before this one)`, so a stop can take its counts
# back; and where it leaves its markers, None outside an inner process.
_RUNNING: dict[str, tuple[Path, int]] = {}
# Reentrant: a second signal can land in `_stop` while it holds the lock.
_LOCK = threading.RLock()
_KIT: list[Path] = []
_DEFAULT_TIMEOUT = [TIMEOUT_SECONDS]


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
            _started(folder, tried, request)
            try:
                got = converter(folder / request["body"], folder / TMP_OUTPUT, request)
                result = None if got is None else _shape(got)
            except Exception as exc:
                _say(f"{folder.name}: the converter raised {type(exc).__name__}: {exc}")
                result, failed = UNREADABLE, True
            finally:
                _ended(folder)
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


def _timeout(request: dict) -> float:
    given = request.get("options", {}).get("timeout_seconds")
    if isinstance(given, (int, float)) and not isinstance(given, bool) and given > 0:
        return float(given)
    return _DEFAULT_TIMEOUT[0]


def _started(folder: Path, tried: int, request: dict) -> None:
    with _LOCK:
        _RUNNING[folder.name] = (folder, tried)
    for kit in _KIT:
        (kit / f"{CONVERTING}{folder.name}").write_text(str(_timeout(request)), encoding="ascii")


def _ended(folder: Path) -> None:
    with _LOCK:
        _RUNNING.pop(folder.name, None)
    for kit in _KIT:
        (kit / f"{CONVERTING}{folder.name}").unlink(missing_ok=True)


def _take_back(folder: Path, tried: int) -> None:
    """Set a running folder's count back to what it held before this attempt: a stop is not the file's fault."""
    fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if tried:
            _write_new(fd, TMP_ATTEMPTS, str(tried).encode("ascii"))
        else:
            os.unlink(TMP_ATTEMPTS, dir_fd=fd)
    finally:
        os.close(fd)


def _stop(_signum, _frame) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    with _LOCK:
        running = list(_RUNNING.values())
    for folder, tried in running:
        try:
            _take_back(folder, tried)
        except OSError:
            pass
    os._exit(0)


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


def _tried(folder: Path) -> int:
    try:
        fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return 0
    try:
        return _attempts(fd)
    except OSError:
        return 0
    finally:
        os.close(fd)


def _outer_gone(kit: Path) -> bool:
    """True once the outer process no longer holds its lock. Its parent is the jail's runtime, which outlives it, and
    a killed outer process its own parent has not reaped still answers a signal 0."""
    try:
        fd = os.open(kit / OUTER_LOCK, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return True
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(fd)
    return True


def _inner(engine: str, factory: Factory, threads: int, store: Path, scratch: Path, timings: Timings) -> int:
    """Answer requests until a converter raises, `RECYCLE_AFTER` files are done, or the outer process is gone.

    A folder an earlier inner process already tried runs alone, so a crash charges its attempt to that folder only.
    """
    parent = os.getppid()
    kit = scratch / KIT_DIR
    kit.mkdir(exist_ok=True)
    _KIT[:] = [kit]
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    converter = factory({"engine": engine, "store": store, "scratch": scratch, "threads": threads})
    done, failed = 0, False
    running: dict[Future, str] = {}
    alone: str | None = None
    skipped: set[str] = set()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        while True:
            if os.getppid() != parent or _outer_gone(kit):
                # The whole group, not a return: leaving the pool waits for a convert that may never end, and a
                # converter's own children would outlive an exit.
                os.killpg(os.getpgrp(), signal.SIGKILL)
            stopping = failed or done >= RECYCLE_AFTER
            if stopping and not running:
                return RECYCLE_EXIT
            if not stopping and alone is None:
                room = min(threads, RECYCLE_AFTER - done) - len(running)
                waiting = pending(store, set(running.values()) | skipped)
                suspects = [folder for folder in waiting if _tried(folder)]
                if suspects:
                    if not running:
                        alone = suspects[0].name
                        running[pool.submit(_work, suspects[0], converter)] = alone
                else:
                    for folder in waiting[: max(room, 0)]:
                        running[pool.submit(_work, folder, converter)] = folder.name
            if not running:
                skipped.clear()
                time.sleep(timings.scan)
                continue
            finished, _ = wait(running, timeout=timings.scan, return_when=FIRST_COMPLETED)
            for future in finished:
                name = running.pop(future)
                if name == alone:
                    alone = None
                outcome = future.result()
                if outcome is None:
                    # A request that does not read waits for the next idle scan, not the next free worker.
                    skipped.add(name)
                    continue
                done += 1
                failed = failed or outcome
                (kit / ANSWERED).touch()


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


def _import_root(factory: Factory) -> Path | None:
    """The directory the factory's top-level package imports from. An editable engine's code lies outside the
    interpreter's prefix, and the inner process imports it before the factory runs. Never the home or above it."""
    name = getattr(factory, "__module__", None) or ""
    if name == "__main__":
        # Under `python -m pkg`, the module is `__main__`; its spec still names the package it ran from.
        spec = getattr(sys.modules.get(name), "__spec__", None)
        name = getattr(spec, "name", None) or name
    for module in (sys.modules.get(name.split(".")[0]), sys.modules.get(name)):
        file = getattr(module, "__file__", None)
        if file:
            path = Path(file).resolve()
            root = path.parent.parent if path.name == "__init__.py" else path.parent
            home = Path.home().resolve()
            return None if root == home or root in home.parents else root
    return None


def _outer(store: Path, scratch: Path, reads: Iterable[str | os.PathLike], unjailed: bool, timings: Timings) -> int:
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
    backoff, crashes = timings.backoff, 0
    child: list[subprocess.Popen] = []
    stopping: list[int] = []

    def stop(signum, _frame) -> None:
        stopping.append(signum)
        if child and child[-1].poll() is None:
            _signal_group(child[-1], signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        _wipe(scratch)
        lock = _hold(scratch / KIT_DIR)
        try:
            # Its own process group, so a kill reaches the jail's runtime and the inner process both.
            child.append(subprocess.Popen(argv, cwd=cwd, env=env, start_new_session=True))
            code = _watch(child[-1], scratch / KIT_DIR, stopping, timings)
        finally:
            os.close(lock)
        child.clear()
        if stopping:
            return 0
        if code == RECYCLE_EXIT:
            backoff, crashes = timings.backoff, 0
            continue
        crashes = 0 if (scratch / KIT_DIR / ANSWERED).exists() else crashes + 1
        if crashes >= CRASH_LIMIT:
            _say(f"{CRASH_LIMIT} inner processes in a row ended with no file answered; stopping")
            return 1
        _say(f"the inner process exited {code}; starting another in {backoff:g}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, timings.backoff_ceiling)
    return 0


def _hold(kit: Path) -> int:
    kit.mkdir()
    fd = os.open(kit / OUTER_LOCK, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def _signal_group(child: subprocess.Popen, signum: int) -> None:
    try:
        os.killpg(child.pid, signum)
    except ProcessLookupError:
        pass


def _marker(dir_fd: int, name: str) -> tuple[float, float] | None:
    """A marker's start and timeout, read as the jail wrote it: never through a link, a FIFO or a large file."""
    try:
        fd = os.open(name, _READ, dir_fd=dir_fd)
    except OSError:
        return None
    try:
        held = os.fstat(fd)
        if not stat.S_ISREG(held.st_mode) or held.st_size > _MARKER_CAP:
            return None
        return held.st_mtime, float(os.read(fd, _MARKER_CAP).decode("ascii"))
    except (OSError, ValueError, UnicodeDecodeError):
        return None
    finally:
        os.close(fd)


def _overdue(kit: Path, now: float) -> str | None:
    """The first running convert past its timeout, by the inner process's markers."""
    try:
        dir_fd = os.open(kit, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return None
    try:
        names = [one for one in os.listdir(dir_fd) if one.startswith(CONVERTING)]
        for name in names:
            found = _marker(dir_fd, name)
            if found and now - found[0] > found[1]:
                return name[len(CONVERTING) :]
    except OSError:
        return None
    finally:
        os.close(dir_fd)
    return None


def _watch(child: subprocess.Popen, kit: Path, stopping: list[int], timings: Timings) -> int:
    """Wait for the inner process; kill it where a convert runs past its timeout, which then counts as an attempt."""
    stop_seen: float | None = None
    while True:
        try:
            return child.wait(timings.poll)
        except subprocess.TimeoutExpired:
            pass
        if stopping:
            stop_seen = stop_seen or time.monotonic()
            if time.monotonic() - stop_seen > timings.stop_escalate:
                _say(f"the inner process did not stop in {timings.stop_escalate:g}s; killing it")
                _signal_group(child, signal.SIGKILL)
                return child.wait()
            continue
        late = _overdue(kit, time.time())
        if late:
            _say(f"{late}: the convert ran past its timeout; killing the inner process")
            _signal_group(child, signal.SIGKILL)
            return child.wait()


def serve(
    engine: str,
    factory: Factory,
    threads: int = 1,
    reads: Iterable[str | os.PathLike] = (),
    env: Mapping[str, str] | None = None,
    argv: list[str] | None = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
    timings: Timings = Timings(),
) -> int:
    """Run the daemon for `engine` and answer its exit code; `argv` carries `--store`, `--cache`, `--unjailed`.

    `timeout_seconds` is a convert's time for a request whose options carry no `timeout_seconds` of their own.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--store", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--unjailed", action="store_true")
    args, _rest = parser.parse_known_args(sys.argv[1:] if argv is None else argv)
    store = Path(args.store).absolute()
    scratch = Path(args.cache).absolute() / "tmp"
    if os.environ.pop(INNER_ENV, "") == "1":
        _apply_env(scratch, env or {})
        _DEFAULT_TIMEOUT[0] = float(timeout_seconds)
        return _inner(engine, factory, max(1, threads), store, scratch, timings)
    store.mkdir(parents=True, exist_ok=True)
    root = _import_root(factory)
    return _outer(store, scratch, [*reads, *([root] if root else [])], args.unjailed, timings)
