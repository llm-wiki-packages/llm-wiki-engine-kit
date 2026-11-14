"""Turn a convert function into a daemon that serves one engine's request store.

The machine-config contract owns the store (`<store>/<machine>/<key>/`). Each machine that submits writes only its own
segment, and a sync tool (Dropbox, Google Drive, rsync) may carry the store between machines, so a folder can arrive in
any order and half written: the daemon converts a body only once its sha256 is the request's, and names each output
file's sha256 and size in `result.json`'s `outputs`. An engine passes `serve` a factory. The factory builds the converter, and it runs only in the inner process, inside the jail:

    def make_converter(daemon: dict) -> Callable[[Path, Path, dict], dict | None]: ...
    serve(engine="docling", factory=make_converter, threads=1, reads=[...], env={...})

`daemon` holds `engine`, `sections`, `store`, `scratch` and `threads`. The converter gets the body's path, where to
write, and the request. For a text section (`convert`, `transcribe`) it writes the markdown to that path; for a media
section (`narrate`) the path is an empty folder, and it writes the section's files into it (`output.mp3`, and
`output.vtt` if it has one). It answers None for a converted file, or `{"reason": <reason>, "traits": [...]}`. The kit
publishes no file the host would refuse: it answers a reason instead.

Started as the outer process, `serve` re-execs itself under `nono` and imports nothing of the engine. The inner process
knows itself by `INNER_ENV`, never by its argv, so an engine that hands `serve` its own `argv` still stops at one level.
Every name in a request folder is the host's or this daemon's, so a read refuses a link and a write goes to a `.tmp-`
name first. Any other name, a sync tool's debris included, is never read or written.
"""

from __future__ import annotations

import argparse
import codecs
import fcntl
import hashlib
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
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Callable, Iterable, Mapping

REQUEST, RESULT, OUTPUT = "request.json", "result.json", "output.md"
MP3, VTT = "output.mp3", "output.vtt"
TMP_OUTPUT, TMP_RESULT, TMP_ATTEMPTS = ".tmp-output.md", ".tmp-result.json", ".tmp-attempts"
# The folder a media section's converter writes its files into, inside the request folder: a rename then publishes
# each file's own inode.
TMP_MEDIA = ".tmp-out"
# The host's: the files an answer to each section may hold, the first the one a converted answer must list; and each
# file's cap. The host refuses an answer past them, so the kit answers a reason instead of publishing one.
SECTION_OUTPUTS = {"convert": (OUTPUT,), "transcribe": (OUTPUT,), "narrate": (MP3, VTT)}
TEXT_SECTIONS = ("convert", "transcribe")
OUTPUT_CAPS = {OUTPUT: 4 * 16 * 1024 * 1024 + 64 * 1024, MP3: 512 * 1024 * 1024, VTT: 4 * 16 * 1024 * 1024 + 64 * 1024}
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
UNSUPPORTED = {"reason": "unsupported", "traits": []}
_BODY = re.compile(r"body(\.[A-Za-z0-9]+)*")
# A submitting machine's segment and a request's key, as the host names them; any other name is not a request.
_MACHINE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
_KEY = _SHA256 = re.compile(r"[0-9a-f]{64}")
_CHUNK = 1024 * 1024
_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
# The largest body a host copies into the store; a larger one is answered `too_large`, never hashed or converted.
MAX_BODY_BYTES = 2 * 1024 * 1024 * 1024
TOO_LARGE = {"reason": "too_large", "traits": []}
# A body whose ctime is this recent is not hashed yet: a write in the same tick would not show in its stat. The ctime,
# not the mtime: the local kernel sets it, and a sync tool keeps the submitter's mtime, which may be in the future.
RACY_SECONDS = 2.0
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
    racy: float = RACY_SECONDS


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


def folder_fd(folder: Path) -> int:
    """`<store>/<machine>/<key>/` opened level by level from the store, never through a link: a segment or a folder
    swapped for a link after the scan raises `OSError`."""
    top = os.open(folder.parent.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        segment = os.open(folder.parent.name, _DIR, dir_fd=top)
    finally:
        os.close(top)
    try:
        return os.open(folder.name, _DIR, dir_fd=segment)
    finally:
        os.close(segment)


def read_request(folder: Path) -> dict | None:
    """The folder's `request.json`, or None where it is a link, not a regular file, over the cap or off its shape."""
    try:
        held = folder_fd(folder)
    except OSError:
        return None
    try:
        fd = os.open(REQUEST, _READ, dir_fd=held)
    except OSError:
        return None
    finally:
        os.close(held)
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
    if not isinstance(request.get("sha256"), str) or not _SHA256.fullmatch(request["sha256"]):
        return None
    if not isinstance(request.get("section", "convert"), str):
        return None
    return request


def ident(folder: Path) -> str:
    """A request folder's name across every machine's segment: `<machine>.<key>`, a file name of its own."""
    return f"{folder.parent.name}.{folder.name}"


class _Memo(OrderedDict):
    """A dict that holds at most `cap` entries, evicting the least recently used: a store with more waiting folders
    than the cap still keeps the folders it looked at last."""

    def __init__(self, cap: int = 65536):
        super().__init__()
        self.cap = cap

    def get(self, key, default=None):
        if key in self:
            self.move_to_end(key)
            return self[key]
        return default

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)
        while len(self) > self.cap:
            self.popitem(last=False)


class HashBudget:
    """How many bodies one pass of the main loop may hash: as many as it has free workers, so a backlog keeps every
    worker busy, and no more, since a hash runs on the main thread and delays the check that the outer process is
    still there."""

    def __init__(self, hashes: int):
        self.left = hashes

    def take(self) -> bool:
        if self.left <= 0:
            return False
        self.left -= 1
        return True


# A body is seen as `(device, inode, size, mtime_ns, ctime_ns)`. The last sight per folder, so a body is hashed only
# once two scans in a row see it alike; and each digest by its sight, so a body that holds still is hashed once, a
# ready folder that waits for a free worker included.
_SEEN: _Memo = _Memo()
_HASHED: _Memo = _Memo()
_RACY = [RACY_SECONDS]
_clock = time.time


def ready(folder: Path, budget: HashBudget | None = None) -> bool:
    """Whether the folder's request is one to answer now. A sync tool carries the body and `request.json` in any order,
    and a body half written, so a regular body is ready only once two scans in a row see it alike, its ctime is
    `RACY_SECONDS` old, and its sha256 is the request's; until then the request waits, never tried nor answered. A
    hash takes one from `budget`, and none left reads as not ready yet. A body that is not a regular file, or past
    `MAX_BODY_BYTES`, is ready at once: its answer is `unreadable` or `too_large`."""
    request = read_request(folder)
    if request is None:
        return False
    try:
        fd = folder_fd(folder)
    except OSError:
        return False
    try:
        try:
            held = os.stat(request["body"], dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(held.st_mode) or held.st_size > MAX_BODY_BYTES:
            return True
        seen = (held.st_dev, held.st_ino, held.st_size, held.st_mtime_ns, held.st_ctime_ns)
        if _SEEN.get(ident(folder)) != seen:
            _SEEN[ident(folder)] = seen
            return False
        digest = _HASHED.get(seen)
        if digest is None:
            if _clock() - held.st_ctime_ns / 1e9 < _RACY[0] or (budget is not None and not budget.take()):
                return False
            digest = _hash(fd, request["body"], seen)
        return digest == request["sha256"]
    except OSError:
        return False
    finally:
        os.close(fd)


def _hash(fd: int, name: str, seen: tuple[int, int, int, int, int]) -> str | None:
    """The body's sha256, cached by its sight, where it is still the file `seen` describes."""
    body = os.open(name, _READ, dir_fd=fd)
    try:
        held = os.fstat(body)
        if (held.st_dev, held.st_ino, held.st_size, held.st_mtime_ns, held.st_ctime_ns) != seen:
            return None
        digest = hashlib.sha256()
        while chunk := os.read(body, _CHUNK):
            digest.update(chunk)
    finally:
        os.close(body)
    found = digest.hexdigest()
    _HASHED[seen] = found
    return found


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


def _clear(fd: int, name: str) -> None:
    """`name` in the folder `fd` removed, a folder with all it holds; a link is unlinked, never followed. A folder the
    converter left without write permission is given it back, never through a link, and removed again."""
    try:
        held = os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(held.st_mode):
        os.unlink(name, dir_fd=fd)
        return
    try:
        shutil.rmtree(name, dir_fd=fd)
    except OSError:
        _writable(fd, name)
        shutil.rmtree(name, dir_fd=fd)


def _writable(fd: int, name: str) -> None:
    """Every folder in the tree at `name` in the folder `fd` made the owner's to list and write; a link is never
    followed, nor a file's mode touched."""
    pending = [(os.dup(fd), name)]
    while pending:
        parent, one = pending.pop()
        try:
            if not stat.S_ISDIR(os.stat(one, dir_fd=parent, follow_symlinks=False).st_mode):
                continue
            os.chmod(one, 0o700, dir_fd=parent, follow_symlinks=False)
            held = os.open(one, _DIR, dir_fd=parent)
        except OSError:
            continue
        finally:
            os.close(parent)
        try:
            for entry in os.listdir(held):
                pending.append((os.dup(held), entry))
        finally:
            os.close(held)


def _cleared(fd: int, names: Iterable[str]) -> bool:
    """Whether every name in `names` is gone from the folder `fd` (`_clear`); a name that stays is said, not raised,
    so a leftover that will not go answers rather than retrying until the host's deadline."""
    for name in names:
        try:
            _clear(fd, name)
        except OSError as exc:
            _say(f"{name} did not clear: {exc}")
            return False
    return True


def _mp3_head(head: bytes) -> bool:
    """The host's check: an MP3 opens with an ID3 tag or an MPEG frame sync (eleven set bits)."""
    return head.startswith(b"ID3") or (len(head) >= 2 and head[0] == 0xFF and head[1] & 0xE0 == 0xE0)


def _checked(held: int, name: str) -> tuple[str, int, tuple[int, int]] | dict:
    """The open output `held`, to be published as `name`: its sha256, size and identity, read from this one inode.
    A failure answer where the host would refuse it: not a regular file of one name, past its cap, not UTF-8 where
    it is text, or not opening as its name says; or changed while it was read."""
    before = os.fstat(held)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        return UNREADABLE
    cap = OUTPUT_CAPS[name]
    if before.st_size > cap:
        return TOO_LARGE
    os.fsync(held)
    digest, size, head = hashlib.sha256(), 0, b""
    text = None if name == MP3 else codecs.getincrementaldecoder("utf-8")()
    try:
        while chunk := os.read(held, _CHUNK):
            size += len(chunk)
            if size > cap:
                return TOO_LARGE
            if len(head) < 16:
                head += chunk[: 16 - len(head)]
            digest.update(chunk)
            if text is not None:
                text.decode(chunk)
        if text is not None:
            text.decode(b"", final=True)
    except UnicodeDecodeError:
        return UNREADABLE
    after = os.fstat(held)
    if (after.st_size, after.st_mtime_ns) != (size, before.st_mtime_ns):
        return UNREADABLE
    if name == MP3 and not _mp3_head(head):
        return UNREADABLE
    if name == VTT and not head.removeprefix(b"\xef\xbb\xbf").startswith(b"WEBVTT"):
        return UNREADABLE
    return digest.hexdigest(), size, (before.st_dev, before.st_ino)


def _publish_outputs(fd: int, section: str) -> dict:
    """The converter's files checked, then each renamed into the folder `fd` under its answer name: a converted
    answer's `result.json`, or a failure answer with nothing published. Every file is checked before any is renamed,
    and each published name is the very inode its digest was read from."""
    names = SECTION_OUTPUTS[section]
    media = names != (OUTPUT,)
    try:
        source = os.open(TMP_MEDIA, _DIR, dir_fd=fd) if media else os.dup(fd)
    except OSError:
        return UNREADABLE
    if media:
        # The converter may have taken write permission off its own folder; the renames out of it need it.
        os.fchmod(source, 0o700)
    # Each checked file stays open until its published name is compared: an inode still open is never freed, so its
    # number cannot pass to a file swapped in after the check.
    opened: list[int] = []
    try:
        found: list[tuple[str, str, tuple[str, int, tuple[int, int]]]] = []
        for name in names:
            src = name if media else TMP_OUTPUT
            try:
                held = os.open(src, _READ, dir_fd=source)
            except FileNotFoundError:
                if name == names[0]:
                    return UNREADABLE
                continue
            except OSError:
                return UNREADABLE
            opened.append(held)
            checked = _checked(held, name)
            if isinstance(checked, dict):
                return checked
            found.append((src, name, checked))
        published = []
        for src, name, (_sha, _size, identity) in found:
            _publish_from(source, src, fd, name)
            published.append(name)
            now = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if (now.st_dev, now.st_ino) != identity:
                for one in published:
                    _clear(fd, one)
                return UNREADABLE
        outputs = {name: {"sha256": sha, "size": size} for _src, name, (sha, size, _identity) in found}
        return {"reason": None, "traits": [], "outputs": outputs}
    finally:
        for held in opened:
            os.close(held)
        os.close(source)


def _publish_from(source: int, src: str, fd: int, dst: str) -> None:
    """Rename `src` in the folder `source` to `dst` in the folder `fd`, then sync `fd`."""
    os.replace(src, dst, src_dir_fd=source, dst_dir_fd=fd)
    os.fsync(fd)


def answer(folder: Path, request: dict, converter: Converter, *, sections: Iterable[str] = TEXT_SECTIONS) -> bool:
    """Convert one request and answer it, each output file first and `result.json` last, each through a `.tmp-` name.
    A converted answer's `result.json` maps each output file to its sha256 and size (`outputs`), so a host across a
    sync tool lands it only once every file arrived whole.

    `sections` are the sections this engine serves; a request for another is answered `unsupported`, never tried. A
    text section's converter writes `.tmp-output.md`; a media section's writes its files into the empty folder
    `.tmp-out`. Every file is checked as the host checks it before any is published (`_publish_outputs`).

    True where the converter raised: the inner process then recycles. Each attempt is counted in `.tmp-attempts`
    before the convert, so a convert that kills the process is tried `MAX_ATTEMPTS` times, then answered `unreadable`.
    The body and `request.json` are never written.
    """
    section = request.get("section", "convert")
    fd = folder_fd(folder)
    try:
        # A dead daemon's leftovers, its published outputs included: a reason answer then stands alone.
        clean = _cleared(fd, (TMP_OUTPUT, TMP_MEDIA, *SECTION_OUTPUTS.get(section, ())))
        failed = False
        tried = _attempts(fd)
        if not clean:
            result = UNREADABLE
        elif tried >= MAX_ATTEMPTS:
            _say(f"{ident(folder)}: {tried} attempts ended the process; answering unreadable")
            result = UNREADABLE
        elif section not in sections or section not in SECTION_OUTPUTS:
            result = UNSUPPORTED
        elif not _regular(fd, request["body"]):
            result = UNREADABLE
        elif os.stat(request["body"], dir_fd=fd, follow_symlinks=False).st_size > MAX_BODY_BYTES:
            result = TOO_LARGE
        else:
            media = SECTION_OUTPUTS[section] != (OUTPUT,)
            if media:
                os.mkdir(TMP_MEDIA, 0o755, dir_fd=fd)
            _write_new(fd, TMP_ATTEMPTS, str(tried + 1).encode("ascii"))
            _started(folder, tried, request)
            try:
                got = converter(folder / request["body"], folder / (TMP_MEDIA if media else TMP_OUTPUT), request)
                result = None if got is None else _shape(got)
            except Exception as exc:
                _say(f"{ident(folder)}: the converter raised {type(exc).__name__}: {exc}")
                result, failed = UNREADABLE, True
            finally:
                _ended(folder)
        if result is None:
            result = _publish_outputs(fd, section)
        # A leftover that stays costs the folder some space, not its answer.
        _cleared(fd, (TMP_OUTPUT, TMP_MEDIA))
        _write_new(fd, TMP_RESULT, (json.dumps(result) + "\n").encode("utf-8"))
        _publish(fd, TMP_RESULT, RESULT)
        try:
            os.unlink(TMP_ATTEMPTS, dir_fd=fd)
        except FileNotFoundError:
            pass
    finally:
        os.close(fd)
    _say(f"{ident(folder)}: answered reason={result['reason']}")
    return failed


def _timeout(request: dict) -> float:
    given = request.get("options", {}).get("timeout_seconds")
    if isinstance(given, (int, float)) and not isinstance(given, bool) and given > 0:
        return float(given)
    return _DEFAULT_TIMEOUT[0]


def _started(folder: Path, tried: int, request: dict) -> None:
    with _LOCK:
        _RUNNING[ident(folder)] = (folder, tried)
    for kit in _KIT:
        (kit / f"{CONVERTING}{ident(folder)}").write_text(str(_timeout(request)), encoding="ascii")


def _ended(folder: Path) -> None:
    with _LOCK:
        _RUNNING.pop(ident(folder), None)
    for kit in _KIT:
        (kit / f"{CONVERTING}{ident(folder)}").unlink(missing_ok=True)


def _take_back(folder: Path, tried: int) -> None:
    """Set a running folder's count back to what it held before this attempt: a stop is not the file's fault."""
    fd = folder_fd(folder)
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


def _dirs(parent: Path, pattern: re.Pattern) -> list[Path]:
    """Each real directory in `parent` whose name is `pattern`, by name; a link or a sync tool's debris is skipped."""
    try:
        entries = sorted(os.scandir(parent), key=lambda entry: entry.name)
    except OSError:
        return []
    found = []
    for entry in entries:
        try:
            if pattern.fullmatch(entry.name) and entry.is_dir(follow_symlinks=False):
                found.append(Path(entry.path))
        except OSError:
            continue
    return found


def pending(store: Path, busy: set[str]) -> list[Path]:
    """Every request folder in every machine's segment of `store` that has a `request.json`, no `result.json` and is
    not in `busy` (by `ident`), by key, then machine."""
    found = []
    for segment in _dirs(store, _MACHINE):
        for folder in _dirs(segment, _KEY):
            if ident(folder) not in busy and not _absent(folder, REQUEST) and _absent(folder, RESULT):
                found.append(folder)
    return sorted(found, key=lambda folder: (folder.name, folder.parent.name))


# --- the inner process ----------------------------------------------------------


def _work(folder: Path, converter: Converter, sections: Iterable[str] = TEXT_SECTIONS) -> bool | None:
    """One folder's answer; None where its request does not read, or the folder went away or turned into a link
    mid-answer (a host prune). The main thread asked `ready` first."""
    request = read_request(folder)
    if request is None:
        return None
    try:
        return answer(folder, request, converter, sections=sections)
    except OSError as exc:
        _say(f"{ident(folder)}: skipped, the folder did not take the answer: {exc}")
        return None


def _tried(folder: Path) -> int:
    try:
        fd = folder_fd(folder)
    except OSError:
        return 0
    try:
        return _attempts(fd)
    except OSError:
        return 0
    finally:
        os.close(fd)


def _outer_handle(kit: Path) -> int | None:
    """The lock file of the outer process that started this inner one, opened once. A new outer process wipes the
    scratch and locks a new file at the same path, so a lookup by path would read the new outer as this one's."""
    try:
        return os.open(kit / OUTER_LOCK, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return None


def _outer_gone(handle: int | None) -> bool:
    """True once this inner process's own outer process no longer holds its lock. Its parent is the jail's runtime,
    which outlives it, and a killed outer process its own parent has not reaped still answers a signal 0."""
    if handle is None:
        return True
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _inner(
    engine: str,
    factory: Factory,
    threads: int,
    store: Path,
    scratch: Path,
    timings: Timings,
    sections: tuple[str, ...] = TEXT_SECTIONS,
) -> int:
    """Answer requests until a converter raises, `RECYCLE_AFTER` files are done, or the outer process is gone.

    A folder an earlier inner process already tried runs alone, so a crash charges its attempt to that folder only.
    A folder whose request did not read is passed over until the next scan. A folder not `ready` is never submitted,
    and costs a stat per pass: a pass hashes at most one body per free worker, and a body is hashed once per sight.
    """
    parent = os.getppid()
    kit = scratch / KIT_DIR
    kit.mkdir(exist_ok=True)
    _KIT[:] = [kit]
    outer = _outer_handle(kit)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    daemon = {"engine": engine, "sections": sections, "store": store, "scratch": scratch, "threads": threads}
    converter = factory(daemon)
    done, failed = 0, False
    running: dict[Future, str] = {}
    alone: str | None = None
    skipped: set[str] = set()
    cleared = time.monotonic()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        while True:
            if os.getppid() != parent or _outer_gone(outer):
                # The whole group, not a return: leaving the pool waits for a convert that may never end, and a
                # converter's own children would outlive an exit.
                os.killpg(os.getpgrp(), signal.SIGKILL)
            stopping = failed or done >= RECYCLE_AFTER
            if stopping and not running:
                return RECYCLE_EXIT
            if not stopping and alone is None:
                if time.monotonic() - cleared >= timings.scan:
                    skipped.clear()
                    cleared = time.monotonic()
                room = min(threads, RECYCLE_AFTER - done) - len(running)
                budget = HashBudget(max(1, room))
                waiting = [folder for folder in pending(store, set(running.values()) | skipped) if ready(folder, budget)]
                suspects = [folder for folder in waiting if _tried(folder)]
                if suspects:
                    if not running:
                        alone = ident(suspects[0])
                        running[pool.submit(_work, suspects[0], converter, sections)] = alone
                else:
                    for folder in waiting[: max(room, 0)]:
                        running[pool.submit(_work, folder, converter, sections)] = ident(folder)
            if not running:
                time.sleep(timings.scan)
                continue
            finished, _ = wait(running, timeout=timings.scan, return_when=FIRST_COMPLETED)
            for future in finished:
                name = running.pop(future)
                if name == alone:
                    alone = None
                outcome = future.result()
                if outcome is None:
                    # A request that does not read waits for the next scan, not the next free worker.
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
    sections: Iterable[str] = TEXT_SECTIONS,
) -> int:
    """Run the daemon for `engine` and answer its exit code; `argv` carries `--store`, `--cache`, `--unjailed`.

    `timeout_seconds` is a convert's time for a request whose options carry no `timeout_seconds` of their own.
    `sections` are the request sections the engine serves (`SECTION_OUTPUTS`); a request for another is answered
    `unsupported`.
    """
    sections = tuple(dict.fromkeys(sections))
    unknown = [one for one in sections if one not in SECTION_OUTPUTS]
    if unknown or not sections:
        raise ValueError(f"sections must be some of {sorted(SECTION_OUTPUTS)}, not {unknown or 'none'}")
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
        _RACY[0] = timings.racy
        return _inner(engine, factory, max(1, threads), store, scratch, timings, sections)
    store.mkdir(parents=True, exist_ok=True)
    root = _import_root(factory)
    return _outer(store, scratch, [*reads, *([root] if root else [])], args.unjailed, timings)
