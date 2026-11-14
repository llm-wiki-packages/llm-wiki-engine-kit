"""The engine kit's daemon against a request store, unjailed, with stand-in converters."""

import collections
import contextlib
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import llm_wiki_engine_kit as kit

KIT = Path(kit.__file__).resolve().parents[1]
TESTS = Path(__file__).resolve().parent
WAIT = 60
# Sub-second waits, so a daemon test costs its own work and not the service's patience.
ESCALATE = 0.5
FAST = ["--scan-seconds", "0.05", "--poll-seconds", "0.05", "--backoff-seconds", "0.05"]
FAST += ["--backoff-ceiling-seconds", "0.2", "--stop-escalate-seconds", str(ESCALATE), "--racy-seconds", "0"]


@pytest.fixture(autouse=True)
def settled(monkeypatch):
    """Every body's last change counts as long past, unless a test sets the clock back; each test starts unseen."""
    monkeypatch.setattr(kit, "_clock", lambda: time.time() + 10)
    kit._SEEN.clear()
    kit._HASHED.clear()


MACHINE = "box-a"


def key_of(name: str) -> str:
    """A request key that sorts as `name` does."""
    return name.encode("utf-8").hex().ljust(64, "0")


def a_folder(store: Path, name: str, text: str = "hello", *, options=None, machine: str = MACHINE) -> Path:
    """A request folder at `<store>/<machine>/<key>/`, as a host submits it."""
    folder = store / machine / key_of(name)
    folder.mkdir(parents=True)
    (folder / "body.txt").write_text(text, encoding="utf-8")
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    request = {"v": 1, "engine": "stub", "section": "convert", "sha256": sha, "body": "body.txt"}
    (folder / "request.json").write_text(json.dumps({**request, "options": options or {}}), encoding="utf-8")
    return folder


def kill_group(pid: int) -> None:
    """A teardown's kill. On macOS, `killpg` answers EPERM, not ESRCH, for a group that holds only zombies."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)


def converted(text: str) -> dict:
    data = text.encode("utf-8")
    return {"reason": None, "traits": [], "outputs": {"output.md": {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}}}


def result_of(folder: Path) -> dict:
    return json.loads((folder / "result.json").read_text(encoding="utf-8"))


def wait_for(path: Path, proc=None) -> None:
    deadline = time.monotonic() + WAIT
    while not path.exists():
        if proc is not None and proc.poll() is not None:
            out, err = proc.communicate()
            pytest.fail(f"the daemon exited {proc.returncode} before {path.name}: {err.decode()[-2000:]}")
        if time.monotonic() > deadline:
            pytest.fail(f"no {path} after {WAIT}s")
        time.sleep(0.05)


@pytest.fixture
def daemon(tmp_path):
    started = []

    def start(factory: str, *extra: str, jailed: bool = False, env=None):
        argv = [sys.executable, "-m", "llm_wiki_engine_kit", "serve", "--store", str(tmp_path / "store")]
        argv += ["--cache", str(tmp_path / "cache"), "--factory", f"kitstubs:{factory}", *FAST, *extra]
        if not jailed:
            argv.append("--unjailed")
        full = {**os.environ, "PYTHONPATH": os.pathsep.join([str(KIT), str(TESTS)]), **(env or {})}
        proc = subprocess.Popen(argv, env=full, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        started.append(proc)
        return proc

    yield start
    for proc in started:
        if proc.poll() is None:
            kill_group(proc.pid)
        proc.communicate()


# --- one answer, in process ---------------------------------------------------


def test_result_json_lands_only_after_the_whole_output_md(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    seen = []
    real = os.replace

    def replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
        if os.fsdecode(dst).endswith("result.json"):
            output = folder / "output.md"
            seen.append(output.read_text(encoding="utf-8") if output.exists() else None)
        real(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(os, "replace", replace)

    def convert(body, out, request):
        out.write_text("# whole\n", encoding="utf-8")

    assert kit.answer(folder, kit.read_request(folder), convert) is False
    assert seen == ["# whole\n"]
    assert result_of(folder) == converted("# whole\n")
    assert sorted(p.name for p in folder.iterdir()) == ["body.txt", "output.md", "request.json", "result.json"]


def test_while_the_converter_works_the_folder_holds_no_output_and_no_result(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    entered, release = threading.Event(), threading.Event()

    def convert(body, out, request):
        out.write_text("# par", encoding="utf-8")
        entered.set()
        release.wait(WAIT)
        out.write_text("# done\n", encoding="utf-8")

    worker = threading.Thread(target=kit.answer, args=(folder, kit.read_request(folder), convert))
    worker.start()
    assert entered.wait(WAIT)
    try:
        assert not (folder / "output.md").exists()
        assert not (folder / "result.json").exists()
        assert all(p.name.startswith(".tmp-") for p in folder.iterdir() if p.name not in ("body.txt", "request.json"))
    finally:
        release.set()
        worker.join(WAIT)
    assert (folder / "output.md").read_text(encoding="utf-8") == "# done\n"


def test_a_failure_answer_writes_the_reason_and_no_output(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    assert kit.answer(folder, kit.read_request(folder), lambda body, out, request: {"reason": "no_text_layer"}) is False
    assert result_of(folder) == {"reason": "no_text_layer", "traits": []}
    assert not (folder / "output.md").exists()


def test_a_raising_converter_answers_unreadable_and_asks_for_a_recycle(tmp_path):
    folder = a_folder(tmp_path / "store", "k")

    def convert(body, out, request):
        raise RuntimeError("the parser broke")

    assert kit.answer(folder, kit.read_request(folder), convert) is True
    assert result_of(folder) == {"reason": "unreadable", "traits": []}


def test_leftover_tmp_names_from_a_dead_daemon_are_written_over(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    (folder / ".tmp-output.md").write_text("stale", encoding="utf-8")
    (folder / ".tmp-result.json").symlink_to(tmp_path / "victim")

    def convert(body, out, request):
        out.write_text("# fresh\n", encoding="utf-8")

    kit.answer(folder, kit.read_request(folder), convert)
    assert (folder / "output.md").read_text(encoding="utf-8") == "# fresh\n"
    assert result_of(folder) == converted("# fresh\n")
    assert not (tmp_path / "victim").exists()


def test_the_daemon_never_writes_the_body_or_the_request(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    names = ("body.txt", "request.json")

    def held():
        return {name: ((folder / name).read_bytes(), os.stat(folder / name)) for name in names}

    before = held()
    time.sleep(0.01)

    def convert(body, out, request):
        out.write_text("# text\n", encoding="utf-8")

    kit.answer(folder, kit.read_request(folder), convert)
    after = held()
    for name in names:
        assert after[name][0] == before[name][0], name
        old, new = before[name][1], after[name][1]
        assert (new.st_ino, new.st_mtime_ns, new.st_ctime_ns) == (old.st_ino, old.st_mtime_ns, old.st_ctime_ns), name


def test_each_answer_file_is_synced_before_its_rename_and_the_folder_after(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    events = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        events.append(("fsync", os.fstat(fd).st_ino))
        real_fsync(fd)

    def replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
        events.append(("replace", os.path.basename(os.fsdecode(dst))))
        real_replace(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)

    def convert(body, out, request):
        out.write_text("# text\n", encoding="utf-8")

    kit.answer(folder, kit.read_request(folder), convert)
    output, result, where = ((folder / n).stat().st_ino for n in ("output.md", "result.json", "."))
    replaced = [e for e in events if e[0] == "replace"]
    assert replaced == [("replace", "output.md"), ("replace", "result.json")]
    at = events.index(("replace", "output.md"))
    last = events.index(("replace", "result.json"))
    assert ("fsync", output) in events[:at]
    assert ("fsync", where) in events[at:last]
    assert ("fsync", result) in events[at:last]
    assert ("fsync", where) in events[last:]


def test_a_folder_tried_the_most_times_answers_unreadable_without_a_convert(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    calls = []

    def convert(body, out, request):
        calls.append(body)
        out.write_text("# text\n", encoding="utf-8")

    (folder / ".tmp-attempts").write_text(str(kit.MAX_ATTEMPTS), encoding="utf-8")
    assert kit.answer(folder, kit.read_request(folder), convert) is False
    assert calls == []
    assert result_of(folder) == {"reason": "unreadable", "traits": []}


def test_each_attempt_is_counted_before_the_convert(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    seen = []

    def convert(body, out, request):
        seen.append((folder / ".tmp-attempts").read_text(encoding="utf-8"))
        out.write_text("# text\n", encoding="utf-8")

    kit.answer(folder, kit.read_request(folder), convert)
    assert seen == ["1"]
    assert not (folder / ".tmp-attempts").exists()


def test_pythonpath_is_carried_only_inside_a_granted_tree(tmp_path, monkeypatch):
    granted, other = tmp_path / "granted", tmp_path / "other"
    (granted / "src").mkdir(parents=True)
    other.mkdir()
    monkeypatch.chdir(other)
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(granted / "src"), str(other), ".", "rel/dir"]))
    assert kit._carried_pythonpath([granted]) == str((granted / "src").resolve())
    assert str(other.resolve()) not in map(str, kit._reads([granted]))
    assert Path.cwd().resolve() not in kit._reads([granted])


def test_the_jail_reads_the_tree_the_factorys_top_package_imports_from(tmp_path, monkeypatch):
    root = tmp_path / "editable"
    (root / "eng" / "sub").mkdir(parents=True)
    (root / "eng" / "__init__.py").write_text("", encoding="utf-8")
    (root / "eng" / "sub" / "__init__.py").write_text("def factory(daemon):\n    return None\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(root))
    import eng.sub

    seen = []
    monkeypatch.setattr(kit, "_outer", lambda store, scratch, reads, unjailed, timings: seen.extend(reads) or 0)
    kit.serve("stub", eng.sub.factory, argv=["--store", str(tmp_path / "store"), "--cache", str(tmp_path / "cache")])
    assert root.resolve() in [Path(one).resolve() for one in seen]


def test_a_factory_imported_from_the_home_adds_no_read_of_the_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "loose.py").write_text("def factory(daemon):\n    return None\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    import loose

    assert kit._import_root(loose.factory) is None


def _factory_module(path: Path, name: str, spec_name: str | None):
    """A module as Python loads it, with `factory` defined in it and recorded under `name`."""
    import importlib.util
    import types

    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__spec__ = importlib.util.spec_from_file_location(spec_name, path) if spec_name else None
    exec("def factory(daemon):\n    return None\n", module.__dict__)
    return module


@pytest.mark.unit
def test_a_factory_imported_from_above_the_home_adds_no_read_of_it(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    module = _factory_module(tmp_path / "above.py", "above", "above")
    monkeypatch.setitem(sys.modules, "above", module)
    assert kit._import_root(module.factory) is None


@pytest.mark.unit
def test_a_single_file_module_grants_its_own_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "mods").mkdir()
    module = _factory_module(tmp_path / "mods" / "solo.py", "solo", "solo")
    monkeypatch.setitem(sys.modules, "solo", module)
    assert kit._import_root(module.factory) == (tmp_path / "mods").resolve()


@pytest.mark.unit
def test_a_factory_in_a_package_run_with_dash_m_grants_the_folder_above_the_package(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    package = tmp_path / "src" / "eng"
    package.mkdir(parents=True)
    top = _factory_module(package / "__init__.py", "eng", "eng")
    main = _factory_module(package / "__main__.py", "__main__", "eng.__main__")
    monkeypatch.setitem(sys.modules, "eng", top)
    monkeypatch.setitem(sys.modules, "__main__", main)
    assert kit._import_root(main.factory) == (tmp_path / "src").resolve()


# --- finding work, in process -------------------------------------------------


def test_a_folder_with_result_json_is_left_alone(tmp_path):
    store = tmp_path / "store"
    done = a_folder(store, "done")
    (done / "result.json").write_text('{"reason": null}', encoding="utf-8")
    waiting = a_folder(store, "waiting")
    assert kit.pending(store, set()) == [waiting]
    assert kit.pending(store, {kit.ident(waiting)}) == []


def test_every_machines_segment_is_served_and_sync_debris_is_not(tmp_path):
    store = tmp_path / "store"
    mine, theirs = a_folder(store, "a"), a_folder(store, "a", machine="box-b")
    (store / ".dropbox.cache").mkdir()
    (store / "~$x").write_text("x")
    (store / "box-b (conflicted copy)").mkdir()
    a_folder(store / "box-b (conflicted copy)", "c", machine="")
    for name in (".output.md.Ab12Cd", "result (conflicted copy).json", "output (1).md"):
        (store / MACHINE / name).mkdir()
        (store / MACHINE / name / "request.json").write_text("{}")
    a_folder(store, "old-layout", machine="").rename(store / ("e" * 64))
    assert kit.pending(store, set()) == [mine, theirs]
    assert kit.ident(mine) != kit.ident(theirs)


def test_debris_inside_a_request_folder_is_left_alone(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    debris = ("~$x", "result (conflicted copy).json", "output (1).md", ".output.md.Ab12Cd")
    for name in debris:
        (folder / name).write_text('{"reason": "timeout"}', encoding="utf-8")
    assert kit._work(folder, lambda body, out, request: out.write_text("# ok\n") and None) is False
    assert result_of(folder) == converted("# ok\n")
    assert all((folder / name).read_text() == '{"reason": "timeout"}' for name in debris)


def aged(path: Path, seconds: float = 100) -> None:
    """Set `path`'s mtime `seconds` back, past the window where a digest is not cached."""
    then = time.time() - seconds
    os.utime(path, (then, then))


@pytest.mark.parametrize("arrived", [None, "hel", "hellO"])
def test_a_body_that_has_not_fully_arrived_is_not_ready_until_it_matches_and_holds_still(tmp_path, arrived):
    folder = a_folder(tmp_path / "store", "k")
    (folder / "body.txt").unlink()
    if arrived is not None:
        (folder / "body.txt").write_text(arrived, encoding="utf-8")
    assert kit.ready(folder) is False and kit.ready(folder) is False
    assert sorted(p.name for p in folder.iterdir()) == sorted(["request.json", *([] if arrived is None else ["body.txt"])])
    (folder / "body.txt").unlink(missing_ok=True)
    (folder / "body.txt").write_text("hello", encoding="utf-8")
    assert kit.ready(folder) is False, "a body seen once may still be growing"
    assert kit.ready(folder) is True
    assert kit._work(folder, lambda body, out, request: out.write_text("# ok\n") and None) is False
    assert result_of(folder) == converted("# ok\n")


def test_a_body_grown_in_place_is_hashed_again_once_it_holds_still(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    body = folder / "body.txt"
    body.write_text("he", encoding="utf-8")
    aged(body)
    before = body.stat()
    assert kit.ready(folder) is False and kit.ready(folder) is False
    with open(body, "a", encoding="utf-8") as held:
        held.write("llo")
    os.utime(body, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert body.stat().st_ino == before.st_ino
    assert kit.ready(folder) is False
    assert kit.ready(folder) is True


def test_a_body_rewritten_in_place_at_one_size_is_hashed_again_once_it_holds_still(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    body = folder / "body.txt"
    body.write_text("hellO", encoding="utf-8")
    aged(body, 200)
    assert kit.ready(folder) is False and kit.ready(folder) is False
    with open(body, "r+", encoding="utf-8") as held:
        held.write("hello")
    aged(body, 100)
    assert kit.ready(folder) is False
    assert kit.ready(folder) is True


def counted_hashes(monkeypatch) -> list:
    hashed = []
    real = kit.hashlib.sha256
    monkeypatch.setattr(kit.hashlib, "sha256", lambda *a: hashed.append(1) or real(*a))
    return hashed


def test_a_body_with_a_future_mtime_that_holds_still_is_hashed_once(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    future = time.time() + 86400
    os.utime(folder / "body.txt", (future, future))
    hashed = counted_hashes(monkeypatch)
    assert kit.ready(folder) is False
    for _ in range(5):
        assert kit.ready(folder) is True
    assert hashed == [1]


def test_a_body_changed_under_two_seconds_ago_is_not_hashed_yet(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    monkeypatch.setattr(kit, "_clock", time.time)
    hashed = counted_hashes(monkeypatch)
    assert kit.ready(folder) is False and kit.ready(folder) is False
    assert hashed == []
    monkeypatch.setattr(kit, "_clock", lambda: time.time() + kit.RACY_SECONDS + 1)
    assert kit.ready(folder) is True and hashed == [1]


def test_hashing_is_bounded_per_pass(tmp_path, monkeypatch):
    store = tmp_path / "store"
    first, second = a_folder(store, "a"), a_folder(store, "b")
    hashed = counted_hashes(monkeypatch)
    assert kit.ready(first) is False and kit.ready(second) is False
    budget = kit.HashBudget(1)
    assert (kit.ready(first, budget), kit.ready(second, budget)) == (True, False)
    assert hashed == [1]
    assert kit.ready(second, kit.HashBudget(1)) is True


def test_a_full_memo_evicts_its_least_recently_used_entry():
    memo = kit._Memo(cap=2)
    memo["a"], memo["b"] = 1, 2
    assert memo.get("a") == 1
    memo["c"] = 3
    assert sorted(memo) == ["a", "c"]


def test_a_body_still_changing_is_never_hashed(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    body = folder / "body.txt"
    hashed = []
    real = kit.hashlib.sha256
    monkeypatch.setattr(kit.hashlib, "sha256", lambda *a: hashed.append(1) or real(*a))
    for size in range(1, 6):
        body.write_text("hello"[:size], encoding="utf-8")
        assert kit.ready(folder) is False
    assert hashed == []


@pytest.mark.parametrize("kind", ["link", "dir", "fifo"])
def test_a_body_that_is_not_a_regular_file_is_answered_unreadable_at_once(tmp_path, kind):
    folder = a_folder(tmp_path / "store", "k")
    body = folder / "body.txt"
    body.unlink()
    if kind == "link":
        (tmp_path / "elsewhere").write_text("hello")
        body.symlink_to(tmp_path / "elsewhere")
    elif kind == "dir":
        body.mkdir()
    else:
        os.mkfifo(body)
    assert kit.ready(folder) is True
    assert kit._work(folder, lambda body, out, request: pytest.fail("converted")) is False
    assert result_of(folder) == {"reason": "unreadable", "traits": []}


def test_a_body_past_the_hosts_cap_is_answered_too_large_without_a_hash(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    monkeypatch.setattr(kit, "MAX_BODY_BYTES", 3)
    monkeypatch.setattr(kit.hashlib, "sha256", lambda *a: pytest.fail("hashed"))
    assert kit.ready(folder) is True
    assert kit._work(folder, lambda body, out, request: pytest.fail("converted")) is False
    assert result_of(folder) == {"reason": "too_large", "traits": []}


def test_a_link_at_the_machine_or_the_key_level_is_never_served(tmp_path):
    store = tmp_path / "store"
    mine = a_folder(store, "a")
    elsewhere = a_folder(tmp_path / "elsewhere", "b")
    (store / "box-c").symlink_to(elsewhere.parent)
    (store / MACHINE / key_of("c")).symlink_to(elsewhere)
    assert kit.pending(store, set()) == [mine]


def test_a_segment_swapped_for_a_link_after_the_scan_is_never_followed(tmp_path):
    store = tmp_path / "store"
    folder = a_folder(store, "a")
    assert kit.pending(store, set()) == [folder]
    (store / MACHINE).rename(store / "real")
    (store / MACHINE).symlink_to(store / "real")
    assert kit.read_request(folder) is None
    assert kit._work(folder, lambda body, out, request: out.write_text("# x\n") and None) is None
    assert not (store / "real" / folder.name / "result.json").exists()


def test_a_request_with_no_body_sha256_does_not_read(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    request = json.loads((folder / "request.json").read_text())
    for sha in (None, "x", "A" * 64):
        (folder / "request.json").write_text(json.dumps({**request, "sha256": sha}), encoding="utf-8")
        assert kit.read_request(folder) is None


def test_a_request_json_that_is_a_link_does_not_read(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    real = tmp_path / "elsewhere.json"
    (folder / "request.json").rename(real)
    (folder / "request.json").symlink_to(real)
    assert kit.read_request(folder) is None


def test_a_request_json_over_64_kib_does_not_read(tmp_path):
    folder = a_folder(tmp_path / "store", "k", options={"pad": "x" * kit.REQUEST_CAP})
    assert (folder / "request.json").stat().st_size > kit.REQUEST_CAP
    assert kit.read_request(folder) is None


def test_a_request_whose_body_leaves_the_folder_does_not_read(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    doc = {"body": "../other/body.txt", "sha256": "0" * 64, "options": {}}
    (folder / "request.json").write_text(json.dumps(doc), encoding="utf-8")
    assert kit.read_request(folder) is None


# --- the daemon, spawned ------------------------------------------------------


def test_the_factory_never_runs_in_the_outer_process(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k")
    record = tmp_path / "factory-pids"
    proc = daemon("echo", env={"KIT_STUB_RECORD": str(record)})
    wait_for(folder / "result.json", proc)
    lines = (folder / "output.md").read_text(encoding="utf-8").splitlines()
    factory = int(lines[-2].split()[1])
    convert = int(lines[-1].split()[1])
    assert lines[0] == "hello"
    assert convert == factory
    assert record.read_text(encoding="utf-8").split() == [str(factory)]
    assert factory != proc.pid


def test_after_an_exception_the_inner_process_exits_and_a_new_one_takes_the_next(tmp_path, daemon):
    store = tmp_path / "store"
    first, second = a_folder(store, "a"), a_folder(store, "b")
    record = tmp_path / "convert-pids"
    proc = daemon("raising", env={"KIT_STUB_RECORD": str(record)})
    wait_for(second / "result.json", proc)
    assert result_of(first) == result_of(second) == {"reason": "unreadable", "traits": []}
    pids = record.read_text(encoding="utf-8").split()
    assert len(set(pids)) == 2, pids


def test_after_fifty_files_the_inner_process_exits_and_a_new_one_answers_the_next(tmp_path, daemon):
    store = tmp_path / "store"
    folders = [a_folder(store, f"{n:03d}") for n in range(kit.RECYCLE_AFTER + 1)]
    proc = daemon("echo")
    for folder in folders:
        wait_for(folder / "result.json", proc)
    pids = collections.Counter(f.joinpath("output.md").read_text(encoding="utf-8").split()[-1] for f in folders)
    assert sorted(pids.values()) == [1, kit.RECYCLE_AFTER]
    assert folders[-1].joinpath("output.md").read_text(encoding="utf-8").split()[-1] != str(proc.pid)


def _tree_cpu_seconds(root: int) -> float:
    """The user and system CPU time of `root` and every live descendant, from `/proc`: the inner process runs in a
    session of its own, so a process group would miss it."""
    stats = {}
    for entry in os.listdir("/proc"):
        if entry.isdigit():
            try:
                stats[int(entry)] = Path(f"/proc/{entry}/stat").read_text().rsplit(")", 1)[1].split()
            except OSError:
                continue
    tree, grew = {root}, True
    while grew:
        found = {pid for pid, fields in stats.items() if int(fields[1]) in tree} - tree
        tree |= found
        grew = bool(found)
    ticks = sum(int(stats[pid][11]) + int(stats[pid][12]) for pid in tree if pid in stats)
    return ticks / os.sysconf("SC_CLK_TCK")


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads CPU time from /proc")
def test_many_waiting_folders_cost_one_scan_per_pass_not_one_per_folder(tmp_path, daemon):
    store = tmp_path / "store"
    for n in range(300):
        folder = a_folder(store, f"{n:03d}")
        (folder / "body.txt").write_text("not yet", encoding="utf-8")
    proc = daemon("echo", "--scan-seconds", "0.5")
    time.sleep(1.5)
    start = _tree_cpu_seconds(proc.pid)
    time.sleep(3)
    used = _tree_cpu_seconds(proc.pid) - start
    assert proc.poll() is None
    assert used < 1.0, f"{used:.2f}s of CPU in 3s idle with 300 waiting folders"


def test_a_backlog_of_new_bodies_keeps_every_worker_busy(tmp_path, daemon):
    store = tmp_path / "store"
    folders = [a_folder(store, f"{n:03d}", options={"slow": 0.5}) for n in range(20)]
    started = time.monotonic()
    proc = daemon("crash", "--threads", "4", "--scan-seconds", "0.5")
    for folder in folders:
        wait_for(folder / "result.json", proc)
    took = time.monotonic() - started
    # Four workers fed in step take 5 rounds of 0.5 s plus a scan or two (about 3.2 s); one hash per pass feeds them
    # about one body per scan (about 6.3 s).
    assert took < 5.0, f"20 short converts on 4 workers took {took:.1f}s"


def test_a_body_still_syncing_is_answered_once_it_arrives_whole(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k", text="hello")
    (folder / "body.txt").write_text("hel", encoding="utf-8")
    proc = daemon("echo")
    time.sleep(0.5)
    assert sorted(p.name for p in folder.iterdir()) == ["body.txt", "request.json"]
    (folder / ".body.txt.Ab12Cd").write_text("hello", encoding="utf-8")
    (folder / ".body.txt.Ab12Cd").rename(folder / "body.txt")
    wait_for(folder / "result.json", proc)
    assert result_of(folder)["reason"] is None
    assert (folder / "output.md").read_text(encoding="utf-8").startswith("hello\n")


def test_after_a_kill_mid_convert_a_restart_answers_the_folder(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k")
    proc = daemon("stall")
    wait_for(folder / ".tmp-output.md", proc)
    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(WAIT)
    assert not (folder / "result.json").exists()
    again = daemon("echo")
    wait_for(folder / "result.json", again)
    assert result_of(folder) == converted((folder / "output.md").read_text(encoding="utf-8"))
    assert (folder / "output.md").read_text(encoding="utf-8").startswith("hello\n")


def test_the_scratch_is_wiped_at_start_and_is_the_inner_home(tmp_path, daemon):
    stale = tmp_path / "cache" / "tmp" / "stale"
    stale.parent.mkdir(parents=True)
    stale.write_text("old", encoding="utf-8")
    folder = a_folder(tmp_path / "store", "k", options={"outside": str(tmp_path / "x"), "port": 9, "canary": __file__})
    proc = daemon("probe")
    wait_for(folder / "result.json", proc)
    assert not stale.exists()
    assert "home-env True" in (folder / "output.md").read_text(encoding="utf-8")


def test_a_crashing_convert_is_tried_twice_then_answered_unreadable_and_the_next_folder_answered(tmp_path, daemon):
    store = tmp_path / "store"
    crashing = a_folder(store, "a", options={"crash": True})
    fine = a_folder(store, "b", text="after")
    proc = daemon("crash")
    wait_for(fine / "result.json", proc)
    wait_for(crashing / "result.json", proc)
    assert result_of(crashing) == {"reason": "unreadable", "traits": []}
    assert (fine / "output.md").read_text(encoding="utf-8") == "after"
    assert proc.poll() is None


def test_with_two_threads_a_crash_charges_only_the_folder_that_crashed(tmp_path, daemon):
    store = tmp_path / "store"
    crashing = a_folder(store, "a", options={"crash": True, "delay": 0.2})
    slower = a_folder(store, "b", text="innocent", options={"slow": 0.8})
    proc = daemon("crash", "--threads", "2")
    wait_for(slower / "result.json", proc)
    wait_for(crashing / "result.json", proc)
    assert result_of(slower) == converted("innocent")
    assert (slower / "output.md").read_text(encoding="utf-8") == "innocent"
    assert result_of(crashing) == {"reason": "unreadable", "traits": []}


def test_a_stop_takes_back_the_attempt_of_the_folder_in_progress(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k")
    for _ in range(kit.MAX_ATTEMPTS):
        proc = daemon("stall")
        wait_for(folder / ".tmp-output.md", proc)
        assert (folder / ".tmp-attempts").read_text(encoding="utf-8") == "1"
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(WAIT) == 0
        assert not (folder / ".tmp-attempts").exists()
        (folder / ".tmp-output.md").unlink()
    assert not (folder / "result.json").exists()


def _alive(pid: int) -> bool:
    """True while the process runs: a zombie, unreaped, still answers a signal 0."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    if not Path("/proc/self").exists():
        # macOS has no /proc: `ps` prints the state, and nothing once the process is reaped.
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        return bool(state) and not state.startswith("Z")
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii") as held:
            return held.read().rsplit(")", 1)[1].split()[0] != "Z"
    # Reaped between the signal and the read.
    except FileNotFoundError:
        return False
    except OSError:
        return True


def _gone_within(pid: int, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return False


class _Exited(Exception):
    pass


def test_a_second_signal_inside_the_stop_does_not_deadlock_and_both_signals_are_ignored_first(tmp_path, monkeypatch):
    folder = a_folder(tmp_path / "store", "k")
    (folder / ".tmp-attempts").write_text("1", encoding="ascii")
    calls = []

    class Reentering(dict):
        def values(self):
            # A second SIGTERM lands while the stop holds the lock.
            if "reentered" not in calls:
                calls.append("reentered")
                try:
                    kit._stop(signal.SIGTERM, None)
                except _Exited:
                    pass
            return super().values()

    def exit_(code):
        calls.append(("exit", code))
        raise _Exited

    monkeypatch.setattr(kit, "_RUNNING", Reentering({"k": (folder, 0)}))
    monkeypatch.setattr(os, "_exit", exit_)
    monkeypatch.setattr(signal, "signal", lambda signum, handler: calls.append((signum, handler)))
    stopper = threading.Thread(target=lambda: pytest.raises(_Exited, kit._stop, signal.SIGTERM, None), daemon=True)
    stopper.start()
    stopper.join(5)
    assert not stopper.is_alive(), "the stop deadlocked on its own lock"
    assert calls[:2] == [(signal.SIGTERM, signal.SIG_IGN), (signal.SIGINT, signal.SIG_IGN)]
    assert not (folder / ".tmp-attempts").exists()


@pytest.mark.parametrize("burst", [2, 8])
def test_signals_back_to_back_stop_the_inner_process_and_take_the_attempt_back(tmp_path, daemon, burst):
    folder = a_folder(tmp_path / "store", "k")
    record = tmp_path / "inner-pid"
    proc = daemon("stall", env={"KIT_STUB_RECORD": str(record)})
    wait_for(folder / ".tmp-output.md", proc)
    inner = int(record.read_text(encoding="utf-8").split()[0])
    for _ in range(burst):
        os.kill(inner, signal.SIGTERM)
    assert _gone_within(inner, 10), "the inner process hung in its stop"
    assert not (folder / ".tmp-attempts").exists()


def test_an_inner_process_deaf_to_sigterm_is_killed_once_the_stop_runs_long(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k")
    proc = daemon("deaf")
    wait_for(folder / ".tmp-output.md", proc)
    started = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(WAIT) == 0
    assert time.monotonic() - started >= ESCALATE * 0.8


def test_an_inner_process_sees_its_own_outer_gone_after_a_new_outer_takes_the_scratch(tmp_path):
    scratch = tmp_path / "scratch"
    kit._wipe(scratch)
    first = kit._hold(scratch / kit.KIT_DIR)
    handle = kit._outer_handle(scratch / kit.KIT_DIR)
    assert not kit._outer_gone(handle)
    os.close(first)
    kit._wipe(scratch)
    second = kit._hold(scratch / kit.KIT_DIR)
    try:
        assert kit._outer_gone(handle)
    finally:
        os.close(second)


def test_when_the_outer_process_is_killed_the_converters_own_child_goes_too(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k")
    record = tmp_path / "child-pid"
    proc = daemon("spawner", env={"KIT_STUB_RECORD": str(record)})
    deadline = time.monotonic() + WAIT
    while not record.exists() or not record.read_text(encoding="utf-8").strip():
        assert time.monotonic() < deadline, "the converter started no child"
        time.sleep(0.05)
    child = int(record.read_text(encoding="utf-8").split()[0])
    try:
        os.kill(proc.pid, signal.SIGKILL)
        assert _gone_within(child, 15), "the converter's child outlived the daemon"
    finally:
        if _alive(child):
            with contextlib.suppress(ProcessLookupError):
                os.kill(child, signal.SIGKILL)
    assert not (folder / "result.json").exists()


def test_a_marker_that_is_a_fifo_does_not_hang_the_timeout_check(tmp_path):
    kit_dir = tmp_path / ".kit"
    kit_dir.mkdir()
    os.mkfifo(kit_dir / f"{kit.CONVERTING}trap")
    found = []
    reader = threading.Thread(target=lambda: found.append(kit._overdue(kit_dir, time.time() + 10_000)), daemon=True)
    reader.start()
    reader.join(5)
    assert not reader.is_alive(), "the timeout check hung on a FIFO"
    assert found == [None]


def test_a_kit_folder_that_is_a_link_is_not_followed(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    (real / f"{kit.CONVERTING}k").write_text("1", encoding="ascii")
    (tmp_path / ".kit").symlink_to(real)
    assert kit._overdue(real, time.time() + 10) == "k"
    assert kit._overdue(tmp_path / ".kit", time.time() + 10) is None


def test_an_oversized_marker_is_not_read(tmp_path):
    kit_dir = tmp_path / ".kit"
    kit_dir.mkdir()
    (kit_dir / f"{kit.CONVERTING}k").write_text("1" + " " * 4096, encoding="ascii")
    assert kit._overdue(kit_dir, time.time() + 10) is None


def test_a_convert_past_its_timeout_is_killed_and_counts_as_an_attempt(tmp_path, daemon):
    folder = a_folder(tmp_path / "store", "k", options={"timeout_seconds": 0.3})
    proc = daemon("stall")
    wait_for(folder / "result.json", proc)
    assert result_of(folder) == {"reason": "unreadable", "traits": []}
    assert proc.poll() is None


def test_a_factory_that_always_fails_stops_the_daemon_after_five_crashes(tmp_path, daemon):
    a_folder(tmp_path / "store", "k")
    proc = daemon("broken")
    try:
        proc.wait(WAIT)
    except subprocess.TimeoutExpired:
        pytest.fail(f"the daemon still runs after {WAIT}s of crashes")
    assert proc.returncode == 1
    assert f"{kit.CRASH_LIMIT} inner processes in a row".encode() in proc.stderr.read()


def test_a_folder_removed_mid_convert_is_skipped_and_the_daemon_goes_on(tmp_path, daemon):
    store = tmp_path / "store"
    gone = a_folder(store, "a", options={"vanish": True})
    fine = a_folder(store, "b", text="after")
    proc = daemon("crash")
    wait_for(fine / "result.json", proc)
    assert not gone.exists()
    assert proc.poll() is None


def test_a_module_planted_in_the_scratch_or_the_cwd_never_runs(tmp_path, daemon):
    store = tmp_path / "store"
    marker = tmp_path / "planted-ran"
    a_folder(store, "a", options={"plant": True, "marker": str(marker)})
    after = a_folder(store, "b")
    proc = daemon("plant")
    wait_for(after / "result.json", proc)
    assert not marker.exists()
    assert (after / "output.md").read_text(encoding="utf-8") == "# clean\n"


def test_an_engine_that_passes_its_own_argv_serves_and_does_not_re_exec_without_end(tmp_path):
    folder = a_folder(tmp_path / "store", "k")
    argv = ["--store", str(tmp_path / "store"), "--cache", str(tmp_path / "cache"), "--unjailed"]
    code = "import sys, kitstubs, llm_wiki_engine_kit as k; "
    code += f"sys.exit(k.serve('stub', kitstubs.echo, argv={argv!r}, timings=k.Timings(scan=0.05)))"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(KIT), str(TESTS)])}
    proc = subprocess.Popen([sys.executable, "-c", code], env=env, stderr=subprocess.PIPE, start_new_session=True)
    try:
        deadline = time.monotonic() + 15
        while not (folder / "result.json").exists() and time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.05)
        assert (folder / "result.json").exists()
    finally:
        kill_group(proc.pid)
        proc.communicate()


def test_with_no_nono_the_jailed_daemon_exits_2_and_says_so(tmp_path, daemon):
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    proc = daemon("echo", jailed=True, env={"PATH": str(empty), "LLM_WIKI_NONO": ""})
    out, err = proc.communicate(timeout=WAIT)
    assert proc.returncode == 2
    assert b"no nono on PATH" in err
