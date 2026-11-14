"""The kit's own jail: a converter inside it cannot write outside its grants, reach the network or read the home."""

import contextlib
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

import llm_wiki_engine_kit as kit

pytestmark = pytest.mark.sandbox

KIT = Path(kit.__file__).resolve().parents[1]
TESTS = Path(__file__).resolve().parent
# Outside the temp dir, which nono's system groups grant: there a refused write could not be refused.
LIVE_ROOT = Path.home() / ".cache" / "llm-wiki-next-live"
WAIT = 60


@pytest.fixture
def live():
    if shutil.which("nono") is None:
        pytest.fail("no nono on PATH; this module is the `sandbox` lane, so its absence is a broken box")
    root = LIVE_ROOT / f"kit-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    root.mkdir(parents=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def listener():
    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen(8)
    yield held.getsockname()[1]
    held.close()


def request_folder(store: Path, name: str, options: dict) -> Path:
    """A request at `<store>/<machine>/<key>/`, as a host submits it."""
    folder = store / "box-a" / name.encode("utf-8").hex().ljust(64, "0")
    folder.mkdir(parents=True)
    (folder / "body.txt").write_text("hello", encoding="utf-8")
    request = {"body": "body.txt", "sha256": hashlib.sha256(b"hello").hexdigest(), "options": options}
    (folder / "request.json").write_text(json.dumps(request), encoding="utf-8")
    return folder


def probed(root: Path, port: int, *, jailed: bool) -> list[str]:
    store = root / "store"
    canary = root / "canary.txt"
    canary.write_text("secret", encoding="utf-8")
    options = {"outside": str(root / "outside.txt"), "port": port, "canary": str(canary)}
    folder = request_folder(store, "k", options)
    argv = [sys.executable, "-m", "llm_wiki_engine_kit", "serve", "--store", str(store), "--cache", str(root / "cache")]
    argv += ["--factory", "kitstubs:probe", "--read", str(TESTS), "--scan-seconds", "0.05", "--poll-seconds", "0.05"]
    if not jailed:
        argv.append("--unjailed")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(KIT), str(TESTS)])}
    proc = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        deadline = time.monotonic() + WAIT
        while not (folder / "result.json").exists():
            if proc.poll() is not None:
                pytest.fail(f"the daemon exited {proc.returncode}: {proc.communicate()[1].decode()[-2000:]}")
            if time.monotonic() > deadline:
                pytest.fail(f"no answer after {WAIT}s")
            time.sleep(0.05)
    finally:
        if proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
    return (folder / "output.md").read_text(encoding="utf-8").splitlines()


def test_unjailed_the_probe_reaches_all_three(live, listener):
    lines = probed(live, listener, jailed=False)
    assert lines[:3] == ["write allowed", "connect allowed", "home allowed"]


def test_jailed_the_probe_reaches_none_of_them(live, listener):
    lines = probed(live, listener, jailed=True)
    assert [line.split()[:2] for line in lines[:3]] == [["write", "refused"], ["connect", "refused"], ["home", "refused"]]
    assert lines[3] == "home-env True"
    assert not (live / "outside.txt").exists()


def test_jailed_a_module_the_converter_plants_never_runs_in_the_next_inner_process(live):
    store = live / "store"
    marker = store / "planted-ran"
    first = request_folder(store, "a", {"plant": True, "marker": str(marker)})
    second = request_folder(store, "b", {})
    argv = [sys.executable, "-m", "llm_wiki_engine_kit", "serve", "--store", str(store), "--cache", str(live / "cache")]
    argv += ["--factory", "kitstubs:plant", "--read", str(TESTS), "--scan-seconds", "0.05", "--poll-seconds", "0.05"]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(KIT), str(TESTS)])}
    proc = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        deadline = time.monotonic() + WAIT
        while not (second / "result.json").exists():
            if proc.poll() is not None:
                pytest.fail(f"the daemon exited {proc.returncode}: {proc.communicate()[1].decode()[-2000:]}")
            if time.monotonic() > deadline:
                pytest.fail(f"no answer after {WAIT}s")
            time.sleep(0.05)
    finally:
        if proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
    assert json.loads((first / "result.json").read_text(encoding="utf-8"))["reason"] == "unreadable"
    assert not marker.exists()
    assert (second / "output.md").read_text(encoding="utf-8") == "# clean\n"


def test_jailed_an_inner_process_whose_outer_was_killed_ends_when_a_new_outer_takes_the_scratch(live):
    store = live / "store"
    store.mkdir()
    record = store / ".pids"
    argv = [sys.executable, "-m", "llm_wiki_engine_kit", "serve", "--store", str(store), "--cache", str(live / "cache")]
    # A scan slower than the new outer's start, as the default is: the new lock is in place at the orphan's next look.
    argv += ["--factory", "kitstubs:echo", "--read", str(TESTS), "--scan-seconds", "3", "--poll-seconds", "0.05"]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(KIT), str(TESTS)]), "KIT_STUB_RECORD": str(record)}
    # No pipes: an orphan would hold them open, and reading them would wait on it.
    quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL, "start_new_session": True}
    first = subprocess.Popen(argv, env=env, **quiet)
    second, orphan = None, None
    try:
        deadline = time.monotonic() + WAIT
        while not record.exists() or not record.read_text(encoding="utf-8").strip():
            assert first.poll() is None, f"the daemon exited {first.returncode}"
            assert time.monotonic() < deadline, "the first inner process built no converter"
            time.sleep(0.05)
        orphan = int(record.read_text(encoding="utf-8").split()[0])
        # The outer alone, as a service manager's kill of its main process: the jail's runtime stays the parent.
        os.kill(first.pid, signal.SIGKILL)
        first.wait(WAIT)
        second = subprocess.Popen(argv, env=env, **quiet)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                os.kill(orphan, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            pytest.fail("the killed outer's inner process still runs beside the new outer's")
    finally:
        for proc in (first, second):
            if proc is not None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(WAIT)
        if orphan is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(orphan), signal.SIGKILL)


def test_jailed_a_narrate_converter_writes_its_audio_into_the_requests_out_folder(live):
    store = live / "store"
    folder = request_folder(store, "k", {})
    request = json.loads((folder / "request.json").read_text(encoding="utf-8"))
    (folder / "request.json").write_text(json.dumps({**request, "section": "narrate"}), encoding="utf-8")
    argv = [sys.executable, "-m", "llm_wiki_engine_kit", "serve", "--store", str(store), "--cache", str(live / "cache")]
    argv += ["--factory", "kitstubs:narrator", "--section", "narrate", "--read", str(TESTS)]
    argv += ["--scan-seconds", "0.05", "--poll-seconds", "0.05"]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(KIT), str(TESTS)])}
    proc = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        deadline = time.monotonic() + WAIT
        while not (folder / "result.json").exists():
            if proc.poll() is not None:
                pytest.fail(f"the daemon exited {proc.returncode}: {proc.communicate()[1].decode()[-2000:]}")
            if time.monotonic() > deadline:
                pytest.fail(f"no answer after {WAIT}s")
            time.sleep(0.05)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
    result = json.loads((folder / "result.json").read_text(encoding="utf-8"))
    assert set(result["outputs"]) == {"output.mp3", "output.vtt"}
    assert (folder / "output.mp3").read_bytes().startswith(b"ID3")
    assert not (folder / ".tmp-out").exists()
