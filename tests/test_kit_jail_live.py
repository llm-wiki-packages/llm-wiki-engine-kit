"""The kit's own jail: a converter inside it cannot write outside its grants, reach the network or read the home."""

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


def probed(root: Path, port: int, *, jailed: bool) -> list[str]:
    store = root / "store"
    folder = store / "k"
    folder.mkdir(parents=True)
    canary = root / "canary.txt"
    canary.write_text("secret", encoding="utf-8")
    (folder / "body.txt").write_text("hello", encoding="utf-8")
    options = {"outside": str(root / "outside.txt"), "port": port, "canary": str(canary)}
    (folder / "request.json").write_text(json.dumps({"body": "body.txt", "options": options}), encoding="utf-8")
    argv = [sys.executable, "-m", "llm_wiki_engine_kit", "serve", "--store", str(store), "--cache", str(root / "cache")]
    argv += ["--factory", "kitstubs:probe", "--read", str(TESTS)]
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
