"""Stand-in factories the daemon tests name with `--factory kitstubs:<name>`."""

import ctypes
import os
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path


def _record(pid: int) -> None:
    record = os.environ.get("KIT_STUB_RECORD")
    if record:
        with open(record, "a", encoding="utf-8") as held:
            held.write(f"{pid}\n")


def echo(daemon):
    """Copies the body to `out`, with the pid that built the converter and the pid that ran it.

    Each call also appends its pid to the file `KIT_STUB_RECORD` names, wherever it runs.
    """
    built = os.getpid()
    record = os.environ.get("KIT_STUB_RECORD")
    if record:
        with open(record, "a", encoding="utf-8") as held:
            held.write(f"{built}\n")

    def convert(body: Path, out: Path, request: dict):
        text = body.read_text(encoding="utf-8")
        out.write_text(f"{text}\nfactory {built}\nconvert {os.getpid()}\n", encoding="utf-8")
        return None

    return convert


def raising(daemon):
    """Appends its pid to the file `KIT_STUB_RECORD` names, then raises."""

    def convert(body: Path, out: Path, request: dict):
        with open(os.environ["KIT_STUB_RECORD"], "a", encoding="utf-8") as held:
            held.write(f"{os.getpid()}\n")
        raise RuntimeError("the parser broke")

    return convert


def stall(daemon):
    """Writes half an output, then hangs: the test kills it mid-convert. Its pid goes to `KIT_STUB_RECORD`, if set."""

    def convert(body: Path, out: Path, request: dict):
        _record(os.getpid())
        out.write_text("half", encoding="utf-8")
        time.sleep(600)

    return convert


def probe(daemon):
    """Tries what the jail must refuse, and writes one line per attempt."""

    def attempt(action) -> str:
        try:
            action()
        except OSError as exc:
            return f"refused {type(exc).__name__}"
        return "allowed"

    def connect(port: int) -> None:
        with socket.create_connection(("127.0.0.1", port), timeout=5):
            pass

    def convert(body: Path, out: Path, request: dict):
        options = request["options"]
        lines = [
            "write " + attempt(lambda: Path(options["outside"]).write_text("x", encoding="utf-8")),
            "connect " + attempt(lambda: connect(int(options["port"]))),
            "home " + attempt(lambda: Path(options["canary"]).read_text(encoding="utf-8")),
            "home-env " + str(os.environ["HOME"] == os.environ["TMPDIR"]),
        ]
        out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return None

    return convert


def plant(daemon):
    """With `plant` in the options, leaves a module named like the kit in the scratch and the cwd, then raises."""

    def convert(body: Path, out: Path, request: dict):
        options = request.get("options", {})
        if not options.get("plant"):
            out.write_text("# clean\n", encoding="utf-8")
            return None
        code = f"open({options['marker']!r}, 'w').write('ran')\n"
        for root in {os.environ["TMPDIR"], os.getcwd()}:
            planted = Path(root, "llm_wiki_engine_kit")
            try:
                planted.mkdir(exist_ok=True)
                (planted / "__init__.py").write_text(code, encoding="utf-8")
                (planted / "__main__.py").write_text(code, encoding="utf-8")
            except OSError:
                pass
        raise RuntimeError("planted")

    return convert


def crash(daemon):
    """`crash` kills the process with SIGSEGV after `delay` seconds; `vanish` removes the folder mid-convert; `slow`
    waits that many seconds before it writes."""

    def convert(body: Path, out: Path, request: dict):
        options = request.get("options", {})
        if options.get("crash"):
            time.sleep(options.get("delay", 0))
            # No core dump: a core handler such as apport costs about a second per crash, and ignores the limit.
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            if sys.platform == "linux":
                ctypes.CDLL(None).prctl(4, 0, 0, 0, 0)  # PR_SET_DUMPABLE, 0
            # To this thread: `os.kill` lands on the main thread, and the worker could publish before the process dies.
            signal.pthread_kill(threading.get_ident(), signal.SIGSEGV)
        if options.get("vanish"):
            shutil.rmtree(body.parent)
            return None
        time.sleep(options.get("slow", 0))
        out.write_text(body.read_text(encoding="utf-8"), encoding="utf-8")
        return None

    return convert


def broken(daemon):
    raise RuntimeError("the engine does not import")


def deaf(daemon):
    """Ignores SIGTERM in the inner process, then stalls like `stall`: only a SIGKILL ends it."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return stall(daemon)


def spawner(daemon):
    """Starts a long child of its own, records the child's pid, then hangs."""

    def convert(body: Path, out: Path, request: dict):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
        _record(child.pid)
        time.sleep(600)

    return convert


def narrator(daemon):
    """Writes a tagged MP3 and a transcript of the body into the out folder."""

    def convert(body: Path, out: Path, request: dict):
        text = body.read_text(encoding="utf-8")
        (out / "output.mp3").write_bytes(b"ID3\x04\x00\x00\x00\x00\x00\x00" + text.encode("utf-8"))
        (out / "output.vtt").write_text(f"WEBVTT\n\n00:00.000 --> 00:01.000\n{text}\n", encoding="utf-8")
        return None

    return convert
