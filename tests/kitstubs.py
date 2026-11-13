"""Stand-in factories the daemon tests name with `--factory kitstubs:<name>`."""

import os
import socket
import time
from pathlib import Path


def echo(daemon):
    """Copies the body to `out`, with the pid that built the converter and the pid that ran it."""
    built = os.getpid()

    def convert(body: Path, out: Path, request: dict):
        text = body.read_text(encoding="utf-8")
        out.write_text(f"{text}\nfactory {built}\nconvert {os.getpid()}\n", encoding="utf-8")
        return None

    return convert


def raising(daemon):
    """Leaves its pid in the scratch, then raises."""

    def convert(body: Path, out: Path, request: dict):
        Path(os.environ["TMPDIR"], f"pid-{os.getpid()}").touch()
        raise RuntimeError("the parser broke")

    return convert


def stall(daemon):
    """Writes half an output, then hangs: the test kills it mid-convert."""

    def convert(body: Path, out: Path, request: dict):
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
