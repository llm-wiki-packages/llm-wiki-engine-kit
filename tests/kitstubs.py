"""Stand-in factories the daemon tests name with `--factory kitstubs:<name>`."""

import os
import shutil
import signal
import socket
import time
from pathlib import Path


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
    """`crash` in the options kills the process with SIGSEGV; `vanish` removes the folder mid-convert."""

    def convert(body: Path, out: Path, request: dict):
        options = request.get("options", {})
        if options.get("crash"):
            os.kill(os.getpid(), signal.SIGSEGV)
        if options.get("vanish"):
            shutil.rmtree(body.parent)
            return None
        out.write_text(body.read_text(encoding="utf-8"), encoding="utf-8")
        return None

    return convert
