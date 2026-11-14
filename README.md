# llm-wiki-engine-kit

The engine kit turns a convert function into a jailed daemon. The daemon
serves one engine's LLM-wiki engine request store. An async
extract engine, such as
[llm-wiki-engine-docling](https://github.com/llm-wiki-packages/llm-wiki-engine-docling),
is a convert function plus this kit.

The kit is one module, `llm_wiki_engine_kit`. It uses the standard library
only.

## Use it in an engine

Depend on the commit sha of a release tag, by git URL, in the engine's
`pyproject.toml`. Name the tag in a comment beside it:

```toml
dependencies = [
    # llm-wiki-engine-kit v0.1.0
    "llm-wiki-engine-kit @ git+https://github.com/llm-wiki-packages/llm-wiki-engine-kit@31e4c5a640f02d1ee826d8e54a20537c5ae9698c",
]
```

Pin the commit, not the tag. A tag can move, and a commit cannot.
`uv tool install` reads no lock, so this line is the engine's only pin.
`git rev-parse <tag>^{commit}` gives a tag's commit.

The repo is public, so the install needs no GitHub credentials.

## The contract: a factory and a converter

The engine passes `serve` a factory. The factory builds the converter. It
runs only in the inner process, inside the jail:

```python
from llm_wiki_engine_kit import serve

def make_converter(daemon: dict):
    import heavy_engine  # imported inside the jail, never before it
    def convert(body: Path, out: Path, request: dict) -> dict | None:
        out.write_text(heavy_engine.to_markdown(body))
        return None
    return convert

serve(engine="docling", factory=make_converter, threads=1, reads=[...], env={...})
```

- `daemon` holds `engine`, `sections`, `store`, `scratch` and `threads`.
- `sections` are the request sections the engine serves. The default is
  `("convert", "transcribe")`, the text sections. `serve` refuses a
  section the host has no outputs for. A request for a section the engine
  does not serve is answered `unsupported`, and never tried.
- The converter gets the body's path, where to write, and the request. The
  request's `section` says which section it is.
- It answers `None` for a converted file. For a file it did not convert, it
  answers `{"reason": <reason>, "traits": [...]}`.
- An exception answers `{"reason": "unreadable", "traits": []}`.

What the converter writes depends on the section:

| Section | Where to write | Files |
| :-- | :-- | :-- |
| `convert`, `transcribe` | a file path | the markdown, UTF-8 |
| `narrate` | an empty folder | `output.mp3`, and `output.vtt` if it has a transcript |

A narrate engine serves only that section:

```python
def make_narrator(daemon: dict):
    import tts  # imported inside the jail
    def narrate(body: Path, out: Path, request: dict) -> dict | None:
        options = request["options"]  # voice, cast, pronounce, tags
        tts.speak(body.read_text(), out / "output.mp3", out / "output.vtt", **options)
        return None
    return narrate

serve(engine="kokoro", factory=make_narrator, sections=["narrate"], reads=[...])
```

The narrate body, `body.txt`, is the text to speak, in UTF-8. A podcast's
lines can carry a `<v Label>` speaker tag. The converter can leave other
files in the folder; the kit removes them.

The kit checks each file as the host checks it, before it publishes any.
It answers a reason, and publishes nothing, where the host would refuse
a file:

| File | Cap | Must |
| :-- | :-- | :-- |
| `output.md` | 64 MiB + 64 KiB | be UTF-8 |
| `output.mp3` | 512 MiB | open with an ID3 tag or an MPEG frame sync |
| `output.vtt` | 64 MiB + 64 KiB | be UTF-8 and open with `WEBVTT` (a byte order mark first is allowed) |

- A file past its cap answers `too_large`.
- A file that fails another check answers `unreadable`. So does a file
  that is not a regular file with one name (a link, a hard link, a FIFO),
  and a narrate answer with no `output.mp3`.
- `serve` reads `--store <dir>`, `--cache <dir>` and `--unjailed` from
  `argv` (default `sys.argv[1:]`). It ignores other arguments, so the
  engine can parse its own.
- `reads` are more trees the jail may read, such as the engine's models.
- `env` is set in the inner process before the factory runs.
- `timeout_seconds` is a convert's time when the request's options name
  none (default 1800).

To try a converter by hand:

```sh
python -m llm_wiki_engine_kit serve --store <dir> --cache <dir> --factory <module:function> [--section <section>] [--read <dir>] [--unjailed]
```

Repeat `--section` for each section the engine serves.

## The jail

`serve` jails itself before it imports anything of the engine. The outer
process runs the same program again under `nono run -s --block-net`. Only
the inner process calls the factory. So no engine code runs before the
jail.

The jail grants:

- read: the interpreter's trees (`sys.prefix`, `sys.base_prefix`), the
  kit, the directory the engine's own package imports from, and the
  engine's `reads`. The engine's import directory is never the home or a
  directory above it.
- write: the engine's request store (`--store`), and one scratch
  directory, `<cache>/tmp/`. The outer process wipes the scratch before
  each start. A converter writes its outputs only in its request folder's
  `.tmp-` names, which are in the store.
- the system paths that nono's default groups grant. These include the
  system temp directory (`/tmp`), which the jail can write.

The jail has no network. The inner process sets `HOME` and `TMPDIR` to the
scratch, plus `HF_HUB_OFFLINE=1`, `DO_NOT_TRACK=1` and
`PYTHONNOUSERSITE=1`. It starts with `-P -s`, so it never imports a file
that a converter wrote into the cwd or the user's site directory.

With no nono, the daemon exits 2 and says so. It never runs unjailed on
its own. `--unjailed` skips the jail, for an author's trials only.

## The guarantee level

The guarantee is "jailed by default, with the jail as the boundary".

One daemon serves every wiki on a machine, because the request store is
machine-wide. So a parser exploit inside the jail can answer any wiki's
pending requests with forged text. It can do that until the inner process
recycles: after 50 files, or after any converter exception. It cannot read
a wiki, the home, the network, or anything outside its grants.

Forged text is a content risk, the same as a hostile document's own text.
The host treats every byte of an answer as hostile when it lands it.

## The request store

The daemon implements the daemon's side of the request store,
`<store>/<machine>/<key>/`. Each machine that submits writes only its own
`<machine>` segment, and the daemon serves every segment. The rules for the
store belong to llm-wiki-plugins: its machine-config contract
(`docs/contracts/dev/machine-config.md`, the section
"`<state>/engine-requests/`"). When this README and that contract disagree,
the contract wins.

The store can be a folder that a sync tool, such as Dropbox, Google Drive
in mirror mode, or rsync, carries from another machine. Files then arrive
in any order, and a file can be half written for a while.

What the daemon does:

1. **Find work.** Every 2 seconds, it lists each machine's segment of the
   store. It takes each request folder that holds `request.json` and no
   `result.json`. A segment name is a machine id: 1 to 64 characters of
   `[a-z0-9-]`, starting with a letter or digit. A folder name is a 64-hex
   key. It skips every other name, such as a sync tool's debris, and a
   link at either level. It opens each level from its parent with
   `O_NOFOLLOW`, so a segment swapped for a link after the scan is never
   followed.
2. **Read the request.** It reads `request.json` with `O_NOFOLLOW` and a
   64 KiB cap. It skips a folder whose request is a link, is not a regular
   file, or is off its shape. A request with no `section` is a `convert`.
   It never writes `body<suffix>` or `request.json`.
3. **Wait for the body.** It converts only when two scans in a row see
   the body unchanged (inode, size, mtime and ctime), its ctime is at
   least 2 seconds old, and its sha256 is the request's `sha256`. Until
   then, the request waits. The daemon does not count an attempt and does
   not answer. It uses the ctime, because the local kernel sets it; a sync
   tool keeps the submitter's mtime, which can be in the future. It hashes
   a body once per change, and per pass of its loop at most one body per
   free worker. The hash runs on the loop's own thread, so new large
   bodies delay the next pass by their hash time.
   - A body that is not a regular file (a link, a directory, a FIFO) is
     answered `unreadable` at once.
   - A body over 2 GiB, the host's own cap, is answered `too_large` and
     never hashed.
   - The hash reads the whole body. On a store that streams files on
     demand, that read can block. Use Google Drive in mirror mode only.
4. **Convert.** It calls the converter in one of `threads` workers. A
   text section's converter writes `.tmp-output.md`. A media section's
   converter writes into the folder `.tmp-out/`, which the kit makes empty
   in the request folder.
5. **Check.** It checks every output file (the table in "The contract"),
   before it publishes any. It reads each file's sha256 and size from the
   one inode it opened, never through a link.
6. **Answer, result last.** It renames each output file into place, then
   writes `result.json`. A text answer has `output.md`. A narrate answer
   has `output.mp3`, then `output.vtt` if there is one. After each rename
   it checks that the name holds the inode it read; if not, it removes what
   it published and answers `unreadable`. It writes `result.json` under a
   `.tmp-` name first, then renames it into place. A converted answer's
   `result.json` maps each output file to its sha256 and size:
   `"outputs": {"output.md": {"sha256": "...", "size": 123}}`, or
   `"outputs": {"output.mp3": {...}, "output.vtt": {...}}`. The host lands
   it only when every listed file is present and matches.

The daemon writes only the section's output files, `result.json` and its
own `.tmp-` names. It removes its `.tmp-` names before it writes
`result.json`, except `.tmp-attempts`, which it removes after. It never
reads or removes any other name in a request folder. The `.tmp-` names can
sync to the host; the host never reads them.

How it handles failures:

- **Timeout.** Each convert gets `timeout_seconds`. A request's options can
  name their own. Past it, the outer process kills the inner process, and
  that counts as an attempt.
- **Attempts.** The daemon keeps an attempt count in the folder under a
  `.tmp-` name. A folder whose convert ended the inner process twice is
  answered `unreadable` at the next start.
- **Stop.** A stop takes back the attempts in progress. A stop not done in
  10 seconds ends with a kill of the inner process's group.
- **Crashes.** The outer process starts a crashed inner process again after
  a backoff, doubled per crash up to 30 seconds. Five inner processes in a
  row that answer no file stop the daemon with a non-zero exit, so its
  service reads as failed.

## Environment variables

| Variable | Meaning |
| :-- | :-- |
| `LLM_WIKI_NONO` | the absolute path of nono. The engine's user service sets it. The kit reads it before `PATH`, and a blank value counts as unset. With neither, the daemon exits 2. |
| `LLM_WIKI_ENGINE_KIT_INNER` | set to `1` by the outer process on the inner process it starts. The inner process removes it when it reads it. It marks the inner process, so an engine that passes `serve` its own argv does not re-exec again. |

## Tests

```sh
uv run --locked pytest -q
```

The `sandbox` rows (`tests/test_kit_jail_live.py`) run the real jail. They
need nono on `PATH` and a kernel that enforces the jail. They fail, and do
not skip, without nono. CI installs a pinned nono on Ubuntu and runs every
row.

## License

MIT. See `LICENSE`.
