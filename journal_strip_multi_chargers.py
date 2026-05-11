#!/usr/bin/env python3
"""
journal_strip_multi_chargers.py

Reads chargepoint.service journal entries filtered by the pattern
'04 64 <byte>' (excluding 10, 11, e1), then:
  1. Saves a full journal file: <custom_id>_<start>_<end>_journal.log
  2. Detects CHAdeMO (start: 81, end: ad) and CCS (start: 32, end: 22)
     charging sessions and saves each to its own file:
     <custom_id>_<start>_<end>_chademo.log
     <custom_id>_<start>_<end>_ccs.log

Multi-charger behavior:
  - Looks for carregadores.dsv in the current directory.
  - Expected format:
        custom_id, ip
        "politécnico", 10.53.1.21
        "outro carregador", 10.53.1.22

SSH behavior:
  - asyncssh
  - default user: admin
  - default port: 5022
  - default key: ~/.ssh/id_ed25519_cp
  - known_hosts=None

Progress/log/UI behavior:
  - --since and --until limit the journalctl time window.
  - --progress streams stdout and periodically reports raw/matching line counts.
  - A timestamped log file is created by default:
        yyyy-mm-dd_hh-mm-ss_journal-split-multi.log
  - --textual opens a Textual table with one progress/status row per charger.
  - --no-log disables the log file.

Important:
  - Textual is run outside asyncio.run(), avoiding nested event loop errors.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import shlex
import subprocess
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import asyncssh  # type: ignore
except ImportError:
    asyncssh = None  # type: ignore

try:
    from textual.app import App, ComposeResult
    from textual.widgets import DataTable, Footer, Static
except ImportError:
    App = None  # type: ignore
    ComposeResult = Any  # type: ignore
    DataTable = None  # type: ignore
    Footer = None  # type: ignore
    Static = None  # type: ignore


CHARGERS_FILE = "carregadores.dsv"
DEFAULT_SSH_USER = "admin"
DEFAULT_SSH_PORT = 5022
DEFAULT_SSH_KEY = Path.home() / ".ssh" / "id_ed25519_cp"
DEFAULT_PROGRESS_INTERVAL_S = 2.0

GREP_PATTERN = re.compile(r"04 64 (?!10|11|e1)[0-9a-f]{2}.*")
START_BYTES = {"81": "chademo", "32": "ccs"}
END_BYTES = {"chademo": "ad", "ccs": "22"}


@dataclass(frozen=True)
class Charger:
    custom_id: str
    ip: str


@dataclass
class JournalReadStats:
    raw_lines: int = 0
    matching_lines: int = 0
    elapsed_s: float = 0.0


@dataclass(frozen=True)
class ChargerEvent:
    custom_id: str
    ip: str
    status: str
    raw_lines: int = 0
    matching_lines: int = 0
    sessions: int = 0
    files: int = 0
    elapsed_s: float = 0.0
    message: str = ""


@dataclass
class ProcessResult:
    raw_lines: int = 0
    matching_lines: int = 0
    sessions: int = 0
    files: int = 0
    elapsed_s: float = 0.0
    ok: bool = True
    message: str = ""


class EventLogger:
    """Small timestamped text logger used by both plain and Textual modes."""

    def __init__(self, path: Path | None) -> None:
        self.path = path
        self._handle = None

        if self.path is not None:
            self._handle = self.path.open("a", encoding="utf-8")

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def write(self, text: str) -> None:
        if self._handle is None:
            return

        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._handle.write(f"{stamp} {text}\n")
        self._handle.flush()

    def event(self, event: ChargerEvent) -> None:
        self.write(
            f"[{event.custom_id}] status={event.status} ip={event.ip} "
            f"raw={event.raw_lines} matching={event.matching_lines} "
            f"sessions={event.sessions} files={event.files} "
            f"elapsed={format_elapsed(event.elapsed_s)} message={event.message}"
        )


class EventHub:
    """Dispatches events to log file, console and optional Textual queue."""

    def __init__(
        self,
        logger: EventLogger,
        *,
        console: bool,
        progress: bool,
        queue: asyncio.Queue[ChargerEvent] | None = None,
    ) -> None:
        self.logger = logger
        self.console = console
        self.progress = progress
        self.queue = queue

    async def emit(self, event: ChargerEvent) -> None:
        self.logger.event(event)

        if self.queue is not None:
            await self.queue.put(event)

        if self.console:
            self._print_event(event)

    def _print_event(self, event: ChargerEvent) -> None:
        if event.status == "reading" and self.progress:
            print(
                f"[{event.custom_id}] raw={event.raw_lines} "
                f"matching={event.matching_lines} "
                f"elapsed={format_elapsed(event.elapsed_s)}",
                flush=True,
            )
            return

        if event.status == "connecting":
            print(f"\n[{event.custom_id}] Connecting to {event.ip}...", flush=True)
            return

        if event.status == "command":
            print(f"[{event.custom_id}] Running: {event.message}", flush=True)
            return

        if event.status == "processing":
            print(f"[{event.custom_id}] Processing...", flush=True)
            return

        if event.status == "saving":
            print(f"[{event.custom_id}] {event.message}", flush=True)
            return

        if event.status == "done":
            print(
                f"[{event.custom_id}] Done. raw={event.raw_lines} "
                f"matching={event.matching_lines} sessions={event.sessions} "
                f"files={event.files} elapsed={format_elapsed(event.elapsed_s)}",
                flush=True,
            )
            return

        if event.status == "failed":
            print(f"[{event.custom_id}] FAILED: {event.message}", file=sys.stderr, flush=True)
            return

        if event.message:
            print(f"[{event.custom_id}] {event.status}: {event.message}", flush=True)


def make_default_log_path() -> Path:
    """Return yyyy-mm-dd_hh-mm-ss_journal-split-multi.log."""
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return Path(f"{stamp}_journal-split-multi.log")


def validate_unique_custom_ids(chargers: list[Charger]) -> None:
    """Refuse duplicate custom_id values to avoid output file collisions."""
    seen: set[str] = set()
    duplicates: set[str] = set()

    for charger in chargers:
        if charger.custom_id in seen:
            duplicates.add(charger.custom_id)
        seen.add(charger.custom_id)

    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(
            "Duplicate custom_id value(s) in charger list: "
            f"{duplicate_list}. Each custom_id must be unique because it is "
            "used as the output filename prefix."
        )


def load_chargers(path: Path) -> list[Charger]:
    """Load chargers from a CSV/DSV-like file with columns custom_id, ip."""
    if not path.exists():
        raise FileNotFoundError(
            f"Charger list file not found: {path}\n"
            f"Create {CHARGERS_FILE} in the current directory."
        )

    raw_lines = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    if not raw_lines:
        raise ValueError(f"Charger list file is empty: {path}")

    reader = csv.reader(raw_lines, delimiter=",", skipinitialspace=True)
    rows = [row for row in reader if row]

    if not rows:
        raise ValueError(f"No valid rows found in charger list file: {path}")

    first = [cell.strip().lower() for cell in rows[0]]
    has_header = len(first) >= 2 and first[0] == "custom_id" and first[1] == "ip"
    data_rows = rows[1:] if has_header else rows

    chargers: list[Charger] = []

    for line_number, row in enumerate(data_rows, start=2 if has_header else 1):
        if len(row) < 2:
            print(
                f"Skipping invalid row {line_number}: expected 2 columns, got {len(row)}",
                file=sys.stderr,
            )
            continue

        custom_id = row[0].strip().strip('"').strip("'")
        ip = row[1].strip().strip('"').strip("'")

        if not custom_id or not ip:
            print(f"Skipping invalid row {line_number}: empty custom_id or ip", file=sys.stderr)
            continue

        chargers.append(Charger(custom_id=custom_id, ip=ip))

    if not chargers:
        raise ValueError(f"No valid chargers found in charger list file: {path}")

    validate_unique_custom_ids(chargers)
    return chargers


def safe_filename_part(text: str) -> str:
    """Convert custom_id to a filesystem-safe prefix."""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_text).strip("._-")
    return safe or "charger"


def build_journalctl_command(since: str | None = None, until: str | None = None) -> str:
    """Build a shell-safe remote journalctl command."""
    parts = ["journalctl", "-u", "chargepoint.service", "--no-pager"]

    if since:
        parts.extend(["--since", shlex.quote(since)])

    if until:
        parts.extend(["--until", shlex.quote(until)])

    return " ".join(parts)


def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as HH:MM:SS."""
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def filter_one_journal_line(raw: str) -> str | None:
    """Return the matching payload portion of a journal line, or None."""
    m = GREP_PATTERN.search(raw)
    return m.group(0) if m else None


def filter_journal_output(stdout: str) -> list[str]:
    """Return only the payload portion matching GREP_PATTERN from journal output."""
    lines: list[str] = []

    for raw in stdout.splitlines():
        matched = filter_one_journal_line(raw)
        if matched:
            lines.append(matched)

    return lines


def run_local_journal(since: str | None = None, until: str | None = None) -> list[str]:
    """Run local journalctl and return filtered lines."""
    command = build_journalctl_command(since=since, until=until)
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return filter_journal_output(result.stdout)


async def run_remote_journal(
    charger: Charger,
    args: argparse.Namespace,
    hub: EventHub,
) -> tuple[list[str], JournalReadStats, bool, str]:
    """Run journalctl remotely over SSH using asyncssh and return filtered lines."""
    stats = JournalReadStats()
    lines: list[str] = []
    start_time = time.monotonic()
    last_progress_time = start_time

    if asyncssh is None:
        message = "Missing dependency: asyncssh. Install it with: python3 -m pip install asyncssh"
        await hub.emit(ChargerEvent(charger.custom_id, charger.ip, "failed", message=message))
        return lines, stats, False, message

    if not args.ssh_key.exists():
        message = f"SSH key not found: {args.ssh_key}"
        await hub.emit(ChargerEvent(charger.custom_id, charger.ip, "failed", message=message))
        return lines, stats, False, message

    command = build_journalctl_command(since=args.since, until=args.until)
    await hub.emit(ChargerEvent(charger.custom_id, charger.ip, "command", message=command))

    try:
        conn = await asyncssh.connect(
            charger.ip,
            port=args.ssh_port,
            username=args.ssh_user,
            client_keys=[str(args.ssh_key)],
            known_hosts=None,
            connect_timeout=args.connect_timeout,
        )

        try:
            process = await conn.create_process(command)

            async def consume_stdout() -> None:
                nonlocal last_progress_time

                async for raw_line in process.stdout:
                    stats.raw_lines += 1
                    matched = filter_one_journal_line(raw_line)

                    if matched:
                        lines.append(matched)
                        stats.matching_lines += 1

                    stats.elapsed_s = time.monotonic() - start_time
                    now = time.monotonic()

                    if args.progress and (now - last_progress_time) >= args.progress_interval:
                        last_progress_time = now
                        await hub.emit(
                            ChargerEvent(
                                charger.custom_id,
                                charger.ip,
                                "reading",
                                raw_lines=stats.raw_lines,
                                matching_lines=stats.matching_lines,
                                elapsed_s=stats.elapsed_s,
                            )
                        )

            await asyncio.wait_for(consume_stdout(), timeout=args.command_timeout)
            await asyncio.wait_for(process.wait(), timeout=10)

            if process.exit_status != 0:
                stderr = ""

                if process.stderr is not None:
                    stderr = await process.stderr.read()

                message = f"journal command failed, exit={process.exit_status}: {stderr.strip()}"
                await hub.emit(
                    ChargerEvent(
                        charger.custom_id,
                        charger.ip,
                        "failed",
                        raw_lines=stats.raw_lines,
                        matching_lines=stats.matching_lines,
                        elapsed_s=stats.elapsed_s,
                        message=message,
                    )
                )
                return lines, stats, False, message

        finally:
            conn.close()
            await conn.wait_closed()

    except asyncio.TimeoutError:
        message = f"SSH/journal timeout after {args.command_timeout}s"
        await hub.emit(
            ChargerEvent(
                charger.custom_id,
                charger.ip,
                "failed",
                raw_lines=stats.raw_lines,
                matching_lines=stats.matching_lines,
                elapsed_s=stats.elapsed_s,
                message=message,
            )
        )
        return lines, stats, False, message

    except (asyncssh.Error, OSError) as exc:  # type: ignore[union-attr]
        message = f"SSH connection failed: {exc}"
        await hub.emit(
            ChargerEvent(
                charger.custom_id,
                charger.ip,
                "failed",
                raw_lines=stats.raw_lines,
                matching_lines=stats.matching_lines,
                elapsed_s=stats.elapsed_s,
                message=message,
            )
        )
        return lines, stats, False, message

    stats.elapsed_s = time.monotonic() - start_time

    if args.progress:
        await hub.emit(
            ChargerEvent(
                charger.custom_id,
                charger.ip,
                "reading",
                raw_lines=stats.raw_lines,
                matching_lines=stats.matching_lines,
                elapsed_s=stats.elapsed_s,
            )
        )

    return lines, stats, True, ""


def get_dt(line: str) -> str:
    """Extract datetime from field 29 (index 28)."""
    fields = line.split()

    if len(fields) > 28:
        dt_str = fields[28].split(".")[0]
        return dt_str.replace("T", "_").replace(":", "-")

    return "unknown"


def get_byte(line: str) -> str | None:
    """Return the byte string at field 3 (index 2)."""
    fields = line.split()
    return fields[2] if len(fields) > 2 else None


def save_file(lines: list[str], start_dt: str, end_dt: str, suffix: str, prefix: str = "") -> str:
    """Save lines to a log file and return the filename."""
    clean_prefix = safe_filename_part(prefix) if prefix else ""

    if clean_prefix:
        filename = f"{clean_prefix}_{start_dt}_{end_dt}_{suffix}.log"
    else:
        filename = f"{start_dt}_{end_dt}_{suffix}.log"

    Path(filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return filename


def detect_sessions(lines: list[str]) -> list[tuple[str, str, str, list[str]]]:
    """Scan lines and group them into charging sessions."""
    sessions: list[tuple[str, str, str, list[str]]] = []
    current_session: str | None = None
    current_lines: list[str] = []
    current_start_dt: str | None = None
    last_byte: str | None = None

    def close_current() -> None:
        if current_session is None or current_start_dt is None or not current_lines:
            return

        end_dt = get_dt(current_lines[-1])
        sessions.append((current_session, current_start_dt, end_dt, list(current_lines)))

    for line in lines:
        byte = get_byte(line)
        dt = get_dt(line)

        if byte in START_BYTES:
            new_type = START_BYTES[byte]

            if current_session is None:
                current_session = new_type
                current_lines = [line]
                current_start_dt = dt

            elif new_type != current_session:
                close_current()
                current_session = new_type
                current_lines = [line]
                current_start_dt = dt

            else:
                if last_byte == END_BYTES[current_session]:
                    close_current()
                    current_session = new_type
                    current_lines = [line]
                    current_start_dt = dt
                else:
                    current_lines.append(line)

        else:
            if current_session is not None:
                current_lines.append(line)

        last_byte = byte

    if current_session and current_lines:
        close_current()

    return sessions


async def process_lines(
    lines: list[str],
    prefix: str,
    charger: Charger,
    hub: EventHub,
    stats: JournalReadStats,
) -> ProcessResult:
    """Save full journal and detected session files for one charger/source."""
    if not lines:
        message = "No matching lines found."
        await hub.emit(
            ChargerEvent(
                charger.custom_id,
                charger.ip,
                "done",
                raw_lines=stats.raw_lines,
                matching_lines=stats.matching_lines,
                elapsed_s=stats.elapsed_s,
                message=message,
            )
        )
        return ProcessResult(
            raw_lines=stats.raw_lines,
            matching_lines=stats.matching_lines,
            elapsed_s=stats.elapsed_s,
            ok=True,
            message=message,
        )

    files = 0
    sessions = detect_sessions(lines)

    filename = save_file(lines, get_dt(lines[0]), get_dt(lines[-1]), "journal", prefix=prefix)
    files += 1
    await hub.emit(
        ChargerEvent(
            charger.custom_id,
            charger.ip,
            "saving",
            raw_lines=stats.raw_lines,
            matching_lines=stats.matching_lines,
            sessions=len(sessions),
            files=files,
            elapsed_s=stats.elapsed_s,
            message=f"Saved: {filename} ({len(lines)} lines)",
        )
    )

    for session_type, start_dt, end_dt, session_lines in sessions:
        filename = save_file(session_lines, start_dt, end_dt, session_type, prefix=prefix)
        files += 1
        await hub.emit(
            ChargerEvent(
                charger.custom_id,
                charger.ip,
                "saving",
                raw_lines=stats.raw_lines,
                matching_lines=stats.matching_lines,
                sessions=len(sessions),
                files=files,
                elapsed_s=stats.elapsed_s,
                message=f"Saved: {filename} ({len(session_lines)} lines)",
            )
        )

    message = f"Saved {files} file(s)."
    await hub.emit(
        ChargerEvent(
            charger.custom_id,
            charger.ip,
            "done",
            raw_lines=stats.raw_lines,
            matching_lines=stats.matching_lines,
            sessions=len(sessions),
            files=files,
            elapsed_s=stats.elapsed_s,
            message=message,
        )
    )

    return ProcessResult(
        raw_lines=stats.raw_lines,
        matching_lines=stats.matching_lines,
        sessions=len(sessions),
        files=files,
        elapsed_s=stats.elapsed_s,
        ok=True,
        message=message,
    )


async def process_charger(
    index: int,
    total: int,
    charger: Charger,
    args: argparse.Namespace,
    hub: EventHub,
) -> ProcessResult:
    """Collect and process one charger."""
    await hub.emit(
        ChargerEvent(
            charger.custom_id,
            charger.ip,
            "connecting",
            message=f"{index}/{total} {charger.ip}:{args.ssh_port}",
        )
    )

    lines, stats, ok, message = await run_remote_journal(charger, args, hub)

    if not ok:
        return ProcessResult(
            raw_lines=stats.raw_lines,
            matching_lines=stats.matching_lines,
            elapsed_s=stats.elapsed_s,
            ok=False,
            message=message,
        )

    await hub.emit(
        ChargerEvent(
            charger.custom_id,
            charger.ip,
            "processing",
            raw_lines=stats.raw_lines,
            matching_lines=stats.matching_lines,
            elapsed_s=stats.elapsed_s,
        )
    )

    return await process_lines(lines, charger.custom_id, charger, hub, stats)


async def process_charger_with_limit(
    semaphore: asyncio.Semaphore,
    index: int,
    total: int,
    charger: Charger,
    args: argparse.Namespace,
    hub: EventHub,
) -> ProcessResult:
    """Run one charger task while respecting the max-parallel semaphore."""
    async with semaphore:
        return await process_charger(
            index=index,
            total=total,
            charger=charger,
            args=args,
            hub=hub,
        )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Extract chargepoint.service journal payloads and split charging sessions. "
            "By default, reads carregadores.dsv and collects logs from each charger over SSH."
        )
    )

    parser.add_argument("--chargers-file", default=CHARGERS_FILE)
    parser.add_argument("--ssh-user", default=DEFAULT_SSH_USER)
    parser.add_argument("--ssh-port", type=int, default=DEFAULT_SSH_PORT)
    parser.add_argument("--ssh-key", type=Path, default=DEFAULT_SSH_KEY)
    parser.add_argument("--connect-timeout", type=int, default=3)
    parser.add_argument("--command-timeout", type=int, default=180)

    parser.add_argument(
        "--since",
        default=None,
        help='Limit journalctl with --since, e.g. "2 days ago" or "2026-05-01 00:00:00".',
    )

    parser.add_argument(
        "--until",
        default=None,
        help='Limit journalctl with --until, e.g. "2026-05-02 00:00:00".',
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help="Stream remote journal output and print raw/matching line counters.",
    )

    parser.add_argument(
        "--progress-interval",
        type=float,
        default=DEFAULT_PROGRESS_INTERVAL_S,
        help=f"Seconds between progress prints. Default: {DEFAULT_PROGRESS_INTERVAL_S}",
    )

    parser.add_argument(
        "--max-parallel",
        type=int,
        default=0,
        help=(
            "Maximum simultaneous SSH collections. "
            "Use 0 to process all chargers in parallel. Default: 0."
        ),
    )

    parser.add_argument(
        "--textual",
        action="store_true",
        help="Show a Textual table with one status/progress row per charger.",
    )

    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file path. Default: timestamped *_journal-split-multi.log.",
    )

    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Disable timestamped log file creation.",
    )

    parser.add_argument(
        "--local",
        action="store_true",
        help="Run only against the local machine. In this mode carregadores.dsv is not used.",
    )

    return parser.parse_args()


def resolve_max_parallel(requested: int, charger_count: int) -> int:
    """Convert --max-parallel into an effective concurrency value."""
    if requested <= 0:
        return charger_count

    return min(requested, charger_count)


def make_logger(args: argparse.Namespace) -> EventLogger:
    """Create the logger according to CLI options."""
    if args.no_log:
        return EventLogger(None)

    return EventLogger(args.log_file or make_default_log_path())


async def run_collection(
    chargers: list[Charger],
    args: argparse.Namespace,
    hub: EventHub,
) -> list[ProcessResult]:
    """Run all charger tasks with the configured concurrency."""
    max_parallel = resolve_max_parallel(args.max_parallel, len(chargers))
    semaphore = asyncio.Semaphore(max_parallel)

    hub.logger.write(f"START journal_split_multi chargers={len(chargers)}")
    hub.logger.write(f"ssh user={args.ssh_user} port={args.ssh_port} key={args.ssh_key}")
    hub.logger.write(f"max_parallel={max_parallel}")
    hub.logger.write(f"window since={args.since or '-'} until={args.until or '-'}")

    return await asyncio.gather(
        *(
            process_charger_with_limit(
                semaphore=semaphore,
                index=index,
                total=len(chargers),
                charger=charger,
                args=args,
                hub=hub,
            )
            for index, charger in enumerate(chargers, start=1)
        )
    )


def print_startup(chargers: list[Charger], args: argparse.Namespace, logger: EventLogger) -> None:
    """Print plain-mode startup details."""
    max_parallel = resolve_max_parallel(args.max_parallel, len(chargers))

    print(f"Loaded {len(chargers)} charger(s) from {args.chargers_file}")
    print(f"Using SSH key: {args.ssh_key}")
    print(f"Using SSH user/port: {args.ssh_user}/{args.ssh_port}")
    print(f"Using max parallel SSH collections: {max_parallel}")

    if logger.path is not None:
        print(f"Writing log file: {logger.path}")

    if args.since or args.until:
        print(f"Using journal window: since={args.since or '-'} until={args.until or '-'}")


async def run_plain(args: argparse.Namespace, chargers: list[Charger], logger: EventLogger) -> None:
    """Run the current terminal-oriented mode."""
    print_startup(chargers, args, logger)
    hub = EventHub(logger, console=True, progress=args.progress)
    results = await run_collection(chargers, args, hub)
    ok_count = sum(1 for result in results if result.ok)
    failed_count = len(results) - ok_count
    logger.write(f"END journal_split_multi ok={ok_count} failed={failed_count}")
    print(f"\nDone. ok={ok_count} failed={failed_count}")


async def run_local(args: argparse.Namespace, logger: EventLogger) -> None:
    """Run local journal mode."""
    print("Reading local journal (this may take a moment)...")

    if logger.path is not None:
        print(f"Writing log file: {logger.path}")

    if args.since or args.until:
        print(f"Journal window: since={args.since or '-'} until={args.until or '-'}")

    logger.write("START local journal_split")
    lines = run_local_journal(since=args.since, until=args.until)
    stats = JournalReadStats(raw_lines=len(lines), matching_lines=len(lines), elapsed_s=0.0)
    charger = Charger("local", "localhost")
    hub = EventHub(logger, console=True, progress=args.progress)
    await process_lines(lines, "local", charger, hub, stats)
    logger.write("END local journal_split")
    print("\nDone.")


if App is not None:

    class JournalSplitTextualApp(App):  # type: ignore[misc]
        """Textual interface with one progress/status row per charger."""

        CSS = """
        #header {
            height: 3;
            padding: 1;
            background: $boost;
        }

        #summary {
            height: 1;
            padding-left: 1;
        }

        DataTable {
            height: 1fr;
        }
        """

        BINDINGS = [
            ("q", "quit", "Quit"),
        ]

        def __init__(
            self,
            chargers: list[Charger],
            args: argparse.Namespace,
            logger: EventLogger,
        ) -> None:
            super().__init__()
            self.chargers = chargers
            self.args = args
            self.logger = logger
            self.queue: asyncio.Queue[ChargerEvent] = asyncio.Queue()
            self.results: list[ProcessResult] = []

        def compose(self) -> ComposeResult:
            log_text = str(self.logger.path) if self.logger.path else "disabled"
            yield Static(
                f"Journal Split Multi\n"
                f"chargers={len(self.chargers)}  "
                f"max_parallel={resolve_max_parallel(self.args.max_parallel, len(self.chargers))}  "
                f"log={log_text}",
                id="header",
            )
            yield DataTable()
            yield Static("pending", id="summary")
            yield Footer()

        def on_mount(self) -> None:
            self.table = self.query_one(DataTable)
            self.summary = self.query_one("#summary", Static)
            self.table.cursor_type = None
            self.table.zebra_stripes = True

            columns = [
                ("ID", "id", 14),
                ("IP", "ip", 13),
                ("Status", "status", 12),
                ("Raw", "raw", 10),
                ("Matching", "matching", 10),
                ("Sessions", "sessions", 8),
                ("Files", "files", 6),
                ("Elapsed", "elapsed", 9),
                ("Message", "message", 60),
            ]

            for label, key, width in columns:
                self.table.add_column(label, key=key, width=width)

            for charger in self.chargers:
                self.table.add_row(
                    charger.custom_id,
                    charger.ip,
                    "pending",
                    "0",
                    "0",
                    "0",
                    "0",
                    "00:00:00",
                    "",
                    key=charger.custom_id,
                )

            self.run_worker(self.run_textual_collection(), exclusive=True)

        async def run_textual_collection(self) -> None:
            # Force progress events for the UI, even if user forgot --progress.
            self.args.progress = True

            hub = EventHub(
                self.logger,
                console=False,
                progress=True,
                queue=self.queue,
            )

            collection_task = asyncio.create_task(run_collection(self.chargers, self.args, hub))

            while True:
                if collection_task.done() and self.queue.empty():
                    break

                try:
                    event = await asyncio.wait_for(self.queue.get(), timeout=0.2)
                except asyncio.TimeoutError:
                    continue

                self.apply_event(event)

            self.results = await collection_task
            ok_count = sum(1 for result in self.results if result.ok)
            failed_count = len(self.results) - ok_count
            self.logger.write(f"END journal_split_multi ok={ok_count} failed={failed_count}")
            self.update_summary(final=True)

        def apply_event(self, event: ChargerEvent) -> None:
            self.table.update_cell(event.custom_id, "status", event.status)
            self.table.update_cell(event.custom_id, "raw", str(event.raw_lines))
            self.table.update_cell(event.custom_id, "matching", str(event.matching_lines))
            self.table.update_cell(event.custom_id, "sessions", str(event.sessions))
            self.table.update_cell(event.custom_id, "files", str(event.files))
            self.table.update_cell(event.custom_id, "elapsed", format_elapsed(event.elapsed_s))
            self.table.update_cell(event.custom_id, "message", event.message[:60])
            self.update_summary()

        def update_summary(self, final: bool = False) -> None:
            done = sum(
                1
                for charger in self.chargers
                if self.table.get_cell(charger.custom_id, "status") == "done"
            )
            failed = sum(
                1
                for charger in self.chargers
                if self.table.get_cell(charger.custom_id, "status") == "failed"
            )
            running = len(self.chargers) - done - failed

            prefix = "finished" if final else "running"
            self.summary.update(
                f"{prefix}: running={running} done={done} failed={failed}"
            )


async def async_cli_main(args: argparse.Namespace, logger: EventLogger) -> None:
    """Async entry point only for non-Textual modes."""
    if args.local:
        await run_local(args, logger)
        return

    chargers = load_chargers(Path(args.chargers_file))
    await run_plain(args, chargers, logger)


def run_textual_sync(args: argparse.Namespace, logger: EventLogger) -> None:
    """Run Textual outside asyncio.run(), avoiding nested event loop errors."""
    if args.local:
        print("Error: --textual is not supported with --local in this version.", file=sys.stderr)
        sys.exit(1)

    if App is None:
        print(
            "Error: Textual is not installed. Install it with: python3 -m pip install textual",
            file=sys.stderr,
        )
        sys.exit(1)

    chargers = load_chargers(Path(args.chargers_file))

    assert JournalSplitTextualApp is not None
    app = JournalSplitTextualApp(chargers, args, logger)
    app.run()


def main() -> None:
    """Program entry point.

    Textual is intentionally executed outside asyncio.run(). Textual's App.run()
    creates and manages its own event loop.
    """
    args = parse_args()
    logger = make_logger(args)

    try:
        if args.textual:
            run_textual_sync(args, logger)
        else:
            asyncio.run(async_cli_main(args, logger))
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        logger.close()


if __name__ == "__main__":
    main()
