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

SSH behavior follows the working pattern from themall6_Claude.py:
  - asyncssh
  - default user: admin
  - default port: 5022
  - default key: ~/.ssh/id_ed25519_cp
  - known_hosts=None

Progress behavior:
  - --since and --until limit the journalctl time window.
  - --progress streams stdout and periodically reports raw/matching line counts.
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
from pathlib import Path

try:
    import asyncssh  # type: ignore
except ImportError:
    asyncssh = None  # type: ignore


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


def maybe_print_progress(
    label: str,
    stats: JournalReadStats,
    last_print_time: float,
    interval_s: float,
    force: bool = False,
) -> float:
    """Print progress periodically and return the updated last_print_time."""
    now = time.monotonic()

    if not force and (now - last_print_time) < interval_s:
        return last_print_time

    print(
        f"[{label}] raw={stats.raw_lines} "
        f"matching={stats.matching_lines} "
        f"elapsed={format_elapsed(stats.elapsed_s)}",
        flush=True,
    )
    return now


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
    ssh_user: str,
    ssh_port: int,
    ssh_key: Path,
    connect_timeout: int,
    command_timeout: int,
    since: str | None,
    until: str | None,
    progress: bool,
    progress_interval_s: float,
) -> list[str]:
    """
    Run journalctl remotely over SSH using asyncssh and return filtered lines.

    With --progress, stdout is streamed and raw/matching line counters are printed.
    """
    if asyncssh is None:
        print(
            "Missing dependency: asyncssh. Install it with: python3 -m pip install asyncssh",
            file=sys.stderr,
        )
        return []

    if not ssh_key.exists():
        print(f"[{charger.custom_id}] SSH key not found: {ssh_key}", file=sys.stderr)
        return []

    command = build_journalctl_command(since=since, until=until)
    lines: list[str] = []
    stats = JournalReadStats()
    start_time = time.monotonic()
    last_print_time = start_time

    try:
        conn = await asyncssh.connect(
            charger.ip,
            port=ssh_port,
            username=ssh_user,
            client_keys=[str(ssh_key)],
            known_hosts=None,
            connect_timeout=connect_timeout,
        )

        try:
            if progress:
                print(f"[{charger.custom_id}] Running: {command}", flush=True)

            process = await conn.create_process(command)

            async def consume_stdout() -> None:
                nonlocal last_print_time

                async for raw_line in process.stdout:
                    stats.raw_lines += 1
                    matched = filter_one_journal_line(raw_line)

                    if matched:
                        lines.append(matched)
                        stats.matching_lines += 1

                    stats.elapsed_s = time.monotonic() - start_time

                    if progress:
                        last_print_time = maybe_print_progress(
                            charger.custom_id,
                            stats,
                            last_print_time,
                            progress_interval_s,
                        )

            await asyncio.wait_for(consume_stdout(), timeout=command_timeout)
            await asyncio.wait_for(process.wait(), timeout=10)

            if process.exit_status != 0:
                stderr = ""

                if process.stderr is not None:
                    stderr = await process.stderr.read()

                print(
                    f"[{charger.custom_id}] journal command failed "
                    f"(exit status {process.exit_status})\n"
                    f"stderr: {stderr.strip()}",
                    file=sys.stderr,
                )
                return []

        finally:
            conn.close()
            await conn.wait_closed()

    except asyncio.TimeoutError:
        print(
            f"[{charger.custom_id}] SSH/journal timeout after {command_timeout}s",
            file=sys.stderr,
        )
        return []

    except (asyncssh.Error, OSError) as exc:  # type: ignore[union-attr]
        print(f"[{charger.custom_id}] SSH connection failed: {exc}", file=sys.stderr)
        return []

    stats.elapsed_s = time.monotonic() - start_time

    if progress:
        maybe_print_progress(
            charger.custom_id,
            stats,
            last_print_time,
            progress_interval_s,
            force=True,
        )

    return lines


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


def save_file(lines: list[str], start_dt: str, end_dt: str, suffix: str, prefix: str = "") -> None:
    """Save lines to a log file."""
    clean_prefix = safe_filename_part(prefix) if prefix else ""

    if clean_prefix:
        filename = f"{clean_prefix}_{start_dt}_{end_dt}_{suffix}.log"
    else:
        filename = f"{start_dt}_{end_dt}_{suffix}.log"

    Path(filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Saved: {filename}  ({len(lines)} lines)")


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


def process_lines(lines: list[str], prefix: str = "") -> None:
    """Save full journal and detected session files for one charger/source."""
    if not lines:
        print("  No matching lines found.")
        return

    print(f"  Total matching lines: {len(lines)}")

    print("  Writing full journal file:")
    save_file(lines, get_dt(lines[0]), get_dt(lines[-1]), "journal", prefix=prefix)

    sessions = detect_sessions(lines)
    print(f"  Detected {len(sessions)} charging session(s):")

    for session_type, start_dt, end_dt, session_lines in sessions:
        save_file(session_lines, start_dt, end_dt, session_type, prefix=prefix)


async def process_charger(
    index: int,
    total: int,
    charger: Charger,
    ssh_user: str,
    ssh_port: int,
    ssh_key: Path,
    connect_timeout: int,
    command_timeout: int,
    since: str | None,
    until: str | None,
    progress: bool,
    progress_interval_s: float,
) -> None:
    """Collect and process one charger."""
    print(f"\n[{index}/{total}] {charger.custom_id}  Connecting to {charger.ip}:{ssh_port}...")

    if since or until:
        print(
            f"[{index}/{total}] {charger.custom_id}  "
            f"Journal window: since={since or '-'} until={until or '-'}"
        )

    lines = await run_remote_journal(
        charger,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        ssh_key=ssh_key,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
        since=since,
        until=until,
        progress=progress,
        progress_interval_s=progress_interval_s,
    )

    print(f"[{index}/{total}] {charger.custom_id}  Processing...")
    process_lines(lines, prefix=charger.custom_id)


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
        "--local",
        action="store_true",
        help="Run only against the local machine. In this mode carregadores.dsv is not used.",
    )

    return parser.parse_args()


async def async_main() -> None:
    """Async entry point."""
    args = parse_args()

    if args.local:
        print("Reading local journal (this may take a moment)...")

        if args.since or args.until:
            print(f"Journal window: since={args.since or '-'} until={args.until or '-'}")

        lines = run_local_journal(since=args.since, until=args.until)
        process_lines(lines)
        print("\nDone.")
        return

    chargers_path = Path(args.chargers_file)

    try:
        chargers = load_chargers(chargers_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(chargers)} charger(s) from {chargers_path}")
    print(f"Using SSH key: {args.ssh_key}")
    print(f"Using SSH user/port: {args.ssh_user}/{args.ssh_port}")

    if args.since or args.until:
        print(f"Using journal window: since={args.since or '-'} until={args.until or '-'}")

    for index, charger in enumerate(chargers, start=1):
        await process_charger(
            index=index,
            total=len(chargers),
            charger=charger,
            ssh_user=args.ssh_user,
            ssh_port=args.ssh_port,
            ssh_key=args.ssh_key,
            connect_timeout=args.connect_timeout,
            command_timeout=args.command_timeout,
            since=args.since,
            until=args.until,
            progress=args.progress,
            progress_interval_s=args.progress_interval,
        )

    print("\nDone.")


def main() -> None:
    """Program entry point."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
