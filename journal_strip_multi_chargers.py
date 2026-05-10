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

  - For each charger, the script connects through SSH and runs:
        journalctl -u chargepoint.service

SSH behavior follows the working pattern from themall6_Claude.py:
  - asyncssh
  - default user: admin
  - default port: 5022
  - default key: ~/.ssh/id_ed25519_cp
  - known_hosts=None

Datetime format in filenames: 2026-05-06_14-49-36
  - Source: field 29 (1-indexed) of each filtered line
  - Milliseconds are stripped
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

try:
    import asyncssh  # type: ignore
except ImportError:  # pragma: no cover - user-facing dependency check
    asyncssh = None  # type: ignore


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

CHARGERS_FILE = "carregadores.dsv"
DEFAULT_SSH_USER = "admin"
DEFAULT_SSH_PORT = 5022
DEFAULT_SSH_KEY = Path.home() / ".ssh" / "id_ed25519_cp"

GREP_PATTERN = re.compile(r"04 64 (?!10|11|e1)[0-9a-f]{2}.*")

# field index 2 (0-indexed) -> session type
START_BYTES = {"81": "chademo", "32": "ccs"}

# session type -> its closing byte
END_BYTES = {"chademo": "ad", "ccs": "22"}


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Charger:
    custom_id: str
    ip: str


# --------------------------------------------------------------------------- #
# Charger list parsing
# --------------------------------------------------------------------------- #

def load_chargers(path: Path) -> list[Charger]:
    """
    Load chargers from a DSV/CSV-like file.

    Expected rows:
        custom_id, ip
        "politécnico", 10.53.1.21

    The first row is treated as a header if it contains custom_id and ip.
    Empty lines and lines starting with # are ignored.
    """
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
    """
    Convert custom_id to a filesystem-safe prefix while preserving readability.

    Examples:
        politécnico -> politecnico
        CP 001 / Teste -> CP_001_Teste
    """
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_text).strip("._-")
    return safe or "charger"


# --------------------------------------------------------------------------- #
# Journal collection
# --------------------------------------------------------------------------- #

def filter_journal_output(stdout: str) -> list[str]:
    """Return only the payload portion matching GREP_PATTERN from journal output."""
    lines: list[str] = []

    for raw in stdout.splitlines():
        m = GREP_PATTERN.search(raw)
        if m:
            lines.append(m.group(0))

    return lines


def run_local_journal() -> list[str]:
    """Run local journalctl and return filtered lines."""
    result = subprocess.run(
        ["journalctl", "-u", "chargepoint.service"],
        capture_output=True,
        text=True,
    )
    return filter_journal_output(result.stdout)


async def run_remote_journal(
    charger: Charger,
    ssh_user: str,
    ssh_port: int,
    ssh_key: Path,
    connect_timeout: int,
    command_timeout: int,
) -> list[str]:
    """
    Run journalctl remotely over SSH using asyncssh and return filtered lines.

    This intentionally mirrors themall6_Claude.py:
    - explicit client key
    - known_hosts=None
    - no dependency on ssh-agent behavior
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
            result = await asyncio.wait_for(
                conn.run("journalctl -u chargepoint.service", check=False),
                timeout=command_timeout,
            )
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

    if result.exit_status != 0:
        error_text = result.stderr.strip() or result.stdout.strip()
        print(
            f"[{charger.custom_id}] journal command failed "
            f"(exit status {result.exit_status})\n"
            f"stderr/stdout: {error_text}",
            file=sys.stderr,
        )
        return []

    return filter_journal_output(result.stdout)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def get_dt(line: str) -> str:
    """
    Extract datetime from field 29 (index 28).

    Input  : '... 2026-05-06T14:49:36.160 ...'
    Output : '2026-05-06_14-49-36'
    """
    fields = line.split()

    if len(fields) > 28:
        dt_str = fields[28].split(".")[0]          # strip milliseconds
        return dt_str.replace("T", "_").replace(":", "-")

    return "unknown"


def get_byte(line: str) -> str | None:
    """Return the byte string at field 3 (index 2)."""
    fields = line.split()
    return fields[2] if len(fields) > 2 else None


def save_file(lines: list[str], start_dt: str, end_dt: str, suffix: str, prefix: str = "") -> None:
    """
    Save lines to a log file.

    If prefix is provided, filename becomes:
        <prefix>_<start>_<end>_<suffix>.log
    """
    clean_prefix = safe_filename_part(prefix) if prefix else ""

    if clean_prefix:
        filename = f"{clean_prefix}_{start_dt}_{end_dt}_{suffix}.log"
    else:
        filename = f"{start_dt}_{end_dt}_{suffix}.log"

    Path(filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Saved: {filename}  ({len(lines)} lines)")


# --------------------------------------------------------------------------- #
# Session detection
# --------------------------------------------------------------------------- #

def detect_sessions(lines: list[str]) -> list[tuple[str, str, str, list[str]]]:
    """
    Scan lines and group them into charging sessions.

    Rules:
    - A session opens on the first occurrence of its start byte (81 / 32).
    - A session closes (and a new one of the same type opens) when the start
      byte reappears after the session's own end byte (ad / 22).
    - A session closes immediately when the start byte of the OTHER type is seen.
    - All lines between open and close belong to the session.

    Returns a list of (session_type, start_dt, end_dt, lines) tuples.
    """
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


# --------------------------------------------------------------------------- #
# Processing
# --------------------------------------------------------------------------- #

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
    charger: Charger,
    ssh_user: str,
    ssh_port: int,
    ssh_key: Path,
    connect_timeout: int,
    command_timeout: int,
) -> None:
    print(f"\n[{charger.custom_id}] Reading journal from {charger.ip}:{ssh_port}...")
    lines = await run_remote_journal(
        charger,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        ssh_key=ssh_key,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
    )
    process_lines(lines, prefix=charger.custom_id)


# --------------------------------------------------------------------------- #
# CLI / Main
# --------------------------------------------------------------------------- #

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract chargepoint.service journal payloads and split charging sessions. "
            "By default, reads carregadores.dsv and collects logs from each charger over SSH."
        )
    )

    parser.add_argument(
        "--chargers-file",
        default=CHARGERS_FILE,
        help=f"Path to charger list file. Default: {CHARGERS_FILE}",
    )

    parser.add_argument(
        "--ssh-user",
        default=DEFAULT_SSH_USER,
        help=f"Remote SSH user. Default: {DEFAULT_SSH_USER}",
    )

    parser.add_argument(
        "--ssh-port",
        type=int,
        default=DEFAULT_SSH_PORT,
        help=f"Remote SSH port. Default: {DEFAULT_SSH_PORT}",
    )

    parser.add_argument(
        "--ssh-key",
        type=Path,
        default=DEFAULT_SSH_KEY,
        help=f"SSH private key path. Default: {DEFAULT_SSH_KEY}",
    )

    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=3,
        help="SSH connection timeout in seconds. Default: 3",
    )

    parser.add_argument(
        "--command-timeout",
        type=int,
        default=180,
        help="Remote journal command timeout in seconds. Default: 180",
    )

    parser.add_argument(
        "--local",
        action="store_true",
        help=(
            "Run only against the local machine, preserving the old behavior. "
            "In this mode carregadores.dsv is not used."
        ),
    )

    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()

    if args.local:
        print("Reading local journal (this may take a moment)...")
        lines = run_local_journal()
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

    for charger in chargers:
        await process_charger(
            charger,
            ssh_user=args.ssh_user,
            ssh_port=args.ssh_port,
            ssh_key=args.ssh_key,
            connect_timeout=args.connect_timeout,
            command_timeout=args.command_timeout,
        )

    print("\nDone.")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
