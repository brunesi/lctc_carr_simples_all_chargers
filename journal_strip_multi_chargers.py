#!/usr/bin/env python3
"""
journal_strip.py

Reads chargepoint.service journal entries filtered by the pattern
'04 64 <byte>' (excluding 10, 11, e1), then:
  1. Saves a full journal file: <custom_id>_<start>_<end>_journal.log
  2. Detects CHAdeMO (start: 81, end: ad) and CCS (start: 32, end: 22)
     charging sessions and saves each to its own file:
     <custom_id>_<start>_<end>_chademo.log
     <custom_id>_<start>_<end>_ccs.log

New multi-charger behavior:
  - Looks for carregadores.dsv in the current directory.
  - Expected format:
        custom_id, ip
        "politécnico", 10.53.1.21
        "outro carregador", 10.53.1.22

  - For each charger, the script connects through SSH and runs:
        journalctl -u chargepoint.service

Assumptions:
  - SSH key pairs are already correctly configured locally and remotely.
  - The remote SSH user is 'admin' by default.
  - The SSH port is 22 by default.

Datetime format in filenames: 2026-05-06_14-49-36
  - Source: field 29 (1-indexed) of each filtered line
  - Milliseconds are stripped
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

CHARGERS_FILE = "carregadores.dsv"
DEFAULT_SSH_USER = "admin"
DEFAULT_SSH_PORT = 22

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
            print(f"Skipping invalid row {line_number}: expected 2 columns, got {len(row)}", file=sys.stderr)
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


def run_remote_journal(charger: Charger, ssh_user: str, ssh_port: int, timeout: int) -> list[str]:
    """
    Run journalctl remotely over SSH and return filtered lines.

    BatchMode=yes prevents SSH from hanging waiting for a password if key auth fails.
    """
    ssh_target = f"{ssh_user}@{charger.ip}"
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-p", str(ssh_port),
        ssh_target,
        "journalctl -u chargepoint.service",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"[{charger.custom_id}] SSH/journal timeout after {timeout}s", file=sys.stderr)
        return []

    if result.returncode != 0:
        print(
            f"[{charger.custom_id}] SSH/journal command failed "
            f"(return code {result.returncode})\n"
            f"stderr: {result.stderr.strip()}",
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
                # First session ever
                current_session = new_type
                current_lines = [line]
                current_start_dt = dt

            elif new_type != current_session:
                # Transition to the other session type
                close_current()
                current_session = new_type
                current_lines = [line]
                current_start_dt = dt

            else:
                # Same type: new session only if previous ended with its end byte
                if last_byte == END_BYTES[current_session]:
                    close_current()
                    current_session = new_type
                    current_lines = [line]
                    current_start_dt = dt
                else:
                    # Still within the same session
                    current_lines.append(line)

        else:
            if current_session is not None:
                current_lines.append(line)
            # Lines outside any session are not written to session files
            # but are still present in the full journal file.

        last_byte = byte

    # Close the last open session
    if current_session and current_lines:
        close_current()

    return sessions


# --------------------------------------------------------------------------- #
# Processing
# --------------------------------------------------------------------------- #

def process_lines(lines: list[str], prefix: str = "") -> None:
    """
    Save full journal and detected session files for one charger/source.
    """
    if not lines:
        print("  No matching lines found.")
        return

    print(f"  Total matching lines: {len(lines)}")

    # --- Full journal file ---
    print("  Writing full journal file:")
    save_file(lines, get_dt(lines[0]), get_dt(lines[-1]), "journal", prefix=prefix)

    # --- Session files ---
    sessions = detect_sessions(lines)
    print(f"  Detected {len(sessions)} charging session(s):")

    for session_type, start_dt, end_dt, session_lines in sessions:
        save_file(session_lines, start_dt, end_dt, session_type, prefix=prefix)


def process_charger(charger: Charger, ssh_user: str, ssh_port: int, timeout: int) -> None:
    print(f"\n[{charger.custom_id}] Reading journal from {charger.ip}...")
    lines = run_remote_journal(charger, ssh_user=ssh_user, ssh_port=ssh_port, timeout=timeout)
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
        "--timeout",
        type=int,
        default=180,
        help="Timeout in seconds for each remote journal command. Default: 180",
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


def main() -> None:
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

    for charger in chargers:
        process_charger(
            charger,
            ssh_user=args.ssh_user,
            ssh_port=args.ssh_port,
            timeout=args.timeout,
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
