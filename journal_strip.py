#!/usr/bin/env python3
"""
journal_strip.py

Reads all chargepoint.service journal entries filtered by the pattern
'04 64 <byte>' (excluding 10, 11, e1), then:
  1. Saves a full journal file: <start>_<end>_journal.log
  2. Detects CHAdeMO (start: 81, end: ad) and CCS (start: 32, end: 22)
     charging sessions and saves each to its own file:
     <start>_<end>_chademo.log  /  <start>_<end>_ccs.log

Datetime format in filenames: 2026-05-06_14-49-36
  - Source: field 29 (1-indexed) of each filtered line
  - Milliseconds are stripped
"""

import subprocess
import re
from pathlib import Path

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

GREP_PATTERN = re.compile(r'04 64 (?!10|11|e1)[0-9a-f]{2}.*')

# field index 2 (0-indexed) → session type
START_BYTES = {'81': 'chademo', '32': 'ccs'}

# session type → its closing byte
END_BYTES = {'chademo': 'ad', 'ccs': '22'}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def run_journal() -> list[str]:
    """Run journalctl and return filtered lines."""
    result = subprocess.run(
        ['journalctl', '-u', 'chargepoint.service'],
        capture_output=True,
        text=True,
    )
    lines = []
    for raw in result.stdout.splitlines():
        m = GREP_PATTERN.search(raw)
        if m:
            lines.append(m.group(0))
    return lines


def get_dt(line: str) -> str:
    """
    Extract datetime from field 29 (index 28).
    Input  : '... 2026-05-06T14:49:36.160 ...'
    Output : '2026-05-06_14-49-36'
    """
    fields = line.split()
    if len(fields) > 28:
        dt_str = fields[28].split('.')[0]          # strip milliseconds
        return dt_str.replace('T', '_').replace(':', '-')
    return 'unknown'


def get_byte(line: str) -> str | None:
    """Return the byte string at field 3 (index 2)."""
    fields = line.split()
    return fields[2] if len(fields) > 2 else None


def save_file(lines: list[str], start_dt: str, end_dt: str, suffix: str) -> None:
    filename = f"{start_dt}_{end_dt}_{suffix}.log"
    Path(filename).write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(f"  Saved: {filename}  ({len(lines)} lines)")


# --------------------------------------------------------------------------- #
# Session detection
# --------------------------------------------------------------------------- #

def detect_sessions(lines: list[str]) -> list[tuple]:
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
    sessions = []
    current_session: str | None = None
    current_lines: list[str] = []
    current_start_dt: str | None = None
    last_byte: str | None = None

    def close_current():
        end_dt = get_dt(current_lines[-1])
        sessions.append((current_session, current_start_dt, end_dt, list(current_lines)))

    for line in lines:
        byte = get_byte(line)
        dt   = get_dt(line)

        if byte in START_BYTES:
            new_type = START_BYTES[byte]

            if current_session is None:
                # First session ever
                current_session  = new_type
                current_lines    = [line]
                current_start_dt = dt

            elif new_type != current_session:
                # Transition to the other session type
                close_current()
                current_session  = new_type
                current_lines    = [line]
                current_start_dt = dt

            else:
                # Same type: new session only if previous ended with its end byte
                if last_byte == END_BYTES[current_session]:
                    close_current()
                    current_session  = new_type
                    current_lines    = [line]
                    current_start_dt = dt
                else:
                    # Still within the same session
                    current_lines.append(line)

        else:
            if current_session is not None:
                current_lines.append(line)
            # Lines outside any session are not written to session files
            # (they are still present in the full journal file)

        last_byte = byte

    # Close the last open session
    if current_session and current_lines:
        close_current()

    return sessions


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    print("Reading journal (this may take a moment)...")
    lines = run_journal()

    if not lines:
        print("No matching lines found.")
        return

    print(f"Total matching lines: {len(lines)}\n")

    # --- Full journal file ---
    print("Writing full journal file:")
    save_file(lines, get_dt(lines[0]), get_dt(lines[-1]), 'journal')

    # --- Session files ---
    sessions = detect_sessions(lines)
    print(f"\nDetected {len(sessions)} charging session(s):")

    for session_type, start_dt, end_dt, session_lines in sessions:
        save_file(session_lines, start_dt, end_dt, session_type)

    print("\nDone.")


if __name__ == '__main__':
    main()
