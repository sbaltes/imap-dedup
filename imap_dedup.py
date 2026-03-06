#!/usr/bin/env python3
"""Maildir email deduplication script with IMAP server-side deletion.

Scans a Maildir hierarchy (synced by OfflineIMAP) to find duplicate emails,
then removes them via IMAP.  Duplicates are detected by Message-ID header
(primary) or header+body SHA-256 fingerprint (fallback).

Workflow: scan → export plan → apply plan (move to Trash or permanent delete).
"""

from __future__ import annotations

import sys

if sys.version_info < (3, 10):
    sys.exit("ERROR: Python 3.10 or newer is required.")

import argparse
import difflib
import email.errors
import email.header
import email.message
import email.parser
import email.policy
import hashlib
import imaplib
import json
import netrc
import os
import re
import ssl
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METHOD_MESSAGE_ID = "message-id"
METHOD_FINGERPRINT = "fingerprint"

FLAG_SCORES = {
    "F": 10,  # Flagged / starred
    "R": 5,   # Replied
    "S": 2,   # Seen
    "P": 1,   # Passed / forwarded
}

DEFAULT_EXCLUDE_FOLDERS = {"Trash", "Junk", "Drafts"}

INTERACTIVE_PAGE_SIZE = 25


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class MessageInfo:
    path: Path
    folder: str           # display name, e.g. "0_Sent"
    subject: str
    date: str
    flags: str            # raw flag characters after :2,
    size: int
    mtime: float
    dedup_key: str
    method: str           # METHOD_MESSAGE_ID or METHOD_FINGERPRINT
    message_id: str | None = None  # normalized Message-ID, None for fingerprinted
    from_addr: str = "(unknown)"


@dataclass
class DuplicateGroup:
    keep: MessageInfo
    duplicates: list[MessageInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dedup key functions
# ---------------------------------------------------------------------------


def normalize_message_id(raw: str) -> str:
    """Strip angle brackets, whitespace, and lowercase a Message-ID."""
    return raw.strip().strip("<>").strip().lower()


def decode_header(raw) -> str:
    """Decode RFC 2047 encoded header value to a Unicode string."""
    raw = str(raw)
    try:
        parts = email.header.decode_header(raw)
        decoded = []
        for data, charset in parts:
            if isinstance(data, bytes):
                decoded.append(data.decode(charset or "ascii", errors="replace"))
            else:
                decoded.append(data)
        return "".join(decoded)
    except (ValueError, LookupError):
        return raw


def compute_fingerprint(msg_bytes: bytes) -> str:
    """SHA-256 fingerprint from Date + From + To + Cc + Subject + body."""
    parser = email.parser.BytesParser(policy=email.policy.compat32)
    msg = parser.parsebytes(msg_bytes)

    parts = []
    for hdr in ("Date", "From", "To", "Cc", "Subject"):
        val = str(msg.get(hdr, ""))
        if hdr in ("From", "To", "Cc"):
            val = val.lower()
        parts.append(val)

    # Decode body (null byte separators prevent collisions across part boundaries)
    body = b""
    if msg.is_multipart():
        payload_parts = []
        for part in msg.walk():
            payload = part.get_payload(decode=True)
            if payload:
                payload_parts.append(payload)
        body = b"\x00".join(payload_parts)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload

    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8", errors="replace"))
    h.update(body)
    return h.hexdigest()


def get_dedup_key(
    filepath: Path,
    raw: bytes | None = None,
    msg: email.message.Message | None = None,
) -> tuple[str, str]:
    """Return (dedup_key, method) for a message file.

    Tries Message-ID first (header-only parse). Falls back to full-body
    fingerprint if Message-ID is missing.

    If *raw* is provided, uses it instead of reading from *filepath*.
    If *msg* is provided, uses it instead of parsing headers from *raw*.
    """
    if raw is None:
        raw = filepath.read_bytes()

    if msg is None:
        # Fast path: header-only parse for Message-ID
        # (compat32 policy does case-insensitive header lookup)
        header_parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
        msg = header_parser.parsebytes(raw)
    mid = msg.get("Message-ID")

    if mid:
        normalized = normalize_message_id(mid)
        if normalized:
            return normalized, METHOD_MESSAGE_ID

    # Fallback: full fingerprint
    return compute_fingerprint(raw), METHOD_FINGERPRINT


def get_message_info(filepath: Path, folder: str) -> MessageInfo | None:
    """Parse a message file and return MessageInfo, or None on error."""
    try:
        raw = filepath.read_bytes()
    except OSError as e:
        print(f"  WARNING: Cannot read {filepath}: {e}", file=sys.stderr)
        return None

    try:
        # Single header parse for both Message-ID and display fields
        header_parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
        msg = header_parser.parsebytes(raw)
    except (ValueError, UnicodeDecodeError, email.errors.MessageError) as e:
        print(f"  WARNING: Cannot parse {filepath}: {e}", file=sys.stderr)
        return None

    dedup_key, method = get_dedup_key(filepath, raw=raw, msg=msg)

    subject = decode_header(msg.get("Subject", "(no subject)"))
    date = msg.get("Date", "(no date)")
    from_addr = decode_header(msg.get("From", "(unknown)"))
    flags = parse_flags(filepath.name)
    message_id = dedup_key if method == METHOD_MESSAGE_ID else None
    size = len(raw)

    try:
        mtime = filepath.stat().st_mtime
    except OSError:
        mtime = 0.0

    return MessageInfo(
        path=filepath,
        folder=folder,
        subject=subject,
        date=date,
        flags=flags,
        size=size,
        mtime=mtime,
        dedup_key=dedup_key,
        method=method,
        message_id=message_id,
        from_addr=from_addr,
    )


# ---------------------------------------------------------------------------
# Maildir flag parsing
# ---------------------------------------------------------------------------


def parse_flags(filename: str) -> str:
    """Extract flag characters from a Maildir filename (after :2,)."""
    idx = filename.find(":2,")
    if idx == -1:
        return ""
    return filename[idx + 3:]


# ---------------------------------------------------------------------------
# Retention scoring
# ---------------------------------------------------------------------------


def _depth_priority(folder_name: str) -> int:
    """Depth-based folder priority: count dot-separated segments."""
    return (folder_name.count(".") + 1) * 10


def get_folder_priority(folder_name: str) -> int:
    """Score a folder for retention priority."""
    lower = folder_name.lower()

    # Trash, Junk, Drafts get lowest priority
    if lower in ("trash", "junk", "drafts"):
        return 0

    # Sent folders get highest priority
    if lower in ("0_sent", "sent"):
        return 100

    return _depth_priority(folder_name)


def compute_retention_score(
    info: MessageInfo, sender: str | None = None,
) -> tuple[int, int, int, float, int]:
    """Compute a sortable retention score (higher = keep).

    Returns a tuple for lexicographic comparison:
      (sender_ok, flag_score, folder_priority, -mtime, size)

    *sender_ok* is 1 normally, 0 when the message is in a Sent folder but
    the From header does not match *sender*.  This is the primary criterion
    so a non-matching Sent copy can never beat other copies regardless of
    flag scores.

    The -mtime term intentionally keeps the oldest (original) copy:
    older messages have smaller mtime, so -mtime is larger, scoring higher.
    """
    flag_score = sum(FLAG_SCORES.get(c, 0) for c in info.flags)
    folder_pri = get_folder_priority(info.folder)
    sender_ok = 1
    if sender is not None and folder_pri == 100:
        from_lower = info.from_addr.lower()
        if not all(word in from_lower for word in sender.lower().split()):
            sender_ok = 0
            folder_pri = _depth_priority(info.folder)
    return (sender_ok, flag_score, folder_pri, -info.mtime, info.size)


def decide_keep(
    messages: list[MessageInfo], sender: str | None = None,
) -> DuplicateGroup:
    """Given a list of duplicate messages, decide which to keep."""
    scored = sorted(
        messages,
        key=lambda m: compute_retention_score(m, sender=sender),
        reverse=True,
    )
    return DuplicateGroup(keep=scored[0], duplicates=scored[1:])


# ---------------------------------------------------------------------------
# Folder discovery
# ---------------------------------------------------------------------------


def folder_display_name(dirname: str) -> str:
    """Convert directory name to display name: strip leading dot."""
    if dirname.startswith("."):
        return dirname[1:]
    return dirname


def discover_folders(
    maildir_root: Path,
    include: list[str] | None = None,
    exclude: set[str] | None = None,
) -> list[tuple[str, Path]]:
    """Discover Maildir folders under root.

    Returns list of (display_name, path) tuples.
    Handles root INBOX and dot-prefix subfolders.
    """
    if exclude is None:
        exclude = set()

    folders = []

    # Root INBOX — cur/ and new/ directly under maildir_root
    if _has_maildir_subdirs(maildir_root):
        if include is None or "INBOX" in include:
            if "INBOX" not in exclude:
                folders.append(("INBOX", maildir_root))

    # Dot-prefix subfolders
    try:
        entries = sorted(maildir_root.iterdir())
    except OSError:
        entries = []

    for entry in entries:
        if not entry.is_dir():
            continue
        name = entry.name
        if not name.startswith("."):
            continue

        display = folder_display_name(name)

        if not _has_maildir_subdirs(entry):
            continue

        if include is not None and display not in include:
            continue
        if display in exclude:
            continue

        folders.append((display, entry))

    return folders


def _has_maildir_subdirs(path: Path) -> bool:
    """Check if path has cur/ or new/ subdirectories."""
    return (path / "cur").is_dir() or (path / "new").is_dir()


# ---------------------------------------------------------------------------
# Message scanning
# ---------------------------------------------------------------------------


def scan_folder(
    folder_name: str, folder_path: Path, verbose: bool = False,
    progress_every: int = 0,
) -> list[MessageInfo]:
    """Scan cur/ and new/ in a folder, return list of MessageInfo.

    If *progress_every* > 0, prints a counter every that many messages
    (e.g. " 1000... 2000...") to indicate progress on large folders.
    """
    messages = []
    for subdir in ("cur", "new"):
        dirpath = folder_path / subdir
        if not dirpath.is_dir():
            continue
        try:
            entries = list(dirpath.iterdir())
        except OSError as e:
            print(f"  WARNING: Cannot list {dirpath}: {e}", file=sys.stderr)
            continue

        for filepath in entries:
            if not filepath.is_file():
                continue
            info = get_message_info(filepath, folder_name)
            if info is not None:
                messages.append(info)
                if (progress_every > 0
                        and len(messages) % progress_every == 0):
                    print(f" {len(messages)}...", end="", flush=True)

    return messages


# ---------------------------------------------------------------------------
# Duplicate grouping
# ---------------------------------------------------------------------------


def find_duplicates(
    messages: list[MessageInfo],
    same_folder_only: bool = False,
    sender: str | None = None,
) -> list[DuplicateGroup]:
    """Group messages by dedup_key and return groups with duplicates."""
    groups: dict[str | tuple[str, str], list[MessageInfo]] = {}
    for m in messages:
        key: str | tuple[str, str] = (m.folder, m.dedup_key) if same_folder_only else m.dedup_key
        groups.setdefault(key, []).append(m)

    result = []
    for group_messages in groups.values():
        if len(group_messages) > 1:
            result.append(decide_keep(group_messages, sender=sender))

    return result


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def format_size(nbytes: int) -> str:
    """Format byte count as human-readable string."""
    for unit, threshold in [("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)]:
        if nbytes >= threshold:
            return f"{nbytes / threshold:.1f} {unit}"
    return f"{nbytes} B"


def print_report(
    groups: list[DuplicateGroup],
    verbose: bool = False,
    quiet: bool = False,
) -> None:
    """Print dry-run report of duplicates found."""
    if quiet:
        return

    total_dupes = sum(len(g.duplicates) for g in groups)
    total_size = sum(d.size for g in groups for d in g.duplicates)

    by_method = {METHOD_MESSAGE_ID: 0, METHOD_FINGERPRINT: 0}
    for g in groups:
        by_method[g.keep.method] += 1

    if verbose:
        for i, group in enumerate(groups, 1):
            all_msgs = [group.keep] + group.duplicates
            print(f"\n--- Duplicate group {i} (method: {group.keep.method}) ---")
            print(f"  Subject: {group.keep.subject}")
            print(f"  Date: {group.keep.date}")
            print(f"  Key: {group.keep.dedup_key[:60]}")
            print(f"  Copies: {len(all_msgs)}")
            print(f"  KEEP:   [{group.keep.folder}] {group.keep.path.name}")
            print(f"          Flags: {group.keep.flags or '(none)'} | "
                  f"Size: {format_size(group.keep.size)}")
            for dup in group.duplicates:
                print(f"  DELETE: [{dup.folder}] {dup.path.name}")
                print(f"          Flags: {dup.flags or '(none)'} | "
                      f"Size: {format_size(dup.size)}")

    print(f"\n{'=' * 60}")
    print(f"Duplicate groups:    {len(groups)}")
    print(f"Duplicate messages:  {total_dupes}")
    print(f"Space reclaimable:   {format_size(total_size)}")
    print(f"By Message-ID:       {by_method[METHOD_MESSAGE_ID]} groups")
    print(f"By fingerprint:      {by_method[METHOD_FINGERPRINT]} groups")


# ---------------------------------------------------------------------------
# Interactive review
# ---------------------------------------------------------------------------


def group_by_delete_folder(
    groups: list[DuplicateGroup],
) -> dict[str, list[tuple[DuplicateGroup, list[MessageInfo]]]]:
    """Reorganize duplicate groups by the folder of their DELETE messages.

    A single group may appear under multiple folders if its duplicates span
    folders.  Returns {folder_name: [(group, dupes_in_folder), ...]}.
    """
    result: dict[str, list[tuple[DuplicateGroup, list[MessageInfo]]]] = {}
    for group in groups:
        by_folder: dict[str, list[MessageInfo]] = {}
        for dup in group.duplicates:
            by_folder.setdefault(dup.folder, []).append(dup)
        for folder, dupes in by_folder.items():
            result.setdefault(folder, []).append((group, dupes))
    return result


def format_interactive_entry(
    index: int,
    group: DuplicateGroup,
    folder_dupes: list[MessageInfo],
) -> str:
    """Format one duplicate group for interactive display."""
    lines = []
    lines.append(
        f"  {index}) Subject: {group.keep.subject}"
    )
    lines.append(
        f"     Date: {group.keep.date}    From: {group.keep.from_addr}"
    )
    lines.append(
        f"     KEEP:   [{group.keep.folder}]  Flags: {group.keep.flags or '(none)'}  "
        f" Size: {format_size(group.keep.size)}"
    )
    for dup in folder_dupes:
        lines.append(
            f"     DELETE: [{dup.folder}]  Flags: {dup.flags or '(none)'}  "
            f" Size: {format_size(dup.size)}"
        )
    return "\n".join(lines)


def interactive_prompt(prompt: str, valid_chars: str) -> str | None:
    """Read a single-character response from the user.

    Returns the character (lowercased) or None on EOF / Ctrl-C.
    """
    while True:
        try:
            answer = input(prompt).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return None
        if answer in valid_chars:
            return answer
        print(f"  Please enter one of: {', '.join(valid_chars)}")


def review_folder_interactive(
    folder: str,
    entries: list[tuple[DuplicateGroup, list[MessageInfo]]],
) -> list[tuple[DuplicateGroup, list[MessageInfo]]] | None:
    """Interactive review for one folder.

    Returns list of accepted entries, or None if user chose to quit.
    Paginates the initial display in chunks of INTERACTIVE_PAGE_SIZE.
    """
    print(f"\n{'=' * 80}")
    print(f" {folder} -- {len(entries)} duplicate(s) to review")
    print(f"{'=' * 80}\n")

    # Paginated display
    total = len(entries)
    page = INTERACTIVE_PAGE_SIZE
    shown = 0
    while shown < total:
        end = min(shown + page, total)
        for i in range(shown, end):
            group, dupes = entries[i]
            print(format_interactive_entry(i + 1, group, dupes))
            print()
        shown = end
        if shown < total:
            remaining = total - shown
            ch = interactive_prompt(
                f" -- Showing {shown}/{total} -- "
                f"[m]ore ({remaining} remaining) / "
                f"[a]ccept all / [s]kip all / [r]eview one-by-one / [q]uit: ",
                "masrq",
            )
            if ch is None or ch == "q":
                return None
            if ch == "a":
                return list(entries)
            if ch == "s":
                return []
            if ch == "r":
                return _review_one_by_one(entries)
            # ch == "m": continue to next page

    choice = interactive_prompt(
        f" Action for {folder}? [a]ccept all / [s]kip all / [r]eview one-by-one / [q]uit: ",
        "asrq",
    )
    if choice is None or choice == "q":
        return None
    if choice == "a":
        return list(entries)
    if choice == "s":
        return []

    return _review_one_by_one(entries)


def render_email_for_diff(path: Path) -> list[str]:
    """Read an email file and return lines suitable for difflib comparison."""
    try:
        raw = path.read_bytes()
    except OSError:
        return [f"(could not read {path})\n"]

    parser = email.parser.BytesParser(policy=email.policy.default)
    msg = parser.parsebytes(raw)

    lines: list[str] = []
    for hdr in ("Date", "From", "To", "Cc", "Subject", "Message-ID"):
        val = msg.get(hdr)
        if val is not None:
            lines.append(f"{hdr}: {val}\n")
    lines.append("\n")

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct.startswith("multipart/"):
                continue
            lines.append(f"--- part: {ct} ---\n")
            payload = part.get_content()
            if isinstance(payload, str):
                for line in payload.splitlines(keepends=True):
                    lines.append(line if line.endswith("\n") else line + "\n")
            elif isinstance(payload, bytes):
                lines.append(f"(binary content, {len(payload)} bytes)\n")
    else:
        payload = msg.get_content()
        if isinstance(payload, str):
            for line in payload.splitlines(keepends=True):
                lines.append(line if line.endswith("\n") else line + "\n")
        elif isinstance(payload, bytes):
            lines.append(f"(binary content, {len(payload)} bytes)\n")

    return lines


def _compact_inline_pair(
    keep_line: str, dup_line: str, *, use_color: bool = False,
) -> tuple[str, str]:
    """Render a compact inline diff for a changed line pair.

    Returns two lines showing the changed portions highlighted with optional
    ANSI color, preserving short equal segments and showing context around
    collapsed ones.
    """
    sm = difflib.SequenceMatcher(None, keep_line, dup_line)
    keep_parts: list[str] = []
    dup_parts: list[str] = []

    RED = "\033[31m"
    GREEN = "\033[32m"
    RESET = "\033[0m"

    context_chars = 8
    min_collapse = 20

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            seg = keep_line[i1:i2]
            if len(seg) < min_collapse:
                keep_parts.append(seg)
                dup_parts.append(seg)
            else:
                prefix = seg[:context_chars]
                suffix = seg[-context_chars:]
                collapsed = f"{prefix}...{suffix}"
                keep_parts.append(collapsed)
                dup_parts.append(collapsed)
        else:
            if tag in ("replace", "delete"):
                chunk = keep_line[i1:i2]
                if use_color:
                    keep_parts.append(f"{RED}{chunk}{RESET}")
                else:
                    keep_parts.append(chunk)
            if tag in ("replace", "insert"):
                chunk = dup_line[j1:j2]
                if use_color:
                    dup_parts.append(f"{GREEN}{chunk}{RESET}")
                else:
                    dup_parts.append(chunk)
    return "".join(keep_parts), "".join(dup_parts)


def show_diff(keep: MessageInfo, dup: MessageInfo) -> None:
    """Print a compact inline diff between the kept and duplicate message."""
    keep_lines = render_email_for_diff(keep.path)
    dup_lines = render_email_for_diff(dup.path)

    diff = list(
        difflib.unified_diff(
            keep_lines,
            dup_lines,
            fromfile=f"KEEP: {keep.folder}",
            tofile=f"DELETE: {dup.folder}",
        )
    )
    if not diff:
        print("  (no differences found)")
        return

    use_color = sys.stdout.isatty()

    # Print header lines (--- and +++)
    for line in diff[:2]:
        print(line, end="" if line.endswith("\n") else "\n")

    # Collect and process hunks
    minus_lines: list[str] = []
    plus_lines: list[str] = []

    def flush_pairs() -> None:
        paired = min(len(minus_lines), len(plus_lines))
        for k in range(paired):
            ml = minus_lines[k].rstrip("\n")
            pl = plus_lines[k].rstrip("\n")
            keep_text, dup_text = _compact_inline_pair(
                ml, pl, use_color=use_color,
            )
            print(f"- {keep_text}")
            print(f"+ {dup_text}")
        # Unpaired lines shown in full
        for k in range(paired, len(minus_lines)):
            print(f"- {minus_lines[k].rstrip(chr(10))}")
        for k in range(paired, len(plus_lines)):
            print(f"+ {plus_lines[k].rstrip(chr(10))}")
        minus_lines.clear()
        plus_lines.clear()

    for line in diff[2:]:
        if line.startswith("@@"):
            flush_pairs()
            continue
        if line.startswith("-"):
            if plus_lines:
                flush_pairs()
            minus_lines.append(line[1:])
        elif line.startswith("+"):
            plus_lines.append(line[1:])
        else:
            # Context line — skip for compact output
            flush_pairs()
    flush_pairs()


def _review_one_by_one(
    entries: list[tuple[DuplicateGroup, list[MessageInfo]]],
) -> list[tuple[DuplicateGroup, list[MessageInfo]]] | None:
    """Review entries one-by-one, return accepted list or None on quit."""
    accepted = []
    for i, (group, dupes) in enumerate(entries, 1):
        print()
        print(format_interactive_entry(i, group, dupes))
        while True:
            ch = interactive_prompt(
                f"  Delete this duplicate? [y]es / [n]o / [d]iff / [q]uit: ",
                "yndq",
            )
            if ch is None or ch == "q":
                return None
            if ch == "d":
                for dup in dupes:
                    show_diff(group.keep, dup)
                continue
            break
        if ch == "y":
            accepted.append((group, dupes))
    return accepted


def run_interactive_review(
    groups: list[DuplicateGroup],
) -> list[DuplicateGroup] | None:
    """Top-level orchestrator for interactive duplicate review.

    Returns list of accepted DuplicateGroups, or None if user quit.
    """
    by_folder = group_by_delete_folder(groups)

    accepted_all: list[DuplicateGroup] = []
    summary: dict[str, tuple[int, int]] = {}  # folder -> (accepted, skipped)

    for folder in sorted(by_folder):
        entries = by_folder[folder]
        result = review_folder_interactive(folder, entries)
        if result is None:
            print("\nQuit — no changes made.")
            return None

        n_accepted = sum(len(dupes) for _, dupes in result)
        n_skipped = sum(len(dupes) for _, dupes in entries) - n_accepted
        summary[folder] = (n_accepted, n_skipped)

        # Build DuplicateGroups for accepted entries
        for group, dupes in result:
            accepted_all.append(DuplicateGroup(keep=group.keep, duplicates=dupes))

    # Print summary table
    total_accepted = sum(a for a, _ in summary.values())
    total_skipped = sum(s for _, s in summary.values())

    print(f"\n{'=' * 80}")
    print(f" Review complete")
    print(f"{'=' * 80}")
    for folder in sorted(summary):
        a, s = summary[folder]
        print(f"  {folder + ':':20s} {a:>4} accepted, {s:>4} skipped")
    print(f"  {'Total:':20s} {total_accepted:>4} accepted, {total_skipped:>4} skipped")

    return accepted_all


# ---------------------------------------------------------------------------
# IMAP helpers
# ---------------------------------------------------------------------------


def imap_quote_folder(folder: str) -> str:
    """Quote an IMAP folder name, escaping backslashes and double quotes per RFC."""
    escaped = folder.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def extract_uid(filename: str) -> int | None:
    """Extract IMAP UID from a Maildir filename (OfflineIMAP format).

    Looks for ,U=<digits> followed by , or : in the filename.
    """
    m = re.search(r",U=(\d+)[,:]", filename)
    return int(m.group(1)) if m else None


def local_to_imap_folder(display_name: str) -> str:
    """Convert local Maildir display name to IMAP folder path.

    Reverses the OfflineIMAP nametrans: INBOX stays, otherwise dots become
    slashes.  E.g. '2_Job.Uni Trier' → '2_Job/Uni Trier'.

    Note: this follows the OfflineIMAP nametrans convention where dots are
    hierarchy separators.  Folder names containing literal dots are not
    supported by this mapping.
    """
    if display_name == "INBOX":
        return "INBOX"
    return display_name.replace(".", "/")


def imap_connect(host: str) -> imaplib.IMAP4_SSL:
    """Connect to IMAP server using TLS and authenticate via ~/.netrc."""
    ctx = ssl.create_default_context()
    conn = imaplib.IMAP4_SSL(host, ssl_context=ctx)
    try:
        try:
            nrc = netrc.netrc()
            auth = nrc.authenticators(host)
        except (FileNotFoundError, netrc.NetrcParseError) as e:
            raise SystemExit(f"ERROR: Cannot read ~/.netrc: {e}")
        if auth is None:
            raise SystemExit(f"ERROR: No entry for {host} in ~/.netrc")
        login, _, password = auth
        if password is None:
            raise SystemExit(f"ERROR: No password for {host} in ~/.netrc")
        conn.login(login, password)
    except BaseException:
        try:
            conn.logout()
        except Exception:
            pass
        raise
    return conn


def imap_parse_list_entry(item: bytes) -> tuple[set[bytes], str] | None:
    """Parse one IMAP LIST response into (attributes_set, folder_name).

    Expected format: (\\Flag1 \\Flag2) "delimiter" "folder_name"
    Returns None if the entry cannot be parsed.
    """
    m = re.match(
        rb'\(([^)]*)\)\s+'
        rb'(?:"[^"]*"|NIL)\s+'
        rb'(?:"((?:[^"\\]|\\.)*)"|(\S+))',
        item,
    )
    if not m:
        return None
    flags_raw = m.group(1)
    # Quoted name in group(2), unquoted in group(3)
    raw_name = m.group(2) if m.group(2) is not None else m.group(3)
    # Unescape backslash-escaped characters in quoted names
    if m.group(2) is not None:
        raw_name = re.sub(rb'\\(.)', rb'\1', raw_name)
    folder_name = raw_name.decode("utf-8", errors="replace")
    attributes = {f.strip() for f in flags_raw.split() if f.strip()}
    return attributes, folder_name


def imap_list_all_folders(conn: imaplib.IMAP4_SSL) -> list[tuple[str, set[bytes]]]:
    """List all IMAP folders with their attributes.

    Returns [(folder_name, attributes_set), ...].
    """
    typ, data = conn.list('""', '*')
    if typ != "OK":
        return []
    results = []
    for item in data:
        if not isinstance(item, bytes):
            continue
        parsed = imap_parse_list_entry(item)
        if parsed:
            attrs, name = parsed
            results.append((name, attrs))
    return results


def imap_find_trash_folder(conn: imaplib.IMAP4_SSL) -> str:
    """Auto-detect the Trash folder via RFC 6154 \\Trash special-use attribute.

    Falls back to "Trash" if no folder with the attribute is found.
    """
    for name, attrs in imap_list_all_folders(conn):
        if b"\\Trash" in attrs:
            return name
    return "Trash"


def imap_fetch_all_message_ids(
    conn: imaplib.IMAP4_SSL,
    folder: str,
    quiet: bool = False,
) -> dict[str, str] | None:
    """Fetch all Message-IDs from a folder.

    Selects folder readonly and fetches Message-ID headers in bulk.
    Returns {normalized_message_id: uid_str}, or None if folder can't be selected.
    """
    typ, data = conn.select(imap_quote_folder(folder), readonly=True)
    if typ != "OK":
        return None

    # Get message count
    try:
        num_messages = int(data[0])
    except (ValueError, TypeError):
        return None
    if num_messages == 0:
        return {}

    typ, data = conn.uid("SEARCH", None, "ALL")
    if typ != "OK" or not data[0]:
        return {}

    uids = data[0].split()
    uid_set = b",".join(uids).decode()

    typ, data = conn.uid("FETCH", uid_set, "(UID BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
    if typ != "OK":
        return {}

    result: dict[str, str] = {}
    hdr_parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)

    # FETCH responses come as tuples: (b'UID FLAGS...', b'header data')
    i = 0
    while i < len(data):
        item = data[i]
        if isinstance(item, tuple) and len(item) == 2:
            # Parse UID from response
            uid_match = re.search(rb"UID\s+(\d+)", item[0])
            if uid_match:
                uid_str = uid_match.group(1).decode()
                raw_header = item[1]
                if isinstance(raw_header, str):
                    raw_header = raw_header.encode("utf-8", errors="replace")
                parsed = hdr_parser.parsebytes(raw_header)
                mid_value = parsed.get("Message-ID")
                if mid_value:
                    mid = normalize_message_id(mid_value)
                    if mid:
                        result[mid] = uid_str
        i += 1

    return result


def imap_copy_messages(
    conn: imaplib.IMAP4_SSL,
    src_folder: str,
    uids: list[str],
    dest_folder: str,
    quiet: bool = False,
) -> int:
    """Copy messages by UID from src_folder to dest_folder.

    Returns count of successfully copied messages.
    """
    if not uids:
        return 0

    typ, _ = conn.select(imap_quote_folder(src_folder))
    if typ != "OK":
        if not quiet:
            print(f"  WARNING: Cannot select {src_folder}", file=sys.stderr)
        return 0

    uid_set = ",".join(uids)
    typ, _ = conn.uid("COPY", uid_set, imap_quote_folder(dest_folder))
    if typ == "OK":
        return len(uids)
    if not quiet:
        print(f"  WARNING: COPY failed from {src_folder} to {dest_folder}",
              file=sys.stderr)
    return 0


def imap_delete_folder(
    conn: imaplib.IMAP4_SSL,
    folder: str,
    quiet: bool = False,
) -> bool:
    """Delete an IMAP folder. Returns True on success."""
    typ, _ = conn.delete(imap_quote_folder(folder))
    if typ == "OK":
        return True
    if not quiet:
        print(f"  WARNING: Cannot delete folder {folder}", file=sys.stderr)
    return False


def imap_folder_message_count(
    conn: imaplib.IMAP4_SSL,
    folder: str,
) -> int:
    """Return the number of messages in a folder, or 0 if it can't be selected."""
    typ, data = conn.select(imap_quote_folder(folder), readonly=True)
    if typ != "OK":
        return 0
    try:
        return int(data[0])
    except (ValueError, TypeError):
        return 0


def clean_hidden_folders(
    imap_host: str,
    dry_run: bool = False,
    rescue_folder: str = "Recovered",
    delete_folders: bool = False,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
    """Detect hidden IMAP folders and clean up their messages.

    Hidden folders are those with \\Noselect or \\NonExistent attributes.
    Duplicated messages (also in normal folders) are deleted; orphaned messages
    are copied to a rescue folder first.

    Returns exit code: 0 on success, 1 on error.
    """
    if not quiet:
        print("Clean hidden folders")
        print(f"Host: {imap_host}")
        print()

    conn = imap_connect(imap_host)

    try:
        # 1. List all folders with attributes
        all_folders = imap_list_all_folders(conn)
        if not all_folders:
            if not quiet:
                print("No folders found.")
            return 0

        # 2. Classify: hidden vs normal
        hidden_attrs = {b"\\Noselect", b"\\NonExistent"}
        hidden_folders = []
        normal_folders = []
        for name, attrs in all_folders:
            if attrs & hidden_attrs:
                hidden_folders.append((name, attrs))
            else:
                normal_folders.append((name, attrs))

        if not hidden_folders:
            if not quiet:
                print("No hidden folders found.")
            return 0

        if not quiet:
            print(f"Found {len(hidden_folders)} hidden folder(s):")
            for name, attrs in hidden_folders:
                attr_str = " ".join(a.decode("utf-8", errors="replace") for a in sorted(attrs))
                print(f"  {name}  [{attr_str}]")
            print()

        # 3. Fetch Message-IDs from hidden folders
        hidden_messages: dict[str, dict[str, str]] = {}  # folder -> {mid: uid}
        for name, _ in hidden_folders:
            mids = imap_fetch_all_message_ids(conn, name, quiet=quiet)
            if mids is None:
                if not quiet:
                    print(f"  {name}: cannot select (truly non-existent), skipping")
                continue
            hidden_messages[name] = mids
            if not quiet:
                print(f"  {name}: {len(mids)} message(s)")

        total_hidden_msgs = sum(len(m) for m in hidden_messages.values())
        if total_hidden_msgs == 0:
            if not quiet:
                print("\nNo messages in hidden folders.")
            if delete_folders and not dry_run:
                for name, _ in hidden_folders:
                    if name in hidden_messages:
                        if imap_delete_folder(conn, name, quiet=quiet):
                            if not quiet:
                                print(f"  Deleted empty folder: {name}")
            return 0

        # 4. Fetch Message-IDs from all normal folders
        if not quiet:
            print(f"\nScanning {len(normal_folders)} normal folder(s)...")

        normal_message_ids: set[str] = set()
        for name, _ in normal_folders:
            mids = imap_fetch_all_message_ids(conn, name, quiet=quiet)
            if mids is not None:
                normal_message_ids.update(mids.keys())
                if verbose:
                    print(f"  {name}: {len(mids)} message(s)")

        if not quiet:
            print(f"  Total unique Message-IDs in normal folders: {len(normal_message_ids)}")

        # 5. Cross-reference: classify hidden messages
        total_duplicated = 0
        total_orphaned = 0
        folder_report: dict[str, tuple[list[tuple[str, str]], list[tuple[str, str]]]] = {}

        for folder, mids in hidden_messages.items():
            duplicated = []  # [(mid, uid), ...]
            orphaned = []    # [(mid, uid), ...]
            for mid, uid in mids.items():
                if mid in normal_message_ids:
                    duplicated.append((mid, uid))
                else:
                    orphaned.append((mid, uid))
            folder_report[folder] = (duplicated, orphaned)
            total_duplicated += len(duplicated)
            total_orphaned += len(orphaned)

        # 6. Safety report
        if not quiet:
            print(f"\n{'=' * 60}")
            print(f"Safety report")
            print(f"{'=' * 60}")
            for folder, (duplicated, orphaned) in sorted(folder_report.items()):
                print(f"  {folder}:")
                print(f"    Duplicated (safe to delete): {len(duplicated)}")
                print(f"    Orphaned (will rescue):      {len(orphaned)}")
                if verbose:
                    for mid, uid in orphaned:
                        print(f"      UID {uid}: {mid}")
            print(f"\n  Total duplicated: {total_duplicated}")
            print(f"  Total orphaned:   {total_orphaned}")

        if dry_run:
            if not quiet:
                print(f"\nDry run — no changes made.")
            return 0

        # 7. Copy orphaned messages to rescue folder
        if total_orphaned > 0:
            # Create rescue folder if needed
            conn.create(imap_quote_folder(rescue_folder))  # OK if already exists
            typ, _ = conn.select(imap_quote_folder(rescue_folder))
            if typ != "OK":
                print(f"ERROR: Cannot create/access rescue folder {rescue_folder}", file=sys.stderr)
                return 1
            if not quiet:
                print(f"\nCopying {total_orphaned} orphaned message(s) to {rescue_folder}...")

            copied = 0
            for folder, (_, orphaned) in sorted(folder_report.items()):
                if not orphaned:
                    continue
                orphan_uids = [uid for _, uid in orphaned]
                n = imap_copy_messages(conn, folder, orphan_uids, rescue_folder, quiet=quiet)
                copied += n
                if not quiet:
                    print(f"  {folder}: copied {n}/{len(orphan_uids)} orphan(s)")

            if copied < total_orphaned:
                print(f"ERROR: Only copied {copied}/{total_orphaned} orphans. "
                      f"Aborting to prevent data loss.", file=sys.stderr)
                return 1

        # 8. Delete all messages from hidden folders
        if not quiet:
            print(f"\nDeleting messages from hidden folders...")

        total_deleted = 0
        for folder, (duplicated, orphaned) in sorted(folder_report.items()):
            all_uids = [uid for _, uid in duplicated] + [uid for _, uid in orphaned]
            if not all_uids:
                continue

            # Re-verify Message-IDs before deletion
            current_mids = imap_fetch_all_message_ids(conn, folder, quiet=quiet)
            if current_mids is None:
                if not quiet:
                    print(f"  {folder}: cannot re-select, skipping deletion")
                continue

            verified_uids = []
            for mid, uid in duplicated + orphaned:
                if current_mids.get(mid) == uid:
                    verified_uids.append(uid)
                elif verbose:
                    print(f"  {folder}: UID {uid} changed, skipping")

            if not verified_uids:
                continue

            typ, _ = conn.select(imap_quote_folder(folder))
            if typ != "OK":
                continue

            uid_set = ",".join(verified_uids)
            typ, _ = conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")
            if typ == "OK":
                conn.expunge()
                total_deleted += len(verified_uids)
                if not quiet:
                    print(f"  {folder}: deleted {len(verified_uids)} message(s)")

        # 9. Delete empty hidden folders if requested
        folders_deleted = 0
        if delete_folders:
            if not quiet:
                print(f"\nDeleting empty hidden folders...")
            for name, _ in hidden_folders:
                if name not in hidden_messages:
                    continue
                count = imap_folder_message_count(conn, name)
                if count == 0:
                    if imap_delete_folder(conn, name, quiet=quiet):
                        folders_deleted += 1
                        if not quiet:
                            print(f"  Deleted: {name}")
                elif not quiet:
                    print(f"  {name}: still has {count} message(s), skipping")

        # 10. Summary
        if not quiet:
            print(f"\n{'=' * 60}")
            print(f"Summary")
            print(f"{'=' * 60}")
            print(f"  Messages deleted: {total_deleted}")
            if total_orphaned > 0:
                print(f"  Orphans rescued to {rescue_folder}: {total_orphaned}")
            if delete_folders:
                print(f"  Folders deleted: {folders_deleted}")

    finally:
        try:
            conn.logout()
        except (imaplib.IMAP4.error, OSError):
            pass

    return 0


def prune_noselect_folders(
    imap_host: str,
    dry_run: bool = False,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
    """Delete \\Noselect hierarchy nodes that contain no messages transitively.

    Returns exit code: 0 on success, 1 on error.
    """
    if not quiet:
        print("Prune empty \\Noselect folders")
        print(f"Host: {imap_host}")
        print()

    conn = imap_connect(imap_host)

    try:
        all_folders = imap_list_all_folders(conn)
        if not all_folders:
            if not quiet:
                print("No folders found.")
            return 0

        # Find \Noselect folders
        noselect_folders = [(name, attrs) for name, attrs in all_folders
                            if b"\\Noselect" in attrs]

        if not noselect_folders:
            if not quiet:
                print("No \\Noselect folders found.")
            return 0

        if not quiet:
            print(f"Found {len(noselect_folders)} \\Noselect folder(s):")
            for name, _ in noselect_folders:
                print(f"  {name}")
            print()

        # Build list of all folder names for descendant checks
        all_folder_names = [(name, attrs) for name, attrs in all_folders]

        # Check each \Noselect folder for messages (direct + transitive)
        prunable = []
        for ns_name, ns_attrs in noselect_folders:
            # Check direct messages
            direct_count = imap_folder_message_count(conn, ns_name)

            # Find descendants (folders starting with ns_name + delimiter)
            # Common delimiters: "/" and "."
            descendant_total = 0
            has_descendants = False
            for fname, fattrs in all_folder_names:
                if fname != ns_name and (fname.startswith(ns_name + "/") or
                                          fname.startswith(ns_name + ".")):
                    has_descendants = True
                    count = imap_folder_message_count(conn, fname)
                    descendant_total += count
                    if verbose:
                        print(f"  {fname}: {count} message(s)")

            total = direct_count + descendant_total
            if total == 0:
                prunable.append(ns_name)
                if not quiet:
                    desc = f" ({descendant_total} in descendants)" if has_descendants else ""
                    print(f"  Prunable: {ns_name}{desc}")
            elif not quiet:
                print(f"  Keeping:  {ns_name} ({total} messages)")

        if not prunable:
            if not quiet:
                print("\nNo \\Noselect folders to prune.")
            return 0

        if not quiet:
            print(f"\n{len(prunable)} folder(s) to prune.")

        if dry_run:
            if not quiet:
                print("Dry run — no changes made.")
            return 0

        # Delete in reverse-depth order (deepest first)
        prunable.sort(key=lambda n: n.count("/") + n.count("."), reverse=True)

        deleted = 0
        for name in prunable:
            if imap_delete_folder(conn, name, quiet=quiet):
                deleted += 1
                if not quiet:
                    print(f"  Deleted: {name}")

        if not quiet:
            print(f"\nDeleted {deleted}/{len(prunable)} folder(s).")

    finally:
        try:
            conn.logout()
        except (imaplib.IMAP4.error, OSError):
            pass

    return 0


def imap_verify_and_delete(
    conn: imaplib.IMAP4_SSL,
    folder: str,
    entries: list[dict],
    delete: bool = False,
    permanent: bool = True,
    trash_folder: str = "Trash",
    verbose: bool = False,
    quiet: bool = False,
    batch_size: int = 500,
) -> tuple[int, int, int]:
    """Verify and optionally delete messages in one IMAP folder.

    When *delete* is True:
    - *permanent*: STORE +FLAGS (\\Deleted) + EXPUNGE
    - not *permanent*: UID MOVE to *trash_folder*

    Returns (verified, mismatched, deleted).
    """
    typ, data = conn.select(imap_quote_folder(folder))
    if typ != "OK":
        if not quiet:
            print(f"  WARNING: Cannot select folder {folder}: {data}", file=sys.stderr)
        return 0, len(entries), 0

    verified = 0
    mismatched = 0
    uids_to_delete: list[str] = []
    hdr_parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
    uid_re = re.compile(rb"UID (\d+)")

    # Build lookup: uid_str -> entry
    uid_to_entry: dict[str, dict] = {}
    for entry in entries:
        uid_to_entry[str(entry["uid"])] = entry

    all_uids = list(uid_to_entry.keys())
    total_batches = (len(all_uids) + batch_size - 1) // batch_size

    for batch_idx in range(0, len(all_uids), batch_size):
        batch_uids = all_uids[batch_idx : batch_idx + batch_size]
        uid_set = ",".join(batch_uids)
        batch_num = batch_idx // batch_size + 1

        if not quiet and total_batches > 1:
            print(f"    Verifying batch {batch_num}/{total_batches} ({len(batch_uids)} messages)...")

        typ, data = conn.uid("FETCH", uid_set, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")

        # Track which UIDs we got responses for
        found_uids: set[str] = set()
        batch_mismatched = 0

        if typ == "OK" and data:
            # Parse multi-message response: data is a flat list where each
            # message is a (envelope_bytes, header_bytes) tuple followed by b")"
            for item in data:
                if not isinstance(item, tuple) or len(item) < 2:
                    continue
                envelope, raw_header = item[0], item[1]
                # Extract UID from envelope line
                m = uid_re.search(envelope)
                if not m:
                    continue
                uid_str = m.group(1).decode()
                if uid_str not in uid_to_entry:
                    continue
                found_uids.add(uid_str)

                if isinstance(raw_header, str):
                    raw_header = raw_header.encode("utf-8", errors="replace")

                parsed = hdr_parser.parsebytes(raw_header)
                mid_value = parsed.get("Message-ID")
                fetched_mid = normalize_message_id(mid_value) if mid_value else ""

                entry = uid_to_entry[uid_str]
                expected_mid = entry["message_id"]

                if fetched_mid == expected_mid:
                    verified += 1
                    if delete:
                        uids_to_delete.append(uid_str)
                else:
                    batch_mismatched += 1
                    mismatched += 1

        # UIDs not found in response count as mismatched (already deleted)
        batch_not_found = 0
        for uid_str in batch_uids:
            if uid_str not in found_uids:
                mismatched += 1
                batch_not_found += 1

        if verbose and (batch_mismatched or batch_not_found):
            print(f"    Batch {batch_num}/{total_batches}: "
                  f"{batch_mismatched} mismatched, {batch_not_found} not found")

    deleted = 0
    if uids_to_delete:
        # Batch UIDs to avoid exceeding IMAP command length limits.
        if permanent:
            for i in range(0, len(uids_to_delete), batch_size):
                batch = uids_to_delete[i : i + batch_size]
                uid_set = ",".join(batch)
                typ, _ = conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")
                if typ == "OK":
                    deleted += len(batch)
                else:
                    print(f"  ERROR: Failed to flag messages for deletion in {folder}",
                          file=sys.stderr)
                    break
            if deleted:
                conn.expunge()
                if not quiet:
                    print(f"    Deleted {deleted} message(s) from {folder}")
        else:
            for i in range(0, len(uids_to_delete), batch_size):
                batch = uids_to_delete[i : i + batch_size]
                uid_set = ",".join(batch)
                typ, _ = conn.uid("MOVE", uid_set, imap_quote_folder(trash_folder))
                if typ == "OK":
                    deleted += len(batch)
                else:
                    print(f"  ERROR: Failed to move messages to {trash_folder} in {folder}",
                          file=sys.stderr)
                    break
            if deleted and not quiet:
                print(f"    Moved {deleted} message(s) from {folder} to {trash_folder}")

    return verified, mismatched, deleted


# ---------------------------------------------------------------------------
# Export / Apply plan
# ---------------------------------------------------------------------------


def export_plan(
    groups: list[DuplicateGroup],
    output_path: str,
    imap_host: str,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
    """Export dedup plan as JSON manifest for IMAP deletion.

    Returns exit code: 0 on success, 1 on error.
    """
    plan_groups = []
    skipped_no_mid = 0
    skipped_no_uid = 0

    for group in groups:
        # Skip fingerprint-only groups — no Message-ID to verify on IMAP
        if group.keep.method != METHOD_MESSAGE_ID:
            skipped_no_mid += 1
            continue

        keep_uid = extract_uid(group.keep.path.name)
        if keep_uid is None:
            skipped_no_uid += 1
            continue

        keep_entry = {
            "uid": keep_uid,
            "imap_folder": local_to_imap_folder(group.keep.folder),
            "message_id": group.keep.message_id,
            "subject": str(group.keep.subject),
        }

        delete_entries = []
        skip_group = False
        for dup in group.duplicates:
            dup_uid = extract_uid(dup.path.name)
            if dup_uid is None:
                skipped_no_uid += 1
                skip_group = True
                break
            delete_entries.append({
                "uid": dup_uid,
                "imap_folder": local_to_imap_folder(dup.folder),
                "message_id": dup.message_id,
                "subject": str(dup.subject),
                "flags": dup.flags,
                "size": dup.size,
            })

        if skip_group:
            continue

        plan_groups.append({
            "keep": keep_entry,
            "delete": delete_entries,
        })

    total_deletions = sum(len(g["delete"]) for g in plan_groups)

    plan = {
        "version": 1,
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "imap_host": imap_host,
        "stats": {
            "groups": len(plan_groups),
            "total_deletions": total_deletions,
            "skipped_no_message_id": skipped_no_mid,
            "skipped_no_uid": skipped_no_uid,
        },
        "groups": plan_groups,
    }

    try:
        with open(output_path, "w") as f:
            json.dump(plan, f, indent=2)
    except OSError as e:
        print(f"ERROR: Cannot write {output_path}: {e}", file=sys.stderr)
        return 1

    if not quiet:
        print(f"\nExported IMAP deletion plan to {output_path}")
        print(f"  Groups: {len(plan_groups)}")
        print(f"  Total deletions: {total_deletions}")
        if skipped_no_mid:
            print(f"  Skipped (no Message-ID): {skipped_no_mid}")
        if skipped_no_uid:
            print(f"  Skipped (no UID in filename): {skipped_no_uid}")
        print(f"\nTo verify:  python3 imap_dedup.py --apply {output_path} --dry-run")
        print(f"To apply:   python3 imap_dedup.py --apply {output_path}")

    return 0


def apply_plan(
    plan_path: str,
    dry_run: bool = False,
    permanent: bool = False,
    imap_trash: str | None = None,
    imap_host_override: str | None = None,
    folders_filter: list[str] | None = None,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
    """Apply an IMAP deletion plan: verify UIDs and delete.

    By default, moves duplicates to Trash.  With *permanent*, uses
    STORE+EXPUNGE.  With *dry_run*, only verifies without deleting.

    Returns exit code: 0 (all OK), 1 (error), 2 (some mismatches).
    """
    try:
        with open(plan_path) as f:
            plan = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"ERROR: Cannot read plan {plan_path}: {e}", file=sys.stderr)
        return 1

    if plan.get("version") != 1:
        print(f"ERROR: Unsupported plan version: {plan.get('version')}",
              file=sys.stderr)
        return 1

    # Warn if plan is old
    try:
        created_str = plan["created"]
        # "Z" suffix not supported by fromisoformat before Python 3.11
        if created_str.endswith("Z"):
            created_str = created_str[:-1] + "+00:00"
        created = datetime.fromisoformat(created_str)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
        if age_hours > 24:
            print(f"WARNING: Plan is {age_hours:.0f} hours old. "
                  "Consider re-exporting for safety.", file=sys.stderr)
    except (KeyError, ValueError):
        pass

    imap_host = imap_host_override or plan.get("imap_host")
    if not imap_host:
        print("ERROR: No IMAP host in plan and --imap-host not specified",
              file=sys.stderr)
        return 1
    groups = plan.get("groups", [])

    if not groups:
        print("Plan contains no groups.")
        return 0

    # Group deletions by IMAP folder
    by_folder: dict[str, list[dict]] = {}
    for group in groups:
        for entry in group["delete"]:
            folder = entry["imap_folder"]
            if folders_filter is not None:
                # Match against local display name or IMAP folder name
                local_name = folder.replace("/", ".")
                if folder not in folders_filter and local_name not in folders_filter:
                    continue
            by_folder.setdefault(folder, []).append(entry)

    total_entries = sum(len(v) for v in by_folder.values())
    if total_entries == 0:
        print("No entries to process (all filtered out?).")
        return 0

    if dry_run:
        mode = "VERIFY (dry-run)"
    elif permanent:
        mode = "PERMANENT DELETE"
    else:
        mode = "MOVE TO TRASH"
    if not quiet:
        print(f"IMAP plan apply — {mode}")
        print(f"Host: {imap_host}")
        print(f"Folders: {len(by_folder)}")
        print(f"Messages to process: {total_entries}")
        print()

    # Connect to IMAP
    if not quiet:
        print(f"Connecting to {imap_host}...")
    conn = imap_connect(imap_host)

    # Auto-detect Trash folder if needed
    trash_folder = "Trash"
    if not dry_run and not permanent:
        trash_folder = imap_trash or imap_find_trash_folder(conn)
        if not quiet:
            print(f"Trash folder: {trash_folder}")

    total_verified = 0
    total_mismatched = 0
    total_deleted = 0

    interrupted = False
    try:
        for folder in sorted(by_folder):
            entries = by_folder[folder]
            if not quiet:
                print(f"  {folder} ({len(entries)} messages)...")

            verified, mismatched, deleted = imap_verify_and_delete(
                conn, folder, entries,
                delete=not dry_run, permanent=permanent,
                trash_folder=trash_folder,
                verbose=verbose, quiet=quiet,
            )
            total_verified += verified
            total_mismatched += mismatched
            total_deleted += deleted
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        interrupted = True
        try:
            conn.shutdown()
        except OSError:
            pass
        return 1
    finally:
        if not interrupted:
            try:
                conn.logout()
            except (imaplib.IMAP4.error, OSError):
                pass

    if not quiet:
        print(f"\n{'=' * 60}")
        print(f"Verified:   {total_verified}")
        print(f"Mismatched: {total_mismatched}")
        if dry_run:
            print(f"\nThis was a dry run. Run without --dry-run to apply.")
        else:
            action = "Permanently deleted" if permanent else "Moved to Trash"
            print(f"{action}: {total_deleted}")

    if total_mismatched > 0:
        return 2
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deduplicate emails in a Maildir hierarchy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s ~/Maildir --export plan.json --imap-host HOST     Scan + export plan\n"
            "  %(prog)s ~/Maildir --interactive --export plan.json --imap-host HOST  Interactive review + export\n"
            "  %(prog)s --apply plan.json --dry-run                       Verify plan (dry-run)\n"
            "  %(prog)s --apply plan.json                                 Apply: move to Trash\n"
            "  %(prog)s --apply plan.json --permanent                     Apply: permanent delete\n"
            "  %(prog)s --clean-hidden --imap-host HOST --dry-run         Report hidden folders\n"
            "  %(prog)s --clean-hidden --imap-host HOST                   Clean hidden folders\n"
            "  %(prog)s --prune-noselect --imap-host HOST --dry-run       Report empty \\Noselect folders\n"
            "  %(prog)s --prune-noselect --imap-host HOST                 Prune empty \\Noselect folders\n"
        ),
    )
    parser.add_argument(
        "maildir_path",
        nargs="?",
        default=os.path.expanduser("~/Maildir"),
        help="Root Maildir directory (default: ~/Maildir)",
    )
    parser.add_argument(
        "-p", "--permanent",
        action="store_true",
        help="Permanently delete (EXPUNGE) instead of moving to Trash",
    )
    parser.add_argument(
        "-s", "--same-folder-only",
        action="store_true",
        help="Only deduplicate within each folder (not across folders)",
    )
    parser.add_argument(
        "-f", "--folders",
        nargs="+",
        metavar="FOLDER",
        help="Only scan these folders (display names, e.g. '0_Sent')",
    )
    parser.add_argument(
        "-x", "--exclude-folders",
        nargs="+",
        metavar="FOLDER",
        default=list(DEFAULT_EXCLUDE_FOLDERS),
        help=f"Exclude these folders (default: {' '.join(sorted(DEFAULT_EXCLUDE_FOLDERS))})",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show details for every duplicate group",
    )
    verbosity.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Summary only — suppress per-group output",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "-e", "--export",
        metavar="FILE",
        help="Export dedup plan as JSON for IMAP deletion",
    )
    mode.add_argument(
        "-a", "--apply",
        metavar="FILE",
        help="Apply plan via IMAP (deletes duplicates; use -d for dry-run)",
    )
    mode.add_argument(
        "--clean-hidden",
        action="store_true",
        help="Detect and clean hidden IMAP folders (\\Noselect/\\NonExistent)",
    )
    mode.add_argument(
        "--prune-noselect",
        action="store_true",
        help="Delete empty \\Noselect hierarchy nodes (no messages transitively)",
    )
    parser.add_argument(
        "-d", "--dry-run",
        action="store_true",
        help="Preview without making changes (for --apply, --clean-hidden, --prune-noselect)",
    )
    parser.add_argument(
        "--delete-folders",
        action="store_true",
        help="Also delete empty hidden folders after cleaning (requires --clean-hidden)",
    )
    parser.add_argument(
        "--rescue-folder",
        metavar="FOLDER",
        default="Recovered",
        help="Destination folder for orphaned messages (default: Recovered)",
    )
    parser.add_argument(
        "-i", "--interactive",
        action="store_true",
        help="Interactively review duplicates folder-by-folder before export",
    )
    parser.add_argument(
        "-H", "--imap-host",
        metavar="HOST",
        default=None,
        help="IMAP server hostname (required for --export)",
    )
    parser.add_argument(
        "-T", "--imap-trash",
        metavar="FOLDER",
        default=None,
        help="Override Trash folder name (auto-detected by default)",
    )
    parser.add_argument(
        "-S", "--sender",
        metavar="IDENTITY",
        help="Your sender identity (bag-of-words match against From header). "
             "When set, Sent folder priority only applies if From matches.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    # --- Validation ---
    if args.dry_run and not (args.apply or args.clean_hidden or args.prune_noselect):
        print("ERROR: --dry-run requires --apply, --clean-hidden, or --prune-noselect",
              file=sys.stderr)
        return 1

    if args.permanent and not args.apply:
        print("ERROR: --permanent requires --apply", file=sys.stderr)
        return 1

    if args.delete_folders and not args.clean_hidden:
        print("ERROR: --delete-folders requires --clean-hidden", file=sys.stderr)
        return 1

    if (args.clean_hidden or args.prune_noselect) and not args.imap_host:
        mode_name = "--clean-hidden" if args.clean_hidden else "--prune-noselect"
        print(f"ERROR: {mode_name} requires --imap-host", file=sys.stderr)
        return 1

    if args.interactive:
        for conflict, name in [
            (args.quiet, "--quiet"),
            (args.apply, "--apply"),
        ]:
            if conflict:
                print(f"ERROR: --interactive is incompatible with {name}",
                      file=sys.stderr)
                return 1
        if not sys.stdin.isatty():
            print("ERROR: --interactive requires a terminal (TTY)",
                  file=sys.stderr)
            return 1

    # --- Clean hidden folders mode ---
    if args.clean_hidden:
        return clean_hidden_folders(
            imap_host=args.imap_host,
            dry_run=args.dry_run,
            rescue_folder=args.rescue_folder,
            delete_folders=args.delete_folders,
            verbose=args.verbose,
            quiet=args.quiet,
        )

    # --- Prune noselect mode ---
    if args.prune_noselect:
        return prune_noselect_folders(
            imap_host=args.imap_host,
            dry_run=args.dry_run,
            verbose=args.verbose,
            quiet=args.quiet,
        )

    # --- Apply mode: no local scan needed ---
    if args.apply:
        return apply_plan(
            plan_path=args.apply,
            dry_run=args.dry_run,
            permanent=args.permanent,
            imap_trash=args.imap_trash,
            imap_host_override=args.imap_host,
            folders_filter=args.folders,
            verbose=args.verbose,
            quiet=args.quiet,
        )

    # --- Validate --imap-host for --export ---
    if args.export and not args.imap_host:
        print("ERROR: --export requires --imap-host", file=sys.stderr)
        return 1

    # --- Local scan mode (report / export) ---
    maildir_root = Path(args.maildir_path).expanduser().resolve()

    if not maildir_root.is_dir():
        print(f"ERROR: {maildir_root} is not a directory", file=sys.stderr)
        return 1

    exclude = set(args.exclude_folders)
    include = args.folders  # None means all

    # Discover folders
    folders = discover_folders(maildir_root, include=include, exclude=exclude)

    if not folders:
        print("No folders found to scan.")
        return 0

    if not args.quiet:
        mode = "EXPORT" if args.export else "SCAN"
        print(f"Maildir dedup — {mode}")
        print(f"Root: {maildir_root}")
        print(f"Folders to scan: {len(folders)}")
        if args.same_folder_only:
            print("Mode: same-folder-only")
        print()

    # Scan all folders
    all_messages: list[MessageInfo] = []
    for folder_name, folder_path in folders:
        if not args.quiet:
            print(f"Scanning {folder_name}...", end="", flush=True)
        msgs = scan_folder(folder_name, folder_path, verbose=args.verbose,
                           progress_every=1000 if not args.quiet else 0)
        if not args.quiet:
            print(f" {len(msgs)} messages")
        all_messages.extend(msgs)

    if not args.quiet:
        mid_count = sum(1 for m in all_messages if m.method == METHOD_MESSAGE_ID)
        fp_count = sum(1 for m in all_messages if m.method == METHOD_FINGERPRINT)
        print(f"\nTotal messages scanned: {len(all_messages)}")
        print(f"  With Message-ID: {mid_count}")
        print(f"  Fingerprinted:   {fp_count}")

    # Find duplicates
    groups = find_duplicates(
        all_messages,
        same_folder_only=args.same_folder_only,
        sender=args.sender,
    )

    if not groups:
        if not args.quiet:
            print("\nNo duplicates found.")
        return 0

    # Interactive mode
    if args.interactive:
        accepted = run_interactive_review(groups)
        if accepted is None:
            return 0
        total_accepted = sum(len(g.duplicates) for g in accepted)
        if total_accepted == 0:
            print("\nNothing to delete.")
            return 0
        if args.export:
            return export_plan(
                accepted,
                output_path=args.export,
                imap_host=args.imap_host,
                verbose=args.verbose,
            )
        print("\nUse --export to save plan for IMAP deletion.")
        return 0

    # Report
    print_report(groups, verbose=args.verbose, quiet=args.quiet)

    # Export mode
    if args.export:
        return export_plan(
            groups,
            output_path=args.export,
            imap_host=args.imap_host,
            verbose=args.verbose,
            quiet=args.quiet,
        )

    if not args.quiet:
        print("\nUse --export to save plan for IMAP deletion.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
