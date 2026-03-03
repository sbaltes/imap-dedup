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


def decode_header(raw: str) -> str:
    """Decode RFC 2047 encoded header value to a Unicode string."""
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


def _review_one_by_one(
    entries: list[tuple[DuplicateGroup, list[MessageInfo]]],
) -> list[tuple[DuplicateGroup, list[MessageInfo]]] | None:
    """Review entries one-by-one, return accepted list or None on quit."""
    accepted = []
    for i, (group, dupes) in enumerate(entries, 1):
        print()
        print(format_interactive_entry(i, group, dupes))
        ch = interactive_prompt(
            f"  Delete this duplicate? [y]es / [n]o / [q]uit: ", "ynq"
        )
        if ch is None or ch == "q":
            return None
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
    return conn


def imap_find_trash_folder(conn: imaplib.IMAP4_SSL) -> str:
    """Auto-detect the Trash folder via RFC 6154 \\Trash special-use attribute.

    Falls back to "Trash" if no folder with the attribute is found.
    """
    typ, data = conn.list('', '*')
    if typ == "OK":
        for item in data:
            if isinstance(item, bytes) and b"\\Trash" in item:
                # LIST response format: (flags) delimiter "folder name"
                # Extract the quoted folder name after the last space
                m = re.search(rb'"([^"]*)"$', item)
                if m:
                    return m.group(1).decode("utf-8", errors="replace")
                # Fallback: last space-separated token
                parts = item.split()
                if parts:
                    return parts[-1].decode("utf-8", errors="replace").strip('"')
    return "Trash"


def imap_verify_and_delete(
    conn: imaplib.IMAP4_SSL,
    folder: str,
    entries: list[dict],
    delete: bool = False,
    permanent: bool = True,
    trash_folder: str = "Trash",
    verbose: bool = False,
    quiet: bool = False,
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

    for entry in entries:
        uid = str(entry["uid"])
        expected_mid = entry["message_id"]

        typ, data = conn.uid("FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
        if typ != "OK" or not data or data[0] is None:
            if verbose:
                print(f"    UID {uid}: not found on server (already deleted?)")
            mismatched += 1
            continue

        # Parse the fetched header (using BytesHeaderParser to handle folded headers)
        raw_header = data[0][1] if isinstance(data[0], tuple) else data[0]
        if isinstance(raw_header, str):
            raw_header = raw_header.encode("utf-8", errors="replace")

        hdr_parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
        parsed = hdr_parser.parsebytes(raw_header)
        mid_value = parsed.get("Message-ID")
        fetched_mid = normalize_message_id(mid_value) if mid_value else ""

        if fetched_mid == expected_mid:
            verified += 1
            if delete:
                uids_to_delete.append(uid)
            if verbose:
                subj = entry.get("subject", "")
                print(f"    UID {uid}: verified ✓  {subj[:60]}")
        else:
            mismatched += 1
            if verbose:
                print(f"    UID {uid}: Message-ID mismatch!")
                print(f"      Expected: {expected_mid}")
                print(f"      Got:      {fetched_mid}")

    deleted = 0
    if uids_to_delete:
        uid_set = ",".join(uids_to_delete)
        if permanent:
            typ, _ = conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")
            if typ == "OK":
                conn.expunge()
                deleted = len(uids_to_delete)
                if not quiet:
                    print(f"    Deleted {deleted} message(s) from {folder}")
            else:
                print(f"  ERROR: Failed to flag messages for deletion in {folder}",
                      file=sys.stderr)
        else:
            typ, _ = conn.uid("MOVE", uid_set, imap_quote_folder(trash_folder))
            if typ == "OK":
                deleted = len(uids_to_delete)
                if not quiet:
                    print(f"    Moved {deleted} message(s) from {folder} to {trash_folder}")
            else:
                print(f"  ERROR: Failed to move messages to {trash_folder} in {folder}",
                      file=sys.stderr)

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
            "subject": group.keep.subject,
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
                "subject": dup.subject,
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
    finally:
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
    parser.add_argument(
        "-d", "--dry-run",
        action="store_true",
        help="Verify plan against IMAP without deleting (requires --apply)",
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
    if args.dry_run and not args.apply:
        print("ERROR: --dry-run requires --apply", file=sys.stderr)
        return 1

    if args.permanent and not args.apply:
        print("ERROR: --permanent requires --apply", file=sys.stderr)
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
