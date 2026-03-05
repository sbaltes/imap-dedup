"""Comprehensive test suite for imap_dedup.py."""

import imaplib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import imap_dedup


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_email_bytes():
    """Factory returning RFC 822 email bytes."""

    def _make(
        message_id="test@example.com",
        subject="Test Subject",
        from_addr="sender@example.com",
        to_addr="recipient@example.com",
        cc_addr=None,
        date="Mon, 01 Jan 2024 12:00:00 +0000",
        body="Hello, world!",
    ):
        headers = []
        if message_id:
            headers.append(f"Message-ID: <{message_id}>")
        headers.append(f"Subject: {subject}")
        headers.append(f"From: {from_addr}")
        headers.append(f"To: {to_addr}")
        if cc_addr:
            headers.append(f"Cc: {cc_addr}")
        headers.append(f"Date: {date}")
        headers.append("MIME-Version: 1.0")
        headers.append("Content-Type: text/plain; charset=utf-8")
        raw = "\r\n".join(headers) + "\r\n\r\n" + body
        return raw.encode("utf-8")

    return _make


@pytest.fixture
def maildir_tree(tmp_path, sample_email_bytes):
    """Factory creating Maildir directory structures.

    Takes a dict {folder_name: [(filename, message_id, subject), ...]}.
    Returns (root, files_dict) where files_dict maps folder_name to list of Paths.
    """

    def _make(folders_spec):
        root = tmp_path / "Maildir"
        files = {}
        for folder_name, messages in folders_spec.items():
            if folder_name == "INBOX":
                folder_dir = root
            else:
                folder_dir = root / f".{folder_name}"
            for sub in ("cur", "new", "tmp"):
                (folder_dir / sub).mkdir(parents=True, exist_ok=True)
            folder_files = []
            for filename, mid, subject in messages:
                filepath = folder_dir / "cur" / filename
                filepath.write_bytes(sample_email_bytes(
                    message_id=mid, subject=subject,
                ))
                folder_files.append(filepath)
            files[folder_name] = folder_files
        return root, files

    return _make


@pytest.fixture
def make_message_info(tmp_path):
    """Factory creating MessageInfo instances with placeholder files."""

    def _make(
        folder="INBOX",
        flags="S",
        size=1000,
        mtime=1700000000.0,
        dedup_key="abc@example.com",
        method="message-id",
        filename=None,
        from_addr="(unknown)",
        subject="Test",
        date="Mon, 01 Jan 2024 12:00:00 +0000",
    ):
        if filename is None:
            filename = f"{int(mtime)}.M100,S={size},U=42:2,{flags}"
        filepath = tmp_path / folder / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_bytes(b"placeholder")

        return imap_dedup.MessageInfo(
            path=filepath,
            folder=folder,
            subject=subject,
            date=date,
            flags=flags,
            size=size,
            mtime=mtime,
            dedup_key=dedup_key,
            method=method,
            message_id=dedup_key if method == "message-id" else None,
            from_addr=from_addr,
        )

    return _make


# ===========================================================================
# Phase 1: Pure functions (no I/O, no mocks)
# ===========================================================================


class TestDecodeHeader:
    def test_plain_ascii(self):
        assert imap_dedup.decode_header("Hello World") == "Hello World"

    def test_utf8_base64(self):
        # =?UTF-8?B?w5xiZXJ3ZWlzdW5n?= → "Überweisung"
        assert imap_dedup.decode_header("=?UTF-8?B?w5xiZXJ3ZWlzdW5n?=") == "Überweisung"

    def test_utf8_quoted_printable(self):
        assert imap_dedup.decode_header("=?UTF-8?Q?=C3=9Cberweisung?=") == "Überweisung"

    def test_mixed_encoded_and_plain(self):
        raw = "=?UTF-8?B?UmU6?= plain text"
        result = imap_dedup.decode_header(raw)
        assert "Re:" in result
        assert "plain text" in result

    def test_iso8859_1(self):
        # =?ISO-8859-1?Q?=FC?= → "ü"
        assert imap_dedup.decode_header("=?ISO-8859-1?Q?=FC?=") == "ü"

    def test_passthrough_on_error(self):
        raw = "not encoded at all"
        assert imap_dedup.decode_header(raw) == raw


class TestNormalizeMessageId:
    def test_strips_angle_brackets(self):
        assert imap_dedup.normalize_message_id("<abc@example.com>") == "abc@example.com"

    def test_strips_whitespace(self):
        assert imap_dedup.normalize_message_id("  <abc@example.com>  ") == "abc@example.com"

    def test_lowercases(self):
        assert imap_dedup.normalize_message_id("<ABC@Example.COM>") == "abc@example.com"

    def test_no_brackets(self):
        assert imap_dedup.normalize_message_id("abc@example.com") == "abc@example.com"

    def test_empty_string(self):
        assert imap_dedup.normalize_message_id("") == ""

    def test_whitespace_only(self):
        assert imap_dedup.normalize_message_id("   ") == ""


class TestParseFlags:
    def test_standard_flags(self):
        assert imap_dedup.parse_flags("1234567890.M100,S=5000,U=42:2,FRS") == "FRS"

    def test_seen_only(self):
        assert imap_dedup.parse_flags("1234567890.M100,S=5000,U=42:2,S") == "S"

    def test_empty_after_marker(self):
        assert imap_dedup.parse_flags("1234567890.M100,S=5000,U=42:2,") == ""

    def test_no_marker(self):
        assert imap_dedup.parse_flags("1234567890.M100,S=5000,U=42") == ""

    def test_complex_offlineimap_filename(self):
        assert imap_dedup.parse_flags(
            "1609459200.12345_67890.hostname,U=123,FMD5=abc123:2,FRS"
        ) == "FRS"


class TestGetFolderPriority:
    def test_trash_lowest(self):
        assert imap_dedup.get_folder_priority("Trash") == 0

    def test_junk_lowest(self):
        assert imap_dedup.get_folder_priority("Junk") == 0

    def test_drafts_lowest(self):
        assert imap_dedup.get_folder_priority("Drafts") == 0

    def test_case_insensitive(self):
        assert imap_dedup.get_folder_priority("TRASH") == 0
        assert imap_dedup.get_folder_priority("junk") == 0

    def test_sent_highest(self):
        assert imap_dedup.get_folder_priority("Sent") == 100
        assert imap_dedup.get_folder_priority("0_Sent") == 100

    def test_inbox(self):
        assert imap_dedup.get_folder_priority("INBOX") == 10

    def test_nested_folder(self):
        assert imap_dedup.get_folder_priority("2_Job.Uni Trier") == 20


class TestFolderDisplayName:
    def test_strips_leading_dot(self):
        assert imap_dedup.folder_display_name(".Sent") == "Sent"

    def test_inbox_unchanged(self):
        assert imap_dedup.folder_display_name("INBOX") == "INBOX"

    def test_nested_folder(self):
        assert imap_dedup.folder_display_name(".2_Job.Uni Trier") == "2_Job.Uni Trier"


class TestFormatSize:
    def test_zero_bytes(self):
        assert imap_dedup.format_size(0) == "0 B"

    def test_bytes(self):
        assert imap_dedup.format_size(500) == "500 B"

    def test_kilobytes(self):
        assert imap_dedup.format_size(1024) == "1.0 KB"

    def test_kilobytes_fractional(self):
        assert imap_dedup.format_size(1536) == "1.5 KB"

    def test_megabytes(self):
        assert imap_dedup.format_size(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self):
        assert imap_dedup.format_size(1024 * 1024 * 1024) == "1.0 GB"


class TestPrintReport:
    def test_summary_output(self, make_message_info, capsys):
        keep = make_message_info(dedup_key="a@b.com", flags="F", filename="f1:2,F")
        dup = make_message_info(dedup_key="a@b.com", flags="S", size=2000, filename="f2:2,S",
                                mtime=1700000001.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        imap_dedup.print_report([group])
        out = capsys.readouterr().out
        assert "Duplicate groups:    1" in out
        assert "Duplicate messages:  1" in out
        assert "By Message-ID:       1 groups" in out
        assert "By fingerprint:      0 groups" in out

    def test_quiet_suppresses_output(self, make_message_info, capsys):
        keep = make_message_info(dedup_key="a@b.com", flags="F", filename="f1:2,F")
        dup = make_message_info(dedup_key="a@b.com", flags="S", filename="f2:2,S",
                                mtime=1700000001.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        imap_dedup.print_report([group], quiet=True)
        assert capsys.readouterr().out == ""

    def test_verbose_shows_details(self, make_message_info, capsys):
        keep = make_message_info(dedup_key="a@b.com", flags="F", filename="f1:2,F")
        dup = make_message_info(dedup_key="a@b.com", flags="S", filename="f2:2,S",
                                mtime=1700000001.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        imap_dedup.print_report([group], verbose=True)
        out = capsys.readouterr().out
        assert "KEEP:" in out
        assert "DELETE:" in out
        assert "Duplicate group 1" in out


class TestExtractUid:
    def test_standard_offlineimap(self):
        assert imap_dedup.extract_uid("1234567890.M100,S=5000,U=42:2,FRS") == 42

    def test_uid_with_comma_after(self):
        assert imap_dedup.extract_uid("1234567890.M100,U=99,S=5000:2,S") == 99

    def test_no_uid(self):
        assert imap_dedup.extract_uid("1234567890.M100,S=5000:2,S") is None

    def test_large_uid(self):
        assert imap_dedup.extract_uid("1234567890.M100,S=5000,U=123456:2,S") == 123456

    def test_missing_delimiter(self):
        # U= not followed by , or : → no match
        assert imap_dedup.extract_uid("1234567890.M100,U=42") is None


class TestLocalToImapFolder:
    def test_inbox_unchanged(self):
        assert imap_dedup.local_to_imap_folder("INBOX") == "INBOX"

    def test_simple_folder(self):
        assert imap_dedup.local_to_imap_folder("Sent") == "Sent"

    def test_dots_to_slashes(self):
        assert imap_dedup.local_to_imap_folder("2_Job.Uni Trier") == "2_Job/Uni Trier"

    def test_deeply_nested(self):
        assert imap_dedup.local_to_imap_folder("A.B.C") == "A/B/C"


# ===========================================================================
# Phase 2: Scoring and grouping logic
# ===========================================================================


class TestComputeRetentionScore:
    @pytest.mark.parametrize("higher_kwargs, lower_kwargs, description", [
        (dict(flags="F", filename="f1:2,F"),
         dict(flags="S", filename="f2:2,S"),
         "flagged beats seen"),
        (dict(folder="Sent", flags="S", filename="f3:2,S"),
         dict(folder="INBOX", flags="S", filename="f4:2,S"),
         "sent folder beats inbox"),
        (dict(mtime=1700000000.0, flags="", filename="f5"),
         dict(mtime=1700000100.0, flags="", filename="f6"),
         "older mtime beats newer (keeps original)"),
        (dict(size=5000, flags="", filename="f7"),
         dict(size=1000, flags="", filename="f8"),
         "larger size beats smaller"),
    ])
    def test_score_ordering(self, make_message_info, higher_kwargs, lower_kwargs, description):
        higher = make_message_info(dedup_key="a", **higher_kwargs)
        lower = make_message_info(dedup_key="a", **lower_kwargs)
        assert imap_dedup.compute_retention_score(higher) > imap_dedup.compute_retention_score(lower), description

    def test_no_flags_score_zero(self, make_message_info):
        msg = make_message_info(flags="", dedup_key="a", filename="f9")
        score = imap_dedup.compute_retention_score(msg)
        assert score[1] == 0


class TestDecideKeep:
    def test_keeps_highest_scored(self, make_message_info):
        flagged = make_message_info(flags="F", dedup_key="a", filename="f1:2,F")
        plain = make_message_info(flags="", dedup_key="a", filename="f2")
        group = imap_dedup.decide_keep([flagged, plain])
        assert group.keep == flagged
        assert group.duplicates == [plain]

    def test_duplicates_exclude_keep(self, make_message_info):
        a = make_message_info(flags="FRS", dedup_key="a", filename="f1:2,FRS")
        b = make_message_info(flags="S", dedup_key="a", filename="f2:2,S")
        group = imap_dedup.decide_keep([a, b])
        assert group.keep not in group.duplicates

    def test_two_messages(self, make_message_info):
        a = make_message_info(flags="S", size=2000, dedup_key="a", filename="f1:2,S")
        b = make_message_info(flags="S", size=1000, dedup_key="a", filename="f2:2,S")
        group = imap_dedup.decide_keep([a, b])
        assert len(group.duplicates) == 1

    def test_flag_priority_over_size(self, make_message_info):
        flagged = make_message_info(flags="F", size=100, dedup_key="a", filename="f1:2,F")
        large = make_message_info(flags="", size=10000, dedup_key="a", filename="f2")
        group = imap_dedup.decide_keep([flagged, large])
        assert group.keep == flagged

    def test_folder_tiebreaker(self, make_message_info):
        sent = make_message_info(folder="Sent", flags="S", size=1000, dedup_key="a", filename="f1:2,S")
        inbox = make_message_info(folder="INBOX", flags="S", size=1000, dedup_key="a", filename="f2:2,S")
        group = imap_dedup.decide_keep([sent, inbox])
        assert group.keep == sent


class TestFindDuplicates:
    def test_no_duplicates(self, make_message_info):
        a = make_message_info(dedup_key="a", filename="f1")
        b = make_message_info(dedup_key="b", filename="f2")
        assert imap_dedup.find_duplicates([a, b]) == []

    def test_cross_folder_grouping(self, make_message_info):
        a = make_message_info(folder="INBOX", dedup_key="same", filename="f1")
        b = make_message_info(folder="Sent", dedup_key="same", filename="f2")
        groups = imap_dedup.find_duplicates([a, b])
        assert len(groups) == 1

    def test_same_folder_only_separates(self, make_message_info):
        a = make_message_info(folder="INBOX", dedup_key="same", filename="f1")
        b = make_message_info(folder="Sent", dedup_key="same", filename="f2")
        groups = imap_dedup.find_duplicates([a, b], same_folder_only=True)
        assert len(groups) == 0  # One in each folder, no group > 1

    def test_same_folder_groups_within(self, make_message_info):
        a = make_message_info(folder="INBOX", flags="F", dedup_key="same", filename="f1:2,F")
        b = make_message_info(folder="INBOX", flags="", dedup_key="same", filename="f2")
        groups = imap_dedup.find_duplicates([a, b], same_folder_only=True)
        assert len(groups) == 1

    def test_three_copies(self, make_message_info):
        a = make_message_info(flags="F", dedup_key="same", filename="f1:2,F")
        b = make_message_info(flags="S", dedup_key="same", filename="f2:2,S")
        c = make_message_info(flags="", dedup_key="same", filename="f3")
        groups = imap_dedup.find_duplicates([a, b, c])
        assert len(groups) == 1
        assert len(groups[0].duplicates) == 2

    def test_multiple_groups(self, make_message_info):
        a1 = make_message_info(dedup_key="x", filename="f1")
        a2 = make_message_info(dedup_key="x", filename="f2")
        b1 = make_message_info(dedup_key="y", filename="f3")
        b2 = make_message_info(dedup_key="y", filename="f4")
        groups = imap_dedup.find_duplicates([a1, a2, b1, b2])
        assert len(groups) == 2

    def test_single_message_no_group(self, make_message_info):
        a = make_message_info(dedup_key="solo", filename="f1")
        groups = imap_dedup.find_duplicates([a])
        assert groups == []


class TestSenderAwarePriority:
    """Tests for --sender bag-of-words matching on Sent folder priority."""

    def test_sent_priority_with_matching_sender(self, make_message_info):
        """Sent keeps priority 100 when From matches sender identity."""
        msg = make_message_info(
            folder="Sent", flags="S", filename="f1:2,S",
            from_addr="Sebastian Baltes <seb@example.com>",
        )
        score = imap_dedup.compute_retention_score(msg, sender="Sebastian Baltes")
        assert score[2] == 100

    def test_sent_priority_with_reversed_name(self, make_message_info):
        """Bag-of-words: 'Baltes, Sebastian' matches sender 'Sebastian Baltes'."""
        msg = make_message_info(
            folder="Sent", flags="S", filename="f2:2,S",
            from_addr="Baltes, Sebastian <seb@example.com>",
        )
        score = imap_dedup.compute_retention_score(msg, sender="Sebastian Baltes")
        assert score[2] == 100

    def test_sent_priority_with_nonmatching_sender(self, make_message_info):
        """Sent demoted when From doesn't match sender identity."""
        msg = make_message_info(
            folder="Sent", flags="S", filename="f3:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        score = imap_dedup.compute_retention_score(msg, sender="Sebastian Baltes")
        # Sent has depth 1 → demoted to 1 * 10 = 10
        assert score[2] == 10

    def test_sent_priority_without_sender_arg(self, make_message_info):
        """Backward compat: without sender, Sent always gets 100."""
        msg = make_message_info(
            folder="Sent", flags="S", filename="f4:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        score = imap_dedup.compute_retention_score(msg, sender=None)
        assert score[2] == 100

    def test_sender_ok_zero_for_mismatch(self, make_message_info):
        """sender_ok is 0 when Sent folder and From doesn't match sender."""
        msg = make_message_info(
            folder="Sent", flags="S", filename="f20:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        score = imap_dedup.compute_retention_score(msg, sender="Sebastian Baltes")
        assert score[0] == 0

    def test_sender_ok_one_for_match(self, make_message_info):
        """sender_ok is 1 when Sent folder and From matches sender."""
        msg = make_message_info(
            folder="Sent", flags="S", filename="f21:2,S",
            from_addr="Sebastian Baltes <seb@example.com>",
        )
        score = imap_dedup.compute_retention_score(msg, sender="Sebastian Baltes")
        assert score[0] == 1

    def test_non_sent_folder_unaffected_by_sender(self, make_message_info):
        """INBOX priority unchanged regardless of sender arg."""
        msg = make_message_info(
            folder="INBOX", flags="S", filename="f5:2,S",
            from_addr="Sebastian Baltes <seb@example.com>",
        )
        score_with = imap_dedup.compute_retention_score(msg, sender="Sebastian Baltes")
        score_without = imap_dedup.compute_retention_score(msg, sender=None)
        assert score_with[2] == score_without[2]

    def test_sent_loses_to_inbox_when_sender_mismatch(self, make_message_info):
        """decide_keep picks INBOX when Sent copy doesn't match sender."""
        sent = make_message_info(
            folder="Sent", flags="S", size=1000, dedup_key="a", filename="f6:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        inbox = make_message_info(
            folder="INBOX", flags="S", size=1001, dedup_key="a", filename="f7:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        group = imap_dedup.decide_keep([sent, inbox], sender="Sebastian Baltes")
        # Sent demoted to 10 (same as INBOX); INBOX wins on size tiebreaker
        assert group.keep == inbox

    def test_sent_wins_when_sender_matches(self, make_message_info):
        """decide_keep picks Sent when From matches sender."""
        sent = make_message_info(
            folder="Sent", flags="S", size=1000, dedup_key="a", filename="f8:2,S",
            from_addr="Sebastian Baltes <seb@example.com>",
        )
        inbox = make_message_info(
            folder="INBOX", flags="S", size=1000, dedup_key="a", filename="f9:2,S",
            from_addr="Sebastian Baltes <seb@example.com>",
        )
        group = imap_dedup.decide_keep([sent, inbox], sender="Sebastian Baltes")
        assert group.keep == sent

    def test_sender_threading_through_find_duplicates(self, make_message_info):
        """find_duplicates threads sender to decide_keep."""
        sent = make_message_info(
            folder="Sent", flags="S", size=1000, dedup_key="same", filename="f10:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        inbox = make_message_info(
            folder="INBOX", flags="S", size=1001, dedup_key="same", filename="f11:2,S",
            from_addr="Alice Smith <alice@example.com>",
        )
        groups = imap_dedup.find_duplicates([sent, inbox], sender="Sebastian Baltes")
        assert len(groups) == 1
        # Sent demoted to 10 (same as INBOX); INBOX wins on size tiebreaker
        assert groups[0].keep == inbox


# ===========================================================================
# Phase 3: File I/O tests
# ===========================================================================


class TestComputeFingerprint:
    def test_deterministic(self, tmp_path, sample_email_bytes):
        data = sample_email_bytes()
        fp1 = imap_dedup.compute_fingerprint(data)
        fp2 = imap_dedup.compute_fingerprint(data)
        assert fp1 == fp2

    def test_different_body(self, sample_email_bytes):
        fp1 = imap_dedup.compute_fingerprint(sample_email_bytes(body="Hello"))
        fp2 = imap_dedup.compute_fingerprint(sample_email_bytes(body="Goodbye"))
        assert fp1 != fp2

    def test_different_subject(self, sample_email_bytes):
        fp1 = imap_dedup.compute_fingerprint(sample_email_bytes(subject="A"))
        fp2 = imap_dedup.compute_fingerprint(sample_email_bytes(subject="B"))
        assert fp1 != fp2

    def test_from_to_cc_case_insensitive(self, sample_email_bytes):
        fp1 = imap_dedup.compute_fingerprint(
            sample_email_bytes(from_addr="User@Example.COM", to_addr="Other@Example.COM")
        )
        fp2 = imap_dedup.compute_fingerprint(
            sample_email_bytes(from_addr="user@example.com", to_addr="other@example.com")
        )
        assert fp1 == fp2

    def test_multipart_valid_hex(self):
        raw = (
            b"Subject: Test\r\nFrom: a@b.com\r\nTo: c@d.com\r\n"
            b"Date: Mon, 01 Jan 2024 12:00:00 +0000\r\n"
            b"MIME-Version: 1.0\r\n"
            b'Content-Type: multipart/mixed; boundary="BOUNDARY"\r\n\r\n'
            b"--BOUNDARY\r\n"
            b"Content-Type: text/plain\r\n\r\n"
            b"Part one\r\n"
            b"--BOUNDARY\r\n"
            b"Content-Type: text/plain\r\n\r\n"
            b"Part two\r\n"
            b"--BOUNDARY--\r\n"
        )
        fp = imap_dedup.compute_fingerprint(raw)
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)


class TestGetDedupKey:
    def test_returns_message_id(self, tmp_path, sample_email_bytes):
        f = tmp_path / "msg1"
        f.write_bytes(sample_email_bytes(message_id="unique@test.com"))
        key, method = imap_dedup.get_dedup_key(f)
        assert key == "unique@test.com"
        assert method == "message-id"

    def test_falls_back_to_fingerprint(self, tmp_path, sample_email_bytes):
        f = tmp_path / "msg2"
        f.write_bytes(sample_email_bytes(message_id=None))
        key, method = imap_dedup.get_dedup_key(f)
        assert method == "fingerprint"
        assert len(key) == 64

    def test_empty_message_id_uses_fingerprint(self, tmp_path):
        f = tmp_path / "msg3"
        f.write_bytes(b"Message-ID: <>\r\nSubject: Test\r\n\r\nBody")
        key, method = imap_dedup.get_dedup_key(f)
        assert method == "fingerprint"

    def test_case_insensitive_header_name(self, tmp_path):
        f = tmp_path / "msg4"
        f.write_bytes(b"message-id: <lower@test.com>\r\nSubject: Test\r\n\r\nBody")
        key, method = imap_dedup.get_dedup_key(f)
        assert key == "lower@test.com"
        assert method == "message-id"

    def test_normalizes_message_id(self, tmp_path, sample_email_bytes):
        f = tmp_path / "msg5"
        f.write_bytes(sample_email_bytes(message_id="UPPER@Test.COM"))
        key, method = imap_dedup.get_dedup_key(f)
        assert key == "upper@test.com"

    def test_pre_parsed_msg_matches_default(self, tmp_path, sample_email_bytes):
        """Passing a pre-parsed msg produces identical results to parsing from raw."""
        import email.parser
        import email.policy

        raw = sample_email_bytes(message_id="preparsed@test.com")
        f = tmp_path / "msg6"
        f.write_bytes(raw)

        key1, method1 = imap_dedup.get_dedup_key(f)

        parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
        msg = parser.parsebytes(raw)
        key2, method2 = imap_dedup.get_dedup_key(f, raw=raw, msg=msg)

        assert (key1, method1) == (key2, method2)

    def test_pre_parsed_msg_fingerprint_fallback(self, tmp_path, sample_email_bytes):
        """Pre-parsed msg falls back to fingerprint when no Message-ID."""
        import email.parser
        import email.policy

        raw = sample_email_bytes(message_id=None)
        f = tmp_path / "msg7"
        f.write_bytes(raw)

        key1, method1 = imap_dedup.get_dedup_key(f)

        parser = email.parser.BytesHeaderParser(policy=email.policy.compat32)
        msg = parser.parsebytes(raw)
        key2, method2 = imap_dedup.get_dedup_key(f, raw=raw, msg=msg)

        assert method1 == method2 == "fingerprint"
        assert key1 == key2


class TestGetMessageInfo:
    def test_valid_message(self, tmp_path, sample_email_bytes):
        f = tmp_path / "1234567890.M100,S=5000,U=42:2,S"
        f.write_bytes(sample_email_bytes(message_id="valid@test.com", subject="Hello"))
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info is not None
        assert info.dedup_key == "valid@test.com"
        assert info.message_id == "valid@test.com"
        assert info.subject == "Hello"
        assert info.folder == "INBOX"
        assert info.flags == "S"
        assert info.method == "message-id"

    def test_missing_file(self, tmp_path):
        f = tmp_path / "nonexistent"
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info is None

    def test_missing_subject_default(self, tmp_path):
        f = tmp_path / "msg:2,S"
        f.write_bytes(b"Message-ID: <x@y.com>\r\nDate: Mon, 01 Jan 2024 12:00:00 +0000\r\n\r\nBody")
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info.subject == "(no subject)"

    def test_missing_date_default(self, tmp_path):
        f = tmp_path / "msg:2,S"
        f.write_bytes(b"Message-ID: <x@y.com>\r\nSubject: Test\r\n\r\nBody")
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info.date == "(no date)"

    def test_flags_from_filename(self, tmp_path, sample_email_bytes):
        f = tmp_path / "1234567890.M100,S=5000,U=42:2,FRS"
        f.write_bytes(sample_email_bytes())
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info.flags == "FRS"

    def test_decodes_encoded_subject(self, tmp_path):
        f = tmp_path / "msg_enc:2,S"
        f.write_bytes(
            b"Message-ID: <enc@test.com>\r\n"
            b"Subject: =?UTF-8?B?w5xiZXJ3ZWlzdW5n?=\r\n"
            b"From: =?UTF-8?Q?M=C3=BCller?= <mueller@test.com>\r\n"
            b"Date: Mon, 01 Jan 2024 12:00:00 +0000\r\n\r\nBody"
        )
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info.subject == "Überweisung"
        assert "Müller" in info.from_addr

    def test_empty_message_id_fallback(self, tmp_path):
        f = tmp_path / "msg:2,"
        f.write_bytes(b"Subject: Test\r\nDate: Mon, 01 Jan 2024 12:00:00 +0000\r\n\r\nBody")
        info = imap_dedup.get_message_info(f, "INBOX")
        assert info.method == "fingerprint"
        assert len(info.dedup_key) == 64
        assert info.message_id is None


class TestDiscoverFolders:
    def test_finds_inbox(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        folders = imap_dedup.discover_folders(root)
        names = [n for n, _ in folders]
        assert "INBOX" in names

    def test_finds_subfolders(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        for name in (".Sent", ".Trash"):
            for sub in ("cur", "new", "tmp"):
                (root / name / sub).mkdir(parents=True)
        folders = imap_dedup.discover_folders(root, exclude=set())
        names = [n for n, _ in folders]
        assert "Sent" in names
        assert "Trash" in names

    def test_excludes_by_name(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        for sub in ("cur", "new", "tmp"):
            (root / ".Trash" / sub).mkdir(parents=True)
        folders = imap_dedup.discover_folders(root, exclude={"Trash"})
        names = [n for n, _ in folders]
        assert "Trash" not in names

    def test_include_filter(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        for name in (".Sent", ".Trash"):
            for sub in ("cur", "new", "tmp"):
                (root / name / sub).mkdir(parents=True)
        folders = imap_dedup.discover_folders(root, include=["Sent"], exclude=set())
        names = [n for n, _ in folders]
        assert "Sent" in names
        assert "Trash" not in names
        assert "INBOX" not in names

    def test_skips_non_maildir_dirs(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / ".NotMaildir").mkdir(parents=True)  # no cur/new/tmp
        folders = imap_dedup.discover_folders(root, exclude=set())
        names = [n for n, _ in folders]
        assert "NotMaildir" not in names

    def test_empty_root(self, tmp_path):
        root = tmp_path / "EmptyMaildir"
        root.mkdir()
        folders = imap_dedup.discover_folders(root)
        assert folders == []

    def test_excludes_inbox(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        folders = imap_dedup.discover_folders(root, exclude={"INBOX"})
        names = [n for n, _ in folders]
        assert "INBOX" not in names


class TestScanFolder:
    def test_scans_cur_and_new(self, tmp_path, sample_email_bytes):
        folder = tmp_path / "folder"
        for sub in ("cur", "new", "tmp"):
            (folder / sub).mkdir(parents=True)
        (folder / "cur" / "msg1:2,S").write_bytes(sample_email_bytes(message_id="a@b.com"))
        (folder / "new" / "msg2").write_bytes(sample_email_bytes(message_id="c@d.com"))
        messages = imap_dedup.scan_folder("Test", folder)
        assert len(messages) == 2

    def test_skips_subdirectories(self, tmp_path, sample_email_bytes):
        folder = tmp_path / "folder"
        (folder / "cur").mkdir(parents=True)
        (folder / "cur" / "subdir").mkdir()
        (folder / "cur" / "msg1:2,S").write_bytes(sample_email_bytes())
        messages = imap_dedup.scan_folder("Test", folder)
        assert len(messages) == 1

    def test_empty_folder(self, tmp_path):
        folder = tmp_path / "folder"
        for sub in ("cur", "new", "tmp"):
            (folder / sub).mkdir(parents=True)
        messages = imap_dedup.scan_folder("Test", folder)
        assert messages == []

    def test_missing_new_dir(self, tmp_path, sample_email_bytes):
        folder = tmp_path / "folder"
        (folder / "cur").mkdir(parents=True)
        (folder / "cur" / "msg1:2,S").write_bytes(sample_email_bytes())
        messages = imap_dedup.scan_folder("Test", folder)
        assert len(messages) == 1


# ===========================================================================
# Phase 4: IMAP mock tests
# ===========================================================================


class TestImapConnect:
    @patch("imaplib.IMAP4_SSL")
    @patch("netrc.netrc")
    @patch("ssl.create_default_context")
    def test_successful_login(self, mock_ssl, mock_netrc, mock_imap):
        mock_netrc.return_value.authenticators.return_value = ("user@test.com", None, "secret")
        conn = imap_dedup.imap_connect("imap.test.com")
        mock_imap.return_value.login.assert_called_once_with("user@test.com", "secret")

    @patch("netrc.netrc", side_effect=FileNotFoundError("~/.netrc not found"))
    @patch("ssl.create_default_context")
    @patch("imaplib.IMAP4_SSL")
    def test_missing_netrc(self, mock_imap, mock_ssl, mock_netrc):
        with pytest.raises(SystemExit, match="Cannot read ~/.netrc"):
            imap_dedup.imap_connect("imap.test.com")

    @patch("netrc.netrc")
    @patch("ssl.create_default_context")
    @patch("imaplib.IMAP4_SSL")
    def test_no_host_entry(self, mock_imap, mock_ssl, mock_netrc):
        mock_netrc.return_value.authenticators.return_value = None
        with pytest.raises(SystemExit, match="No entry for"):
            imap_dedup.imap_connect("imap.test.com")

    @patch("netrc.netrc")
    @patch("ssl.create_default_context")
    @patch("imaplib.IMAP4_SSL")
    def test_no_password(self, mock_imap, mock_ssl, mock_netrc):
        mock_netrc.return_value.authenticators.return_value = ("user", None, None)
        with pytest.raises(SystemExit, match="No password"):
            imap_dedup.imap_connect("imap.test.com")

    @patch("netrc.netrc", side_effect=FileNotFoundError("~/.netrc not found"))
    @patch("ssl.create_default_context")
    @patch("imaplib.IMAP4_SSL")
    def test_connection_closed_on_netrc_error(self, mock_imap, mock_ssl, mock_netrc):
        with pytest.raises(SystemExit):
            imap_dedup.imap_connect("imap.test.com")
        mock_imap.return_value.logout.assert_called_once()

    @patch("netrc.netrc")
    @patch("ssl.create_default_context")
    @patch("imaplib.IMAP4_SSL")
    def test_connection_closed_on_login_error(self, mock_imap, mock_ssl, mock_netrc):
        mock_netrc.return_value.authenticators.return_value = ("user", None, "pass")
        mock_imap.return_value.login.side_effect = imaplib.IMAP4.error("login failed")
        with pytest.raises(imaplib.IMAP4.error):
            imap_dedup.imap_connect("imap.test.com")
        mock_imap.return_value.logout.assert_called_once()


class TestImapVerifyAndDelete:
    def _make_fetch_response(self, message_id):
        """Create realistic IMAP FETCH response."""
        header = f"Message-ID: <{message_id}>\r\n\r\n".encode()
        return [(b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {35}", header), b")"]

    def test_verify_only(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", self._make_fetch_response("abc@ex.com"))
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert verified == 1
        assert mismatched == 0
        assert deleted == 0
        # STORE should NOT be called
        store_calls = [c for c in conn.uid.call_args_list if c[0][0] == "STORE"]
        assert len(store_calls) == 0

    def test_delete_mode(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.side_effect = [
            ("OK", self._make_fetch_response("abc@ex.com")),  # FETCH
            ("OK", None),  # STORE
        ]
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=True,
        )
        assert verified == 1
        assert deleted == 1
        conn.expunge.assert_called_once()

    def test_message_id_mismatch(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", self._make_fetch_response("other@ex.com"))
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=True,
        )
        assert mismatched == 1
        assert deleted == 0

    def test_uid_not_found(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", [None])
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert mismatched == 1
        assert verified == 0

    def test_folder_select_failure(self):
        conn = MagicMock()
        conn.select.return_value = ("NO", [b"Folder not found"])
        entries = [{"uid": 42, "message_id": "abc@ex.com"}, {"uid": 43, "message_id": "def@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries,
        )
        assert verified == 0
        assert mismatched == 2
        assert deleted == 0

    def test_store_failure(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.side_effect = [
            ("OK", self._make_fetch_response("abc@ex.com")),  # FETCH
            ("NO", None),  # STORE fails
        ]
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=True,
        )
        assert verified == 1
        assert deleted == 0

    def test_mixed_results(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"2"])
        conn.uid.side_effect = [
            ("OK", self._make_fetch_response("abc@ex.com")),  # FETCH match
            ("OK", self._make_fetch_response("wrong@ex.com")),  # FETCH mismatch
        ]
        entries = [
            {"uid": 42, "message_id": "abc@ex.com"},
            {"uid": 43, "message_id": "def@ex.com"},
        ]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert verified == 1
        assert mismatched == 1

    def test_bytes_header_parsing(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        # Return raw bytes header
        header = b"Message-ID: <bytes@test.com>\r\n\r\n"
        conn.uid.return_value = ("OK", [(b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {30}", header), b")"])
        entries = [{"uid": 1, "message_id": "bytes@test.com"}]
        verified, _, _ = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert verified == 1

    def test_folded_message_id_header(self):
        """RFC 2822 folded headers where Message-ID wraps to continuation line."""
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        # Folded header: value on continuation line
        header = b"Message-ID:\r\n    <very-long-id@example.com>\r\n\r\n"
        conn.uid.return_value = ("OK", [(b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {50}", header), b")"])
        entries = [{"uid": 1, "message_id": "very-long-id@example.com"}]
        verified, mismatched, _ = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert verified == 1
        assert mismatched == 0

    def test_folded_message_id_with_tabs(self):
        """Folded header using tab as continuation whitespace."""
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        header = b"Message-ID:\r\n\t<tabbed-id@example.com>\r\n\r\n"
        conn.uid.return_value = ("OK", [(b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {45}", header), b")"])
        entries = [{"uid": 1, "message_id": "tabbed-id@example.com"}]
        verified, mismatched, _ = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert verified == 1
        assert mismatched == 0

    def test_move_to_trash(self):
        """Non-permanent delete uses MOVE to trash folder."""
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.side_effect = [
            ("OK", self._make_fetch_response("abc@ex.com")),  # FETCH
            ("OK", None),  # MOVE
        ]
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=True, permanent=False,
            trash_folder="Trash",
        )
        assert verified == 1
        assert deleted == 1
        # Should call MOVE, not STORE
        move_calls = [c for c in conn.uid.call_args_list if c[0][0] == "MOVE"]
        assert len(move_calls) == 1
        assert move_calls[0][0][2] == '"Trash"'
        conn.expunge.assert_not_called()

    def test_permanent_delete_uses_store_expunge(self):
        """Permanent delete uses STORE+EXPUNGE."""
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.side_effect = [
            ("OK", self._make_fetch_response("abc@ex.com")),  # FETCH
            ("OK", None),  # STORE
        ]
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=True, permanent=True,
        )
        assert verified == 1
        assert deleted == 1
        store_calls = [c for c in conn.uid.call_args_list if c[0][0] == "STORE"]
        assert len(store_calls) == 1
        conn.expunge.assert_called_once()

    def test_short_tuple_in_fetch_response(self):
        """A tuple with length < 2 should count as mismatch, not IndexError."""
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", [(b"1 (FLAGS (\\Seen))",)])
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=False,
        )
        assert mismatched == 1
        assert verified == 0

    def test_move_failure(self):
        """MOVE failure results in deleted=0."""
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.side_effect = [
            ("OK", self._make_fetch_response("abc@ex.com")),  # FETCH
            ("NO", None),  # MOVE fails
        ]
        entries = [{"uid": 42, "message_id": "abc@ex.com"}]
        verified, mismatched, deleted = imap_dedup.imap_verify_and_delete(
            conn, "INBOX", entries, delete=True, permanent=False,
            trash_folder="Trash",
        )
        assert verified == 1
        assert deleted == 0


class TestImapFindTrashFolder:
    def test_detects_trash_attribute(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\HasNoChildren \\Trash) "/" "Deleted Messages"',
            b'(\\HasNoChildren \\Sent) "/" "Sent"',
        ])
        assert imap_dedup.imap_find_trash_folder(conn) == "Deleted Messages"

    def test_fallback_to_trash(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\HasNoChildren \\Sent) "/" "Sent"',
        ])
        assert imap_dedup.imap_find_trash_folder(conn) == "Trash"

    def test_list_failure(self):
        conn = MagicMock()
        conn.list.return_value = ("NO", [])
        assert imap_dedup.imap_find_trash_folder(conn) == "Trash"


# ===========================================================================
# Phase 5: Integration tests
# ===========================================================================


class TestExportPlan:
    def _make_group(self, tmp_path, keep_mid="keep@test.com", dup_mid=None,
                    keep_method="message-id", keep_filename=None, dup_filename=None,
                    dup_flags="S", dup_size=1000):
        if dup_mid is None:
            dup_mid = keep_mid
        if keep_filename is None:
            keep_filename = "1234567890.M100,S=5000,U=10:2,FS"
        if dup_filename is None:
            dup_filename = "1234567891.M101,S=5000,U=20:2,S"

        keep_path = tmp_path / keep_filename
        keep_path.parent.mkdir(parents=True, exist_ok=True)
        keep_path.write_bytes(b"keep")
        dup_path = tmp_path / dup_filename
        dup_path.write_bytes(b"dup")

        keep_msg_id = keep_mid if keep_method == "message-id" else None
        dup_msg_id = dup_mid if keep_method == "message-id" else None
        keep = imap_dedup.MessageInfo(
            path=keep_path, folder="INBOX", subject="Test", date="date",
            flags="FS", size=5000, mtime=time.time(),
            dedup_key=keep_mid, method=keep_method, message_id=keep_msg_id,
        )
        dup = imap_dedup.MessageInfo(
            path=dup_path, folder="Sent", subject="Test", date="date",
            flags=dup_flags, size=dup_size, mtime=time.time(),
            dedup_key=dup_mid, method=keep_method, message_id=dup_msg_id,
        )
        return imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])

    def test_json_structure(self, tmp_path):
        group = self._make_group(tmp_path)
        out = str(tmp_path / "plan.json")
        rc = imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        assert rc == 0
        plan = json.loads(Path(out).read_text())
        assert "version" in plan
        assert "created" in plan
        assert "imap_host" in plan
        assert "stats" in plan
        assert "groups" in plan

    def test_version_is_1(self, tmp_path):
        group = self._make_group(tmp_path)
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert plan["version"] == 1

    def test_imap_host_stored(self, tmp_path):
        group = self._make_group(tmp_path)
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "mail.example.org", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert plan["imap_host"] == "mail.example.org"

    def test_fingerprint_only_skipped(self, tmp_path):
        group = self._make_group(tmp_path, keep_method="fingerprint")
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert len(plan["groups"]) == 0
        assert plan["stats"]["skipped_no_message_id"] == 1

    def test_no_uid_keep_skipped(self, tmp_path):
        group = self._make_group(tmp_path, keep_filename="no_uid_msg")
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert len(plan["groups"]) == 0
        assert plan["stats"]["skipped_no_uid"] >= 1

    def test_no_uid_dup_skipped(self, tmp_path):
        group = self._make_group(tmp_path, dup_filename="no_uid_dup")
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert len(plan["groups"]) == 0
        assert plan["stats"]["skipped_no_uid"] >= 1

    def test_delete_entry_keys(self, tmp_path):
        group = self._make_group(tmp_path)
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        entry = plan["groups"][0]["delete"][0]
        assert "uid" in entry
        assert "imap_folder" in entry
        assert "message_id" in entry
        assert "subject" in entry
        assert "flags" in entry
        assert "size" in entry

    def test_invalid_output_path(self, tmp_path):
        group = self._make_group(tmp_path)
        rc = imap_dedup.export_plan([group], "/nonexistent/dir/plan.json", "host", quiet=True)
        assert rc == 1


class TestApplyPlan:
    def _write_plan(self, path, groups=None, version=1, created=None, imap_host="imap.test.com"):
        if created is None:
            created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if groups is None:
            groups = []
        plan = {
            "version": version,
            "created": created,
            "imap_host": imap_host,
            "stats": {"groups": len(groups), "total_deletions": 0,
                       "skipped_no_message_id": 0, "skipped_no_uid": 0},
            "groups": groups,
        }
        Path(path).write_text(json.dumps(plan))

    def test_invalid_plan_file(self):
        rc = imap_dedup.apply_plan("/nonexistent/plan.json", quiet=True)
        assert rc == 1

    def test_invalid_json(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json!!!")
        rc = imap_dedup.apply_plan(str(f), quiet=True)
        assert rc == 1

    def test_unsupported_version(self, tmp_path):
        f = tmp_path / "plan.json"
        self._write_plan(f, version=99)
        rc = imap_dedup.apply_plan(str(f), quiet=True)
        assert rc == 1

    def test_missing_imap_host(self, tmp_path, capsys):
        f = tmp_path / "plan.json"
        # Write plan without imap_host
        plan = {"version": 1, "created": "2024-01-01T00:00:00Z",
                "stats": {}, "groups": [{"keep": {}, "delete": [{}]}]}
        Path(f).write_text(json.dumps(plan))
        rc = imap_dedup.apply_plan(str(f), quiet=True)
        assert rc == 1
        assert "IMAP host" in capsys.readouterr().err

    def test_empty_groups(self, tmp_path):
        f = tmp_path / "plan.json"
        self._write_plan(f, groups=[])
        rc = imap_dedup.apply_plan(str(f), quiet=True)
        assert rc == 0

    def test_stale_plan_warning(self, tmp_path, capsys):
        f = tmp_path / "plan.json"
        old_time = "2020-01-01T00:00:00"
        self._write_plan(f, created=old_time, groups=[])
        imap_dedup.apply_plan(str(f), quiet=True)
        captured = capsys.readouterr()
        assert "hours old" in captured.err

    @patch("imap_dedup.imap_connect")
    def test_imap_host_override(self, mock_connect, tmp_path):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.select.return_value = ("OK", [b"1"])
        mock_conn.uid.return_value = ("OK", [
            (b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {30}", b"Message-ID: <a@b.com>\r\n\r\n"),
            b")",
        ])
        f = tmp_path / "plan.json"
        groups = [{
            "keep": {"uid": 1, "imap_folder": "INBOX", "message_id": "a@b.com", "subject": "T"},
            "delete": [{"uid": 2, "imap_folder": "INBOX", "message_id": "a@b.com",
                        "subject": "T", "flags": "S", "size": 100}],
        }]
        self._write_plan(f, groups=groups)
        imap_dedup.apply_plan(str(f), dry_run=True,
                              imap_host_override="custom.host.com", quiet=True)
        mock_connect.assert_called_once_with("custom.host.com")

    @patch("imap_dedup.imap_connect")
    def test_folders_filter(self, mock_connect, tmp_path):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.select.return_value = ("OK", [b"1"])
        mock_conn.uid.return_value = ("OK", [
            (b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {30}", b"Message-ID: <a@b.com>\r\n\r\n"),
            b")",
        ])
        f = tmp_path / "plan.json"
        groups = [{
            "keep": {"uid": 1, "imap_folder": "INBOX", "message_id": "a@b.com", "subject": "T"},
            "delete": [
                {"uid": 2, "imap_folder": "INBOX", "message_id": "a@b.com",
                 "subject": "T", "flags": "S", "size": 100},
                {"uid": 3, "imap_folder": "Sent", "message_id": "a@b.com",
                 "subject": "T", "flags": "S", "size": 100},
            ],
        }]
        self._write_plan(f, groups=groups)
        imap_dedup.apply_plan(str(f), dry_run=True, folders_filter=["INBOX"], quiet=True)
        # Only INBOX should be selected, not Sent
        select_calls = mock_conn.select.call_args_list
        folders_selected = [c[0][0] for c in select_calls]
        assert '"INBOX"' in folders_selected
        assert '"Sent"' not in folders_selected

    @patch("imap_dedup.imap_connect")
    def test_all_verified_returns_0(self, mock_connect, tmp_path):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.select.return_value = ("OK", [b"1"])
        mock_conn.uid.return_value = ("OK", [
            (b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {30}", b"Message-ID: <a@b.com>\r\n\r\n"),
            b")",
        ])
        f = tmp_path / "plan.json"
        groups = [{
            "keep": {"uid": 1, "imap_folder": "INBOX", "message_id": "a@b.com", "subject": "T"},
            "delete": [{"uid": 2, "imap_folder": "INBOX", "message_id": "a@b.com",
                        "subject": "T", "flags": "S", "size": 100}],
        }]
        self._write_plan(f, groups=groups)
        rc = imap_dedup.apply_plan(str(f), dry_run=True, quiet=True)
        assert rc == 0

    @patch("imap_dedup.imap_connect")
    def test_mismatch_returns_2(self, mock_connect, tmp_path):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.select.return_value = ("OK", [b"1"])
        mock_conn.uid.return_value = ("OK", [
            (b"1 (BODY[HEADER.FIELDS (MESSAGE-ID)] {30}", b"Message-ID: <wrong@b.com>\r\n\r\n"),
            b")",
        ])
        f = tmp_path / "plan.json"
        groups = [{
            "keep": {"uid": 1, "imap_folder": "INBOX", "message_id": "a@b.com", "subject": "T"},
            "delete": [{"uid": 2, "imap_folder": "INBOX", "message_id": "a@b.com",
                        "subject": "T", "flags": "S", "size": 100}],
        }]
        self._write_plan(f, groups=groups)
        rc = imap_dedup.apply_plan(str(f), dry_run=True, quiet=True)
        assert rc == 2


class TestMainCLIValidation:
    def test_export_and_apply_exclusive(self, tmp_path):
        with patch("sys.argv", ["prog", str(tmp_path), "--export", "plan.json", "--apply", "plan.json"]):
            with pytest.raises(SystemExit) as exc_info:
                imap_dedup.main()
            assert exc_info.value.code == 2

    def test_permanent_requires_apply(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        with patch("sys.argv", ["prog", str(root), "--permanent"]):
            rc = imap_dedup.main()
        assert rc == 1

    def test_dry_run_requires_apply(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        with patch("sys.argv", ["prog", str(root), "--dry-run"]):
            rc = imap_dedup.main()
        assert rc == 1

    def test_export_requires_imap_host(self, tmp_path, capsys):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        with patch("sys.argv", ["prog", str(root), "--export", "plan.json"]):
            rc = imap_dedup.main()
        assert rc == 1
        assert "--imap-host" in capsys.readouterr().err

    def test_verbose_and_quiet_exclusive(self, tmp_path):
        with patch("sys.argv", ["prog", str(tmp_path), "-v", "-q"]):
            with pytest.raises(SystemExit) as exc_info:
                imap_dedup.main()
            assert exc_info.value.code == 2

    def test_nonexistent_maildir(self, tmp_path):
        with patch("sys.argv", ["prog", str(tmp_path / "nope")]):
            rc = imap_dedup.main()
        assert rc == 1

    def test_empty_maildir_scan(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        (root / "tmp").mkdir(parents=True)
        with patch("sys.argv", ["prog", str(root)]):
            rc = imap_dedup.main()
        assert rc == 0


class TestImapQuoteFolder:
    def test_simple_folder(self):
        assert imap_dedup.imap_quote_folder("INBOX") == '"INBOX"'

    def test_folder_with_slash(self):
        assert imap_dedup.imap_quote_folder("2_Job/Uni Trier") == '"2_Job/Uni Trier"'

    def test_folder_with_backslash(self):
        assert imap_dedup.imap_quote_folder("folder\\sub") == '"folder\\\\sub"'

    def test_folder_with_double_quote(self):
        assert imap_dedup.imap_quote_folder('folder"name') == '"folder\\"name"'

    def test_folder_with_both_special(self):
        assert imap_dedup.imap_quote_folder('a\\"b') == '"a\\\\\\"b"'


class TestExportPlanTimestamp:
    def test_created_has_z_suffix(self, tmp_path):
        keep_path = tmp_path / "1234567890.M100,S=5000,U=10:2,FS"
        keep_path.write_bytes(b"keep")
        dup_path = tmp_path / "1234567891.M101,S=5000,U=20:2,S"
        dup_path.write_bytes(b"dup")
        keep = imap_dedup.MessageInfo(
            path=keep_path, folder="INBOX", subject="Test", date="date",
            flags="FS", size=5000, mtime=time.time(),
            dedup_key="test@ex.com", method="message-id", message_id="test@ex.com",
        )
        dup = imap_dedup.MessageInfo(
            path=dup_path, folder="Sent", subject="Test", date="date",
            flags="S", size=1000, mtime=time.time(),
            dedup_key="test@ex.com", method="message-id", message_id="test@ex.com",
        )
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert plan["created"].endswith("Z")

    def test_export_uses_message_id_field(self, tmp_path):
        keep_path = tmp_path / "1234567890.M100,S=5000,U=10:2,FS"
        keep_path.write_bytes(b"keep")
        dup_path = tmp_path / "1234567891.M101,S=5000,U=20:2,S"
        dup_path.write_bytes(b"dup")
        keep = imap_dedup.MessageInfo(
            path=keep_path, folder="INBOX", subject="Test", date="date",
            flags="FS", size=5000, mtime=time.time(),
            dedup_key="test@ex.com", method="message-id", message_id="test@ex.com",
        )
        dup = imap_dedup.MessageInfo(
            path=dup_path, folder="Sent", subject="Test", date="date",
            flags="S", size=1000, mtime=time.time(),
            dedup_key="test@ex.com", method="message-id", message_id="test@ex.com",
        )
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        out = str(tmp_path / "plan.json")
        imap_dedup.export_plan([group], out, "imap.test.com", quiet=True)
        plan = json.loads(Path(out).read_text())
        assert plan["groups"][0]["keep"]["message_id"] == "test@ex.com"
        assert plan["groups"][0]["delete"][0]["message_id"] == "test@ex.com"


class TestApplyPlanBackwardsCompat:
    def test_plan_without_z_suffix(self, tmp_path, capsys):
        """Plans created before the Z suffix change should still work."""
        f = tmp_path / "plan.json"
        plan = {
            "version": 1,
            "created": "2020-01-01T00:00:00",  # no Z suffix
            "imap_host": "imap.test.com",
            "stats": {"groups": 0, "total_deletions": 0,
                       "skipped_no_message_id": 0, "skipped_no_uid": 0},
            "groups": [],
        }
        f.write_text(json.dumps(plan))
        rc = imap_dedup.apply_plan(str(f), quiet=True)
        assert rc == 0
        # Should still warn about age
        captured = capsys.readouterr()
        assert "hours old" in captured.err


class TestBuildParser:
    def test_default_maildir_path(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args([])
        assert args.maildir_path == os.path.expanduser("~/Maildir")

    def test_default_exclude_folders(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args([])
        assert set(args.exclude_folders) == imap_dedup.DEFAULT_EXCLUDE_FOLDERS

    def test_interactive_flag(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args(["-i"])
        assert args.interactive is True

    def test_interactive_long_flag(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args(["--interactive"])
        assert args.interactive is True

    def test_dry_run_flag(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args(["-d"])
        assert args.dry_run is True

    def test_dry_run_long_flag(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_imap_trash_flag(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args(["-T", "Deleted Messages"])
        assert args.imap_trash == "Deleted Messages"

    def test_permanent_flag(self):
        parser = imap_dedup.build_parser()
        args = parser.parse_args(["-p"])
        assert args.permanent is True


# ===========================================================================
# Phase 6: Interactive review
# ===========================================================================



class TestGroupByDeleteFolder:
    def test_single_folder(self, make_message_info):
        keep = make_message_info(folder="0_Sent", flags="FRS", dedup_key="a@b.com")
        dup = make_message_info(folder="INBOX", flags="S", dedup_key="a@b.com",
                                mtime=1700000001.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        result = imap_dedup.group_by_delete_folder([group])
        assert "INBOX" in result
        assert len(result["INBOX"]) == 1
        assert result["INBOX"][0][0] is group
        assert result["INBOX"][0][1] == [dup]

    def test_multiple_folders(self, make_message_info):
        keep = make_message_info(folder="0_Sent", flags="FRS", dedup_key="a@b.com")
        dup1 = make_message_info(folder="INBOX", flags="S", dedup_key="a@b.com",
                                 mtime=1700000001.0)
        dup2 = make_message_info(folder="Archive", flags="S", dedup_key="a@b.com",
                                 mtime=1700000002.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup1, dup2])
        result = imap_dedup.group_by_delete_folder([group])
        assert "INBOX" in result
        assert "Archive" in result
        assert len(result["INBOX"]) == 1
        assert len(result["Archive"]) == 1

    def test_empty_groups(self):
        result = imap_dedup.group_by_delete_folder([])
        assert result == {}

    def test_multiple_groups_same_folder(self, make_message_info):
        keep1 = make_message_info(folder="0_Sent", dedup_key="a@b.com")
        dup1 = make_message_info(folder="INBOX", dedup_key="a@b.com",
                                 mtime=1700000001.0)
        keep2 = make_message_info(folder="0_Sent", dedup_key="c@d.com",
                                  mtime=1700000010.0)
        dup2 = make_message_info(folder="INBOX", dedup_key="c@d.com",
                                 mtime=1700000011.0)
        g1 = imap_dedup.DuplicateGroup(keep=keep1, duplicates=[dup1])
        g2 = imap_dedup.DuplicateGroup(keep=keep2, duplicates=[dup2])
        result = imap_dedup.group_by_delete_folder([g1, g2])
        assert len(result["INBOX"]) == 2


class TestFormatInteractiveEntry:
    def test_basic_format(self, make_message_info):
        keep = make_message_info(folder="0_Sent", flags="FRS", size=5300,
                                 subject="Re: Meeting tomorrow at 3pm",
                                 from_addr="alice@example.com")
        dup = make_message_info(folder="INBOX", flags="S", size=5200,
                                mtime=1700000001.0, subject="Re: Meeting tomorrow at 3pm",
                                from_addr="alice@example.com")
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        text = imap_dedup.format_interactive_entry(1, group, [dup])
        assert "1)" in text
        assert "Re: Meeting tomorrow at 3pm" in text
        assert "alice@example.com" in text
        assert "KEEP:" in text
        assert "DELETE:" in text
        assert "0_Sent" in text
        assert "INBOX" in text
        assert "FRS" in text

    def test_no_flags(self, make_message_info):
        keep = make_message_info(folder="INBOX", flags="")
        dup = make_message_info(folder="INBOX", flags="", mtime=1700000001.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
        text = imap_dedup.format_interactive_entry(1, group, [dup])
        assert "(none)" in text

    def test_multiple_delete_entries(self, make_message_info):
        keep = make_message_info(folder="0_Sent")
        dup1 = make_message_info(folder="INBOX", mtime=1700000001.0)
        dup2 = make_message_info(folder="INBOX", mtime=1700000002.0)
        group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup1, dup2])
        text = imap_dedup.format_interactive_entry(1, group, [dup1, dup2])
        assert text.count("DELETE:") == 2


class TestInteractivePrompt:
    @patch("builtins.input", return_value="a")
    def test_valid_input(self, mock_input):
        result = imap_dedup.interactive_prompt("Choose: ", "asrq")
        assert result == "a"

    @patch("builtins.input", return_value="A")
    def test_case_insensitive(self, mock_input):
        result = imap_dedup.interactive_prompt("Choose: ", "asrq")
        assert result == "a"

    @patch("builtins.input", side_effect=["x", "a"])
    def test_retries_on_invalid(self, mock_input):
        result = imap_dedup.interactive_prompt("Choose: ", "asrq")
        assert result == "a"
        assert mock_input.call_count == 2

    @patch("builtins.input", side_effect=EOFError)
    def test_eof_returns_none(self, mock_input):
        result = imap_dedup.interactive_prompt("Choose: ", "asrq")
        assert result is None

    @patch("builtins.input", side_effect=KeyboardInterrupt)
    def test_ctrl_c_returns_none(self, mock_input):
        result = imap_dedup.interactive_prompt("Choose: ", "asrq")
        assert result is None


class TestReviewFolderInteractive:
    def _make_entries(self, make_message_info, n=3):
        entries = []
        for i in range(n):
            keep = make_message_info(folder="0_Sent", dedup_key=f"msg{i}@b.com",
                                     mtime=1700000000.0 + i * 10)
            dup = make_message_info(folder="INBOX", dedup_key=f"msg{i}@b.com",
                                    mtime=1700000000.0 + i * 10 + 1)
            group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
            entries.append((group, [dup]))
        return entries

    @patch("builtins.input", return_value="a")
    def test_accept_all(self, mock_input, make_message_info):
        entries = self._make_entries(make_message_info)
        result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is not None
        assert len(result) == 3

    @patch("builtins.input", return_value="s")
    def test_skip_all(self, mock_input, make_message_info):
        entries = self._make_entries(make_message_info)
        result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result == []

    @patch("builtins.input", return_value="q")
    def test_quit(self, mock_input, make_message_info):
        entries = self._make_entries(make_message_info)
        result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is None

    @patch("builtins.input", side_effect=["r", "y", "n", "y"])
    def test_review_one_by_one(self, mock_input, make_message_info):
        entries = self._make_entries(make_message_info)
        result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is not None
        assert len(result) == 2  # first and third accepted

    @patch("builtins.input", side_effect=["r", "y", "q"])
    def test_review_quit_midway(self, mock_input, make_message_info):
        entries = self._make_entries(make_message_info)
        result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is None

    @patch("builtins.input", side_effect=EOFError)
    def test_eof_returns_none(self, mock_input, make_message_info):
        entries = self._make_entries(make_message_info)
        result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is None

    def test_pagination_more_then_accept(self, make_message_info):
        """With >PAGE_SIZE entries, 'm' shows next page, then 'a' accepts all."""
        entries = self._make_entries(make_message_info, n=30)
        with patch("builtins.input", side_effect=["m", "a"]):
            result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is not None
        assert len(result) == 30

    def test_pagination_accept_early(self, make_message_info):
        """Accept all from the first page prompt without viewing the rest."""
        entries = self._make_entries(make_message_info, n=30)
        with patch("builtins.input", return_value="a"):
            result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is not None
        assert len(result) == 30

    def test_pagination_skip_early(self, make_message_info):
        """Skip all from the first page prompt."""
        entries = self._make_entries(make_message_info, n=30)
        with patch("builtins.input", return_value="s"):
            result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result == []

    def test_pagination_review_from_page_prompt(self, make_message_info):
        """Choose review one-by-one from paginated prompt."""
        entries = self._make_entries(make_message_info, n=30)
        # 'r' from page prompt, then y/n for all 30
        responses = ["r"] + ["y", "n"] * 15
        with patch("builtins.input", side_effect=responses):
            result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is not None
        assert len(result) == 15  # 15 yes, 15 no

    def test_no_pagination_for_small_list(self, make_message_info):
        """Lists smaller than PAGE_SIZE show all at once with no 'm' prompt."""
        entries = self._make_entries(make_message_info, n=5)
        # Single 'a' prompt — no pagination prompt expected
        with patch("builtins.input", return_value="a") as mock_input:
            result = imap_dedup.review_folder_interactive("INBOX", entries)
        assert result is not None
        assert len(result) == 5
        mock_input.assert_called_once()


class TestRunInteractiveReview:
    def _make_groups(self, make_message_info, n=2):
        groups = []
        for i in range(n):
            keep = make_message_info(folder="0_Sent", dedup_key=f"msg{i}@b.com",
                                     mtime=1700000000.0 + i * 10)
            dup = make_message_info(folder="INBOX", dedup_key=f"msg{i}@b.com",
                                    mtime=1700000000.0 + i * 10 + 1)
            groups.append(imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup]))
        return groups

    @patch("builtins.input", side_effect=["a"])
    def test_accept_all_returns_groups(self, mock_input, make_message_info):
        groups = self._make_groups(make_message_info)
        result = imap_dedup.run_interactive_review(groups)
        assert result is not None
        assert len(result) == 2
        assert all(isinstance(g, imap_dedup.DuplicateGroup) for g in result)

    @patch("builtins.input", side_effect=["q"])
    def test_quit_early_returns_none(self, mock_input, make_message_info, capsys):
        groups = self._make_groups(make_message_info)
        result = imap_dedup.run_interactive_review(groups)
        assert result is None
        captured = capsys.readouterr()
        assert "no changes" in captured.out.lower()

    @patch("builtins.input", side_effect=["s"])
    def test_skip_all_returns_empty(self, mock_input, make_message_info, capsys):
        groups = self._make_groups(make_message_info)
        result = imap_dedup.run_interactive_review(groups)
        assert result is not None
        assert len(result) == 0

    def test_summary_table(self, make_message_info, capsys):
        # Two groups in different folders
        keep1 = make_message_info(folder="0_Sent", dedup_key="a@b.com")
        dup1 = make_message_info(folder="INBOX", dedup_key="a@b.com",
                                 mtime=1700000001.0)
        keep2 = make_message_info(folder="0_Sent", dedup_key="c@d.com",
                                  mtime=1700000010.0)
        dup2 = make_message_info(folder="Archive", dedup_key="c@d.com",
                                 mtime=1700000011.0)
        groups = [
            imap_dedup.DuplicateGroup(keep=keep1, duplicates=[dup1]),
            imap_dedup.DuplicateGroup(keep=keep2, duplicates=[dup2]),
        ]
        with patch("builtins.input", side_effect=["a", "s"]):
            result = imap_dedup.run_interactive_review(groups)
        assert result is not None
        assert len(result) == 1  # accepted from INBOX, skipped from Archive
        captured = capsys.readouterr()
        assert "Archive:" in captured.out
        assert "INBOX:" in captured.out
        assert "Total:" in captured.out


class TestInteractiveCLIValidation:
    def test_interactive_with_quiet(self, tmp_path):
        with patch("sys.argv", ["prog", str(tmp_path), "-i", "-q"]):
            rc = imap_dedup.main()
        assert rc == 1

    def test_interactive_export_requires_imap_host(self, tmp_path):
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        with patch("sys.argv", ["prog", str(root), "-i", "--export", "plan.json"]), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = True
            rc = imap_dedup.main()
        assert rc == 1

    def test_interactive_with_apply(self, tmp_path):
        plan = tmp_path / "plan.json"
        plan.write_text('{"version": 1}')
        with patch("sys.argv", ["prog", "-i", "--apply", str(plan)]):
            rc = imap_dedup.main()
        assert rc == 1

    @patch("sys.stdin")
    def test_interactive_requires_tty(self, mock_stdin, tmp_path):
        mock_stdin.isatty.return_value = False
        root = tmp_path / "Maildir"
        (root / "cur").mkdir(parents=True)
        (root / "new").mkdir(parents=True)
        with patch("sys.argv", ["prog", str(root), "-i"]):
            rc = imap_dedup.main()
        assert rc == 1


# ---------------------------------------------------------------------------
# imap_parse_list_entry
# ---------------------------------------------------------------------------


class TestImapParseListEntry:
    def test_basic_entry(self):
        item = b'(\\HasNoChildren) "/" "INBOX"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert b"\\HasNoChildren" in attrs
        assert name == "INBOX"

    def test_noselect_entry(self):
        item = b'(\\Noselect \\HasChildren) "/" "Archive"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert b"\\Noselect" in attrs
        assert b"\\HasChildren" in attrs
        assert name == "Archive"

    def test_multiple_flags(self):
        item = b'(\\Trash \\HasNoChildren) "/" "Trash"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert b"\\Trash" in attrs
        assert name == "Trash"

    def test_folder_with_slash(self):
        item = b'(\\HasNoChildren) "/" "Work/Projects"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        _, name = result
        assert name == "Work/Projects"

    def test_empty_flags(self):
        item = b'() "/" "SomeFolder"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert attrs == {b""}  or attrs == set()  # empty set after filtering
        assert name == "SomeFolder"

    def test_unparseable(self):
        result = imap_dedup.imap_parse_list_entry(b"garbage data")
        assert result is None

    def test_nonexistent_flag(self):
        item = b'(\\NonExistent) "/" "OldFolder"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert b"\\NonExistent" in attrs
        assert name == "OldFolder"

    def test_nil_delimiter(self):
        item = b'(\\All) NIL "INBOX"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert b"\\All" in attrs
        assert name == "INBOX"

    def test_unquoted_folder_name(self):
        item = b'(\\HasNoChildren) "/" INBOX'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        attrs, name = result
        assert b"\\HasNoChildren" in attrs
        assert name == "INBOX"

    def test_escaped_quote_in_folder_name(self):
        item = b'(\\HasNoChildren) "/" "Folder\\"Name"'
        result = imap_dedup.imap_parse_list_entry(item)
        assert result is not None
        _, name = result
        assert name == 'Folder"Name'


# ---------------------------------------------------------------------------
# imap_list_all_folders
# ---------------------------------------------------------------------------


class TestImapListAllFolders:
    def test_basic_listing(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\Noselect) "/" "Archive"',
            b'(\\Trash) "/" "Trash"',
        ])
        result = imap_dedup.imap_list_all_folders(conn)
        assert len(result) == 3
        names = [name for name, _ in result]
        assert "INBOX" in names
        assert "Archive" in names
        assert "Trash" in names

    def test_empty_listing(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [])
        result = imap_dedup.imap_list_all_folders(conn)
        assert result == []

    def test_failed_list(self):
        conn = MagicMock()
        conn.list.return_value = ("NO", [])
        result = imap_dedup.imap_list_all_folders(conn)
        assert result == []

    def test_skips_non_bytes(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            42,  # non-bytes entry
        ])
        result = imap_dedup.imap_list_all_folders(conn)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# imap_find_trash_folder (refactored)
# ---------------------------------------------------------------------------


class TestImapFindTrashFolderRefactored:
    def test_finds_trash_by_attribute(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\Trash) "/" "Deleted Items"',
        ])
        assert imap_dedup.imap_find_trash_folder(conn) == "Deleted Items"

    def test_fallback_to_trash(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
        ])
        assert imap_dedup.imap_find_trash_folder(conn) == "Trash"


# ---------------------------------------------------------------------------
# imap_fetch_all_message_ids
# ---------------------------------------------------------------------------


class TestImapFetchAllMessageIds:
    def test_fetches_message_ids(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"2"])
        conn.uid.side_effect = [
            # SEARCH ALL
            ("OK", [b"1 2"]),
            # FETCH
            ("OK", [
                (b"1 (UID 1 BODY[HEADER.FIELDS (MESSAGE-ID)] {30}",
                 b"Message-ID: <abc@example.com>\r\n\r\n"),
                b")",
                (b"2 (UID 2 BODY[HEADER.FIELDS (MESSAGE-ID)] {30}",
                 b"Message-ID: <def@example.com>\r\n\r\n"),
                b")",
            ]),
        ]
        result = imap_dedup.imap_fetch_all_message_ids(conn, "INBOX")
        assert result is not None
        assert "abc@example.com" in result
        assert "def@example.com" in result
        assert result["abc@example.com"] == "1"
        assert result["def@example.com"] == "2"

    def test_returns_none_on_select_failure(self):
        conn = MagicMock()
        conn.select.return_value = ("NO", [b"Folder not found"])
        result = imap_dedup.imap_fetch_all_message_ids(conn, "NonExistent")
        assert result is None

    def test_empty_folder(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"0"])
        result = imap_dedup.imap_fetch_all_message_ids(conn, "Empty")
        assert result == {}

    def test_search_returns_empty(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", [b""])
        result = imap_dedup.imap_fetch_all_message_ids(conn, "INBOX")
        assert result == {}

    def test_non_numeric_select_response(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"not-a-number"])
        result = imap_dedup.imap_fetch_all_message_ids(conn, "INBOX")
        assert result is None


# ---------------------------------------------------------------------------
# imap_copy_messages
# ---------------------------------------------------------------------------


class TestImapCopyMessages:
    def test_successful_copy(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"5"])
        conn.uid.return_value = ("OK", [b"OK"])
        result = imap_dedup.imap_copy_messages(conn, "Source", ["1", "2"], "Dest")
        assert result == 2
        conn.uid.assert_called_once_with("COPY", "1,2", '"Dest"')

    def test_empty_uids(self):
        conn = MagicMock()
        result = imap_dedup.imap_copy_messages(conn, "Source", [], "Dest")
        assert result == 0
        conn.select.assert_not_called()

    def test_select_failure(self):
        conn = MagicMock()
        conn.select.return_value = ("NO", [b"Error"])
        result = imap_dedup.imap_copy_messages(conn, "Source", ["1"], "Dest")
        assert result == 0

    def test_copy_failure(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"5"])
        conn.uid.return_value = ("NO", [b"Error"])
        result = imap_dedup.imap_copy_messages(conn, "Source", ["1"], "Dest")
        assert result == 0


# ---------------------------------------------------------------------------
# imap_delete_folder
# ---------------------------------------------------------------------------


class TestImapDeleteFolder:
    def test_successful_delete(self):
        conn = MagicMock()
        conn.delete.return_value = ("OK", [b"Done"])
        assert imap_dedup.imap_delete_folder(conn, "OldFolder") is True

    def test_failed_delete(self):
        conn = MagicMock()
        conn.delete.return_value = ("NO", [b"Error"])
        assert imap_dedup.imap_delete_folder(conn, "OldFolder") is False


# ---------------------------------------------------------------------------
# imap_folder_message_count
# ---------------------------------------------------------------------------


class TestImapFolderMessageCount:
    def test_returns_count(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"42"])
        assert imap_dedup.imap_folder_message_count(conn, "INBOX") == 42

    def test_returns_zero_on_failure(self):
        conn = MagicMock()
        conn.select.return_value = ("NO", [b"Error"])
        assert imap_dedup.imap_folder_message_count(conn, "Bad") == 0

    def test_non_numeric_select_response(self):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"not-a-number"])
        assert imap_dedup.imap_folder_message_count(conn, "INBOX") == 0


# ---------------------------------------------------------------------------
# clean_hidden_folders (orchestrator)
# ---------------------------------------------------------------------------


class TestCleanHiddenFolders:
    def _mock_conn(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\Noselect) "/" "HiddenFolder"',
            b'(\\HasNoChildren) "/" "Sent"',
        ])
        return conn

    @patch("imap_dedup.imap_connect")
    def test_no_hidden_folders(self, mock_connect):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
        ])
        mock_connect.return_value = conn
        rc = imap_dedup.clean_hidden_folders("host", quiet=True)
        assert rc == 0

    @patch("imap_dedup.imap_connect")
    def test_dry_run_reports_only(self, mock_connect):
        conn = self._mock_conn()
        mock_connect.return_value = conn

        # Hidden folder has 1 message, also in INBOX (duplicated)
        def mock_select(folder, readonly=False):
            return ("OK", [b"1"])

        conn.select.side_effect = mock_select

        # Mock fetches: hidden has msg, INBOX has same msg, Sent has nothing
        call_count = [0]
        def mock_uid(*args):
            call_count[0] += 1
            if args[0] == "SEARCH":
                return ("OK", [b"1"])
            if args[0] == "FETCH":
                return ("OK", [
                    (b"1 (UID 1 BODY[HEADER.FIELDS (MESSAGE-ID)] {30}",
                     b"Message-ID: <dup@example.com>\r\n\r\n"),
                    b")",
                ])
            return ("OK", [])

        conn.uid.side_effect = mock_uid

        rc = imap_dedup.clean_hidden_folders("host", dry_run=True, quiet=True)
        assert rc == 0
        # Should not have called STORE or expunge
        for call in conn.uid.call_args_list:
            assert call[0][0] != "STORE"

    @patch("imap_dedup.imap_connect")
    def test_empty_hidden_folder_no_messages(self, mock_connect):
        conn = self._mock_conn()
        mock_connect.return_value = conn

        def mock_select(folder, readonly=False):
            if "Hidden" in folder:
                return ("OK", [b"0"])
            return ("OK", [b"0"])

        conn.select.side_effect = mock_select
        rc = imap_dedup.clean_hidden_folders("host", quiet=True)
        assert rc == 0

    @patch("imap_dedup.imap_connect")
    def test_clean_hidden_requires_imap_host_via_cli(self, mock_connect):
        with patch("sys.argv", ["prog", "--clean-hidden"]):
            rc = imap_dedup.main()
        assert rc == 1

    @patch("imap_dedup.imap_connect")
    def test_delete_folders_requires_clean_hidden(self, mock_connect):
        with patch("sys.argv", ["prog", "--delete-folders"]):
            rc = imap_dedup.main()
        assert rc == 1


# ---------------------------------------------------------------------------
# prune_noselect_folders (orchestrator)
# ---------------------------------------------------------------------------


class TestPruneNoselectFolders:
    @patch("imap_dedup.imap_connect")
    def test_no_noselect_folders(self, mock_connect):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
        ])
        mock_connect.return_value = conn
        rc = imap_dedup.prune_noselect_folders("host", quiet=True)
        assert rc == 0

    @patch("imap_dedup.imap_connect")
    def test_dry_run_does_not_delete(self, mock_connect):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\Noselect) "/" "EmptyParent"',
            b'(\\HasNoChildren) "/" "INBOX"',
        ])
        conn.select.return_value = ("OK", [b"0"])
        mock_connect.return_value = conn

        rc = imap_dedup.prune_noselect_folders("host", dry_run=True, quiet=True)
        assert rc == 0
        conn.delete.assert_not_called()

    @patch("imap_dedup.imap_connect")
    def test_prunes_empty_noselect(self, mock_connect):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\Noselect) "/" "EmptyParent"',
            b'(\\HasNoChildren) "/" "INBOX"',
        ])
        conn.select.return_value = ("OK", [b"0"])
        conn.delete.return_value = ("OK", [b"Done"])
        mock_connect.return_value = conn

        rc = imap_dedup.prune_noselect_folders("host", quiet=True)
        assert rc == 0
        conn.delete.assert_called_once()

    @patch("imap_dedup.imap_connect")
    def test_keeps_noselect_with_descendant_messages(self, mock_connect):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\Noselect) "/" "Parent"',
            b'(\\HasNoChildren) "/" "Parent/Child"',
            b'(\\HasNoChildren) "/" "INBOX"',
        ])

        def mock_select(folder, readonly=False):
            if "Child" in folder:
                return ("OK", [b"5"])
            return ("OK", [b"0"])

        conn.select.side_effect = mock_select
        mock_connect.return_value = conn

        rc = imap_dedup.prune_noselect_folders("host", quiet=True)
        assert rc == 0
        conn.delete.assert_not_called()

    @patch("imap_dedup.imap_connect")
    def test_deletes_deepest_first(self, mock_connect):
        conn = MagicMock()
        conn.list.return_value = ("OK", [
            b'(\\Noselect) "/" "A"',
            b'(\\Noselect) "/" "A/B"',
            b'(\\HasNoChildren) "/" "INBOX"',
        ])
        conn.select.return_value = ("OK", [b"0"])
        conn.delete.return_value = ("OK", [b"Done"])
        mock_connect.return_value = conn

        rc = imap_dedup.prune_noselect_folders("host", quiet=True)
        assert rc == 0
        # A/B should be deleted before A
        delete_calls = conn.delete.call_args_list
        assert len(delete_calls) == 2
        assert "A/B" in delete_calls[0][0][0]
        assert delete_calls[1][0][0] == '"A"'

    @patch("imap_dedup.imap_connect")
    def test_prune_noselect_requires_imap_host_via_cli(self, mock_connect):
        with patch("sys.argv", ["prog", "--prune-noselect"]):
            rc = imap_dedup.main()
        assert rc == 1


# ---------------------------------------------------------------------------
# CLI validation for new modes
# ---------------------------------------------------------------------------


class TestNewModeCLIValidation:
    def test_dry_run_with_clean_hidden_is_valid(self):
        """--dry-run should be accepted with --clean-hidden."""
        with patch("sys.argv", ["prog", "--clean-hidden", "--imap-host", "host", "--dry-run"]), \
             patch("imap_dedup.clean_hidden_folders", return_value=0) as mock_fn:
            rc = imap_dedup.main()
        assert rc == 0
        mock_fn.assert_called_once()
        assert mock_fn.call_args[1]["dry_run"] is True

    def test_dry_run_with_prune_noselect_is_valid(self):
        """--dry-run should be accepted with --prune-noselect."""
        with patch("sys.argv", ["prog", "--prune-noselect", "--imap-host", "host", "--dry-run"]), \
             patch("imap_dedup.prune_noselect_folders", return_value=0) as mock_fn:
            rc = imap_dedup.main()
        assert rc == 0
        mock_fn.assert_called_once()
        assert mock_fn.call_args[1]["dry_run"] is True

    def test_rescue_folder_passed_to_clean_hidden(self):
        with patch("sys.argv", ["prog", "--clean-hidden", "--imap-host", "host",
                                 "--rescue-folder", "Rescued"]), \
             patch("imap_dedup.clean_hidden_folders", return_value=0) as mock_fn:
            rc = imap_dedup.main()
        assert rc == 0
        assert mock_fn.call_args[1]["rescue_folder"] == "Rescued"

    def test_delete_folders_passed_to_clean_hidden(self):
        with patch("sys.argv", ["prog", "--clean-hidden", "--imap-host", "host",
                                 "--delete-folders"]), \
             patch("imap_dedup.clean_hidden_folders", return_value=0) as mock_fn:
            rc = imap_dedup.main()
        assert rc == 0
        assert mock_fn.call_args[1]["delete_folders"] is True

    def test_dry_run_alone_fails(self):
        with patch("sys.argv", ["prog", "--dry-run"]):
            rc = imap_dedup.main()
        assert rc == 1


# ---------------------------------------------------------------------------
# render_email_for_diff / show_diff / diff in interactive review
# ---------------------------------------------------------------------------

def _write_email(path, subject="Hello", body="Hello world\n", from_addr="a@b.com",
                 date="Mon, 01 Jan 2024 12:00:00 +0000", extra_headers="",
                 content_type="text/plain"):
    """Write a minimal RFC 5322 email file to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (
        f"From: {from_addr}\n"
        f"To: x@y.com\n"
        f"Subject: {subject}\n"
        f"Date: {date}\n"
        f"Message-ID: <test@example.com>\n"
        f"Content-Type: {content_type}\n"
        f"{extra_headers}"
        f"\n"
        f"{body}"
    )
    path.write_text(raw)


class TestRenderEmailForDiff:
    def test_basic_email(self, tmp_path):
        p = tmp_path / "msg1"
        _write_email(p, subject="Hello", body="Line 1\nLine 2\n")
        lines = imap_dedup.render_email_for_diff(p)
        # Should contain the headers we care about
        assert any("Subject: Hello" in l for l in lines)
        assert any("From: a@b.com" in l for l in lines)
        # Should contain body lines
        assert "Line 1\n" in lines
        assert "Line 2\n" in lines

    def test_missing_file(self, tmp_path):
        p = tmp_path / "nonexistent"
        lines = imap_dedup.render_email_for_diff(p)
        assert len(lines) == 1
        assert "could not read" in lines[0]

    def test_multipart_email(self, tmp_path):
        p = tmp_path / "multipart"
        p.parent.mkdir(parents=True, exist_ok=True)
        raw = (
            "From: a@b.com\n"
            "To: x@y.com\n"
            "Subject: Multi\n"
            "Date: Mon, 01 Jan 2024 12:00:00 +0000\n"
            "Message-ID: <multi@example.com>\n"
            "MIME-Version: 1.0\n"
            'Content-Type: multipart/alternative; boundary="BOUND"\n'
            "\n"
            "--BOUND\n"
            "Content-Type: text/plain\n"
            "\n"
            "Plain text body\n"
            "--BOUND\n"
            "Content-Type: text/html\n"
            "\n"
            "<p>HTML body</p>\n"
            "--BOUND--\n"
        )
        p.write_text(raw)
        lines = imap_dedup.render_email_for_diff(p)
        assert any("text/plain" in l for l in lines)
        assert any("text/html" in l for l in lines)
        assert any("Plain text body" in l for l in lines)

    def test_binary_attachment(self, tmp_path):
        p = tmp_path / "binary_msg"
        p.parent.mkdir(parents=True, exist_ok=True)
        raw = (
            b"From: a@b.com\n"
            b"To: x@y.com\n"
            b"Subject: With attachment\n"
            b"Date: Mon, 01 Jan 2024 12:00:00 +0000\n"
            b"Message-ID: <bin@example.com>\n"
            b"MIME-Version: 1.0\n"
            b'Content-Type: multipart/mixed; boundary="BOUND"\n'
            b"\n"
            b"--BOUND\n"
            b"Content-Type: text/plain\n"
            b"\n"
            b"Some text\n"
            b"--BOUND\n"
            b"Content-Type: application/octet-stream\n"
            b"Content-Transfer-Encoding: base64\n"
            b"\n"
            b"AAAA\n"
            b"--BOUND--\n"
        )
        p.write_bytes(raw)
        lines = imap_dedup.render_email_for_diff(p)
        assert any("binary content" in l for l in lines)
        assert any("Some text" in l for l in lines)

    def test_non_multipart_binary(self, tmp_path):
        p = tmp_path / "bin_single"
        p.parent.mkdir(parents=True, exist_ok=True)
        raw = (
            b"From: a@b.com\n"
            b"To: x@y.com\n"
            b"Subject: Binary\n"
            b"Date: Mon, 01 Jan 2024 12:00:00 +0000\n"
            b"Message-ID: <bin2@example.com>\n"
            b"Content-Type: application/octet-stream\n"
            b"Content-Transfer-Encoding: base64\n"
            b"\n"
            b"AAAA\n"
        )
        p.write_bytes(raw)
        lines = imap_dedup.render_email_for_diff(p)
        assert any("binary content" in l for l in lines)


class TestShowDiff:
    def _make_msg(self, tmp_path, name, folder, body="Hello\n"):
        p = tmp_path / folder / name
        _write_email(p, body=body)
        return imap_dedup.MessageInfo(
            path=p, folder=folder, subject="Hello",
            date="Mon, 01 Jan 2024 12:00:00 +0000", flags="S",
            size=100, mtime=1700000000.0, dedup_key="test@example.com",
            method="message-id", message_id="test@example.com",
        )

    def test_identical_messages(self, tmp_path, capsys):
        keep = self._make_msg(tmp_path, "keep", "Sent")
        dup = self._make_msg(tmp_path, "dup", "INBOX")
        imap_dedup.show_diff(keep, dup)
        out = capsys.readouterr().out
        assert "no differences found" in out

    def test_different_bodies(self, tmp_path, capsys):
        keep = self._make_msg(tmp_path, "keep", "Sent", body="AAA\n")
        dup = self._make_msg(tmp_path, "dup", "INBOX", body="BBB\n")
        imap_dedup.show_diff(keep, dup)
        out = capsys.readouterr().out
        assert "KEEP: Sent" in out
        assert "DELETE: INBOX" in out
        assert "-AAA" in out
        assert "+BBB" in out


class TestReviewOneByOneDiff:
    def _make_entries(self, tmp_path):
        entries = []
        for i in range(2):
            keep_path = tmp_path / "Sent" / f"keep{i}"
            dup_path = tmp_path / "INBOX" / f"dup{i}"
            _write_email(keep_path, body=f"keep body {i}\n")
            _write_email(dup_path, body=f"dup body {i}\n")
            keep = imap_dedup.MessageInfo(
                path=keep_path, folder="Sent", subject="Test",
                date="Mon, 01 Jan 2024 12:00:00 +0000", flags="S",
                size=100, mtime=1700000000.0 + i,
                dedup_key=f"msg{i}@b.com", method="message-id",
                message_id=f"msg{i}@b.com",
            )
            dup = imap_dedup.MessageInfo(
                path=dup_path, folder="INBOX", subject="Test",
                date="Mon, 01 Jan 2024 12:00:00 +0000", flags="S",
                size=100, mtime=1700000001.0 + i,
                dedup_key=f"msg{i}@b.com", method="message-id",
                message_id=f"msg{i}@b.com",
            )
            group = imap_dedup.DuplicateGroup(keep=keep, duplicates=[dup])
            entries.append((group, [dup]))
        return entries

    @patch("builtins.input", side_effect=["d", "y", "n"])
    def test_diff_then_accept(self, mock_input, tmp_path, capsys):
        entries = self._make_entries(tmp_path)
        result = imap_dedup._review_one_by_one(entries)
        assert result is not None
        assert len(result) == 1  # first accepted, second skipped
        out = capsys.readouterr().out
        # diff was shown for the first entry
        assert "KEEP:" in out or "DELETE:" in out

    @patch("builtins.input", side_effect=["d", "d", "y", "y"])
    def test_diff_multiple_times(self, mock_input, tmp_path):
        entries = self._make_entries(tmp_path)
        result = imap_dedup._review_one_by_one(entries)
        assert result is not None
        assert len(result) == 2  # both accepted

    @patch("builtins.input", side_effect=["d", "q"])
    def test_diff_then_quit(self, mock_input, tmp_path):
        entries = self._make_entries(tmp_path)
        result = imap_dedup._review_one_by_one(entries)
        assert result is None

    @patch("builtins.input", side_effect=["d", EOFError])
    def test_diff_then_eof(self, mock_input, tmp_path):
        entries = self._make_entries(tmp_path)
        result = imap_dedup._review_one_by_one(entries)
        assert result is None
