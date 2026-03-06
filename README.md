# imap-dedup

Deduplicate emails in a Maildir hierarchy synced from IMAP via OfflineIMAP.

Duplicates are detected by Message-ID header (primary) or a header+body SHA-256 fingerprint (fallback for emails without Message-ID). Duplicates are removed via IMAP server-side deletion (move to Trash or permanent delete).

## Prerequisites

- Python 3.10+ (enforced at startup)
- [OfflineIMAP](https://www.offlineimap.org/) for syncing your mailbox
- `~/.netrc` with IMAP credentials (used by both OfflineIMAP and imap-dedup)

### Installing OfflineIMAP

```bash
# Linux (Debian/Ubuntu)
sudo apt-get install offlineimap

# macOS
brew install offlineimap certifi
```

### Setting up `~/.netrc`

Create `~/.netrc` with your IMAP server credentials:

```
machine imap.mailbox.org
    login your@email.com
    password your-password
```

Restrict file permissions so only you can read it:

```bash
chmod 600 ~/.netrc
```

This file is used by both `sync-mailbox.sh` (via OfflineIMAP) and `imap_dedup.py` (for IMAP deletion).

### Configuring OfflineIMAP

Copy the included template to `~/.offlineimaprc`:

```bash
cp offlineimaprc ~/.offlineimaprc
```

Edit `~/.offlineimaprc` and adjust:

- **`remoteuser`** — set to your email address (must match the `login` in `~/.netrc`)
- **`remotehost`** — set to your IMAP server hostname (default: `imap.mailbox.org`)
- **`localfolders`** — local Maildir path (default: `~/Maildir`)

The template is preconfigured with `readonly = true` and `createfolders = false`, so OfflineIMAP will only download mail without modifying or deleting anything on the server.

The `sslcacertfile` path is automatically set by `sync-mailbox.sh` based on your OS.

### Syncing your mailbox

Run the included sync script:

```bash
./sync-mailbox.sh
```

This detects the correct CA certificate path for your OS (macOS/Linux), updates `~/.offlineimaprc` accordingly, and runs `offlineimap`. Output is logged to `~/log_sync-mailbox.log`.

## Recommended workflow

**Before you start:**

- `~/.netrc` is configured with IMAP credentials for your server (see [Setting up `~/.netrc`](#setting-up-netrc))
- `~/.offlineimaprc` is configured (see [Configuring OfflineIMAP](#configuring-offlineimap))
- The `--imap-host` flag uses `~/.netrc` to look up the username and password by hostname — no separate user flag is needed

**Steps:**

**1. Sync mailbox**

```bash
./sync-mailbox.sh
```

**2. Interactive review & export plan**

```bash
python3 imap_dedup.py ~/Maildir --interactive --export plan.json --imap-host imap.mailbox.org --sender "Your Name"
```

- `--sender` ensures Sent folder priority only applies to your own emails
- `--interactive` lets you review duplicates folder-by-folder before committing to the plan
- In one-by-one review mode, press `d` to see a unified diff between the kept and deleted copy

**3. Verify (dry-run)**

```bash
python3 imap_dedup.py --apply plan.json --dry-run
```

**4. Apply (move to Trash)**

```bash
python3 imap_dedup.py --apply plan.json
```

**5. Re-sync**

```bash
./sync-mailbox.sh
```

### Additional options

**Scan only (no export or deletion):**

```bash
python3 imap_dedup.py ~/Maildir
python3 imap_dedup.py ~/Maildir --verbose    # show every duplicate group
```

**Permanent delete (EXPUNGE) instead of Trash:**

```bash
python3 imap_dedup.py --apply plan.json --permanent
```

**Custom Trash folder** (overrides RFC 6154 `\Trash` auto-detection):

```bash
python3 imap_dedup.py --apply plan.json --imap-trash "Deleted Messages"
```

### Clean hidden folders

Some IMAP folders are synced by OfflineIMAP but invisible in Apple Mail — typically folders with `\Noselect` or `\NonExistent` attributes (stale hierarchy nodes, folders flagged for deletion but not yet purged). The `--clean-hidden` mode detects these folders, checks if their messages exist in normal folders, rescues orphans, and deletes the duplicated copies.

**Preview (dry-run):**

```bash
python3 imap_dedup.py --clean-hidden --imap-host imap.mailbox.org --dry-run
python3 imap_dedup.py --clean-hidden --imap-host imap.mailbox.org --dry-run -v  # per-message details
```

**Clean (copy orphans to "Recovered", delete from hidden folders):**

```bash
python3 imap_dedup.py --clean-hidden --imap-host imap.mailbox.org
```

**Custom rescue folder:**

```bash
python3 imap_dedup.py --clean-hidden --imap-host imap.mailbox.org --rescue-folder "Rescued"
```

**Also delete empty hidden folders after cleaning:**

```bash
python3 imap_dedup.py --clean-hidden --imap-host imap.mailbox.org --delete-folders
```

Safety: orphaned messages (only in hidden folders) are copied to the rescue folder before deletion. Message-IDs are re-verified before each delete. The `--dry-run` flag previews without changes.

### Prune empty `\Noselect` hierarchy nodes

`\Noselect` folders are hierarchy-only containers that hold subfolders but no messages. Some are stale leftovers. The `--prune-noselect` mode deletes `\Noselect` folders that contain no messages — even transitively (none of their descendants have messages either).

**Preview:**

```bash
python3 imap_dedup.py --prune-noselect --imap-host imap.mailbox.org --dry-run
```

**Prune:**

```bash
python3 imap_dedup.py --prune-noselect --imap-host imap.mailbox.org
```

Folders are deleted deepest-first to respect IMAP hierarchy constraints.

### Clean stale local folders

After deleting folders on the IMAP server (or after `--clean-hidden` / `--prune-noselect` remove them server-side), the local Maildir still contains the corresponding directories. The `--clean-local` mode compares local Maildir folders against the IMAP server and removes local directories that no longer exist on the server.

**Preview (dry-run):**

```bash
python3 imap_dedup.py --clean-local ~/Maildir --imap-host imap.mailbox.org --dry-run
```

**Remove stale folders:**

```bash
python3 imap_dedup.py --clean-local ~/Maildir --imap-host imap.mailbox.org
```

INBOX is never removed. Only directories discovered as valid Maildir folders (with `cur`/`new` subdirectories) are considered.

## CLI reference

```
positional arguments:
  maildir_path                Root Maildir directory (default: ~/Maildir)

options:
  -e, --export FILE           Export dedup plan as JSON
  -a, --apply FILE            Apply plan via IMAP (deletes by default)
  --clean-hidden              Detect and clean hidden IMAP folders (\Noselect/\NonExistent)
  --prune-noselect            Delete empty \Noselect hierarchy nodes (no messages transitively)
  --clean-local               Remove local Maildir folders that no longer exist on the IMAP server
  -d, --dry-run               Preview without changes (for --apply, --clean-hidden, --prune-noselect, --clean-local)
  -p, --permanent             Permanently delete (EXPUNGE) instead of moving to Trash
  --delete-folders            Also delete empty hidden folders (requires --clean-hidden)
  --rescue-folder FOLDER      Destination for orphaned messages (default: Recovered)
  -H, --imap-host HOST        IMAP server hostname (required for --export, --clean-hidden, --prune-noselect)
  -T, --imap-trash FOLDER     Override Trash folder (auto-detected by default)
  -S, --sender IDENTITY       Sender identity (bag-of-words match against From header);
                              Sent folder priority only applies when From matches
  -i, --interactive           Interactively review duplicates folder-by-folder
  -s, --same-folder-only      Only deduplicate within each folder
  -f, --folders FOLDER ...    Only scan/apply these folders
  -x, --exclude-folders FOLDER ..  Exclude these folders (default: Drafts Junk Trash)
  -v, --verbose               Show details for every duplicate group / UID
  -q, --quiet                 Suppress per-group output
```

### Flag combinations

| Mode | Flags |
|------|-------|
| Scan (report only) | `~/Maildir` |
| Export plan | `~/Maildir --export plan.json --imap-host HOST` |
| Interactive export | `~/Maildir --interactive --export plan.json --imap-host HOST` |
| Verify plan (dry-run) | `--apply plan.json --dry-run` |
| Apply: move to Trash | `--apply plan.json` |
| Apply: permanent delete | `--apply plan.json --permanent` |
| Apply: custom Trash | `--apply plan.json --imap-trash "Deleted Messages"` |
| Apply: one folder only | `--apply plan.json --folders FOLDER` |
| Clean hidden (dry-run) | `--clean-hidden --imap-host HOST --dry-run` |
| Clean hidden | `--clean-hidden --imap-host HOST` |
| Clean hidden + delete folders | `--clean-hidden --imap-host HOST --delete-folders` |
| Clean hidden + custom rescue | `--clean-hidden --imap-host HOST --rescue-folder "Rescued"` |
| Prune noselect (dry-run) | `--prune-noselect --imap-host HOST --dry-run` |
| Prune noselect | `--prune-noselect --imap-host HOST` |
| Clean local (dry-run) | `--clean-local ~/Maildir --imap-host HOST --dry-run` |
| Clean local | `--clean-local ~/Maildir --imap-host HOST` |

Mutually exclusive: `--export` / `--apply` / `--clean-hidden` / `--prune-noselect` / `--clean-local`, and `--interactive` / `--quiet` / `--apply`. The `--permanent` flag requires `--apply`. The `--dry-run` flag works with `--apply`, `--clean-hidden`, `--prune-noselect`, and `--clean-local`. The `--delete-folders` flag requires `--clean-hidden`. The `--interactive` flag requires a TTY. Combining `--interactive` with `--export` lets you review duplicates before writing the plan.

## How duplicate detection works

1. **Message-ID** (primary): Emails are grouped by their normalized `Message-ID` header. This catches the vast majority of duplicates.
2. **Fingerprint** (fallback): For emails without a `Message-ID`, a SHA-256 hash of `Date + From + To + Cc + Subject + body` is used.

> **Note:** Fingerprint-only groups (messages without a Message-ID) are shown in scan reports but excluded from IMAP deletion plans, since there is no server-side identifier to verify before deletion.

### Keep/delete decision

When duplicates are found, the tool keeps the "best" copy based on a scoring system:

1. **Flag score** — Flagged (10), Replied (5), Seen (2), Passed (1)
2. **Folder priority** — Sent folders score highest, Trash/Junk lowest, deeper folder hierarchies preferred. With `--sender`, the Sent boost only applies when the From header matches the identity (bag-of-words, case-insensitive).
3. **Modification time** — oldest (original) copy wins
4. **Size** — larger file wins (may contain additional headers from forwarding)

## Safety features

- **Trash by default**: Duplicates are moved to the IMAP Trash folder, not permanently deleted
- **Message-ID verification**: Before deleting any IMAP message, its Message-ID is fetched and compared against the plan — catches UID reassignment or stale plans
- **Dry-run mode**: Use `--dry-run` to verify a plan without making changes
- **Stable IMAP UIDs**: New messages arriving after export get new UIDs not in the plan
- **Fingerprint-only groups skipped**: Groups without Message-ID are excluded from IMAP plans (no server-side verification possible)
- **`BODY.PEEK`**: IMAP header fetches don't set the `\Seen` flag
- **Batch operations**: One `SELECT` per folder for efficiency
- **Stale plan warning**: Plans older than 24 hours trigger a warning
