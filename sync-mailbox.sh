#!/bin/bash
set -Eeuo pipefail

# see also files ~/.offlineimaprc and ~/.netrc

# install dependencies:
# Linux: sudo apt-get install offlineimap
# macOS: brew install offlineimap certifi

# Detect OS and set CA certificate path
find_ca_cert() {
    for path in "$@"; do
        if [ -f "$path" ]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

case "$(uname -s)" in
    Darwin)
        # Apple Silicon (Homebrew), Intel (Homebrew), system fallback
        CA_CERTFILE=$(find_ca_cert \
            /opt/homebrew/etc/ca-certificates/cert.pem \
            /usr/local/etc/ca-certificates/cert.pem \
            /etc/ssl/cert.pem) || true ;;
    Linux)
        # Debian/Ubuntu, RHEL/CentOS
        CA_CERTFILE=$(find_ca_cert \
            /etc/ssl/certs/ca-certificates.crt \
            /etc/pki/tls/certs/ca-bundle.crt) || true ;;
    *)
        echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

if [ -z "$CA_CERTFILE" ]; then
    echo "CA certificate file not found" >&2
    exit 1
fi

# Update sslcacertfile in ~/.offlineimaprc if it doesn't match the OS
if ! grep -q "sslcacertfile = $CA_CERTFILE" ~/.offlineimaprc; then
    echo "Updating sslcacertfile in ~/.offlineimaprc to $CA_CERTFILE"
    case "$(uname -s)" in
        Darwin) sed -i '' "s|sslcacertfile = .*|sslcacertfile = $CA_CERTFILE|" ~/.offlineimaprc ;;
        *)      sed -i    "s|sslcacertfile = .*|sslcacertfile = $CA_CERTFILE|" ~/.offlineimaprc ;;
    esac
fi

get_home_dir() {
    local user="$1"
    case "$(uname -s)" in
        Darwin)
            dscl . -read "/Users/$user" NFSHomeDirectory 2>/dev/null | awk '{print $2}' ;;
        Linux)
            getent passwd "$user" 2>/dev/null | cut -d: -f6 ;;
        *)
            echo "" ;;
    esac
}

sync_mailbox() {
    local USER="$1"
    local USER_DIR="$2"

    if [ -z "$USER_DIR" ]; then
        echo "Could not determine home directory for user '$USER'" >&2
        exit 1
    fi

    cd "$USER_DIR" || { echo "User directory $USER_DIR not found"; exit 1; }

    echo "Syncing mailbox for user '$USER'..."
    FILENAME="$(basename -- "$0" .sh)"
    IMAP_LOGFILE="log_$FILENAME.log"
    rm -f "$IMAP_LOGFILE"
    if offlineimap > "$IMAP_LOGFILE" 2>&1; then
        echo "Mailbox sync for user '$USER' finished."
    else
        echo "Mailbox sync for user '$USER' failed. Log output:" >&2
        cat "$IMAP_LOGFILE" >&2
        exit 1
    fi
}

USERNAME="${USER:-$(id -un)}"
USER_DIR="${HOME:-$(get_home_dir "$USERNAME")}"
sync_mailbox "$USERNAME" "$USER_DIR"

exit 0
