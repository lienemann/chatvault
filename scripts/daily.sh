#!/data/data/com.termux/files/usr/bin/sh
# Daily pipeline: locate the latest encrypted backup, extract, sync contacts.
# Designed to be cron-able. Logs to $XDG_STATE_HOME (default ~/.local/state).
set -eu

LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/chatvault"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily.log"

log() {
    printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$LOG_FILE"
}

DB_DIR="${WA_DB_DIR:-/storage/emulated/0/Android/media/com.whatsapp/WhatsApp/Databases}"

# Pick the freshest .crypt15 (msgstore.db.crypt15 is the rolling latest;
# fall back to the dated rotation if it's missing).
if [ -f "$DB_DIR/msgstore.db.crypt15" ]; then
    BACKUP="$DB_DIR/msgstore.db.crypt15"
else
    BACKUP="$(ls -1t "$DB_DIR"/msgstore-*.db.crypt15 2>/dev/null | head -n1 || true)"
fi

if [ -z "${BACKUP:-}" ] || [ ! -f "$BACKUP" ]; then
    log "no backup file under $DB_DIR — aborting"
    exit 1
fi

log "backup: $BACKUP"

if ! command -v chatvault >/dev/null 2>&1; then
    log "chatvault not on PATH"
    exit 2
fi

if chatvault extract --backup "$BACKUP" >>"$LOG_FILE" 2>&1; then
    log "extract OK"
else
    log "extract FAILED ($?)"
    exit 3
fi

if command -v termux-contact-list >/dev/null 2>&1; then
    if chatvault contacts sync >>"$LOG_FILE" 2>&1; then
        log "contacts OK"
    else
        log "contacts FAILED"
    fi
else
    log "termux-contact-list not present, skipping contacts sync"
fi

# Optional: catch-up media snapshot (no-op if mirror daemon is also running).
if chatvault mirror snapshot >>"$LOG_FILE" 2>&1; then
    log "mirror snapshot OK"
else
    log "mirror snapshot FAILED"
fi

log "done"
