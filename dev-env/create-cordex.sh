#!/usr/bin/env bash
# Restore fake CORDEX tree from ../data/cordex-paths.txt.gz
# Uses a single AWK pass to split dirs/files, then parallel mkdir/truncate.

set -euo pipefail
#set -x

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$(readlink -f "$SCRIPT_DIR/../data")"

ARCHIVE="$DATA_DIR/cordex-paths.txt.gz"
DEST="$DATA_DIR/cordex-tree"
JOBS=$(command -v nproc >/dev/null 2>&1 && nproc || echo 4)

if [[ ! -f "$ARCHIVE" ]]; then
  echo "Archive not found: $ARCHIVE" >&2
  exit 1
fi

mkdir -p "$DEST"

# Temporary files to hold dirs and files
TMPDIR="$(mktemp -d "$DEST/.build.XXXXXX")"
DIRS_FILE="$TMPDIR/dirs.txt"
FILES_FILE="$TMPDIR/files.txt"

echo ">>> Splitting paths into dirs/files (single pass)..."
gzip -cd "$ARCHIVE" | awk -v dfile="$DIRS_FILE" -v ffile="$FILES_FILE" '
  NF == 0 { next }

  # Directory entry (ends with /)
  /\/$/ {
    p = $0
    sub(/\/$/, "", p)        # strip trailing slash
    if (p != "") {
      print p > dfile
    }
    next
  }

  # File entry: record dirname as dir, and full path as file
  {
    path = $0
    dir = path
    # remove last /component to get dirname
    sub(/\/[^\/]+$/, "", dir)
    if (dir != "" ) {
      print dir > dfile
    }
    print path > ffile
  }
'

echo ">>> Creating directories in parallel (JOBS=$JOBS)..."
if [[ -s "$DIRS_FILE" ]]; then
  sort -u "$DIRS_FILE" | sed "s|^|$DEST/|" | \
    xargs -P "$JOBS" -n 200 mkdir -p
fi

echo ">>> Creating empty files in parallel (JOBS=$JOBS)..."
if [[ -s "$FILES_FILE" ]]; then
  sed "s|^|$DEST/|" "$FILES_FILE" | \
    xargs -P "$JOBS" -n 200 truncate -s 0
fi

rm -rf "$TMPDIR"

echo ">>> Done. Restored tree under $DEST"
