#!/usr/bin/env bash
# Restore fake CORDEX tree from ../data/fake-cordex.tar.gz
# It unpacks shards into a temp dir, streams them to mkdir/touch,
# and removes the temp afterwards.

set -euo pipefail
set -x
DATA_DIR=$(readlink -f $(dirname $0)/../data)

ARCHIVE="$DATA_DIR/fake-cordex.tar.gz"
DEST="$DATA_DIR/cordex-tree"
JOBS=$(command -v nproc >/dev/null 2>&1 && nproc || echo 4)

if [[ ! -f "$ARCHIVE" ]]; then
  echo "Archive not found: $ARCHIVE" >&2
  exit 1
fi

tmpdir=$(mktemp -d)
# trap 'rm -rf "$tmpdir"' EXIT

echo ">>> Extracting shard manifests from $ARCHIVE"
tar -xzf "$ARCHIVE" -C "$tmpdir"

mkdir -p "$DEST"

echo ">>> Restoring directories"
for s in "$tmpdir"/dirs.*.zst; do
  [ -e "$s" ] || continue
  zstd -dc "$s" | xargs -0 -I{} mkdir -p -- "$DEST/{}" &
done
wait

echo ">>> Restoring files (parallel: $JOBS per shard)"
for s in "$tmpdir"/files.*.zst; do
  [ -e "$s" ] || continue
  zstd -dc "$s" | xargs -0 -P"$JOBS" -I{} touch -- "$DEST/{}" &
done
wait

echo ">>> Done. Restored tree under $DEST"
