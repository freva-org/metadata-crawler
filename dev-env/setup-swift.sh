#!/bin/sh
set -euo pipefail

AUTH_URL="${SWIFT_AUTH_URL:-http://swift:8080/auth/v1.0}"
USER="${SWIFT_USER:-test:tester}"
KEY="${SWIFT_KEY:-testing}"

# 1) Get token + storage URL (TempAuth)
headers="$(curl -si -H "X-Auth-User: $USER" -H "X-Auth-Key: $KEY" "$AUTH_URL")"
token="$(printf '%s' "$headers" | tr -d '\r' | awk -F': ' '/^X-Auth-Token:/ {print $2}')"
storage_url="$(printf '%s' "$headers" | tr -d '\r' | awk -F': ' '/^X-Storage-Url:/ {print $2}')"

#
if [ -z "${token:-}" ] || [ -z "${storage_url:-}" ]; then
  echo "Failed to authenticate against Swift at $AUTH_URL" >&2
  exit 1
fi

# Helper to upload a directory tree into container/prefix
upload_dir() {
  local src_dir="$1" container="$2" prefix="$3"
  [ -d "$src_dir" ] || return 0
  # create container if missing
  curl -sf -X PUT -H "X-Auth-Token: $token" "$storage_url/$container" >/dev/null
  # upload files
  find "$src_dir" -type f -print0 | while IFS= read -r -d '' f; do
    rel="${f#$src_dir/}"
    url="$storage_url/$container/$prefix$rel"
    curl -sf -X PUT -H "X-Auth-Token: $token" --data-binary @"$f" "$url" >/dev/null
  done
}

# 2) Create containers
# private container for model etc.
curl -sf -X PUT -H "X-Auth-Token: $token" "$storage_url/test" >/dev/null
# public-read container for observations
curl -sf -X PUT -H "X-Auth-Token: $token" \
     -H "X-Container-Read: .r:*,.rlistings" \
     "$storage_url/test-obs" >/dev/null
# public-read container for observations
curl -sf -X PUT -H "X-Auth-Token: $token" \
     -H "X-Container-Read: .r:*" \
     "$storage_url/forbidden" >/dev/null

# 3) Upload content
cp -r /seed/model /tmp/
mkdir -p /tmp/model/nextgems
cp -r /seed/intake/work/bm1235/k202181/ngc4008a /tmp/model/nextgems/
upload_dir /tmp/model        test        "model/"
upload_dir /seed/observations test-obs   "observations/"
upload_dir /seed/observations forbidden   "observations/"
# 4) Quick info
echo "Swift storage: $storage_url"
echo "Public container set: test-obs (anonymous read + list)"
