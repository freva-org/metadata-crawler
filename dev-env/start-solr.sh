#!/usr/bin/env bash
set -euo pipefail

# freva-solr currently gives its first Solr startup only 20 seconds.  On a
# cold Docker host that can be too short, even though Solr becomes healthy a
# few seconds later.  Keep this compatibility patch at the Compose boundary
# until the image exposes the timeout itself.
startup_timeout="${API_SOLR_STARTUP_TIMEOUT:-90}"
patched_start_service="$(mktemp)"
trap 'rm -f "$patched_start_service"' EXIT

sed "s/timeout 20 bash -c/timeout ${startup_timeout} bash -c/" \
  /usr/local/bin/start-service > "$patched_start_service"
exec /usr/bin/env bash "$patched_start_service"
