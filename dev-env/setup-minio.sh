#!/bin/sh

#!/bin/sh
set -eu

# Wait until alias works (depends_on:healthy should already handle this)
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Create bucket and copy seed data
mkdir -p /tmp/nextgems/
cp -r /seed/intake/work/bm1235/k202181/ngc4008a /tmp/nextgems/
mc mb -p local/test/data/obs || true
mc cp --recursive /seed/observations local/test/data/obs/ || true
mc cp --recursive /seed/model        local/test/data/ || true
mc cp --recursive /tmp/nextgems local/test/data/model/ || true
rm -r /tmp/nextgems
# Public-read policy for the observations prefix only
cat >/tmp/policy.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::test/data/obs/*"]
    },
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::test"],
      "Condition": {
        "StringLike": { "s3:prefix": ["data/obs/*"] }
      }
    }
  ]
}
JSON

mc anonymous set-json /tmp/policy.json local/test
mc anonymous list local/test
