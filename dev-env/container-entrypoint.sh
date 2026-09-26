#!/usr/bin/env bash
# Entrypoint of the metadata-crawler test image.
#
# The repository is mounted read-only at $MDC_SRC. The files git knows
# about are copied into a private work tree, so nothing the tests, tox or
# maturin write ends up in the host checkout: no foreign-owned files, no
# clash with the host's .tox, and no compiled extension for the wrong libc
# in src/. Caches (tox envs, pip, cargo) live on the /cache volume and
# survive between runs.
set -euo pipefail

src=${MDC_SRC:-/src}
work=${MDC_WORK:-/work}
cache=${MDC_CACHE:-/cache}
py=${PYTHON_VERSION:?PYTHON_VERSION is not set}

if [[ ! -f "${src}/pyproject.toml" ]]; then
    echo "Mount the repository at ${src}, e.g. -v \"\$PWD:${src}:ro\"" >&2
    exit 64
fi

mkdir -p "${work}" "${cache}/cargo" "${cache}/target-py${py}"

# Copy what git knows about: tracked files plus new files that aren't
# ignored. That skips caches, build output and local files such as keys.
# Files the container user can't read are skipped with a warning.
git_src=(git -c safe.directory='*' -C "${src}")
if "${git_src[@]}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    "${git_src[@]}" ls-files -z --cached --others --exclude-standard \
        | while IFS= read -r -d '' file; do
              # Skip tracked files that were deleted in the checkout.
              if [[ -e "${src}/${file}" || -L "${src}/${file}" ]]; then
                  printf '%s\0' "${file}"
              fi
          done \
        | tar -C "${src}" --exclude='*.so' --ignore-failed-read \
              --null --files-from=- -cf - \
        | tar -C "${work}" -xf -
else
    echo "No git checkout at ${src}, copying the whole tree." >&2
    tar -C "${src}" \
        --exclude=./.git \
        --exclude=./.tox \
        --exclude=./target \
        --exclude='./.*_cache' \
        --exclude=./coverage_report \
        --exclude='*.so' \
        --ignore-failed-read \
        -cf - . | tar -C "${work}" -xf -
fi

# Keep cargo's output per Python version on the cache volume. A symlink is
# used instead of CARGO_TARGET_DIR because tox does not pass that variable
# on to the package build.
ln -sfn "${cache}/target-py${py}" "${work}/target"

cd "${work}"
status=0
"$@" || status=$?

# Optionally hand test reports back to the host.
if [[ -d /reports && -w /reports ]]; then
    for artifact in report.xml coverage.xml coverage_report; do
        if [[ -e "${artifact}" ]]; then
            cp -r "${artifact}" /reports/
        fi
    done
fi
exit "${status}"
