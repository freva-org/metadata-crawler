# Development tasks for metadata-crawler.
#
# Container tooling can be swapped via environment variables, e.g. for podman:
#   COMPOSE="podman compose" CONTAINER_ENGINE=podman just up

set shell := ["bash", "-euo", "pipefail", "-c"]

compose := env("COMPOSE", "docker compose") + " -f docker-compose.yaml"
engine := env("CONTAINER_ENGINE", "docker")
wait_timeout := env("MDC_WAIT_TIMEOUT", "180")

# Test container: image name and default Python version
test_image := "mdc-test"
python := env("MDC_PYTHON", "3.14")
python_versions := "3.11 3.12 3.13 3.14"

# One-shot containers that seed the services and must exit successfully.
setup_services := "postgres-init setup-minio setup-swift"

# List all recipes
default:
    @just --list

# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

# Start all services and wait until they are ready
up:
    {{ compose }} up -d --remove-orphans
    @just wait

# Wait for services to accept connections and for the seed jobs to finish
wait:
    #!/usr/bin/env bash
    set -euo pipefail
    deadline=$((SECONDS + {{ wait_timeout }}))

    wait_for() {
        local name=$1; shift
        until "$@" >/dev/null 2>&1; do
            if (( SECONDS >= deadline )); then
                echo "Timed out waiting for ${name}" >&2
                exit 1
            fi
            sleep 1
        done
        echo "✓ ${name}"
    }

    tcp_open() { (exec 3<>"/dev/tcp/localhost/$1") 2>/dev/null; }

    wait_for "solr" curl -fsS http://localhost:8983/solr/admin/info/system
    wait_for "mongodb" tcp_open 27017
    wait_for "postgres" tcp_open 5432
    wait_for "minio" curl -fsS http://localhost:9000/minio/health/ready
    wait_for "swift" curl -fsS http://localhost:8081/info

# Stop all services, keeping their data
down:
    {{ compose }} down --remove-orphans

# Stop all services and delete all their data (volumes included)
nuke:
    {{ compose }} down -v --remove-orphans

# Wipe all service data and start again with freshly seeded services
reset: nuke up

# Show the status of all services
ps:
    {{ compose }} ps -a

# Follow the logs of all or selected services, e.g. `just logs mongodb`
logs *services:
    {{ compose }} logs -f {{ services }}

# ---------------------------------------------------------------------------
# Tests and checks
# ---------------------------------------------------------------------------

# Run the full test suite with coverage, as in CI
test: up
    tox -e test

# Run pytest in the tox test env without coverage, e.g. `just pytest -k remove`
pytest *args: up
    tox exec -e test -- pytest {{ args }}

# Type checking
types:
    tox -e types

# Linting (isort, flake8, codespell, pydocstyle, bandit, cargo fmt)
lint:
    tox -e lint

# Build the docs
docs:
    tox -e docs

# Run types, lint and docs in parallel, as in CI
check:
    tox run-parallel -e types,lint,docs --parallel-no-spinner

# Format the code the way the lint env checks it
fmt:
    tox exec -e lint -- python3 -m isort --profile black -t py313 -l 79 src
    tox exec -e lint -- python3 -m black src tests
    cargo fmt -p metadata_crawler

# Full CI run from a clean slate: fresh services, all checks, all tests
ci: nuke up check test

# ---------------------------------------------------------------------------
# Tests in a container with a specific Python version
# ---------------------------------------------------------------------------

# Build the test image for a Python version, e.g. `just image 3.12`
image version=python:
    {{ engine }} build -f dev-env/Containerfile \
        --build-arg PYTHON_VERSION={{ version }} \
        -t {{ test_image }}:py{{ version }} dev-env

# Run a command in the test container, e.g. `just in-container 3.12 tox -e types`
in-container version=python *cmd="tox -e test": (image version)
    {{ engine }} run --rm $([ -t 1 ] && echo -t) --network host \
        -v "$PWD:/src:ro,z" -v mdc-cache:/cache \
        {{ test_image }}:py{{ version }} {{ cmd }}

# Run the test suite with a given Python, e.g. `just test-py 3.12`
test-py version=python: up
    @just in-container {{ version }} tox run -e test -x testenv:test.package=wheel

# Run the test suite with every supported Python version
test-matrix: up
    #!/usr/bin/env bash
    set -uo pipefail
    failed=()
    for version in {{ python_versions }}; do
        echo "=== Python ${version} ==="
        just in-container "${version}" tox run -e test -x testenv:test.package=wheel \
            || failed+=("${version}")
    done
    if (( ${#failed[@]} )); then
        echo "Failed: ${failed[*]}" >&2
        exit 1
    fi
    echo "All Python versions passed."

# Drop the cached tox envs, pip and cargo builds of the test containers
clean-container-cache:
    {{ engine }} volume rm -f mdc-cache

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

# Remove test and coverage artifacts
clean:
    rm -rf coverage_report report.xml coverage.xml .coverage .pytest_cache .mypy_cache

# Remove artifacts and all tox environments
clean-all: clean
    rm -rf .tox
