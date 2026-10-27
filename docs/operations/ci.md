# Credential-free CI

[The CI workflow](../../.github/workflows/ci.yml) runs on pushes, pull requests,
and manual workflow dispatch. Its Linux matrix uses Python 3.11 and 3.12.
Only requirements-dev.txt is installed; production environments remain separate.

Each job performs:

1. Ruff using the repository's existing E4/E7/E9/F rule scope.
2. In-memory Python compilation and bash -n for the five shell helpers/installers.
3. Preparation of all three complete function roots and deterministic ZIPs,
   creating the ignored .build/ directory needed by the configured test path.
4. Unit/contract and migration tests.
5. git diff --check.

Checkout and Python setup are pinned to reviewed commit SHAs. Permissions are
limited to contents: read, and checkout does not persist credentials.
The workflow has no GCP authentication, secret inputs, deployment steps, image
publication, or production pipeline execution.

## Test boundaries

- Tests block socket connections.
- The oil handler is executed with fake HTTP, Secret Manager and Pub/Sub
  boundaries and a canary key; successful requests retain their real argument
  contract without logging the key.
- Consumer tests run real fastavro encoding/decoding and the actual row mapper
  code. Beam IO and pipeline construction are fake; no runner or cloud clients
  are started.
- Shell helper tests use a temporary checkout, stub Python commands and stub
  kill calls. They do not touch the local PID record or real streaming processes.

These checks establish local contracts, syntax and packaging. They do not
establish deployed-source equality, cloud permissions, forecast accuracy, or
end-to-end production execution.

## Running the same checks locally

~~~bash
python -m pip install -r requirements-dev.txt
python -m ruff check .
python scripts/development/check_syntax.py
python scripts/deploy/package_functions.py all
python -m pytest -q --basetemp=.build/pytest
git diff --check
~~~

Bash must be available for the complete syntax/helper checks. On Windows the
checker uses Git for Windows Bash when installed. Python syntax alone can be
checked with check_syntax.py --python-only.

The workflow is active on the published review branch. The
[verified hosted run](https://github.com/IsaacOnKeys/Cocoa_Prices/actions/runs/40370821417)
passed all checks, including 55 tests on each Python version, on 2026-10-26.
An initial fresh-checkout failure was resolved by preparing function artifacts
before tests so their configured temporary-directory parent exists. See the
[reliability handoff](reliability-handoff-2026-10-26.md) for release verification.
