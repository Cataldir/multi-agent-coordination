# Coordination and reliability learner companion

Build a versioned message boundary, durable local effect step, receipt-aware Saga, liveness demonstration and integrated order workflow. This directory contains code, authored synthetic fixtures, tests and environment instructions. Exclusive course narration, slides and media are outside it.

This public source edition belongs to [Cataldir/multi-agent-coordination](https://github.com/Cataldir/multi-agent-coordination), repository ID 1413720092. It contains authored, manually reviewed learner source. No clean clone, installation, test or protocol execution has been observed. No code LICENSE or open-source reuse grant has been adopted. Third-party packages and documentation retain their own terms; none of their source is bundled here.

```powershell
git clone https://github.com/Cataldir/multi-agent-coordination.git
Set-Location multi-agent-coordination
```

Record the actual public commit before inspecting the code. The commands below describe the intended learner route; their presence does not establish that installation or execution succeeds.

## Environment acquisition and qualification

Core target: CPython3.13.14, standard-library sqlite3/asyncio/unittest. Obtain the exact interpreter through the learner's approved distribution, verify its provenance and SHA-256, then create a project-local `.venv`. No system or user-global installation is required by this course. The current authoring environment has not qualified interpreter execution and runs no commands below.

```powershell
# Use the approved exact interpreter path supplied by the environment owner.
& $approvedPython31314 -m venv .venv
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -c "import sys, sqlite3; print(sys.executable); print(sqlite3.sqlite_version)"
```

Expected conditions: interpreter version3.13.14, executable beneath this project's `.venv`, and captured SQLite version. The core is a source project: run from its root with no package installation or third-party dependencies. Protect and preserve databases containing failure evidence. All current test results are unobserved.

Optional protocol direct pins are `mcp==1.29.0`, `a2a-sdk==0.3.0`, `httpx==0.28.1`, `uvicorn==0.35.0`. MCP tag resolves to98b7159cb89274964055d2c016e3360a551280d0; A2A tag resolves to504148064d01b5cbd64b4cbe11a17d477acebc9e. A2A0.3 is an intentional legacy edition, not current1.x compatibility. `protocol-requirements.in` is not a fully resolved transitive lock. Before installation, the environment owner must produce the complete version/hash closure and verify these imports/APIs under the chosen platform. Installation remains held until that evidence and resource admission exist.

The qualified acquisition route is a reviewed complete hash-pinned requirements file, installed only through `.venv/Scripts/python.exe -m pip install --require-hashes -r <owner-qualified-lock>`. That lock is currently absent. Do not call the direct-pin file reproducible or use a global pip. No models, datasets, credentials or paid providers are needed.

## Execute core after admission

```powershell
$env:COURSE_EXERCISES='starters.exercises'
.\.venv\Scripts\python.exe -m unittest tests.test_starter_solution -v
$env:COURSE_EXERCISES='solutions.exercises'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m coordination.deadlock
.\.venv\Scripts\python.exe -m coordination.capstone --database learner-success.sqlite --run success
.\.venv\Scripts\python.exe -m coordination.capstone --database learner-fail.sqlite --run fail --fail-at provision
.\.venv\Scripts\python.exe -m coordination.capstone --database learner-unknown.sqlite --run unknown --unknown-at charge
```

Expected starter result: four explicit NotImplementedError errors. Expected complete reference result: every authored assertion passes. Success fixture expects completed with `[2,2400,2,1]`; known provision failure expects compensated with zero; unknown charge expects reconciling with `[2,0,0,0]`. The unknown fixture models no observed local charge; an actual provider may have committed and needs authoritative reconciliation. These are expected conditions, not captured outputs.

## Execute actual protocol exercises after separate qualification

```powershell
.\.venv\Scripts\python.exe -m protocol.mcp_lookup
# In one approved learner terminal, bind only loopback and stop with Ctrl+C.
.\.venv\Scripts\python.exe -m protocol.a2a_server
# In another terminal while that server is active:
.\.venv\Scripts\python.exe -m protocol.a2a_adapter
```

MCP must initialize and perform a real stdio tool call, returning BOOK/8/teaching through application validation. A2A must fetch the included Agent Card, exchange actual JSON-RPC messages/tasks and confirm task identity through get_task. The peer is deterministic and has no model. SDK in-memory task storage demonstrates protocol only and does not establish durable restart. Capture version, Card, identities, result and negative cases. Do not connect arbitrary URLs or add credentials to this smoke.

## Extend

Add a second independent reservation and declare its compensation order. Add a scripted outbox relay/consumer with duplicate delivery before sent marking. Implement an authoritative provider query with applied/definitively-absent/unknown outcomes, preserving the original key. Add a third ordered lock and a bounded cycle test. The extension guide contains hints and reference interpretations.

SQLite proves only a local transaction boundary under its tested assumptions. Delivery can repeat. Compensation can fail. Unknown outcomes remain visible. Correlation is not authority. No actual model, production provider, benchmark or deployment has been executed in this source edition.

## Provenance and release review

All order and incident fixtures are newly authored synthetic teaching data with no PII. The code is newly authored against inspected standard-library and SDK contracts, not copied from the paper simulation. Third-party SDK code remains in its upstream packages with their licenses. Proposed open-code licensing requires the owner's approval and reconciliation with any existing target license. Keep outputs and learner databases out of source control; preserve exact failed transcripts and hashes for review before any cleanup.

Claim-to-artifact: envelope/schema→envelope.py and EnvelopeTests; local transaction→store.py and crash tests; Saga→saga.py and failure/recovery tests; deadlock→deadlock.py and LivenessTests; incident→fixtures/incident32.json; actual protocols→protocol modules with separately required smoke evidence. None is marked tested until its real transcript is supplied.
