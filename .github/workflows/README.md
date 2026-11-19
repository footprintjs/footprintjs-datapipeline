# Hosted checks

`ci.yml` owns the existing Node 22/24 full check, example and disposable Influx
service, plus the Node 22.13 minimum-runtime SQLite check. Preserve those gates.
The shared file-size job is pinned to a reviewed checker commit. Its empty
baseline permits no oversized-source exception or exclusion. Both runtime jobs
finish independently so a failure on one Node profile does not cancel the other.

The source lock pins the development cohort to FootPrint 14.0.0 and Foottrace
2.1.0. The packed consumer verifies shared installed package ownership and
compiler compatibility through the public entry points.

The final candidate must pass ordinary locked installation and all current
acceptance gates. Ordinary push/PR checks do not publish, push source or dispatch
a release. The separate release route below uses the same current gates; a
previous source run cannot replace its release-commit proof.

Each Node 22/24 job runs the short adjacent Node/Python inspector controls
before the unchanged full check, so an unrelated full-suite failure cannot keep
them unreached. The two Node-owned files use `.node-test.mjs` names and the
explicit Node test command; Vitest's configuration and original tests stay
unchanged. After the existing full check/example/dry pack, the real packed
current-cohort consumer retains bounded evidence even after failure. The
original suite and disposable Influx service remain; the packed consumer itself
uses only finite local data and the pure encoder.

The consumer starts from the checked-in lock and installs the actual packed
candidate outside the checkout with npm offline. It verifies the dependency
resolutions against the source lock, the physical package owners and the archive
payload before compiling and running the public consumer. The existing root
candidate artifact paths and both Node release gates are preserved.

The history-measurement test permanently records the child error, signal,
elapsed time and bounded streams before asserting success. Its 80-second child
deadline, 90-second test deadline, workload and storage assertions are unchanged.
The initial null-status failure remains unclassified; later full-matrix and
isolated passes do not prove its cause. The temporary branch diagnostic workflow
was removed after its one retained successful run; the full Node22/24 matrix
continues to exercise the unchanged measurement contract.

## Stable release route

The sole release entrypoint is `.github/workflows/publish.yml`, triggered by a
published GitHub release. Keep that exact filename: npm's owner-configured
trusted publisher binds to it. Version 0.11.0 used a separate pure release PR
with its CHANGELOG and normal lock update. The breaking 0.12 migration includes
its version and lock update in the source PR. Release go still belongs to the
owner.

The route reruns the complete local `ci.yml` at the release event commit,
including both full Node22/24 profiles, minimum-runtime, file-size, examples,
Influx service and packed-consumer checks. Its fixed Node24 producer supplies
the exact archive tested by that consumer. An unprivileged binder checks source,
release tag, run, attempt, manifest and archive identity, then passes one sealed
archive by its artifact ID and digest. The publisher does not repack it.

Only the isolated artifact-only publisher has `id-token: write`. It performs no
source checkout, dependency install or candidate execution and has no token
fallback. The route omits `--provenance` and uses the configured npm trusted
publisher directly. A read-only job downloads public
registry metadata and the actual tarball and requires the expected identity and
bytes. Only HTTP 404 waits, every 30 seconds within one 15-minute budget; other
errors or mismatched bytes stop immediately. A missing readback at the deadline
fails; observing an uncertain publish does not turn a failed publish into success
or authorize a retry.

The [0.11.0 release run](https://github.com/footprintjs/footprintjs-datapipeline/actions/runs/42330317022)
completed this route successfully. Before every later release, the owner must
verify the npm trusted publisher still names this exact workflow and explicitly
give release go for the reviewed version and source. Green checks, source
acceptance and this documentation grant neither a merge nor publication.
Npm binds the repository and workflow filename, so anyone with repository write access could publish from a branch copy.

There is no dispatch or dry publication path. A failure before publish leaves
tags and releases where they are, and its fix goes through an ordinary follow-up
PR, never a publish retry.
