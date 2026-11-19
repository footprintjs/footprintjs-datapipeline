# Current-cohort test driver

This folder is sealed behind `scripts/current-cohort.mjs`. The hosted driver
builds the package with its normal prepack command and installs the resulting
archive in a fresh consumer outside the checkout.

`run.mjs` owns the finite commands and retained evidence. `installed.mjs` reads
ordinary npm owner locations and `graph.mjs` checks the cohort and shared owners.
`archive_payload.py` checks the original pack bytes, the consumer lock and the
complete installed package payload. `consumer.ts` and its `prepared.ts` fixture
compile against installed public declarations and run as emitted ESM over the
actual installed libraries. The adjacent Node and Python controls use inert
path, version and manifest facts or temporary directory and archive data.

## Hosted use

After normal root `npm ci`, run the short controls FIRST in each Node22/24 job,
then keep the existing full check/example before the actual packed consumer:

```sh
node --test scripts/current-cohort/*.node-test.mjs
python3 -m unittest discover -s scripts/current-cohort -p '*_test.py' -v
npm run check
npm run example
npm pack --dry-run
node scripts/current-cohort.mjs "$RUNNER_TEMP/datapipeline-current-cohort-evidence"
```

`graph.node-test.mjs`, `installed.node-test.mjs` and
`installed-bin-owner.node-test.mjs` belong to Node's explicit
`--test` runner. Their names keep that runner distinct from the unchanged
Vitest suite; no Vitest exclusion or old-test selection is added.

The evidence path must be fresh and absolute. Upload only that exact evidence directory with `if: always()`; it contains candidate tar/hash metadata, command arguments/cwd/status/stdout/stderr, actual npm-ls output, the generated consumer manifest/lock, before/after physical owner facts, before/after archive/lock/payload receipts and RESULT. It never includes the install tree. Each command has a finite timeout and hard-refuses failure. A timed-out subprocess is reported as requiring the hosted VM's process-tree teardown; no descendant-cleanup claim is made. No local full/install/build run is proposed.

The facade runs the package's normal `npm pack --pack-destination`, including
its prepack build. One outside-checkout consumer declares that archive and the
exact FootPrint 14.0.0 and Foottrace 2.1.0 floors. The compiler and Node type
versions come from the checked-in source lock. npm uses that lock as the
starting point, resolves the local candidate and installs the consumer in strict
peer mode offline from the cache populated by root `npm ci`. Registry entries
in the resulting consumer lock must match the checked-in versions, URLs and
integrities. The candidate's own version comes from the source manifest.

The evidence root retains `candidate.tgz`, `candidate.json`, `cohort.json`,
before/after installed and archive facts and `RESULT.json`. The unchanged
release workflow preserves its Node 24 candidate only after this check passes.

Both root and candidate resolve FP/FT through their real installed package contexts. Package-owner directory identity is compared, so an ESM/CJS conditional entry-file difference is not a false duplicate. At ordinary direct/scoped node_modules package locations, actual regular package.json DATA supplies the package name/version; npm alias slots therefore count as owners. Missing/unreadable package identity refuses rather than proving absence. The bounded filesystem scan also finds owners absent from npm's lock graph, including copies of the same version. Hidden assets inside packages are scanned normally; only hidden package-store positions at node_modules boundaries are refused. An ordinary `.bin` directory directly below any node_modules is traversed, including ordinary and dot descendants. Only its immediate executable-file links may resolve to a regular file within the canonical consumer node_modules tree; directory, outside, prefix-sibling and deeper links refuse. A linked module-root or whole `.bin` container also refuses. No directory tree disappears merely because its name is `.bin`. Before traversal, direct `.bin` and scope containers also classify any own regular package.json through the same bounded manifest-identity rules as package slots; only ENOENT means a container has no own package. A native identity there counts as a physical owner, even at the same version. Malformed, linked, directory or otherwise unreadable own identities refuse. Normal manifest-less containers and immediate executable-file links retain their existing behavior. Link admission checks kind and containment; it does not execute a command or establish its behavior. Unknown symlink/store layouts refuse; this ordinary npm profile is not a claim about pnpm/workspaces or arbitrary module loaders. The hidden-asset control is a generic prior-rule false-positive class: root reports actual retained FP11 has no hidden archive members; no actual FP11 hosted hidden-asset failure is claimed.

After normal installation and after consumer execution, Python stdlib `tarfile` inspects the SAME originally hashed compressed bytes as DATA without extraction. The candidate root manifest/lock specification and leaf resolved file path must canonically identify the actual original tar; its SHA512 integrity must equal the tar's actual compressed bytes. Original parent-held tar SHA256 and generated-root-lock SHA256 must remain unchanged across both points. Every regular archive file is compared directly with the installed candidate bytes AND permission mode; complete file and parent-directory inventories must match, and explicit archive directory modes match too. Extra files/empty directories, duplicates, links, special members, missing/wrong identities, unsafe paths, integrity changes and byte/mode drift refuse. This is observed package consistency at the two points, not authenticity or an atomic concurrency guarantee.

Inspection limits: 64MiB compressed archive, 16MiB per member, 128MiB total payload, 20,000 archive members, 40,000 installed entries, 4,096-byte/64-component paths, 8MiB receipt; overflow is a failed check, never a truncated pass. Regular files with one link are the supported npm install profile. Receipts retain actual input hashes, compressed/payload bytes, member/file/directory counts and per-file hashes/modes. Installed public ESM runs supply runtime/type compatibility proof; neither archive nor inert metadata checks supplies that by itself.

## Public consumer coverage

The packed TypeScript consumer imports all four public DP doors and actual FP/FT types/values. Compilation has `skipLibCheck:false` and consumer-local Node types, so declaration failures remain real failures. It uses the existing basic example and window/prepared-store contract patterns:

- real finite collect→idempotent reconcile→derive→read/evidence, with a reopened filesystem repository reading the same published observation and a reopened trace store replaying the actual committed native collection/derivation receipts;
- a single explicitly driven real scheduler tick, actual SQLite summary publication, close/reopen and query (no timer or hand-authored summary ID);
- a caller-authored prepared chart with its native decision and control recorder,
  immutable native result, a matching filesystem trace read and Foottrace
  replay, plus terminal failure, pause and pre-aborted execution;
- the pure Influx encoder. No live Influx is contacted by this consumer.

This is an installed public-package smoke, not a replacement for the existing full suite. Exact failure/category/inner-stage ownership, cancellation during asynchronous work, all retained-history/expiry/crash/clock behavior, storage corruption/bounds, and live disposable Influx roundtrips remain in the unchanged owned suites and CI service. The filesystem smoke is a reopened ordinary published-record read; it does not enable or claim to migrate managed retention/history profiles. It does not prove live device/data adoption.

## Controls, mutations and status

Adjacent controls catch wrong current versions, candidate version drift, another resolved owner, nested lock owners and hidden physical owners even with unchanged version/clean lock data. Filesystem controls exercise legal hidden assets plus a nested alias, ordinary/scoped aliases, absent/unreadable/conflicting identity, unclassified symlinks and hidden store refusal. Archive controls use stdlib-generated inert DATA to distinguish exact positive joins from identical-tar/different-path, wrong integrity/root spec, changed held hashes, changed same-version payload/modes, extra members/directories, duplicates and links. Useful copied faults for root: skip hidden/nested traversal; use slot basename instead of manifest identity; drop physical cardinality; remove archive integrity or mode/inventory equality; neutralize the consumer's expected mean/fold/control assertions one at a time. Current controls should fail, originals restored before actual run. No test count, pass or mutation result is claimed by this author.
