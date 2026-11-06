# Development verification

`current-cohort.mjs` is a thin facade for the sealed `current-cohort/` library.
It runs an actual normally packed candidate in an outside-checkout consumer.
Its README and adjacent tests state the proof boundary. No helper here can
publish, change the source manifest/lock or enable application features.

Run full install/build/consumer verification on hosted CI first. The inert
directory, archive-data and graph controls are short local checks; they do not
stand in for installed public-package compatibility.

## Release candidate binding

`publish-candidate.py` is the unprivileged data binder for
`../.github/workflows/publish.yml`. It checks the source manifest and the saved
candidate's package/version, digest and source/run/attempt identity, then copies
the already-tested bytes to the sealed archive. It never builds, installs,
repacks, authenticates or publishes. Complete own-pack/installed-payload proof
remains the existing current-cohort inspector's responsibility; the binder is
not a general hostile-archive validator.

`publish_candidate_test.py` supplies adjacent finite-data controls.
`publish-route.node-test.mjs` parses the actual workflows and exercises selected
publisher/readback bodies with inert npm/curl effects. These controls do not
prove hosted OIDC, real registry publication or live trust configuration. The
normal full release-commit gates and actual tarball readback remain required.
Version 0.11.0 is a separate later pure release PR; the owner-held trusted-
publisher setup and release go are not supplied by any helper or test.
