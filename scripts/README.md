# Development verification

`current-cohort.mjs` is a thin facade for the sealed `current-cohort/` library.
It runs an actual normally packed candidate in an outside-checkout consumer.
Its README and adjacent tests state the proof boundary. No helper here can
publish, change the source manifest/lock or enable application features.

Run full install/build/consumer verification on hosted CI first. The inert
directory, archive-data and graph controls are short local checks; they do not
stand in for installed public-package compatibility.
