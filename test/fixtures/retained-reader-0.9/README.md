# Retained reader 0.9 witness

These two test-only files are byte-identical copies from commit
`b550436f6f84ed99d498ea99b99dc638cc0de0df` in footprintjs-datapipeline.
They are not compiled into the package or exported. The integration test checks
the hashes before using the actual old constructor against a v2 namespace.

| File | Original path | SHA-256 |
| --- | --- | --- |
| ledger.ts | src/storage/retainedRepository/ledger.ts | 20ecb724cdee597d936bffd5f7e64d2594ade90bf467b2bc242c3979767b5fac |
| contracts.ts | src/storage/retainedRepository/contracts.ts | 544b8c464912dc0b953fb7a0230d0445ed135c4e231c15cfff828ba4a01105e9 |

This witnesses rejection at construction, not fencing a previously opened
reader. The plain FilesystemRepository is not a supported reader for v2.
