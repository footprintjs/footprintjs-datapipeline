# Hosted checks

`ci.yml` owns the existing Node 22/24 full check, example and disposable Influx
service, plus the Node 22.13 minimum-runtime SQLite check. Preserve those gates.

`current-cohort-lock.yml` is a temporary branch-only input producer. It changes
only the two intended dependency ranges in a checked-out manifest and asks npm
11.19 for a normal strict public-registry lock, with separate empty npm configs
and lifecycle scripts disabled. The output manifest/lock are retained verbatim
for the source author. A declared singleton is not an installed singleton, and
lock generation is not build, test, package compatibility or publication proof.

Remove this temporary producer when its actual outputs are committed; the final
candidate must pass ordinary locked installation and all current acceptance
gates. No credentials, package publication, source push or release dispatch is
performed by this workflow. The publisher and release version remain separate
review decisions.
