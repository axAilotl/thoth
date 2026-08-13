# PostgreSQL reference

`postgres-reference.sql` is an operational reference, not the portable wire format. It keeps CCF headers, compartment custody, admission, producer batches, commit history, lineages, generation fences, and projections in one database and transaction boundary.

The schema intentionally contains more than three tables; the three-object rule concerns portable semantic kinds, not storage normalization.
