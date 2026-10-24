# Real gRPC schema diagnostic

This companion and `../verify_schema.py` exercise a small, fixed schema sequence against an
explicitly selected Dgraph binary. They provide process correctness evidence only. They do not
qualify throughput, power loss, large rebuild resource budgets, non-root namespaces or the complete
production capability tier.

Build and run only when the coordinator releases the build/server grant:

```sh
source /mnt/fast4tb/gomap-raft-production-20261023T0744Z/env.sh
go build -o /mnt/fast4tb/gomap-raft-production-20261023T0744Z/artifacts/schema-proof ./worker/treedb/raftproduction/schemaproof
python3 worker/treedb/raftproduction/verify_schema.py \
  --dgraph-bin /absolute/path/to/dgraph \
  --proof-bin /mnt/fast4tb/gomap-raft-production-20261023T0744Z/artifacts/schema-proof \
  --policy raft_checkpoint \
  --out /mnt/fast4tb/gomap-raft-production-20261023T0744Z/artifacts/schema-grpc-01
```

`--policy badger` selects ordinary Badger with filesystem survival and synchronous writes, using the
same recipes. Use a different new output directory. Each run records both binaries' SHA-256 and
`go version -m`, source heads/dirty diff hashes, commands, gRPC transcripts, resolved runtime
configuration and posting route at each startup. A source manifest describes the inspected tree; it
cannot prove an arbitrary supplied binary was built from that tree. Preserve a matching build
manifest when promoting evidence. Failed campaigns remain failed artifacts; reruns use new
directories.

The phases cover:

- Enabling count, adding/removing/re-adding reverse while count stays enabled, and
  removing/re-adding count while reverse stays enabled. Root count functions verify both forward and
  reverse membership. A raw gRPC request at the seed's exact commit timestamp verifies old UID
  posting contents after a later delete.
- A refused multi-predicate conversion that must preserve the earlier predicate's count/reverse
  indexes. Successful scalar-to-scalar, scalar-to-list and list-to-list conversions rewrite `"01"`
  and `"1"` into one integer member; unchanged count must become one. Target-typed SET cannot
  duplicate it, and target-typed DELETE must remove it.
- Refusing Unique over existing duplicates, enabling it after fixing the data, same-owner updates
  and committed duplicate refusal. Two pending transactions carry the same index conflict key. Alpha
  closes and reopens while Zero remains alive; the first decision commits and Zero must abort the
  conflicting second decision. Restarting Zero at this point would instead test its old-start abort
  rule, so the launcher deliberately does not do that.

Four successful public ACKs cause immediate adjacent SIGKILL syscalls for Alpha and Zero: reverse
enable, conversion, target-typed delete, and a final Unique CommitNow. The companion emits a small
`ack_cut` JSON line as its first output; the launcher reads it and kills both processes before
artifact writes, status polls or validation reads. It then saves the transcript and restarts the
pair. The response-to-kill interval includes local pipe delivery/JSON decoding and is recorded
rather than described as an atomic crash boundary. An orderly Alpha-only restart additionally
requires an actual two-pending certificate log for the Raft-owned policy before decisions run.

There is no deterministic public RPC pause between frontend schema validation and ordered Raft
application. This driver does not claim to reproduce that delayed-admission race; the source-bound
focused test covers its ordered pre-effect guard. Small fixtures do not force cold splits.
Historical gRPC queries verify posting contents, not a historical schema definition. There are no
automatic retries or measured phases.
