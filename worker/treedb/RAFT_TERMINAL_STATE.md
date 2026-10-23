# Terminal transaction authority and intrinsic controls

The externally recovered consumer uses application metadata `dgraph-normal-delta-v6` and pending
manifest version 2. These pre-alpha formats intentionally reject older certificates. The exact
TreeDB root binds pending records, terminal transaction rows, namespace bans and fixed manifest
accounting together; neither a separate log nor a process-wide terminal map is an authority.

## Transaction decisions and retries

A terminal row is a private raw DATA key ending in big-endian StartTs and an eight-byte CommitTs:
zero means abort, otherwise CommitTs must exceed StartTs. The manifest incrementally counts terminal
rows as well as pending count/charge. Checkpoints read that fixed reference without scanning
history. Recovery and native installation validate all terminal rows/counts with one streaming pass
before pending Oracle restoration and read health. Individual lookups use maintained DB.Get.

An ordered Delta validates its ENTIRE decision batch before any posting effects. Duplicate identical
decisions are coalesced. A contradiction within the batch or against a retained terminal row is a
pure typed semantic rejection: every pending transaction and checksum/frontier remains unchanged. A
repeated terminal decision is a no-op; unrelated unseen decisions in an older watermark still apply.
Decisions at or below explicit semantic floor S are retired and ignored, rather than recreating
rows.

The normal posting writer drains first. Then bounded batches of at most 256 terminal rows remove
corresponding pending records and update manifest accounting under the same lifecycle/owner Apply.
No next semantic entry, Oracle/checksum advancement, certificate or retention cut can interleave.
Any attempted raw publication failure seals the adapter and the apply path fails; startup selects
the previous exact certificate and retained WAL. Partial success is never reported as rejection. A
single 65536-row batch originally exhausted COW batch allocation; bounded publication fixes that
case without additional working flushes, extra certificates or capacity-error retry guesses.

Mutation/admin proposal keys retain exact content/StartTs binding through S. After resolution, a
byte-identical previously successful prewrite with its original key can return cached success
without publishing anything. A new-key prewrite of that transaction is rejected. A changed request
with a retained key is rejected. Expired mutation/admin StartTs is rejected before key lookup, even
for an old successful retry. Pending and terminal rows are mutually exclusive.

## Intrinsic controls

Isolated State, Delta, native Snapshot and PostingFloor requests bypass generic proposal-key
records. State already uses its membership counter; Delta uses terminal rows and ordered checksum
authority; native Snapshot effects/retention are certificate-owned; PostingFloor is an explicit
monotone ordered transition. Transport keys for these kinds have no eternal arbitrary collision
contract with mutation keys. Mixed kinds do not receive this exemption. An old floor below the
current S returns a safe regression error; an equal eligible floor is a no-op. There is no automatic
clock-based expiration.

S retirement deletes terminal rows in batches of at most 256 with bounded iterator/key memory, then
publishes semantic S and retires eligible mutation/admin signatures. A retirement error seals
admission, including errors after earlier batches succeeded. It cannot certify partial deletion with
old S. The preceding exact certificate and every required WAL entry remain the recovery source.

## Checksum source authority

Zero samples its complete checksum map and persisted MembershipState.Counter under the SAME RLock
immediately before Oracle.Send. OracleDelta.checksum_sequence is an additive protobuf field. Tablet
membership changes can emit a new checksum with unchanged MaxAssigned; timestamp assignment is not a
checksum sequence. Stream batching keeps the newest sequence and its exact paired map while
retaining all decisions and maximum assignment. Equal nonzero sequence with different maps fails.

The consumer stores ordered checksum sequence, this group's checksum and SHA-256 identity of that
single canonical checksum map in its certificate. This is a fixed-size source-message identity, not
a rolling aggregate or a pending overlay digest. A newer sequence can change checksum at the same
assignment watermark. Older sequence cannot regress it, even after S retirement, but its unseen
transaction decisions are still processed. Equal sequence/map is idempotent; equal sequence with a
changed map rejects the entire Delta before effects. Certificate construction never samples the
process-local Oracle or its unordered checksum. Abort-only MaxAssigned=0 stays zero.

External recovery refuses checksum maps lacking a source sequence. Upgrade Zero before enabling this
consumer; an old Zero is not a supported mixed-version source for external recovery. Legacy
Badger/ordinary TreeDB consumers keep their previous unsequenced apply behavior. Protobuf generation
uses the maintained protoc/protoc-gen-go versions; additive descriptor bytes rewrap the generated
rawDesc suffix, so its textual diff is larger than the added field.

## Resource contract and evidence limits

Pending state remains capped at 4096 rows/64 MiB logical charge. New prewrites pause when retained
terminal plus pending count reaches 65536; existing pending growth remains subject to its canonical
pending byte limit. Pending-to-terminal decisions have reserved soft-limit headroom. ALL decisions,
including previously unseen global ones, and explicit S requests bypass new-mutation admission.
Consequently terminal count can exceed 65536. This is required to retain correct outcomes, not a
hard whole-log, heap or disk cap. At S=0 terminal rows, mutation/admin signatures and posting
history grow. Certificate JSON remains limited to 1 MiB; overflow retains the preceding cut/WAL and
mutation debt admission eventually pauses. Explicit authenticated operator S retirement is required
for sustained lifetime progress; no fabricated timestamp or local clock makes it automatic.

Focused public-path controls cover whole-batch contradictions, exact/new-key retries, different
checkpoint schedules with replay, old checksum after newer checksum+S, equal-assignment updates,
unseen old aborts, abort-only zero watermark, native transfer/reopen, full pending capacity, real
65536-row terminal history admission/decision/S progress, and partial publication/retirement errors
sealing then recovering the prior cut. Race controls include concurrent real Zero checksum sampling.
These are correctness evidence, not actual-server performance, power-loss qualification or a claim
of bounded S=0 equilibrium. Final exact-source multiprocess/ACK crash qualification remains
separate.
