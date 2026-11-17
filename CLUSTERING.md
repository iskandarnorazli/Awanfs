# Clustering: several nodes, one namespace (#52)

**Status: approved by the owner on 2026-11-17 (decision 64).** Nothing here is built. Cluster
code starts after #26 (metadata segments) lands; until then AwanFS stays single node
(decision 5). Section 10 records the owner's answers.

This replaces `DISTRIBUTED_TOPOLOGY.md`, which came from awan-lang.

## 1. Goal

AwanFS keeps serving, and loses no acknowledged write, through a stated set of failures: one
node, one rack or zone, one disk, a network partition. The set is checked by a TLA+ model and
by failure tests before it is claimed (section 8). Throughput grows with the number of nodes
for workloads that spread over many keys.

Not goals: geo-replication across regions with independent writes, a node count past what one
control plane of 3 to 5 members manages (hundreds of nodes), and zero-pause failover.

## 2. Where AwanFS starts

- One node. Volumes are append-only logs of needles; the order of a key's records is their
  offset order (decisions 51, 56). Recovery rebuilds the index by replaying the logs, and the
  TLA+ model (`verification/tla/ObjectVersions.tla`) checks that an acknowledged write
  survives it (`AckedStateSurvivesRestart`).
- Durability modes (#29): `Fast`, `Group` (default), `Strict`.
- Not yet: the metadata segments that replace redb (#26, decision 59). Past 100,000 objects
  PUTs fall 6x to 8x today (#210); that cliff would repeat on every node, so #26 comes first.

## 3. How others do it

| System | Placement | Metadata | Consistency | Cost |
|---|---|---|---|---|
| MinIO, RustFS | object name hashed to a fixed erasure set; every object erasure-coded across it | none central: each drive keeps the object's metadata beside its shard | strong: write quorum of the set, distributed locks | every small object is N small files; growth adds a pool, existing data does not move |
| Ceph RGW | CRUSH over storage daemons | a monitor quorum (Paxos) holds the cluster map; bucket index in sharded objects | strong per object | operational weight; very large buckets make the index hot |
| SeaweedFS | a master (Raft) assigns volume file ids | a filer maps names to ids, on a pluggable database | strong per volume | the closest to AwanFS (packed volumes); the filer database is the weak point |
| Garage | consistent hashing over an operator-set layout | tables replicated 3 ways, merged as CRDTs | quorum reads and writes, no consensus per write | simple, several sites; last-writer-wins, weaker at heavy writes |
| OpenStack Swift | a ring of partitions to devices | account and container databases (SQLite) | eventual | listing large containers is slow |

`libs/awan-cluster` (awan-lang) is a vocabulary, not a design: it runs in one process, has no
membership agreement, leases, fencing or repair, and its claim that `W + R > N` alone gives
linearizability is wrong.

## 4. The design: replication is recovery

Each partition of the key space has one leader and two followers. The leader appends to its
log as a single node does today and streams the log to the followers; each follower replays it
with the recovery code. A follower's state is what a restart of the leader would build, so the
model and tests that check recovery check replication too.

```
              control plane: 3 or 5 members, Raft
              membership; per partition: leader, followers, epoch
                                  │  changes rarely; never on the request path
         ┌────────────────────────┼────────────────────────┐
     partition p7            partition p8              partition p9
  leader A (epoch 7) ──log──▶ followers B, C replay it (the recovery code)
  answers once the durability mode is met:
     Fast   = leader only
     Group  = leader and one follower synced     (default)
     Strict = every replica synced
  sealed volumes ──▶ erasure-coded later (#27)
```

### 4.1 Placement

- The key space (bucket id, key hash) is split into a fixed number of partitions, a few
  thousand, so moving one is cheap. A partition maps to three nodes in three failure domains
  (racks or zones).
- The partition map lives in the control plane. Changing it (a node joins, leaves or fails)
  moves whole partitions by shipping their sealed volumes and then the log tail.

### 4.2 Writes

- A client may send a request to any node; it is forwarded to the partition's leader. The
  leader takes the key lock, appends the record (data and metadata in one record, as today),
  and streams it to the followers in log order.
- Group commit stays: one sync covers every write that finished during the previous one, on the
  leader and on each follower.
- The answer waits for the durability mode, now counted in replicas.

### 4.3 Reads

- From the leader: always the latest acknowledged write.
- From a follower, optional per request: the follower asks the leader for its committed offset
  and waits until it has replayed that far (a read index), or serves a stale read where the
  client allows it.

### 4.4 Leaders, leases and epochs

- The control plane grants each partition leader a lease of a few seconds and an epoch number.
- Every log record carries its leader's epoch; a follower refuses records from an older epoch.
- A leader that cannot renew its lease stops accepting writes before the lease ends, and a new
  leader is chosen only after it has ended. Two leaders of one partition cannot both acknowledge
  writes.
- Failover promotes the follower with the longest synced log and increments the epoch.

### 4.5 Catch-up and repair

- A follower behind asks for the log from its last offset: the same replay as recovery.
- A node gone longer than a timeout has its partitions copied to other nodes until each has
  three copies again; sealed volumes are copied whole.
- Checksums on every read (as today) find a corrupt copy; it is fetched again from a replica.

### 4.6 Large objects (open decision)

Three-way replication writes 3 bytes for every byte; erasure coding as MinIO does (for example
12 data and 4 parity) writes about 1.33. On large-object ingest limited by the network that is
about 2x in MinIO's favour. Options:

| Option | For | Against |
|---|---|---|
| A. Replicate everything, erasure-code sealed volumes later | one write path; cold data costs 1.33x eventually | bulk ingest at 3x |
| B. Replicate small objects, erasure-code large ones when written (size threshold) | matches MinIO on ingest; small objects keep one-hop reads | two write paths; a large object's shards span K nodes |

Owner's choice (decision 64): B, with the threshold measured (section 8).

## 5. What happens when something fails

| Failure | Reads | Writes | Data |
|---|---|---|---|
| One node | continue from the other copies | continue after about one lease for the partitions it led | no acknowledged write lost |
| One zone (copies in three zones) | continue | continue after failover | none lost |
| Two of a partition's three copies | from the remaining copy if stale reads are allowed, else refused | refused (503) until a second copy is back | none lost if the last copy survives |
| Network partition | the majority side continues; minority leaders stop at lease end | majority side only | none lost; the minority refuses writes (consistency over availability, per partition) |
| A disk returns corrupt bytes | checksum catches it; read from a replica | unaffected | repaired from a replica |
| The control plane loses its majority | continue | continue until leases end, then pause | none lost |

No single point of failure sits above the nodes: any node accepts requests, the load balancer
only checks health, and the control plane has 3 or 5 members and is not on the request path.

Rolling upgrades: one node at a time after moving its leaderships; the log format is versioned
as volumes are (`FORMAT.md`).

## 6. Consistency

Per partition, writes are linearizable: one leader, ordered by the log, fenced by epochs. A
GET from the leader sees every acknowledged write. Listing a bucket merges the partitions'
sorted segments (#26) and is consistent per partition. A `500` still means the outcome is
unknown (decision 62).

## 7. What it would cost, and how it compares

Nothing here is measured: there is no cluster. These are expectations from how each design
works, to be replaced by section 8's benchmark.

| | AwanFS (this design) | Others |
|---|---|---|
| Small PUT, P50 on a LAN | leader write, one round trip, a shared follower sync: guess 0.3 to 2 ms | the same floor; Ceph and SeaweedFS make two dependent writes, MinIO many files on many drives |
| Small GET | one node, zero-copy (one node today: 2.05x to 2.35x MinIO on first GETs, run 43736773027) | erasure-coded designs read K shards |
| P99 | hides one slow follower (Group waits for one of two) | quorums hide one slow node too |
| Large PUT ingest | about 2x worse than erasure-coded designs under option A; even under option B | MinIO, RustFS best |
| Failover | about one lease of write pause per affected partition | none for leaderless or quorum designs |
| One hot key | its partition leader serialises it (the key lock) | leaderless designs spread it but resolve by last writer wins |

## 8. How it is shown to hold

1. **Model before code.** `ObjectVersions.tla` (or a sibling) gains log shipping, leases,
   epochs and failover. Invariants: one leader per partition and epoch; every acknowledged
   write present after any one failure. Each guard gets a configuration with it removed that
   TLC must reject, as today.
2. **Simulation (#213), extended to the network:** seeded runs that kill nodes, partition the
   network, delay messages and step clocks back, checked against the same invariants.
3. **Failure tests on real processes:** a job on three nodes that kills nodes, partitions the
   network and fills disks under load, and checks every acknowledged write afterwards, recording
   each pause.
4. **Benchmark against the others on the same machines:** AwanFS, MinIO, RustFS, Garage and
   SeaweedFS at 3 and 6 nodes; `warp` in distributed mode; small PUT and GET at 64 and 256
   concurrency, 256 MiB PUT; P50, P99, P99.9; and each with one node killed under load.
5. **The availability target: 99.999% of requests per month** (owner, decision 64). That is
   about 26 seconds of failed requests a month, counted per request. A failover pauses writes
   of the partitions the failed node led for about one lease, so the budget sets the lease and
   the failover detection: with 1,000 partitions on 10 nodes, one node failure stalls about a
   tenth of writes for one lease, and a 3-second lease uses about 0.3 s of the budget per
   failure (inferred, not measured). Planned work (upgrades, rebalancing) hands leadership over
   before a node stops, so it pauses nothing. The failure tests measure failover and rebuild
   against the target.

## 9. Order of work, after approval

| Step | Work | Needs |
|---|---|---|
| 0 | #26 metadata segments; #213 simulation; #208 | (single node) |
| 1 | The model of section 8.1 | #26 |
| 2 | The control plane: membership, partition map, leases (an existing Raft library, not our own) | 1 |
| 3 | Log shipping and follower replay; durability modes counted in replicas | 1, 2 |
| 4 | Forwarding, follower reads, failover | 3 |
| 5 | Repair and rebalancing | 4 |
| 6 | Large objects: erasure coding when written (option B), and #27 | 3 |
| 7 | The failure tests and the benchmark of section 8 | 4 |

Issues are filed per step when #26 is done; each carries a gate checked on its
pull request (`AGENTS.md`).

## 10. The owner's decisions (2026-11-17, decision 64)

1. The design is approved: log shipping with partition leaders, Raft only for membership, the
   partition map and leases. Work starts after #26.
2. Large objects: option B, erasure-coded when written above a measured threshold.
3. Availability target: 99.999% of requests per month (section 8.5).
4. Failure domains: racks in one site first; zones later.
