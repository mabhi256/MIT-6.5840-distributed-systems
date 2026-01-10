# 6.5840 Labs

## Lab 1: MapReduce

<https://pdos.csail.mit.edu/6.824/labs/lab-mr.html>

```bash
❯ cd src/main/
❯ bash test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

## Lab 2: Key/Value Server

<https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html>

```bash
❯ cd src/kvsrv1
❯ go test -v
=== RUN   TestReliablePut
One client and reliable Put (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     5 #Ops    0
--- PASS: TestReliablePut (0.00s)
=== RUN   TestPutConcurrentReliable
Test: many clients racing to put values to the same key (reliable network)...
  ... Passed --  time  3.1s #peers 1 #RPCs 65325 #Ops 65325
--- PASS: TestPutConcurrentReliable (3.06s)
=== RUN   TestMemPutManyClientsReliable
Test: memory use many put clients (reliable network)...
  ... Passed --  time  9.4s #peers 1 #RPCs 100000 #Ops    0
--- PASS: TestMemPutManyClientsReliable (17.34s)
=== RUN   TestUnreliableNet
One client (unreliable network)...
  ... Passed --  time  6.6s #peers 1 #RPCs   282 #Ops  218
--- PASS: TestUnreliableNet (6.62s)
PASS
ok      6.5840/kvsrv1   27.036s

❯ cd lock/
❯ go test -v
=== RUN   TestOneClientReliable
Test: 1 lock clients (reliable network)...
  ... Passed --  time  2.0s #peers 1 #RPCs   407 #Ops    0
--- PASS: TestOneClientReliable (2.02s)
=== RUN   TestManyClientsReliable
Test: 10 lock clients (reliable network)...
  ... Passed --  time  2.2s #peers 1 #RPCs 123745 #Ops    0
--- PASS: TestManyClientsReliable (2.20s)
=== RUN   TestOneClientUnreliable
Test: 1 lock clients (unreliable network)...
  ... Passed --  time  2.2s #peers 1 #RPCs    86 #Ops    0
--- PASS: TestOneClientUnreliable (2.17s)
=== RUN   TestManyClientsUnreliable
Test: 10 lock clients (unreliable network)...
  ... Passed --  time  3.5s #peers 1 #RPCs  1768 #Ops    0
--- PASS: TestManyClientsUnreliable (3.48s)
PASS
ok      6.5840/kvsrv1/lock      9.859s
```

## Lab 3: Raft

<https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html>

The main branch implements a replicator goroutine per peer.
The lab3-naive branch spawns goroutine per RPC

```bash
❯ cd src/raft1
❯ go test -v
=== RUN   TestInitialElection3A
Test (3A): initial election (reliable network)...
  ... Passed --  time  3.0s #peers 3 #RPCs    56 #Ops    0
--- PASS: TestInitialElection3A (3.02s)
=== RUN   TestReElection3A
Test (3A): election after network failure (reliable network)...
  ... Passed --  time  6.0s #peers 3 #RPCs   164 #Ops    0
--- PASS: TestReElection3A (5.98s)
=== RUN   TestManyElections3A
Test (3A): multiple elections (reliable network)...
  ... Passed --  time  5.6s #peers 7 #RPCs   564 #Ops    0
--- PASS: TestManyElections3A (5.60s)
=== RUN   TestBasicAgree3B
Test (3B): basic agreement (reliable network)...
  ... Passed --  time  0.6s #peers 3 #RPCs    16 #Ops    0
--- PASS: TestBasicAgree3B (0.63s)
=== RUN   TestRPCBytes3B
Test (3B): RPC byte count (reliable network)...
  ... Passed --  time  1.5s #peers 3 #RPCs    50 #Ops    0
--- PASS: TestRPCBytes3B (1.53s)
=== RUN   TestFollowerFailure3B
Test (3B): test progressive failure of followers (reliable network)...
  ... Passed --  time  4.7s #peers 3 #RPCs   117 #Ops    0
--- PASS: TestFollowerFailure3B (4.69s)
=== RUN   TestLeaderFailure3B
Test (3B): test failure of leaders (reliable network)...
  ... Passed --  time  5.3s #peers 3 #RPCs   198 #Ops    0
--- PASS: TestLeaderFailure3B (5.29s)
=== RUN   TestFailAgree3B
Test (3B): agreement after follower reconnects (reliable network)...
  ... Passed --  time  3.5s #peers 3 #RPCs    83 #Ops    0
--- PASS: TestFailAgree3B (3.54s)
=== RUN   TestFailNoAgree3B
Test (3B): no agreement if too many followers disconnect (reliable network)...
  ... Passed --  time  3.6s #peers 5 #RPCs   198 #Ops    0
--- PASS: TestFailNoAgree3B (3.57s)
=== RUN   TestConcurrentStarts3B
Test (3B): concurrent Start()s (reliable network)...
  ... Passed --  time  0.6s #peers 3 #RPCs    14 #Ops    0
--- PASS: TestConcurrentStarts3B (0.65s)
=== RUN   TestRejoin3B
Test (3B): rejoin of partitioned leader (reliable network)...
  ... Passed --  time  4.1s #peers 3 #RPCs   143 #Ops    0
--- PASS: TestRejoin3B (4.07s)
=== RUN   TestBackup3B
Test (3B): leader backs up quickly over incorrect follower logs (reliable network)...
  ... Passed --  time 17.0s #peers 5 #RPCs  1562 #Ops    0
--- PASS: TestBackup3B (17.02s)
=== RUN   TestCount3B
Test (3B): RPC counts arent too high (reliable network)...
  ... Passed --  time  2.6s #peers 3 #RPCs    44 #Ops    0
--- PASS: TestCount3B (2.55s)
=== RUN   TestPersist13C
Test (3C): basic persistence (reliable network)...
  ... Passed --  time  4.0s #peers 3 #RPCs    75 #Ops    0
--- PASS: TestPersist13C (3.98s)
=== RUN   TestPersist23C
Test (3C): more persistence (reliable network)...
  ... Passed --  time 12.3s #peers 5 #RPCs   344 #Ops    0
--- PASS: TestPersist23C (12.28s)
=== RUN   TestPersist33C
Test (3C): partitioned leader and one follower crash, leader restarts (reliable network)...
  ... Passed --  time  1.7s #peers 3 #RPCs    38 #Ops    0
--- PASS: TestPersist33C (1.75s)
=== RUN   TestFigure83C
Test (3C): Figure 8 (reliable network)...
  ... Passed --  time 30.2s #peers 5 #RPCs   970 #Ops    0
--- PASS: TestFigure83C (30.16s)
=== RUN   TestUnreliableAgree3C
Test (3C): unreliable agreement (unreliable network)...
  ... Passed --  time  2.4s #peers 5 #RPCs   360 #Ops    0
--- PASS: TestUnreliableAgree3C (2.35s)
=== RUN   TestFigure8Unreliable3C
Test (3C): Figure 8 (unreliable) (unreliable network)...
  ... Passed --  time 31.8s #peers 5 #RPCs  3137 #Ops    0
--- PASS: TestFigure8Unreliable3C (31.82s)
=== RUN   TestReliableChurn3C
Test (3C): churn (reliable network)...
  ... Passed --  time 16.3s #peers 5 #RPCs  2163 #Ops    0
--- PASS: TestReliableChurn3C (16.26s)
=== RUN   TestUnreliableChurn3C
Test (3C): unreliable churn (unreliable network)...
  ... Passed --  time 16.2s #peers 5 #RPCs  1130 #Ops    0
--- PASS: TestUnreliableChurn3C (16.16s)
=== RUN   TestSnapshotBasic3D
Test (3D): snapshots basic (reliable network)...
  ... Passed --  time  4.1s #peers 3 #RPCs   152 #Ops    0
--- PASS: TestSnapshotBasic3D (4.07s)
=== RUN   TestSnapshotInstall3D
Test (3D): install snapshots (disconnect) (reliable network)...
  ... Passed --  time 45.7s #peers 3 #RPCs  1152 #Ops    0
--- PASS: TestSnapshotInstall3D (45.74s)
=== RUN   TestSnapshotInstallUnreliable3D
Test (3D): install snapshots (disconnect) (unreliable network)...
  ... Passed --  time 45.2s #peers 3 #RPCs  1174 #Ops    0
--- PASS: TestSnapshotInstallUnreliable3D (45.19s)
=== RUN   TestSnapshotInstallCrash3D
Test (3D): install snapshots (crash) (reliable network)...
  ... Passed --  time 33.3s #peers 3 #RPCs   698 #Ops    0
--- PASS: TestSnapshotInstallCrash3D (33.29s)
=== RUN   TestSnapshotInstallUnCrash3D
Test (3D): install snapshots (crash) (unreliable network)...
  ... Passed --  time 38.0s #peers 3 #RPCs   802 #Ops    0
--- PASS: TestSnapshotInstallUnCrash3D (37.98s)
=== RUN   TestSnapshotAllCrash3D
Test (3D): crash and restart all servers (unreliable network)...
  ... Passed --  time 10.1s #peers 3 #RPCs   311 #Ops    0
--- PASS: TestSnapshotAllCrash3D (10.07s)
=== RUN   TestSnapshotInit3D
Test (3D): snapshot initialization after crash (unreliable network)...
  ... Passed --  time  3.7s #peers 3 #RPCs    90 #Ops    0
--- PASS: TestSnapshotInit3D (3.74s)
PASS
ok      6.5840/raft1    352.991s
```

Run multiple times with the bash script

```bash
for i in {1..20}; do go test || break; done
```

---

## Project Ideas

- High-performance Raft with cluster membership and snapshot chunks
- DFS / HDFS
- Spark implementation
- Object store S3
- Asynchronous replication - eventual consistency like DynamoDB, - Social media likes, DNS
- CDN - distributed cooperative web cache like Cloudflare
- File synchronizer like Google Drive
- Collaborative editor with CRDT / OT like Google Doc - causal consistency
- Distributed block store like Amazon EBS
- Use modern high-speed NIC features (e.g. RDMA or DPDK) to build a high-speed service, perhaps with replication or transactions.
