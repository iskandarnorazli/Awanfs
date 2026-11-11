//! awan-storage/server/tests/erasure_coding_test.rs
//! Reed-Solomon Erasure Coding & Cluster Gossip Heartbeats Tests for Issue #476

use s3_server::{ClusterMembership, ErasureCodec, ErasureError, NodeStatus};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[test]
fn test_erasure_coding_8_plus_4_roundtrip_and_fault_tolerance() {
    let codec = ErasureCodec::new_8_plus_4().expect("create 8+4 codec");
    assert_eq!(codec.data_shards, 8);
    assert_eq!(codec.parity_shards, 4);
    assert_eq!(codec.total_shards(), 12);

    // 1. Create a 1 MiB test payload with deterministic pattern
    let size = 1024 * 1024;
    let mut payload = Vec::with_capacity(size);
    for i in 0..size {
        payload.push((i * 31 % 256) as u8);
    }

    // 2. Encode into 12 shards
    let shards = codec.encode(&payload).expect("encode");
    assert_eq!(shards.len(), 12);

    // 3. Roundtrip decode with all 12 shards
    let mut full_shards: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
    let decoded = codec.decode(&mut full_shards, payload.len()).expect("decode full");
    assert_eq!(decoded, payload, "Decoded payload must match original bit-for-bit");

    // 4. Maximum Tolerable Loss: Drop 4 arbitrary shards (e.g. indices 0, 3, 7, 11)
    let mut damaged_shards = shards.iter().cloned().map(Some).collect::<Vec<_>>();
    damaged_shards[0] = None;
    damaged_shards[3] = None;
    damaged_shards[7] = None;
    damaged_shards[11] = None;

    let reconstructed = codec
        .decode(&mut damaged_shards, payload.len())
        .expect("decode with 4 missing shards");
    assert_eq!(
        reconstructed, payload,
        "Payload reconstructed with 4 lost shards must match original bit-for-bit"
    );

    // 5. Negative Control: Drop 5 shards (exceeding parity capacity of 4) -> must fail safely
    let mut fatal_shards = shards.iter().cloned().map(Some).collect::<Vec<_>>();
    fatal_shards[0] = None;
    fatal_shards[1] = None;
    fatal_shards[2] = None;
    fatal_shards[3] = None;
    fatal_shards[4] = None;

    let fatal_res = codec.decode(&mut fatal_shards, payload.len());
    match fatal_res {
        Err(ErasureError::TooFewShards { available, required }) => {
            assert_eq!(available, 7);
            assert_eq!(required, 8);
        }
        other => panic!("Expected TooFewShards error, got {:?}", other),
    }
}

#[test]
fn test_erasure_coding_4_plus_2_roundtrip_and_loss() {
    let codec = ErasureCodec::new_4_plus_2().expect("create 4+2 codec");
    assert_eq!(codec.data_shards, 4);
    assert_eq!(codec.parity_shards, 2);
    assert_eq!(codec.total_shards(), 6);

    let payload = b"Distributed durability on high-speed NVMe WORM volumes with zero-copy SIMD arithmetic";
    let shards = codec.encode(payload).expect("encode");
    assert_eq!(shards.len(), 6);

    // Drop 2 chunks (indices 1 and 4)
    let mut damaged: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
    damaged[1] = None;
    damaged[4] = None;

    let recovered = codec.decode(&mut damaged, payload.len()).expect("recover");
    assert_eq!(&recovered[..], &payload[..]);

    // Negative control: drop 3 chunks out of 6 (leaving only 3, below required 4) -> must fail
    let mut fatal_damaged: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
    fatal_damaged[0] = None;
    fatal_damaged[1] = None;
    fatal_damaged[2] = None;
    assert!(codec.decode(&mut fatal_damaged, payload.len()).is_err());
}

#[test]
fn test_cluster_heartbeat_protocol_and_failure_detection() {
    let local_id = 1u32;
    let interval = Duration::from_millis(500);
    let start_time = Instant::now();

    let mut cluster = ClusterMembership::new(local_id, interval);

    // Register peers 2, 3, 4
    let addr2: SocketAddr = "127.0.0.1:9102".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:9103".parse().unwrap();
    let addr4: SocketAddr = "127.0.0.1:9104".parse().unwrap();

    cluster.register_peer(2, addr2, start_time);
    cluster.register_peer(3, addr3, start_time);
    cluster.register_peer(4, addr4, start_time);

    assert_eq!(cluster.healthy_count(), 4);
    assert!(cluster.is_quorum_available(3));

    // Simulate 1 second passing (2 intervals) with heartbeats from all nodes
    let t1 = start_time + Duration::from_millis(1000);
    let hb2 = cluster.generate_heartbeat(1000);
    let mut hb_peer = hb2;
    hb_peer.sender_id = 2;
    cluster.receive_heartbeat(&hb_peer, t1);
    hb_peer.sender_id = 3;
    cluster.receive_heartbeat(&hb_peer, t1);
    hb_peer.sender_id = 4;
    cluster.receive_heartbeat(&hb_peer, t1);

    let transitions = cluster.tick(t1);
    assert!(transitions.is_empty(), "All nodes healthy, no transitions");
    assert_eq!(cluster.nodes.get(&2).unwrap().status, NodeStatus::Healthy);

    // Stop heartbeats from node 2; advance time by 1600ms (> 3 intervals since t1)
    let t2 = t1 + Duration::from_millis(1600);
    hb_peer.sender_id = 3;
    cluster.receive_heartbeat(&hb_peer, t2);
    hb_peer.sender_id = 4;
    cluster.receive_heartbeat(&hb_peer, t2);

    let transitions = cluster.tick(t2);
    assert_eq!(transitions, vec![(2, NodeStatus::Degraded)]);
    assert_eq!(cluster.nodes.get(&2).unwrap().status, NodeStatus::Degraded);
    assert_eq!(cluster.healthy_count(), 3);

    // Advance time by another 1600ms (> 6 intervals total since node 2 died)
    let t3 = t2 + Duration::from_millis(1600);
    hb_peer.sender_id = 3;
    cluster.receive_heartbeat(&hb_peer, t3);
    hb_peer.sender_id = 4;
    cluster.receive_heartbeat(&hb_peer, t3);

    let transitions = cluster.tick(t3);
    assert_eq!(transitions, vec![(2, NodeStatus::Dead)]);
    assert_eq!(cluster.nodes.get(&2).unwrap().status, NodeStatus::Dead);

    // Node 2 recovers and sends a heartbeat
    let t4 = t3 + Duration::from_millis(100);
    hb_peer.sender_id = 2;
    assert!(cluster.receive_heartbeat(&hb_peer, t4));
    let transitions = cluster.tick(t4);
    assert_eq!(transitions, vec![(2, NodeStatus::Healthy)]);
    assert_eq!(cluster.nodes.get(&2).unwrap().status, NodeStatus::Healthy);
    assert_eq!(cluster.healthy_count(), 4);
}
