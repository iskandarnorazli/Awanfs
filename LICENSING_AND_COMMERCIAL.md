# Awan Storage Licensing, Enforcement & Commercial Strategy

> **Enterprise Software Licensing Model, Cryptographic Enforcement, and Tiering**

---

## 1. Commercial Strategy & Business Model

Awan Storage uses an **Open-Core / Commercial Enterprise Software Licensing Model** (identical to the successful commercial models of **MinIO Enterprise**, **VAST Data**, and **CockroachDB**).

We sell **software licenses and support subscriptions** to companies operating their own bare-metal servers, private clouds, AI clusters, and agent sandbox infrastructures.

### Why Software Licensing Delivers 85%+ Gross Margins:
- **Zero Infrastructure CapEx:** Customers pay for their own servers, NVMe drives, and electricity.
- **Zero Bandwidth Costs:** Egress bandwidth is handled directly on customer networks (e.g. Hetzner/OVH unmetered pipes).
- **High Enterprise Willingness to Pay:** Customers replace \$30,000/mo AWS S3 bills with \$2,000/mo bare metal + \$1,500/mo Awan Storage license. They save 80% while we earn 85%+ software margins.

---

## 2. Licensing Tiers & Feature Matrix

| Feature | Community / Developer (Free) | Enterprise Standard | Enterprise Hyperscale |
| :--- | :---: | :---: | :---: |
| **Cluster Capacity Limit** | Up to **32 Terabytes** | Up to **500 Terabytes** | **Unlimited Petabytes** |
| **Node Count** | Up to 4 Nodes | Up to 16 Nodes | Unlimited Nodes |
| **Engine Core** | Awan Native Engine | Awan Native Engine | Awan Native Engine |
| **Consensus & Quorum** | $N=3$ Replication | $N=3$ Replication | $8+4$ Erasure Coding |
| **Air-Gapped Offline Operation** | Supported | Supported | Supported |
| **Encryption at Rest** | AES-256-GCM | AES-256-GCM + KMS | Dedicated Hardware HSM |
| **Support SLA** | Community Discord | 24-hour response | 1-hour critical response |
| **Pricing** | **Free (MIT / Apache 2.0)** | **\$15 / TB / month** | **Custom Volume Pricing** |

---

## 3. Cryptographic License Enforcement (Air-Gapped & Tamper-Proof)

Enterprise infrastructure often runs in secure, air-gapped VPCs with **zero outbound internet access**. Therefore, the license enforcement mechanism **must not rely on phone-home telemetry servers or license validation endpoints**.

### A. Asymmetric Cryptographic License File
Every commercial license is an offline Ed25519-signed JSON token (`license.key`):

```json
{
  "customer_id": "boxcompute-corp-102",
  "tier": "enterprise_standard",
  "max_capacity_tb": 500,
  "max_nodes": 16,
  "issued_at": 1700000000,
  "expires_at": 1731542400,
  "hardware_fingerprint": "a3f892c90e...18b",
  "signature": "3e9b14...fe08"
}
```

### B. Offline Verification at Startup
1. **Public Key Embedding:** The Awan Storage native binary embeds the vendor Ed25519 public key.
2. **Signature Verification:** On daemon startup, `storage_engine` verifies the signature of `license.key`. If modified by a single byte, the signature check fails immediately.
3. **Expiration Check:** The daemon compares `expires_at` against the cryptographically synchronized system epoch.
4. **Hardware Binding (Optional Enterprise Addon):** The license can optionally be bound to the CPU processor serial numbers and primary NIC MAC addresses of the cluster nodes to prevent cloning.

### C. Soft-Cap vs Hard-Cap Enforcement Behavior
To prevent accidental data loss in mission-critical environments:
- **Exceeding Capacity:** If stored data exceeds `max_capacity_tb` by $> 10\%$, the engine transitions to **Read-Only / Over-Quota Mode**:
  - `GET`, `HEAD`, and `DELETE` requests continue to execute at full microsecond speed.
  - New `PUT` requests return HTTP `507 Insufficient Storage` with `ErrorCode: LicenseCapacityExceeded`.
  - Cluster logs emit alerts detailing upgrade instructions.

---

## 4. Target Customer Profiles (Initial B2B Outbound)

1. **AI Agent Sandbox Platforms (BoxCompute, E2B, Modal, KotakSakti):**
   - **Pain Point:** Container/microVM snapshot restoration latency (2–4 seconds) and high RAM usage per node.
   - **Pitch:** Sub-15ms snapshot restoration, 3x higher sandbox density per host, 80% lower infrastructure bill.
2. **On-Prem GPU / AI Training Hubs:**
   - **Pain Point:** PyTorch model checkpoints starving \$40k NVIDIA GPUs waiting for S3 downloads.
   - **Pitch:** 19.4 GB/s per core line-rate checkpoint ingestion directly into GPU memory via zero-copy slabs.
3. **Video Streaming, Gaming & VFX Rendering Pipelines:**
   - **Pain Point:** Millions of dollars in cloud egress bills transferring multi-terabyte assets.
   - **Pitch:** Unmetered, zero-egress bare-metal S3 cluster deployed on Hetzner or private racks.
