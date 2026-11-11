//! awan-storage/server/src/erasure.rs
//! SIMD-Accelerated Reed-Solomon Erasure Coding (8+4 and 4+2 Cauchy Distribution)
//! Provides mathematical reconstruction guarantees across arbitrary chunk failures.

use reed_solomon_erasure::galois_8::ReedSolomon;

#[derive(Debug, PartialEq, Eq)]
pub enum ErasureError {
    InvalidConfiguration,
    PayloadEmpty,
    TooFewShards { available: usize, required: usize },
    CodecError(String),
}

impl std::fmt::Display for ErasureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration => write!(f, "Invalid data or parity shard count"),
            Self::PayloadEmpty => write!(f, "Payload cannot be empty for erasure coding"),
            Self::TooFewShards { available, required } => {
                write!(f, "Too few shards available: {} (required {})", available, required)
            }
            Self::CodecError(msg) => write!(f, "Codec error: {}", msg),
        }
    }
}

impl std::error::Error for ErasureError {}

pub struct ErasureCodec {
    pub data_shards: usize,
    pub parity_shards: usize,
    codec: ReedSolomon,
}

impl ErasureCodec {
    /// Creates a new Reed-Solomon codec (e.g. 8 data + 4 parity, or 4 data + 2 parity)
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, ErasureError> {
        if data_shards == 0 || parity_shards == 0 || data_shards + parity_shards > 256 {
            return Err(ErasureError::InvalidConfiguration);
        }
        let codec = ReedSolomon::new(data_shards, parity_shards)
            .map_err(|e| ErasureError::CodecError(format!("{:?}", e)))?;
        Ok(Self {
            data_shards,
            parity_shards,
            codec,
        })
    }

    /// Convenience constructor for 8+4 configuration (standard Awan cluster durability)
    pub fn new_8_plus_4() -> Result<Self, ErasureError> {
        Self::new(8, 4)
    }

    /// Convenience constructor for 4+2 configuration (compact cluster durability)
    pub fn new_4_plus_2() -> Result<Self, ErasureError> {
        Self::new(4, 2)
    }

    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Encodes a raw payload into (data_shards + parity_shards) equal-sized chunks
    pub fn encode(&self, payload: &[u8]) -> Result<Vec<Vec<u8>>, ErasureError> {
        if payload.is_empty() {
            return Err(ErasureError::PayloadEmpty);
        }

        // Shard size must round up so that data_shards * shard_size >= payload.len()
        let shard_size = (payload.len() + self.data_shards - 1) / self.data_shards;
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(self.total_shards());

        for i in 0..self.data_shards {
            let start = (i * shard_size).min(payload.len());
            let end = ((i + 1) * shard_size).min(payload.len());
            let mut chunk = vec![0u8; shard_size];
            if start < end {
                chunk[..end - start].copy_from_slice(&payload[start..end]);
            }
            shards.push(chunk);
        }

        for _ in 0..self.parity_shards {
            shards.push(vec![0u8; shard_size]);
        }

        self.codec
            .encode(&mut shards)
            .map_err(|e| ErasureError::CodecError(format!("{:?}", e)))?;

        Ok(shards)
    }

    /// Reconstructs original payload from any subset of shards containing >= data_shards intact chunks.
    pub fn decode(
        &self,
        shards: &mut [Option<Vec<u8>>],
        original_size: usize,
    ) -> Result<Vec<u8>, ErasureError> {
        if shards.len() != self.total_shards() {
            return Err(ErasureError::InvalidConfiguration);
        }

        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < self.data_shards {
            return Err(ErasureError::TooFewShards {
                available,
                required: self.data_shards,
            });
        }

        self.codec
            .reconstruct(shards)
            .map_err(|e| ErasureError::CodecError(format!("{:?}", e)))?;

        let mut output = Vec::with_capacity(original_size);
        for i in 0..self.data_shards {
            if let Some(ref chunk) = shards[i] {
                let needed = original_size.saturating_sub(output.len());
                let take = needed.min(chunk.len());
                output.extend_from_slice(&chunk[..take]);
            }
        }

        if output.len() != original_size {
            return Err(ErasureError::CodecError("Reconstructed payload size mismatch".to_string()));
        }

        Ok(output)
    }
}
