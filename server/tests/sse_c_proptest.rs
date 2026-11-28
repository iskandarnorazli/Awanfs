//! Property tests for #121 SSE-C AES-256-GCM (`AWE1`) chunk framing and tamper detection.

use proptest::prelude::*;
use s3_server::sse::{
    decrypt_full, decrypt_range_from_chunks, encrypt_full, stored_range_for_plaintext, SseDek, AWE1_CHUNK_PLAIN,
    AWE1_CHUNK_STORED, AWE1_HEADER_LEN,
};

fn boundary_size_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(0usize),
        1usize..=256,
        (AWE1_CHUNK_PLAIN - 8)..=(AWE1_CHUNK_PLAIN + 8),
        (2 * AWE1_CHUNK_PLAIN - 8)..=(2 * AWE1_CHUNK_PLAIN + 8),
        (3 * AWE1_CHUNK_PLAIN)..=(4 * AWE1_CHUNK_PLAIN + 17),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn decrypt_encrypt_roundtrip_and_arbitrary_ranges(
        size in boundary_size_strategy(),
        seed in any::<u8>(),
        key_bytes in any::<[u8; 32]>(),
        prefix in any::<[u8; 4]>(),
        part_num in 0u32..=5u32,
        r_start_frac in 0usize..1000,
        r_len_frac in 1usize..1000,
    ) {
        let dek = SseDek::from_raw(key_bytes);
        let plain: Vec<u8> = (0..size)
            .map(|i| seed.wrapping_add((i.wrapping_mul(31) ^ (i >> 5)) as u8))
            .collect();
        let (stored, _) = encrypt_full(&dek, prefix, part_num, &plain).unwrap();
        let dec = decrypt_full(&dek, Some(prefix), part_num, size as u64, &stored).unwrap();
        prop_assert_eq!(dec.as_slice(), plain.as_slice());

        if !plain.is_empty() {
            let start = (r_start_frac * size) / 1000;
            let max_rem = size - start;
            let len = ((r_len_frac * max_rem) / 1000).max(1);
            let (first_chunk, stored_start, stored_span) =
                stored_range_for_plaintext(size as u64, start as u64, len as u64).unwrap();
            let s0 = stored_start as usize;
            let s1 = s0 + stored_span as usize;
            let sub = decrypt_range_from_chunks(
                &dek,
                prefix,
                part_num,
                size as u64,
                start as u64,
                len as u64,
                first_chunk,
                &stored[s0..s1],
            )
            .unwrap();
            prop_assert_eq!(sub.as_slice(), &plain[start..start + len]);
        }
    }

    #[test]
    fn bit_flip_truncation_and_chunk_swap_always_fail(
        size in (AWE1_CHUNK_PLAIN + 1)..=(2 * AWE1_CHUNK_PLAIN + 128),
        key_bytes in any::<[u8; 32]>(),
        prefix in any::<[u8; 4]>(),
        flip_idx_raw in 0usize..200_000,
        flip_mask in 1u8..=255u8,
        trunc_drop in 1usize..=64,
    ) {
        let dek = SseDek::from_raw(key_bytes);
        let plain: Vec<u8> = (0..size).map(|i| i.wrapping_mul(17) as u8).collect();
        let (stored, _) = encrypt_full(&dek, prefix, 0, &plain).unwrap();

        // 1. Any bit flip inside header, ciphertext, or GCM tag must fail decryption.
        let mut flipped = stored.clone();
        let idx = flip_idx_raw % flipped.len();
        flipped[idx] ^= flip_mask;
        prop_assert!(decrypt_full(&dek, Some(prefix), 0, size as u64, &flipped).is_err());

        // 2. Truncation must fail decryption.
        let cut = stored.len().saturating_sub(trunc_drop.min(stored.len()));
        prop_assert!(decrypt_full(&dek, Some(prefix), 0, size as u64, &stored[..cut]).is_err());

        // 3. Swapping chunk 0 and chunk 1 (each 64 KiB + 16B tag) must fail AEAD chunk_index binding.
        if size >= 2 * AWE1_CHUNK_PLAIN {
            let mut swapped = stored.clone();
            let c0 = AWE1_HEADER_LEN;
            let c1 = AWE1_HEADER_LEN + AWE1_CHUNK_STORED;
            let (left, right) = swapped.split_at_mut(c1);
            left[c0..c0 + AWE1_CHUNK_STORED].swap_with_slice(&mut right[..AWE1_CHUNK_STORED]);
            prop_assert!(decrypt_full(&dek, Some(prefix), 0, size as u64, &swapped).is_err());
        }
    }
}
