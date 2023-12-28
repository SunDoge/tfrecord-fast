
use librscrc::prelude::*;

const MASK_DELTA: u32 = 0xa282ead8;

#[inline]
pub fn get_masked_crc(buf: &[u8]) -> u32 {
    // let crc = crc32c::crc32c(buf);
    let mut crc32c = Crc32C::new_lookup();
    crc32c.update(buf);
    let crc = crc32c.digest();
    ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA)
}

#[inline]
pub fn verify_masked_crc(buf: &[u8], expect: u32) -> bool {
    let found = get_masked_crc(buf);
    found == expect
}
