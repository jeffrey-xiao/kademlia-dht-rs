use rand;
use std::fmt::{Debug, Formatter, Result};

use KEY_LENGTH;

#[derive(Ord, PartialOrd, PartialEq, Eq, Clone, Hash, Serialize, Deserialize, Default, Copy)]
pub struct Key(pub [u8; KEY_LENGTH]);

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let hex_vec: Vec<String> = self.0.iter().map(|b| format!("{:02X}", b)).collect();
        write!(f, "{}", hex_vec.join(""))
    }
}

impl Key {
    pub fn new(data: [u8; KEY_LENGTH]) -> Self {
        Key(data)
    }

    pub fn rand() -> Self {
        let mut ret = Key([0; KEY_LENGTH]);
        for byte in &mut ret.0 {
            *byte = rand::random::<u8>();
        }
        ret
    }

    // generates a random key from [2^(KEY_LENGTH - index - 1), 2^(KEY_LENGTH - index))
    pub fn rand_in_range(index: usize) -> Self {
        let mut ret = Key::rand();
        let bytes = index / 8;
        let bit = index % 8;
        for i in 0..bytes {
            ret.0[i] = 0;
        }
        ret.0[bytes] &= 0xFF >> (bit);
        ret.0[bytes] |= 1 << (8 - bit - 1);
        ret
    }

    pub fn xor(&self, key: &Key) -> Key {
        let mut ret = [0; KEY_LENGTH];
        for (i, byte) in ret.iter_mut().enumerate() {
            *byte = self.0[i] ^ key.0[i];
        }
        Key(ret)
    }

    pub fn leading_zeros(&self) -> usize {
        let mut ret = 0;
        for i in 0..KEY_LENGTH {
            if self.0[i] == 0 {
                ret += 8
            } else {
                return ret + self.0[i].leading_zeros() as usize
            }
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    extern crate num_bigint;
    use self::num_bigint::BigUint;

    use super::Key;
    use KEY_LENGTH;

    #[test]
    fn test_rand_in_range() {
        for i in 0..KEY_LENGTH * 8 {
            let key = BigUint::from_bytes_be(&Key::rand_in_range(i).0);
            let mut lower = [0u8; KEY_LENGTH];
            lower[i / 8] = 1 << ((KEY_LENGTH * 8 - i - 1) % 8);
            assert!(BigUint::from_bytes_be(&lower) <= key);
            assert!(key < BigUint::from_bytes_be(&lower) << 1);
        }
    }

    #[test]
    fn test_leading_zeros() {
        for i in 0..KEY_LENGTH * 8 {
            assert_eq!(Key::rand_in_range(i).leading_zeros(), i);
        }

    }
}
