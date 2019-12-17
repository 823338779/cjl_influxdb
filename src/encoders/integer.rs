use super::simple8b;
use integer_encoding::*;
use std::error::Error;

/// Encoding describes the type of encoding used by an encoded integer block.
enum Encoding {
    Uncompressed = 0,
    Simple8b = 1,
    Rle = 2,
}

/// encode_all encodes a vector of signed integers into dst.
///
/// Deltas between the integers in the vector are first calculated, and these
/// deltas are then zig-zag encoded. The resulting zig-zag encoded deltas are
/// further compressed if possible, either via bit-packing using simple8b or by
/// run-length encoding the deltas if they're all the same.
///
pub fn encode_all<'a>(src: &mut Vec<i64>, dst: &'a mut Vec<u8>) -> Result<(), Box<Error>> {
    dst.truncate(0); // reset buffer.
    if src.len() == 0 {
        return Ok(());
    }

    let mut max: u64 = 0;
    let mut deltas = i64_to_u64_vector(src);
    for i in (1..deltas.len()).rev() {
        deltas[i] = zig_zag_encode(deltas[i].wrapping_sub(deltas[i - 1]) as i64);
        if deltas[i] > max {
            max = deltas[i];
        }
    }
    deltas[0] = zig_zag_encode(src[0]);

    if deltas.len() > 2 {
        let mut use_rle = true;
        for i in 2..deltas.len() {
            if deltas[1] != deltas[i] {
                use_rle = false;
                break;
            }
        }

        // Encode with RLE if possible.
        if use_rle {
            encode_rle(deltas[0], deltas[1], deltas.len() as u64, dst);
            // 4 high bits of first byte used for the encoding type
            dst[0] |= (Encoding::Rle as u8) << 4;
            return Ok(());
        }
    }

    // write block uncompressed
    if max > simple8b::MAX_VALUE {
        let cap = 1 + (deltas.len() * 8); // 8 bytes per value plus header byte
        if dst.capacity() < cap {
            dst.reserve_exact(cap - dst.capacity());
        }
        dst.push((Encoding::Uncompressed as u8) << 4);
        for delta in deltas.iter() {
            dst.extend_from_slice(&delta.to_be_bytes());
        }
        return Ok(());
    }

    // Compress with simple8b
    // first 4 high bits used for encoding type
    dst.push((Encoding::Simple8b as u8) << 4);
    dst.extend_from_slice(&deltas[0].to_be_bytes()); // encode first value
    simple8b::encode_all(&deltas[1..], dst)
}

// zig_zag_encode converts a signed integer into an unsigned one by zig zagging
// negative and positive values across even and odd numbers.
//
// Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
fn zig_zag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

// zig_zag_decode converts a zig zag encoded unsigned integer into an signed
// integer.
fn zig_zag_decode(v: u64) -> i64 {
    ((v >> 1) ^ ((((v & 1) as i64) << 63) >> 63) as u64) as i64
}

// i64_to_u64_vector converts a Vec<i64> to Vec<u64>.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn i64_to_u64_vector(src: &[i64]) -> Vec<u64> {
    src.into_iter().map(|x| *x as u64).collect::<Vec<u64>>()
}

// u64_to_i64_vector converts a Vec<u64> to Vec<i64>.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn u64_to_i64_vector(src: &[u64]) -> Vec<i64> {
    src.into_iter().map(|x| *x as i64).collect::<Vec<i64>>()
}

// encode_rle encodes the value v, delta and count into dst.
//
// v should be the first element of a sequence, delta the difference that each
// value in the sequence differs by, and count the total number of values in the
// sequence.
fn encode_rle(v: u64, delta: u64, count: u64, dst: &mut Vec<u8>) {
    let max_var_int_size = 10; // max number of bytes needed to store var int
    dst.push(0); // save a byte for encoding type
    dst.extend_from_slice(&v.to_be_bytes()); // write the first value in as a byte array.
    let mut n = 9;

    if dst.len() - n <= max_var_int_size {
        dst.resize(n + max_var_int_size, 0);
    }
    n += delta.encode_var(&mut dst[n..]); // encode delta between values

    if dst.len() - n <= max_var_int_size {
        dst.resize(n + max_var_int_size, 0);
    }
    n += count.encode_var(&mut dst[n..]); // encode count of values
    dst.truncate(n);
}

/// decode_all decodes a slice of bytes into a vector of signed integers.
pub fn decode_all<'a>(src: &[u8], dst: &'a mut Vec<i64>) -> Result<(), Box<Error>> {
    if src.len() == 0 {
        return Ok(());
    }
    let encoding = &src[0] >> 4;
    match encoding {
        encoding if encoding == Encoding::Uncompressed as u8 => {
            return decode_uncompressed(&src[1..], dst); // first byte not used
        }
        encoding if encoding == Encoding::Rle as u8 => return decode_rle(&src[1..], dst),
        encoding if encoding == Encoding::Simple8b as u8 => return decode_simple8b(&src[1..], dst),
        _ => return Err(From::from("invalid block encoding")),
    }
}

fn decode_uncompressed(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<Error>> {
    if src.len() == 0 || src.len() & 0x7 != 0 {
        return Err(From::from("invalid uncompressed block length"));
    }

    let count = src.len() / 8;
    if dst.capacity() < count {
        dst.reserve_exact(count - dst.capacity());
    }
    let mut i = 0;
    let mut prev: i64 = 0;
    let mut buf: [u8; 8] = [0; 8];
    while i < src.len() {
        buf.copy_from_slice(&src[i..i + 8]);
        prev = prev.wrapping_add(zig_zag_decode(u64::from_be_bytes(buf)));
        dst.push(prev); // N.B - signed integer...
        i += 8;
    }
    Ok(())
}

// decode_rle decodes an RLE encoded slice containing only unsigned into the
// destination vector.
fn decode_rle(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<Error>> {
    if src.len() < 8 {
        return Err(From::from("not enough data to decode using RLE"));
    }

    let mut i = 8; // Skip first value
    let (delta, n) = u64::decode_var(&src[i..]);
    if n <= 0 {
        return Err(From::from("unable to decode delta"));
    }
    i += n;

    let (count, n) = usize::decode_var(&src[i..]);
    if n <= 0 {
        return Err(From::from("unable to decode count"));
    }

    if dst.capacity() < count {
        dst.reserve_exact(count - dst.capacity());
    }

    // TODO(edd): this should be possible to do in-place without copy.
    let mut a: [u8; 8] = [0; 8];
    a.copy_from_slice(&src[0..8]);
    let mut first = zig_zag_decode(u64::from_be_bytes(a));
    let delta_z = zig_zag_decode(delta);
    for _ in 0..count {
        dst.push(first);
        first = first.wrapping_add(delta_z);
    }
    Ok(())
}

fn decode_simple8b(src: &[u8], dst: &mut Vec<i64>) -> Result<(), Box<Error>> {
    if src.len() < 9 {
        return Err(From::from("not enough data to decode packed timestamp"));
    }

    // TODO(edd): pre-allocate res by counting bytes in encoded slice?
    let mut res = vec![];
    let mut buf: [u8; 8] = [0; 8];
    buf.copy_from_slice(&src[0..8]);
    dst.push(zig_zag_decode(u64::from_be_bytes(buf)));

    simple8b::decode_all(&src[8..], &mut res);
    // TODO(edd): fix this. It's copying, which is slowwwwwwwww.
    let mut next = dst[0];
    for v in res.iter() {
        next += zig_zag_decode(*v);
        dst.push(next);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zig_zag_encoding() {
        let input = vec![-2147483648, -2, -1, 0, 1, 2147483647];
        let exp = vec![4294967295, 3, 1, 0, 2, 4294967294];
        for (i, v) in input.iter().enumerate() {
            let encoded = zig_zag_encode(*v);
            assert_eq!(encoded, exp[i]);

            let decoded = zig_zag_decode(encoded);
            assert_eq!(decoded, input[i]);
        }
    }

    #[test]
    fn encode_all_no_values() {
        let mut src: Vec<i64> = vec![];
        let mut dst = vec![];

        // check for error
        encode_all(&mut src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.to_vec(), vec![]);
    }

    #[test]
    fn encode_all_uncompressed() {
        let mut src: Vec<i64> = vec![-1000, 0, simple8b::MAX_VALUE as i64, 213123421];
        let mut dst = vec![];

        let exp = src.clone();
        encode_all(&mut src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[0] >> 4, Encoding::Uncompressed as u8);
        let mut got = vec![];
        decode_all(&dst, &mut got).expect("failed to decode");

        // verify got same values back
        assert_eq!(got, exp);
    }

    #[test]
    fn encode_all_rle() {
        struct Test {
            name: String,
            input: Vec<i64>,
        }

        let tests = vec![
            Test {
                name: String::from("no delta positive"),
                input: vec![123; 8],
            },
            Test {
                name: String::from("no delta negative"),
                input: vec![-345632452354; 1000],
            },
            Test {
                name: String::from("delta positive"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta negative"),
                input: vec![-350, -200, -50],
            },
            Test {
                name: String::from("delta mixed"),
                input: vec![-35000, -5000, 25000, 55000],
            },
            Test {
                name: String::from("delta descending"),
                input: vec![100, 50, 0, -50, -100, -150],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let mut src = test.input.clone();
            let exp = test.input;
            encode_all(&mut src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(&dst[0] >> 4, Encoding::Rle as u8);
            let mut got = vec![];
            decode_all(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }

    #[test]
    fn encode_all_simple8b() {
        struct Test {
            name: String,
            input: Vec<i64>,
        }

        let tests = vec![
            Test {
                name: String::from("positive"),
                input: vec![1, 11, 3124, 123543256, 2398567984273478],
            },
            Test {
                name: String::from("negative"),
                input: vec![-109290, -1234, -123, -12],
            },
            Test {
                name: String::from("mixed"),
                input: vec![-109290, -1234, -123, -12, 0, 0, 0, 1234, 44444, 4444444],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let mut src = test.input.clone();
            let exp = test.input;
            encode_all(&mut src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[0] >> 4, Encoding::Simple8b as u8);

            let mut got = vec![];
            decode_all(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }
}
