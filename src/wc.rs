use std::io::Read;

pub const BUF_SIZE: usize = 1 << 24;

pub fn main() {
    let mut input = std::fs::File::open("./measurements.txt").unwrap();
    let mut read;
    let mut buf = unsafe {
        Box::<[u8]>::from_raw(
            std::alloc::alloc(std::alloc::Layout::from_size_align_unchecked(
                BUF_SIZE, 16384,
            )) as *mut [u8; BUF_SIZE],
        )
    };
    let mut ws = true;
    let mut w = 0u64;
    let mut l = 0u64;
    let mut c = 0u64;
    loop {
        read = input.read(&mut buf).unwrap();
        if read == 0 {
            break;
        }
        c += read as u64;
        for v in &buf[0..read] {
            let vws = v.is_ascii_whitespace();
            if !vws && ws {
                w += 1;
            }
            ws = vws;
            if *v == b'\n' {
                l += 1;
            }
            // c += 1;
        }
    }
    println!("{l} {w} {c}");
}
