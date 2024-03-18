use crate::compute::{process_chunk, AggMap};
use std::{fmt::Write, io::Read};

// here we can tweak the buffer size. anything decently big but not too big seems to be reasonably
// fast.
pub const BUF_SIZE: usize = 1 << 23;

pub fn main() {
    println!("using chunk size of: {:?}", BUF_SIZE);
    let begin = std::time::Instant::now();

    let mut input = std::fs::File::open("./measurements.txt").unwrap();
    let num_threads = std::thread::available_parallelism().unwrap().get();
    let (buf_tx, buf_rx) = crossbeam::channel::bounded::<Box<[u8; BUF_SIZE]>>(4);
    let (tx, rx) = crossbeam::channel::bounded::<(Box<[u8; BUF_SIZE]>, usize, Option<Vec<u8>>)>(4);
    let mut threads = Vec::new();
    for _ in 0..num_threads {
        let rx = rx.clone();
        let buf_tx = buf_tx.clone();
        threads.push(std::thread::spawn(move || {
            // let mut totals = AggMap::with_capacity_and_hasher(0, <_>::default());
            let mut totals = AggMap::default();
            while let Ok((buf, read, leftover)) = rx.recv() {
                let mut start_offset = 0;
                if let Some(mut leftover) = leftover {
                    while unsafe { *buf.get_unchecked(start_offset) } != b'\n' {
                        leftover.push(buf[start_offset]);
                        start_offset += 1;
                    }
                    leftover.push(b'\n');
                    process_chunk(&leftover, &mut totals);
                }

                let chunk = &buf[start_offset..read];
                process_chunk(chunk, &mut totals);
                buf_tx.try_send(buf).ok();
            }
            totals
        }));
    }

    let mut read;
    let mut leftover = None;
    let mut num_chunks = 0;
    let mut num_allocs = 0;
    loop {
        let mut buf = buf_rx.try_recv().unwrap_or_else(|_| {
            num_allocs += 1;
            Box::new([0u8; BUF_SIZE])
        });
        read = input.read(buf.as_mut_slice()).unwrap();
        if read == 0 {
            break;
        }
        let chopped_tail = buf[0..read].iter().rev().position(|&c| c == b'\n').unwrap();
        let new_leftover: Option<Vec<u8>> = if chopped_tail > 0 {
            let mut r = Vec::with_capacity(64);
            r.extend_from_slice(&buf[(read - chopped_tail)..read]);
            Some(r)
        } else {
            None
        };
        tx.send((buf, read - chopped_tail, leftover)).unwrap();
        leftover = new_leftover;
        num_chunks += 1;
    }
    drop(tx);

    let begin_summarize = std::time::Instant::now();
    let mut totals = AggMap::default();
    for t in threads {
        for (name, agg) in t.join().unwrap() {
            totals.entry(name).or_default().merge(agg);
        }
    }

    let mut result = totals.into_iter().collect::<Vec<_>>();
    result.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let total_counts: f64 = result.iter().map(|v| v.1.count as f64).sum();
    let mut out = String::with_capacity(result.len() * 35);
    for (k, v) in result {
        write!(
            out,
            "{}={:.1}/{:.1}/{:.1}\n",
            k,
            v.sum as f64 / v.count as f64,
            v.min as f64 / 10.,
            v.max as f64 / 10.,
        )
        .unwrap();
    }
    std::fs::write("./result.txt", out).unwrap();

    // We made it!
    let end = std::time::Instant::now();
    println!("elapsed: {}ms", (end - begin).as_millis());
    println!(
        "elapsed (summary only): {}ms",
        (end - begin_summarize).as_millis()
    );
    println!(
        "real input would take roughly: {} seconds",
        (end - begin).as_secs_f64() * (1_000_000_000. / total_counts)
    );

    // Let's just grab both the new result file and the expected result to make sure they're right!
    let output = std::fs::read_to_string("./result.txt").unwrap();
    let expected = std::fs::read_to_string("./result-expected.txt").unwrap();
    if output == expected {
        println!("File matched expected result. :3");
    } else {
        eprintln!("oh no! file did not match expected output");
    }

    println!("num_chunks: {:?}", num_chunks);
    println!("num_allocs: {:?}", num_allocs);
}
