use crossbeam::channel;
use rustc_hash::FxHashMap;
use std::{alloc::Layout, fmt::Write, io::Read};

// welcome to my solution to the one billion row challenge!
//
// this thing is wildly unsafe, but the main function could be very used as a skeleton for most
// other parallel line-based file processing tasks, i think.
//
// latest benchmarking run landed at an average of 2.75 seconds for the full 1B rows, across 10
// runs, on my m1 macbook.
fn main() {
    // println!("using chunk size of: {:?}", BUF_SIZE);
    // let begin = std::time::Instant::now();

    // Step 1. We spin up NUM_CORES worker threads that will process chunks sent from the main
    // reader thread.

    let num_threads = std::thread::available_parallelism().unwrap().get() - 0;
    // This is the channel where we send the actual chunks, along with the used length and any
    // leftover partial record from the prior chunk.
    let (tx, rx) = channel::bounded::<(Box<[u8]>, usize, Option<Vec<u8>>)>(4);
    // This is the hack for doing some hacky buffer pooling. When we're done with a buffer, we send
    // it back over this channel, for potential reuse
    let (buf_tx, buf_rx) = channel::bounded::<Box<[u8]>>(4);
    let mut threads = Vec::new();
    for _ in 0..num_threads {
        let rx = rx.clone();
        let buf_tx = buf_tx.clone();
        threads.push(std::thread::spawn(move || {
            let mut totals = AggMap::default();
            // main work loop. get chunks from the channel and put it into the totals map
            while let Ok((buf, read, leftover)) = rx.recv() {
                let mut start_offset = 0;
                if let Some(mut leftover) = leftover {
                    while buf[start_offset] != b'\n' {
                        leftover.push(buf[start_offset]);
                        start_offset += 1;
                    }
                    leftover.push(b'\n');
                    process_chunk(&leftover, &mut totals);
                }
                process_chunk(&buf[start_offset..read], &mut totals);
                buf_tx.try_send(buf).ok();
            }
            totals
        }));
    }

    let mut input = std::fs::File::open("./measurements.txt").unwrap();
    let mut read;
    let mut leftover = None;
    // let mut num_chunks = 0;
    // let mut num_allocs = 0;
    loop {
        let mut buf = buf_rx.try_recv().unwrap_or_else(|_| unsafe {
            // num_allocs += 1;
            Box::<[u8]>::from_raw(std::alloc::alloc(Layout::from_size_align_unchecked(
                BUF_SIZE, 16384,
            )) as *mut [u8; BUF_SIZE])
        });
        read = input.read(&mut buf).unwrap();
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
        // num_chunks += 1;
    }
    drop(tx);

    // let begin_summarize = std::time::Instant::now();
    let mut totals = AggMap::default();
    for t in threads {
        for (name, agg) in t.join().unwrap() {
            totals.entry(name).or_default().merge(agg);
        }
    }

    let mut result = totals.into_iter().collect::<Vec<_>>();
    result.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    // let total_counts: f64 = result.iter().map(|v| v.1.count as f64).sum();
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

    // // We made it!
    // let end = std::time::Instant::now();
    // println!("elapsed: {}ms", (end - begin).as_millis());
    // println!(
    //     "elapsed (summary only): {}ms",
    //     (end - begin_summarize).as_millis()
    // );
    // println!(
    //     "real input would take roughly: {} seconds",
    //     (end - begin).as_secs_f64() * (1_000_000_000. / total_counts)
    // );

    // // Let's just grab both the new result file and the expected result to make sure they're right!
    // let output = std::fs::read_to_string("./result.txt").unwrap();
    // let expected = std::fs::read_to_string("./result-expected.txt").unwrap();
    // if output == expected {
    //     println!("File matched expected result. :3");
    // } else {
    //     eprintln!("oh no! file did not match expected output");
    // }

    // println!("num_chunks: {:?}", num_chunks);
    // println!("num_allocs: {:?}", num_allocs);

    std::process::exit(0);
}

// here we can tweak the buffer size. anything decently big but not too big seems to be reasonably
// fast.
pub const BUF_SIZE: usize = 1 << 23;

// this can be tweaked - i32 fits all the sums, but barely!
type N = i32;
type StrImpl = String;
type AggMap = FxHashMap<StrImpl, Aggregate>;

#[derive(Debug, PartialEq)]
struct Aggregate {
    count: N,
    sum: N,
    min: N,
    max: N,
}
impl Aggregate {
    #[inline]
    pub fn insert(&mut self, num: N) {
        self.count += 1;
        self.sum += num;
        self.min = self.min.min(num);
        self.max = self.max.max(num);
    }
    #[inline]
    pub fn merge(&mut self, other: Aggregate) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}
impl Default for Aggregate {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0,
            min: N::MAX,
            max: N::MIN,
        }
    }
}

// this chunk parser is real cursed, and will absolutely come with plenty of UB if you point it at
// anything other than a perfectly formatted input.
fn process_chunk(chunk: &[u8], totals: &mut AggMap) {
    unsafe {
        let mut i = chunk.len() - 1;
        loop {
            i -= 1;
            let mut val: N = (*chunk.get_unchecked(i) - b'0') as N
                + ((*chunk.get_unchecked(i - 2) - b'0') as N) * 10;
            i -= 3;
            if let c @ b'0'..=b'9' = *chunk.get_unchecked(i) {
                val += ((c - b'0') as N) * 100;
                i -= 1;
            }
            if *chunk.get_unchecked(i) == b'-' {
                val = -val;
                i -= 1;
            }
            let end = i;
            i -= 1;
            while i > 0 && *chunk.get_unchecked(i - 1) != b'\n' {
                i -= 1;
            }
            let location = std::str::from_utf8_unchecked(&chunk[i..end]);
            if let Some(entry) = totals.get_mut(location) {
                entry.insert(val);
            } else {
                totals.insert(
                    location.to_string(),
                    Aggregate {
                        count: 1,
                        sum: val,
                        min: val,
                        max: val,
                    },
                );
            }
            if i < 2 {
                break;
            }
            i -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_chunk_1() {
        let input = r#"Gobernador Virasora;4.4
Bālgudar;-57.8
Formigine;5.2
Taraz;-43.3
"#;
        let mut out = AggMap::default();
        process_chunk(input.as_bytes(), &mut out);
        println!("out: {:?}", out);

        assert_eq!(
            {
                let mut v = out.keys().collect::<Vec<_>>();
                v.sort();
                v
            },
            vec!["Bālgudar", "Formigine", "Gobernador Virasora", "Taraz",]
        );
        assert_eq!(
            {
                let mut v = out.into_iter().collect::<Vec<_>>();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                v
            },
            vec![
                (
                    StrImpl::from("Bālgudar"),
                    Aggregate {
                        count: 1,
                        sum: -578,
                        min: -578,
                        max: -578
                    }
                ),
                (
                    StrImpl::from("Formigine"),
                    Aggregate {
                        count: 1,
                        sum: 52,
                        min: 52,
                        max: 52
                    }
                ),
                (
                    StrImpl::from("Gobernador Virasora"),
                    Aggregate {
                        count: 1,
                        sum: 44,
                        min: 44,
                        max: 44
                    }
                ),
                (
                    StrImpl::from("Taraz"),
                    Aggregate {
                        count: 1,
                        sum: -433,
                        min: -433,
                        max: -433
                    }
                ),
            ]
        );
    }

    #[test]
    fn test_process_chunk_2() {
        let input = "Craig;-3.5\n";
        let mut out = AggMap::default();
        process_chunk(input.as_bytes(), &mut out);
        let mut out = out.into_iter();
        assert_eq!(
            out.next(),
            Some((
                StrImpl::from("Craig"),
                Aggregate {
                    count: 1,
                    sum: -35,
                    min: -35,
                    max: -35
                }
            ))
        );
    }
}
