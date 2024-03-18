use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;
use smol_str::SmolStr;
use std::fmt::Write;

type StrImpl = SmolStr;
type AggMap = FxHashMap<StrImpl, (usize, i64, i64, i64)>;

pub fn main() {
    let begin = std::time::Instant::now();

    let input = std::fs::File::open("./measurements.txt").unwrap();
    let mmap = unsafe { memmap2::MmapOptions::new().map(&input) }.unwrap();

    // Get some points at which we split the file for each of the threads
    let num_threads = std::thread::available_parallelism().unwrap().get();
    let chunk_len = mmap.len() / num_threads;
    println!("chunk_len: {:?}", chunk_len);
    let mut split_points = Vec::new();
    for point in 0..num_threads {
        split_points.push(point * chunk_len);
    }
    split_points.push(mmap.len());

    // The above split points are not aligned to line endings. Let's fix that!
    let bounds = 1..(split_points.len() - 1);
    for point in split_points[bounds].iter_mut() {
        while mmap[*point - 1] != b'\n' {
            *point -= 1;
        }
    }
    println!("final split_points: {:?}", split_points);

    // spin up a thread for each chunk and process them.
    let result = std::thread::scope(|scope| {
        split_points
            .iter()
            .zip(split_points.iter().skip(1))
            .map(|(start, end)| scope.spawn(|| process_chunk(&mmap[*start..*end])))
            .collect::<Vec<_>>() // note: we collect here to eagerly spin up the threads
            .into_iter()
            .map(|v| v.join().unwrap())
            .reduce(merge_chunks)
            .unwrap()
    });

    let mut result = result.into_iter().collect::<Vec<_>>();
    result.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let total_counts: f64 = result.iter().map(|v| v.1 .0 as f64).sum();
    let mut out = String::with_capacity(result.len() * 35);
    for (k, v) in result {
        write!(
            out,
            "{}={:.1}/{:.1}/{:.1}\n",
            k,
            v.1 as f64 / v.0 as f64,
            v.2 as f64 / 10.,
            v.3 as f64 / 10.,
        )
        .unwrap();
    }
    std::fs::write("./result.txt", out).unwrap();

    let end = std::time::Instant::now();
    println!("elapsed: {}ms", (end - begin).as_millis());
    println!(
        "real input would take roughly: {} seconds",
        (end - begin).as_secs_f64() * (1_000_000_000. / total_counts)
    );
    // Let's just grab both the new result file and the expected result to make sure they're right!
    if total_counts == 1_000_000_000. {
        let output = std::fs::read_to_string("./result.txt").unwrap();
        let expected = std::fs::read_to_string("./result-expected.txt").unwrap();
        assert_eq!(output, expected);
        println!("File matched expected result. :3");
    } else {
        println!("input file was not 1B lines, ignoring assertions");
    }
}

fn merge_chunks(mut a: AggMap, b: AggMap) -> AggMap {
    for (name, (count, sum, min, max)) in b {
        let entry = a.entry(name).or_insert_with(|| (0, 0, i64::MAX, i64::MIN));
        entry.0 += count;
        entry.1 += sum;
        entry.2 = entry.2.min(min);
        entry.3 = entry.3.max(max);
    }
    a
}
fn process_chunk(chunk: &[u8]) -> AggMap {
    let mut totals = FxHashMap::<StrImpl, (usize, i64, i64, i64)>::default();
    for line in chunk.split(|c| *c == b'\n') {
        if line.is_empty() {
            continue;
        }
        let sep_pos = line.iter().rev().position(|c| *c == b';').unwrap();
        let (location, amount) = line.split_at(line.len() - sep_pos);
        let location = unsafe { std::str::from_utf8_unchecked(&location[0..(location.len() - 1)]) };
        let amount: i64 = read_int(amount);
        match totals.entry(StrImpl::from(location)) {
            Entry::Occupied(entry) => {
                let v = entry.into_mut();
                v.0 += 1;
                v.1 += amount;
                v.2 = v.2.min(amount);
                v.3 = v.3.max(amount);
            }
            Entry::Vacant(entry) => {
                entry.insert((1, amount, amount, amount));
            }
        }
    }
    totals
}
fn read_int(s: &[u8]) -> i64 {
    match s {
        [b'-', t @ ..] => -read_int(t),
        [t, b'.', u] => (t - b'0') as i64 * 10 + (u - b'0') as i64,
        [h, t, b'.', u] => (h - b'0') as i64 * 100 + (t - b'0') as i64 * 10 + (u - b'0') as i64,
        _ => panic!("unparseable number: {s:?}"),
    }
}
