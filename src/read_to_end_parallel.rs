use std::io::Read;

use std::fmt::Write;

use crate::compute::AggMap;

pub fn main() {
    let begin_io = std::time::Instant::now();

    let mut input: Vec<u8> = vec![];
    std::fs::File::open("./measurements.txt")
        .unwrap()
        .read_to_end(&mut input)
        .unwrap();

    let begin_compute = std::time::Instant::now();

    // Get some points at which we split the file for each of the threads
    let num_threads = std::thread::available_parallelism().unwrap().get();
    let chunk_len = input.len() / num_threads;
    println!("chunk_len: {:?}", chunk_len);
    let mut split_points = Vec::new();
    for point in 0..num_threads {
        split_points.push(point * chunk_len);
    }
    split_points.push(input.len());

    // The above split points are not aligned to line endings. Let's fix that!
    let bounds = 1..(split_points.len() - 1);
    for point in split_points[bounds].iter_mut() {
        while input[*point - 1] != b'\n' {
            *point -= 1;
        }
    }
    println!("final split_points: {:?}", split_points);

    // spin up a thread for each chunk and process them.
    let result = std::thread::scope(|scope| {
        split_points
            .iter()
            .zip(split_points.iter().skip(1))
            .map(|(start, end)| scope.spawn(|| process_chunk(&input[*start..*end])))
            .collect::<Vec<_>>() // note: we collect here to eagerly spin up the threads
            .into_iter()
            .map(|v| v.join().unwrap())
            .reduce(merge_chunks)
            .unwrap()
    });

    let mut result = result.into_iter().collect::<Vec<_>>();
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

    let end = std::time::Instant::now();
    println!("elapsed (total): {}ms", (end - begin_io).as_millis());
    println!("elapsed (compute): {}ms", (end - begin_compute).as_millis());
    println!(
        "real input would take roughly: {} seconds",
        (end - begin_io).as_secs_f64() * (1_000_000_000. / total_counts)
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
    for (name, r) in b {
        a.entry(name).or_default().merge(r);
    }
    a
}

fn process_chunk(chunk: &[u8]) -> AggMap {
    let mut totals = AggMap::default();
    crate::compute::process_chunk(chunk, &mut totals);
    totals
}
