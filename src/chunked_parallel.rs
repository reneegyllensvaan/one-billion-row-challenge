use rustc_hash::FxHashMap;
use smol_str::SmolStr;
use std::{
    collections::hash_map::Entry,
    fmt::Write,
    io::{Read, Seek},
};

// hello and welcome to my soluion to the one-billion line challenge. i've tried to comment
// what im doing as thoroughly as possible, mostly for my own reference in the future when i
// inevitably go back and copy some of this code when i want some fast chunked file processing.
//
// currently this code runs on the full 1B input on my M1 MBP in like 6.5 seconds, which
// definitely isn't as fast as you can go, but i'm pretty happy with it.
pub fn main() {
    println!("using chunk size of: {:?}", BUF_SIZE);
    let begin = std::time::Instant::now();

    let mut input = std::fs::File::open("./measurements.txt").unwrap();

    // use NUM_CORES threads for processing. this might be worth tweaking. i haven't seen huge
    // differences in different tweaks to this. the important thing is it's roughly this number of
    // threads.
    //
    // previously i spun up  a thread per chunk - that was expensive, probably bc synchronization
    let num_threads = std::thread::available_parallelism().unwrap().get();

    // this is the channel for sending chunks from the reader to the workers. i make space for a
    // couple extra chunks. leaving this bigger means less likelihood of workers waiting, but more
    // memory. beware - too much memory can also mean slower
    let (tx, rx) = crossbeam::channel::bounded::<(Box<[u8; BUF_SIZE]>, usize, usize)>(num_threads);

    // this is pretty common for working with threads - spawning a thread gives back a join handle,
    // that you can use to block on the thread finishing and get the closure's return value back
    // from the thread. this vector collects the join handles.
    let mut threads = Vec::new();
    for _ in 0..num_threads {
        // we grab a clone of the receive handle to move into our closure. not doing this, or
        // trying to clone inside the closure is no good, as we'd either hold a reference or move
        // the same value multiple times. clone once per move is the way to go.
        let rx = rx.clone();

        threads.push(std::thread::spawn(move || {
            // here, we're not collecting every value, just the current count, sum, min, and max.
            // the exercise has us calculating the mean, min, and max, and this is the minimal
            // amount of data we need to capture to keep track of that.
            let mut totals = FxHashMap::<StrImpl, (usize, i64, i64, i64)>::default();

            while let Ok((buf, read, chopped_tail)) = rx.recv() {
                // slice the chunk down to the last newline. this depends on us actually processing
                // that line elsewhere - see where we seek the input stream below in the reader
                // part of the code.
                let chunk = &buf[0..read];
                let chunk = &chunk[0..(chunk.len() - chopped_tail)];

                // splitting chunks when they're still bytes is a little faster, since the split
                // iterator doesn't need to decode utf-8 then.
                for line in chunk.split(|c| *c == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    let sep_pos = line.iter().rev().position(|c| *c == b';').unwrap();
                    let (location, amount) = line.split_at(line.len() - sep_pos);

                    // here we're just totally trusting the input. it's fast, but also completely
                    // unsafe of course. if the file isn't valid utf-8, idk what it'll do. maybe UB
                    let location = unsafe {
                        std::str::from_utf8_unchecked(&location[0..(location.len() - 1)])
                    };

                    let amount: i64 = read_int(amount);
                    // the HashMap::entry API lets us insert-or-update a value and only hash the
                    // key once. we get back an Entry enum, which is either an existing location,
                    // or a newly-reserved slot in the map.
                    match totals.entry(StrImpl::from(location)) {
                        // if there's a value already, we update the value in place
                        Entry::Occupied(entry) => {
                            let v = entry.into_mut();
                            v.0 += 1;
                            v.1 += amount;
                            v.2 = v.2.min(amount);
                            v.3 = v.3.max(amount);
                        }
                        // if there's no existing value, we just insert a new one
                        Entry::Vacant(entry) => {
                            entry.insert((1, amount, amount, amount));
                        }
                    }
                }
            }
            totals
        }));
    }

    // ok, time for our reader routine. this thing reads the input in chunks. tweaking BUF_SIZE is
    // probably worthwhile, tuned to yr workload.
    let mut read;
    loop {
        // this thing allocates a buffer for each chunk read. one might be able to return buffers
        // to a pool or something, but that's more than i've felt necessary. keeping the BUF_SIZE a
        // multiple of the OS page size is probably a good idea - i just made sure to make it a
        // power of two.
        let mut buf = Box::new([0u8; BUF_SIZE]);
        read = input.read(buf.as_mut_slice()).unwrap();
        if read == 0 {
            break;
        }
        // since we're reading in fixed-size chunks, we need to figure out how far back the tail
        // position ends. that's what we do here. we find how long the incomplete final line is,
        // and rewind the input reader position by that much so we'll read it again into the next
        // chunk.
        //
        // note that this'll panic if there's a line longer than a chunk, and is probably slow with
        // long lines in general.
        let chopped_tail = buf[0..read].iter().rev().position(|&c| c == b'\n').unwrap();
        input
            .seek(std::io::SeekFrom::Current(-(chopped_tail as i64)))
            .unwrap();

        // now, just send the chunk to the worker queue! it needs the length and the tail size,
        // to do the same slicing of the last partial line as we did.
        tx.send((buf, read, chopped_tail)).unwrap();
    }

    // THIS PART IS IMPORTANT!
    // the receiver end of a channel stays open until all the sender sides have been closed, which
    // happens when they are dropped. by default, said drop will happen when tx goes out of scope.
    //
    // however, tx stays in scope until all threads have been joined (completed), and threads only
    // finish once their receiver has been closed, which never happens because the receiver only
    // closes once the sender has been dropped.
    //
    // try to remove this or move it after the `for t in threads` block, see what happens.
    drop(tx);

    let mut totals = FxHashMap::<StrImpl, (usize, i64, i64, i64)>::default();
    // now we can join all our threads and get back our partially aggregated map! then, we just
    // need to take those maps and group them up into a big map of aggregates of aggregates!
    for t in threads {
        for (name, (count, sum, min, max)) in t.join().unwrap() {
            let entry = totals
                .entry(name)
                .or_insert_with(|| (0, 0, i64::MAX, i64::MIN));
            // the total of counts is just the sum of counts
            entry.0 += count;
            // the total of sums is also just the sum of sums
            entry.1 += sum;
            // and the min of mins is min, max of maxes is just a max
            entry.2 = entry.2.min(min);
            entry.3 = entry.3.max(max);
        }
    }

    // Finally we simply take our map and tweak it into the output structure we want.
    let mut result = totals.into_iter().collect::<Vec<_>>();
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

    // We made it!
    let end = std::time::Instant::now();
    println!("elapsed: {}ms", (end - begin).as_millis());
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
}

// SmolStr is a cursed alternate string implementation that embeds short strings into the
// len/cap/ptr fields of a struct the same size as a String, and then uses the alignment bits of
// the ptr field to encode the discriminant. the mad geniuses.
type StrImpl = SmolStr;

// here we can tweak the buffer size. anything decently big but not too big seems to be reasonably
// fast.
const BUF_SIZE: usize = 1 << 21;

// welcome to my janky custom float-to-integer parser!
//
// obviously this only works if numbers are formatted exactly like in the problem outline - a value
// between -99.9 and 99.9, always with one decimal place.
fn read_int(s: &[u8]) -> i64 {
    match s {
        // minus negates the value, so we recurse on the tail after it
        [b'-', t @ ..] => -read_int(t),
        // for values below 10.0: parse out each ascii offset from the digits and multiply it by
        // their position in the number
        [t, b'.', u] => (t - b'0') as i64 * 10 + (u - b'0') as i64,
        // this part is for 3-digit measurements.
        [h, t, b'.', u] => (h - b'0') as i64 * 100 + (t - b'0') as i64 * 10 + (u - b'0') as i64,
        _ => panic!("unparseable number: {s:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_int() {
        let input = "-6.1";
        assert_eq!(read_int(input.as_bytes()), -61);
        let input = "-98.7";
        assert_eq!(read_int(input.as_bytes()), -987);
        let input = "78.9";
        assert_eq!(read_int(input.as_bytes()), 789);
        let input = "8.9";
        assert_eq!(read_int(input.as_bytes()), 89);
    }
}
