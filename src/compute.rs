use std::collections::hash_map::Entry;
type N = i32;
pub type StrImpl = arrayvec::ArrayString<36>;
pub type AggMap = rustc_hash::FxHashMap<StrImpl, Aggregate>;

#[derive(Debug, PartialEq)]
pub struct Aggregate {
    pub count: N,
    pub sum: N,
    pub min: N,
    pub max: N,
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

pub fn process_chunk(chunk: &[u8], totals: &mut AggMap) {
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

            match totals.entry(StrImpl::from(location).unwrap()) {
                Entry::Occupied(entry) => {
                    entry.into_mut().insert(val);
                }
                Entry::Vacant(entry) => {
                    entry.insert(Aggregate {
                        count: 1,
                        sum: val,
                        min: val,
                        max: val,
                    });
                }
            }

            if i < 2 {
                break;
            }
            i -= 1;
        }
    }
}

// // welcome to my janky custom float-to-integer parser!
// //
// // obviously this only works if numbers are formatted exactly like in the problem outline - a value
// // between -99.9 and 99.9, always with one decimal place.
// fn read_int(s: &[u8]) -> N {
//     match s {
//         // minus negates the value, so we recurse on the tail after it
//         [b'-', t @ ..] => -read_int(t),
//         // for values below 10.0: parse out each ascii offset from the digits and multiply it by
//         // their position in the number
//         [t, b'.', u] => (t - b'0') as N * 10 + (u - b'0') as N,
//         // this part is for 3-digit measurements.
//         [h, t, b'.', u] => (h - b'0') as N * 100 + (t - b'0') as N * 10 + (u - b'0') as N,
//         _ => panic!("unparseable number: {s:?}"),
//     }
// }
// fn process_chunk(chunk: &[u8], totals: &mut AggMap) {
//     for line in chunk.split(|c| *c == b'\n') {
//         if line.is_empty() {
//             continue;
//         }
//         let sep_pos = unsafe {
//             line.iter()
//                 .rev()
//                 .position(|c| *c == b';')
//                 .unwrap_unchecked()
//         };
//         let (location, amount) = line.split_at(line.len() - sep_pos);
//         let location = unsafe { std::str::from_utf8_unchecked(&location[0..(location.len() - 1)]) };
//         let amount: N = read_int(amount);
//         match totals.entry(StrImpl::from(location).unwrap()) {
//             Entry::Occupied(entry) => {
//                 entry.into_mut().insert(amount);
//             }
//             Entry::Vacant(entry) => {
//                 entry.insert(Aggregate {
//                     count: 1,
//                     sum: amount,
//                     min: amount,
//                     max: amount,
//                 });
//             }
//         }
//     }
// }

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
                v.sort_by_key(|x| x.0);
                v
            },
            vec![
                (
                    StrImpl::from("Bālgudar").unwrap(),
                    Aggregate {
                        count: 1,
                        sum: -578,
                        min: -578,
                        max: -578
                    }
                ),
                (
                    StrImpl::from("Formigine").unwrap(),
                    Aggregate {
                        count: 1,
                        sum: 52,
                        min: 52,
                        max: 52
                    }
                ),
                (
                    StrImpl::from("Gobernador Virasora").unwrap(),
                    Aggregate {
                        count: 1,
                        sum: 44,
                        min: 44,
                        max: 44
                    }
                ),
                (
                    StrImpl::from("Taraz").unwrap(),
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
                StrImpl::from("Craig").unwrap(),
                Aggregate {
                    count: 1,
                    sum: -35,
                    min: -35,
                    max: -35
                }
            ))
        );
    }

    // #[test]
    // fn test_read_int() {
    //     let input = "-6.1";
    //     assert_eq!(read_int(input.as_bytes()), -61);
    //     let input = "-98.7";
    //     assert_eq!(read_int(input.as_bytes()), -987);
    //     let input = "78.9";
    //     assert_eq!(read_int(input.as_bytes()), 789);
    //     let input = "8.9";
    //     assert_eq!(read_int(input.as_bytes()), 89);
    // }
}
