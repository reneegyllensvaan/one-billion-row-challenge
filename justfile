build:
  cargo +nightly build --release

_time TARGET: build
  /usr/bin/time -l ./target/release/one-billion-lines-challenge {{TARGET}}

time-chunked-parallel:
  just _time chunked_parallel
time-chunked-parallel-aligned:
  just _time chunked_parallel_aligned
