pub mod chunked_parallel;
pub mod chunked_parallel_aligned;
pub mod compute;
pub mod mmap_parallel;
// pub mod mmap_sequential;
pub mod read_to_end_parallel;
// pub mod read_to_end_sequential;

fn main() {
    // chunked_parallel::main();
    chunked_parallel_aligned::main();
    // mmap_parallel::main();
    // read_to_end_parallel::main();

    // let kind = std::env::args()
    //     .nth(1)
    //     .unwrap_or_else(|| "chunked_parallel_aligned".to_string());
    // match kind.as_str() {
    //     "chunked_parallel" => chunked_parallel::main(),
    //     "chunked_parallel_aligned" => chunked_parallel_aligned::main(),
    //     "mmap_parallel" => mmap_parallel::main(),
    //     "mmap_sequential" => mmap_sequential::main(),
    //     "read_to_end_sequential" => read_to_end_sequential::main(),
    //     "read_to_end_parallel" => read_to_end_parallel::main(),
    //     v => eprintln!("unknown kind: {v}"),
    // }
}
