use anyhow::Result;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_env = "msvc"))]
// #[allow(non_upper_case_globals)]
#[export_name = "_RJEM_MALLOC_CONF"]
pub static MALLOC_CONF: &[u8] = b"background_thread:true,prof:true,prof_active:true,lg_prof_interval:30,lg_prof_sample:21,prof_prefix:/tmp/jeprof\0";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    ceramic_one::run().await
}
