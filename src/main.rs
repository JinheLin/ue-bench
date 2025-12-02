use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use sqlx::mysql::{MySqlPoolOptions, MySqlConnectOptions};
use sqlx::{Pool, MySql};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::{NaiveDateTime, Duration as ChronoDuration};

/// Solana DEX SQL Benchmarking Tool (5 Scenarios)
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    url: String,

    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,

    #[arg(short, long, default_value_t = 30)]
    duration: u64,

    #[arg(long, default_value_t = 2000)]
    sample_size: usize,

    /// Print detailed log and FULL SQL for EVERY query
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

struct ThreadStats {
    q1_latencies: Vec<u128>,
    q2_latencies: Vec<u128>,
    q3_latencies: Vec<u128>,
    q4_latencies: Vec<u128>,
    q5_latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            q1_latencies: Vec::with_capacity(1000),
            q2_latencies: Vec::with_capacity(1000),
            q3_latencies: Vec::with_capacity(1000),
            q4_latencies: Vec::with_capacity(1000),
            q5_latencies: Vec::with_capacity(1000),
            errors: 0,
        }
    }
}

const START_TIME_STR: &str = "2025-01-01 00:00:00";
const END_TIME_STR: &str = "2025-12-31 23:59:59";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let fmt = "%Y-%m-%d %H:%M:%S";
    let min_ts = NaiveDateTime::parse_from_str(START_TIME_STR, fmt).expect("Invalid time format");
    let max_ts = NaiveDateTime::parse_from_str(END_TIME_STR, fmt).expect("Invalid time format");

    println!("--- Starting Solana DEX Benchmark (Q1-Q5) ---");
    println!("Target DB: {}", args.url);
    println!("Concurrency: {}", args.concurrency);
    println!("Logging: {}", if args.verbose { "ENABLED (Full SQL)" } else { "DISABLED" });
    
    let opts = MySqlConnectOptions::from_str(&args.url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    println!("> Sampling {} addresses from database...", args.sample_size);
    let sampled_addresses = fetch_sample_addresses(&pool, args.sample_size).await?;
    
    if sampled_addresses.is_empty() {
        eprintln!("❌ Error: Table seems empty.");
        return Ok(());
    }
    println!("> Successfully loaded {} unique addresses.", sampled_addresses.len());

    let shared_addresses = Arc::new(sampled_addresses);
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    
    let mut handles = vec![];

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let addresses = shared_addresses.clone(); 
        let verbose = args.verbose;
        let task_min_ts = min_ts;
        let task_max_ts = max_ts;
        
        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();
            
            while start_time.elapsed() < run_duration {
                // Randomly select scenario 1 to 5
                let scenario = rng.gen_range(1..=5);
                
                let result = match scenario {
                    1 => run_q1(&pool, &mut rng, task_min_ts, task_max_ts, &addresses, verbose).await,
                    2 => run_q2(&pool, &mut rng, task_min_ts, task_max_ts, &addresses, verbose).await,
                    3 => run_q3(&pool, &mut rng, task_min_ts, task_max_ts, &addresses, verbose).await,
                    4 => run_q4(&pool, &mut rng, task_min_ts, task_max_ts, &addresses, verbose).await,
                    5 => run_q5(&pool, &mut rng, task_min_ts, task_max_ts, &addresses, verbose).await,
                    _ => Ok(Duration::new(0, 0)),
                };

                match result {
                    Ok(duration) => {
                        let micros = duration.as_micros();
                        match scenario {
                            1 => stats.q1_latencies.push(micros),
                            2 => stats.q2_latencies.push(micros),
                            3 => stats.q3_latencies.push(micros),
                            4 => stats.q4_latencies.push(micros),
                            5 => stats.q5_latencies.push(micros),
                            _ => {}
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        stats.errors += 1;
                    }
                }
            }
            stats
        });
        handles.push(handle);
    }

    let mut total_errors = 0;
    let mut all_q1 = Vec::new();
    let mut all_q2 = Vec::new();
    let mut all_q3 = Vec::new();
    let mut all_q4 = Vec::new();
    let mut all_q5 = Vec::new();

    for handle in handles {
        let stats = handle.await?;
        total_errors += stats.errors;
        all_q1.extend(stats.q1_latencies);
        all_q2.extend(stats.q2_latencies);
        all_q3.extend(stats.q3_latencies);
        all_q4.extend(stats.q4_latencies);
        all_q5.extend(stats.q5_latencies);
    }

    let elapsed = start_time.elapsed();
    let total_requests = all_q1.len() + all_q2.len() + all_q3.len() + all_q4.len() + all_q5.len();
    let qps = total_requests as f64 / elapsed.as_secs_f64();

    println!("\n--- 📊 Benchmark Summary ---");
    println!("Total Time: {:.2?}", elapsed);
    println!("Total Requests: {}", total_requests);
    println!("Total Errors: {}", total_errors);
    println!("QPS: {:.2}", qps);

    println!("\n--- ⏱️  Latency Statistics (ms) ---");
    print_percentiles("Q1 (Simple Select)", &mut all_q1);
    print_percentiles("Q2 (Range Select)", &mut all_q2);
    print_percentiles("Q3 (Force Index)", &mut all_q3);
    print_percentiles("Q4 (Simple Count)", &mut all_q4);
    print_percentiles("Q5 (Range Count)", &mut all_q5);

    Ok(())
}

fn print_percentiles(name: &str, latencies: &mut Vec<u128>) {
    if latencies.is_empty() {
        println!("{}: No data", name);
        return;
    }
    
    latencies.sort_unstable();

    let len = latencies.len() as f64;
    let p50_idx = (len * 0.50) as usize;
    let p95_idx = (len * 0.95) as usize;
    let p99_idx = (len * 0.99) as usize;
    let max_idx = latencies.len() - 1;

    let to_ms = |us: u128| us as f64 / 1000.0;

    println!(
        "{:<25} | Count: {:<6} | P50: {:.2} ms | P95: {:.2} ms | P99: {:.2} ms | Max: {:.2} ms",
        name,
        latencies.len(),
        to_ms(latencies[p50_idx]),
        to_ms(latencies[p95_idx]),
        to_ms(latencies[p99_idx]),
        to_ms(latencies[max_idx])
    );
}

async fn fetch_sample_addresses(pool: &Pool<MySql>, limit: usize) -> Result<Vec<String>, sqlx::Error> {
    let fetch_limit = limit * 2; 
    let query = format!("SELECT token0_address FROM dex_swap_tx_solana LIMIT {}", fetch_limit);
    let rows: Vec<String> = sqlx::query_scalar(&query).fetch_all(pool).await?;
    let unique_set: HashSet<String> = rows.into_iter().collect();
    Ok(unique_set.into_iter().take(limit).collect())
}

fn get_test_address(rng: &mut impl Rng, pool: &[String]) -> String {
    pool.choose(rng).expect("Pool empty").clone()
}

fn get_random_platform(rng: &mut impl Rng) -> i32 {
    *[9, 16].choose(rng).unwrap()
}

fn get_random_time(rng: &mut impl Rng, min: NaiveDateTime, max: NaiveDateTime) -> NaiveDateTime {
    let total_secs = (max - min).num_seconds();
    if total_secs <= 0 { return min; }
    let offset = rng.gen_range(0..total_secs);
    min + ChronoDuration::seconds(offset)
}

// --- SQL Queries ---

async fn run_q1(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    min_ts: NaiveDateTime, 
    max_ts: NaiveDateTime,
    addr_pool: &[String],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let address = get_test_address(rng, addr_pool);
    let platform = get_random_platform(rng);
    let ts_boundary = get_random_time(rng, min_ts, max_ts);

    let start = Instant::now();
    
    let rows = sqlx::query("SELECT * FROM dex_swap_tx_solana WHERE token0_address = ? AND platform = ? AND ts < ? LIMIT 5")
        .bind(&address)
        .bind(platform)
        .bind(ts_boundary)
        .fetch_all(pool)
        .await?;

    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q1] Time: {:?} | Rows: {}", duration, rows.len());
        println!("SQL: SELECT * FROM dex_swap_tx_solana WHERE token0_address = '{}' AND platform = {} AND ts < '{}' LIMIT 5;", 
            address, platform, ts_boundary);
    }

    Ok(duration)
}

async fn run_q2(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    min_ts: NaiveDateTime, 
    max_ts: NaiveDateTime,
    addr_pool: &[String],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let address = get_test_address(rng, addr_pool);
    let platform = get_random_platform(rng);
    let no_anchor: i8 = rng.gen_range(0..=1);
    
    let end_limit = get_random_time(rng, min_ts, max_ts);
    let days_back = rng.gen_range(1..=7);
    let start_limit = std::cmp::max(min_ts, end_limit - ChronoDuration::days(days_back));

    let start = Instant::now();

    let rows = sqlx::query(
        r#"
        SELECT * FROM dex_swap_tx_solana 
        WHERE token0_address = ? 
        AND platform = ? 
        AND no_anchor = ? 
        AND ts >= ? AND ts <= ? 
        ORDER BY ts asc, height asc, tx_id asc, log_id asc 
        LIMIT 10
        "#
    )
    .bind(&address)
    .bind(platform)
    .bind(no_anchor)
    .bind(start_limit)
    .bind(end_limit)
    .fetch_all(pool)
    .await?;

    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q2] Time: {:?} | Rows: {}", duration, rows.len());
        println!("SQL: SELECT * FROM dex_swap_tx_solana WHERE token0_address = '{}' AND platform = {} AND no_anchor = {} AND ts >= '{}' AND ts <= '{}' ORDER BY ts asc, height asc, tx_id asc, log_id asc LIMIT 10;",
            address, platform, no_anchor, start_limit, end_limit);
    }

    Ok(duration)
}

async fn run_q3(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    min_ts: NaiveDateTime, 
    max_ts: NaiveDateTime,
    addr_pool: &[String],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let address = get_test_address(rng, addr_pool);
    let platform = get_random_platform(rng);
    let ts_boundary = get_random_time(rng, min_ts, max_ts);

    let start = Instant::now();

    let rows = sqlx::query(
        r#"
        SELECT * FROM dex_swap_tx_solana USE INDEX (idx_desc) 
        WHERE token0_address = ? 
        AND platform = ? 
        AND ts < ? 
        ORDER BY ts DESC 
        LIMIT 50
        "#
    )
    .bind(&address)
    .bind(platform)
    .bind(ts_boundary)
    .fetch_all(pool)
    .await?;

    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q3] Time: {:?} | Rows: {}", duration, rows.len());
        println!("SQL: SELECT * FROM dex_swap_tx_solana USE INDEX (idx_desc) WHERE token0_address = '{}' AND platform = {} AND ts < '{}' ORDER BY ts DESC LIMIT 50;",
            address, platform, ts_boundary);
    }

    Ok(duration)
}

async fn run_q4(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    min_ts: NaiveDateTime, 
    max_ts: NaiveDateTime,
    addr_pool: &[String],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let address = get_test_address(rng, addr_pool);
    let platform = get_random_platform(rng);
    let ts_boundary = get_random_time(rng, min_ts, max_ts);

    let start = Instant::now();
    
    // query_scalar 用于直接获取单列单行的值 (例如 count)
    // 返回类型通常是 i64
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dex_swap_tx_solana WHERE token0_address = ? AND platform = ? AND ts < ?"
    )
    .bind(&address)
    .bind(platform)
    .bind(ts_boundary)
    .fetch_one(pool)
    .await?;

    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q4] Time: {:?} | Count: {}", duration, count);
        println!("SQL: SELECT count(*) FROM dex_swap_tx_solana WHERE token0_address = '{}' AND platform = {} AND ts < '{}';",
            address, platform, ts_boundary);
    }

    Ok(duration)
}

async fn run_q5(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    min_ts: NaiveDateTime, 
    max_ts: NaiveDateTime,
    addr_pool: &[String],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let address = get_test_address(rng, addr_pool);
    let platform = get_random_platform(rng);
    let no_anchor: i8 = rng.gen_range(0..=1);

    let end_limit = get_random_time(rng, min_ts, max_ts);
    let days_back = rng.gen_range(1..=7);
    let start_limit = std::cmp::max(min_ts, end_limit - ChronoDuration::days(days_back));

    let start = Instant::now();

    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM dex_swap_tx_solana 
        WHERE token0_address = ? 
        AND platform = ? 
        AND no_anchor = ? 
        AND ts >= ? AND ts <= ?
        "#
    )
    .bind(&address)
    .bind(platform)
    .bind(no_anchor)
    .bind(start_limit)
    .bind(end_limit)
    .fetch_one(pool)
    .await?;

    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q5] Time: {:?} | Count: {}", duration, count);
        println!("SQL: SELECT count(*) FROM dex_swap_tx_solana WHERE token0_address = '{}' AND platform = {} AND no_anchor = {} AND ts >= '{}' AND ts <= '{}';",
            address, platform, no_anchor, start_limit, end_limit);
    }

    Ok(duration)
}