use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use sqlx::mysql::{MySqlPoolOptions, MySqlConnectOptions};
use sqlx::{Pool, MySql, FromRow};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc, Duration as ChronoDuration};

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

    /// Max days back for range queries
    #[arg(long, default_value_t = 7)]
    days_back: i64,

    #[arg(long, default_value_t = false)]
    verbose: bool,
}

struct ThreadStats {
    q1_latencies: Vec<u128>,
    q2_latencies: Vec<u128>,
    q3_latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            q1_latencies: Vec::with_capacity(1000),
            q2_latencies: Vec::with_capacity(1000),
            q3_latencies: Vec::with_capacity(1000),
            errors: 0,
        }
    }
}

#[derive(Clone, Debug, FromRow)]
struct SampleData {
    token0_address: String,
    ts: DateTime<Utc>, 
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.days_back < 1 {
        panic!("❌ --days-back must be at least 1");
    }

    println!("--- Starting Solana DEX Benchmark (Smart Sampling & Full SQL) ---");
    println!("Concurrency: {}", args.concurrency);
    println!("Max Days Back: {}", args.days_back);
    println!("Logging: {}", if args.verbose { "ENABLED (Full SQL)" } else { "DISABLED" });
    
    let opts = MySqlConnectOptions::from_str(&args.url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    println!("> Sampling {} rows (address + ts) from database...", args.sample_size);
    let sampled_data = fetch_sample_data(&pool, args.sample_size).await?;
    
    if sampled_data.is_empty() {
        eprintln!("❌ Error: Table seems empty.");
        return Ok(());
    }
    println!("> Successfully loaded {} unique sample points.", sampled_data.len());

    let shared_data = Arc::new(sampled_data);
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    
    let mut handles = vec![];

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let data_pool = shared_data.clone(); 
        let verbose = args.verbose;
        let max_days_back = args.days_back;
        
        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();
            
            while start_time.elapsed() < run_duration {
                let scenario = rng.gen_range(1..=5);
                
                let result = match scenario {
                    1 => run_q1(&pool, &mut rng, &data_pool, verbose).await,
                    2 => run_q2(&pool, &mut rng, &data_pool, max_days_back, verbose).await,
                    3 => run_q3(&pool, &mut rng, &data_pool, verbose).await,
                    _ => Ok(Duration::new(0, 0)),
                };

                match result {
                    Ok(duration) => {
                        let micros = duration.as_micros();
                        match scenario {
                            1 => stats.q1_latencies.push(micros),
                            2 => stats.q2_latencies.push(micros),
                            3 => stats.q3_latencies.push(micros),
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

    for handle in handles {
        let stats = handle.await?;
        total_errors += stats.errors;
        all_q1.extend(stats.q1_latencies);
        all_q2.extend(stats.q2_latencies);
        all_q3.extend(stats.q3_latencies);
    }

    let elapsed = start_time.elapsed();
    let total_requests = all_q1.len() + all_q2.len() + all_q3.len();
    let qps = total_requests as f64 / elapsed.as_secs_f64();

    println!("\n--- 📊 Benchmark Summary ---");
    println!("Total Time: {:.2?}", elapsed);
    println!("Total Requests: {}", total_requests);
    println!("Total Errors: {}", total_errors);
    println!("QPS: {:.2}", qps);

    println!("\n--- ⏱️  Latency Statistics (ms) ---");
    print_percentiles("Q1", &mut all_q1);
    print_percentiles("Q2", &mut all_q2);
    print_percentiles("Q3", &mut all_q3);

    Ok(())
}

fn print_percentiles(name: &str, latencies: &mut Vec<u128>) {
    if latencies.is_empty() {
        println!("{}: No data", name);
        return;
    }
    latencies.sort_unstable();
    let len = latencies.len() as f64;
    let p50 = latencies[(len * 0.50) as usize];
    let p95 = latencies[(len * 0.95) as usize];
    let p99 = latencies[(len * 0.99) as usize];
    let max = latencies[latencies.len() - 1];

    let to_ms = |us: u128| us as f64 / 1000.0;

    println!("{:<25} | Cnt:{:<5} | P50:{:.2}ms | P95:{:.2}ms | P99:{:.2}ms | Max:{:.2}ms",
        name, latencies.len(), to_ms(p50), to_ms(p95), to_ms(p99), to_ms(max));
}

async fn fetch_sample_data(pool: &Pool<MySql>, limit: usize) -> Result<Vec<SampleData>, sqlx::Error> {
    let fetch_limit = limit; 
    let query = format!("SELECT token0_address, ts FROM dex_swap_tx_solana LIMIT {}", fetch_limit);
    let rows: Vec<SampleData> = sqlx::query_as(&query).fetch_all(pool).await?;
    Ok(rows)
}

fn get_test_sample<'a>(rng: &mut impl Rng, pool: &'a [SampleData]) -> &'a SampleData {
    pool.choose(rng).expect("Pool empty")
}

fn get_random_platform(rng: &mut impl Rng) -> i32 {
    *[16].choose(rng).unwrap()
}

fn get_random_no_anchor(rng: &mut impl Rng) -> i8 {
    *[0].choose(rng).unwrap()
}

// --- SQL Queries ---

async fn run_q1(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    data_pool: &[SampleData],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let sample = get_test_sample(rng, data_pool);
    let platform = get_random_platform(rng);
    
    let offset_seconds = rng.gen_range(1..86400); 
    let query_ts = sample.ts + ChronoDuration::seconds(offset_seconds);

    let start = Instant::now();
    let rows = sqlx::query("SELECT * FROM dex_swap_tx_solana WHERE token0_address = ? AND platform = ? AND ts < ? LIMIT 5")
        .bind(&sample.token0_address)
        .bind(platform)
        .bind(query_ts)
        .fetch_all(pool)
        .await?;
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q1] Time: {:?} | Rows: {}", duration.as_millis(), rows.len());
        println!("SQL: SELECT * FROM dex_swap_tx_solana WHERE token0_address = '{}' AND platform = {} AND ts < '{}' LIMIT 5;", 
            sample.token0_address, 
            platform, 
            query_ts.format("%Y-%m-%d %H:%M:%S")
        );
    }
    Ok(duration)
}

async fn run_q2(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    data_pool: &[SampleData],
    max_days_back: i64,
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let sample = get_test_sample(rng, data_pool);
    let platform = get_random_platform(rng);
    let no_anchor: i8 = get_random_no_anchor(rng);
    
    let window_days = rng.gen_range(1..=max_days_back);
    let window_seconds = window_days * 86400;

    let offset_seconds = rng.gen_range(0..window_seconds);
    let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
    let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

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
    .bind(&sample.token0_address)
    .bind(platform)
    .bind(no_anchor)
    .bind(start_limit)
    .bind(end_limit)
    .fetch_all(pool)
    .await?;
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q2] Time: {:?} | Rows: {}", duration.as_millis(), rows.len());
        println!("SQL: SELECT * FROM dex_swap_tx_solana WHERE token0_address = '{}' AND platform = {} AND no_anchor = {} AND ts >= '{}' AND ts <= '{}' ORDER BY ts asc, height asc, tx_id asc, log_id asc LIMIT 10;",
            sample.token0_address, 
            platform, 
            no_anchor, 
            start_limit.format("%Y-%m-%d %H:%M:%S"), 
            end_limit.format("%Y-%m-%d %H:%M:%S")
        );
    }
    Ok(duration)
}

async fn run_q3(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    data_pool: &[SampleData],
    verbose: bool
) -> Result<Duration, sqlx::Error> {
    let sample = get_test_sample(rng, data_pool);
    let platform = get_random_platform(rng);
    
    let offset_seconds = rng.gen_range(1..86400); 
    let query_ts = sample.ts + ChronoDuration::seconds(offset_seconds);

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
    .bind(&sample.token0_address)
    .bind(platform)
    .bind(query_ts)
    .fetch_all(pool)
    .await?;
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[Q3] Time: {:?} | Rows: {}", duration.as_millis(), rows.len());
        println!("SQL: SELECT * FROM dex_swap_tx_solana USE INDEX (idx_desc) WHERE token0_address = '{}' AND platform = {} AND ts < '{}' ORDER BY ts DESC LIMIT 50;",
            sample.token0_address, 
            platform, 
            query_ts.format("%Y-%m-%d %H:%M:%S")
        );
    }
    Ok(duration)
}

