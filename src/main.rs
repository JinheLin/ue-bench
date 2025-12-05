use chrono::NaiveDateTime;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::Parser;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::types::BigDecimal;
use sqlx::{FromRow, MySql, Pool};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

    #[arg(long, default_value_t = false)]
    verify: bool,
}

#[derive(Clone, Debug, FromRow)]
struct SampleData {
    token0_address: String,
    ts: DateTime<Utc>,
}

/// 对应数据库表: `dex_swap_tx_solana`
#[derive(Debug, Clone, FromRow, Eq, PartialEq)]
pub struct DexSwapTxSolana {
    /// Transaction hash (primary key)
    pub tx_hash: String,

    /// Transaction timestamp
    pub ts: NaiveDateTime,

    /// Blockchain platform identifier
    pub platform: i32,

    /// Block height
    pub height: i64,

    /// Internal transaction ID
    pub tx_id: Option<i64>,

    /// Transaction type
    /// 注意: type 是 Rust 关键字，必须使用 r# 前缀，或使用 #[sqlx(rename="type")]
    #[sqlx(rename = "type")]
    pub tx_type: Option<i8>,

    /// Log identifier
    pub log_id: i64,

    pub factory: Option<String>,

    /// Pair contract address
    pub address: Option<String>,

    /// Maker address
    pub maker: Option<String>,

    /// Token0 account address
    pub token0_account: Option<String>,

    /// Token1 account address
    pub token1_account: Option<String>,

    /// Token0 contract address
    pub token0_address: String,

    /// Token1 contract address
    pub token1_address: Option<String>,

    pub token0_symbol: Option<String>,

    pub token1_symbol: Option<String>,

    /// Token0 total supply (DB定义为 varchar)
    pub token0_ts: Option<String>,

    /// Token1 total supply (DB定义为 varchar)
    pub token1_ts: Option<String>,

    /// Base token address
    pub base_address: Option<String>,

    /// Quote price
    pub quote: Option<BigDecimal>,

    /// Token0 price in USD
    pub token0_price_usd: Option<BigDecimal>,

    /// Token1 price in USD
    pub token1_price_usd: Option<BigDecimal>,

    /// Native token price in USD
    pub native_price_usd: Option<BigDecimal>,

    /// Input token amount
    pub amount_in: Option<BigDecimal>,

    /// Output token amount
    pub amount_out: Option<BigDecimal>,

    /// Token0 volume
    pub token0_volume: Option<BigDecimal>,

    /// Token1 volume
    pub token1_volume: Option<BigDecimal>,

    /// Token0 reserve amount
    pub reserve0: Option<BigDecimal>,

    /// Token1 reserve amount
    pub reserve1: Option<BigDecimal>,

    /// Total transaction volume
    pub volume: Option<BigDecimal>,

    /// Buy volume in USD
    pub buy_volume_usd: Option<BigDecimal>,

    /// Sell volume in USD
    pub sell_volume_usd: Option<BigDecimal>,

    /// Liquidity amount in USD
    pub liquidity_usd: Option<BigDecimal>,

    /// Transaction fee amount
    pub fee_amount: Option<BigDecimal>,

    /// Quote token index
    pub quote_index: Option<i8>,

    /// Input token index
    pub token_in_index: Option<i8>,

    /// Exclusion flag (0/1)
    pub exclude: Option<i8>,

    /// Token price exclusion flag (0/1)
    pub exclude_token_price: Option<i8>,

    /// create time
    pub db_create_time: NaiveDateTime,

    /// update time
    pub db_modify_time: NaiveDateTime,

    pub no_anchor: i8,

    /// 是否top，1表示true，0表示false
    pub top: i8,

    /// 是否top，1表示true，0表示false
    pub t0top: i8,

    /// 是否top，1表示true，0表示false
    pub t1top: i8,

    /// Transaction fee (DB定义为 varchar)
    pub fee: Option<String>,

    /// Priority fee (DB定义为 varchar)
    pub priority_fee: Option<String>,

    /// Protocol code
    pub protocol_code: Option<i32>,

    /// Token0 position type: 1=open, 2=close, 3=add, 4=reduce
    pub token0_position_type: Option<i8>,

    /// Token1 position type: 1=open, 2=close, 3=add, 4=reduce
    pub token1_position_type: Option<i8>,
}

struct ThreadStats {
    q1_latencies: Vec<u128>,
    q2_latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            q1_latencies: Vec::with_capacity(1000),
            q2_latencies: Vec::with_capacity(1000),
            errors: 0,
        }
    }
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
    println!(
        "Logging: {}",
        if args.verbose {
            "ENABLED (Full SQL)"
        } else {
            "DISABLED"
        }
    );

    let opts = MySqlConnectOptions::from_str(&args.url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    println!(
        "> Sampling {} rows (address + ts) from database...",
        args.sample_size
    );
    let sampled_data = fetch_sample_data(&pool, args.sample_size).await?;

    if sampled_data.is_empty() {
        eprintln!("❌ Error: Table seems empty.");
        return Ok(());
    }
    println!(
        "> Successfully loaded {} unique sample points.",
        sampled_data.len()
    );

    let shared_data = Arc::new(sampled_data);
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);

    let mut handles = vec![];

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let data_pool = shared_data.clone();
        let verbose = args.verbose;
        let max_days_back = args.days_back;
        let verify = args.verify;

        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();

            while start_time.elapsed() < run_duration {
                let scenario = rng.gen_range(1..=5);

                let result = match scenario {
                    1 => run_q1(&pool, &mut rng, &data_pool, max_days_back, verbose, verify).await,
                    2 => run_q2(&pool, &mut rng, &data_pool, max_days_back, verbose, verify).await,
                    _ => Ok(Duration::new(0, 0)),
                };

                match result {
                    Ok(duration) => {
                        let micros = duration.as_micros();
                        match scenario {
                            1 => stats.q1_latencies.push(micros),
                            2 => stats.q2_latencies.push(micros),
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

    for handle in handles {
        let stats = handle.await?;
        total_errors += stats.errors;
        all_q1.extend(stats.q1_latencies);
        all_q2.extend(stats.q2_latencies);
    }

    let elapsed = start_time.elapsed();
    let total_requests = all_q1.len() + all_q2.len();
    let qps = total_requests as f64 / elapsed.as_secs_f64();

    println!("\n--- 📊 Benchmark Summary ---");
    println!("Total Time: {:.2?}", elapsed);
    println!("Total Requests: {}", total_requests);
    println!("Total Errors: {}", total_errors);
    println!("QPS: {:.2}", qps);

    println!("\n--- ⏱️  Latency Statistics (ms) ---");
    print_percentiles("Q1", &mut all_q1);
    print_percentiles("Q2", &mut all_q2);

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

    println!(
        "{:<25} | Cnt:{:<5} | P50:{:.2}ms | P95:{:.2}ms | P99:{:.2}ms | Max:{:.2}ms",
        name,
        latencies.len(),
        to_ms(p50),
        to_ms(p95),
        to_ms(p99),
        to_ms(max)
    );
}

async fn fetch_sample_data(
    pool: &Pool<MySql>,
    limit: usize,
) -> Result<Vec<SampleData>, sqlx::Error> {
    let fetch_limit = limit;
    let query = format!(
        "SELECT token0_address, ts FROM dex_swap_tx_solana LIMIT {}",
        fetch_limit
    );
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
    max_days_back: i64,
    verbose: bool,
    verify: bool,
) -> Result<Duration, sqlx::Error> {
    let sample = get_test_sample(rng, data_pool);
    let platform = get_random_platform(rng);
    let no_anchor: i8 = get_random_no_anchor(rng);

    let window_days = rng.gen_range(1..=max_days_back);
    let window_seconds = window_days * 86400;

    let offset_seconds = rng.gen_range(0..window_seconds);
    let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
    let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

    let sql = format!(
        r#"
        SELECT * FROM dex_swap_tx_solana USE INDEX (INDEX_NAME)
        WHERE token0_address = {} 
        AND platform = {} 
        AND no_anchor = {} 
        AND ts >= {} AND ts <= {} 
        ORDER BY ts desc, height desc, tx_id desc, log_id desc 
        LIMIT 10
        "#,
        &sample.token0_address, platform, no_anchor, start_limit, end_limit,
    );

    let start = Instant::now();
    let tici_sql = sql.replace("INDEX_NAME", "idx_desc");
    let tici_rows: Vec<DexSwapTxSolana> = sqlx::query_as(&tici_sql).fetch_all(pool).await?;
    let duration = start.elapsed();

    if verify {
        let tikv_sql = sql.replace("INDEX_NAME", "primary");
        let tikv_rows: Vec<DexSwapTxSolana> = sqlx::query_as(&tikv_sql).fetch_all(pool).await?;
        assert_eq!(tici_rows, tikv_rows);
    }

    if verbose {
        println!("---------------------------------------------------");
        println!(
            "[Q1] Time: {:?} | Rows: {}",
            duration.as_millis(),
            tici_rows.len()
        );
        println!("{}", tici_sql);
    }
    Ok(duration)
}

async fn run_q2(
    pool: &Pool<MySql>,
    rng: &mut impl Rng,
    data_pool: &[SampleData],
    max_days_back: i64,
    verbose: bool,
    verify: bool,
) -> Result<Duration, sqlx::Error> {
    let sample = get_test_sample(rng, data_pool);
    let platform = get_random_platform(rng);
    let no_anchor: i8 = get_random_no_anchor(rng);

    let window_days = rng.gen_range(1..=max_days_back);
    let window_seconds = window_days * 86400;

    let offset_seconds = rng.gen_range(0..window_seconds);
    let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
    let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

    let sql = format!(
        r#"
        SELECT * FROM dex_swap_tx_solana USE INDEX (INDEX_NAME)
        WHERE token0_address = {} 
        AND platform = {} 
        AND no_anchor = {} 
        AND ts >= {} AND ts <= {} 
        ORDER BY ts asc, height asc, tx_id asc, log_id asc 
        LIMIT 10
        "#,
        &sample.token0_address, platform, no_anchor, start_limit, end_limit,
    );

    let start = Instant::now();
    let tici_sql = sql.replace("INDEX_NAME", "idx_asc");
    let tici_rows: Vec<DexSwapTxSolana> = sqlx::query_as(&tici_sql).fetch_all(pool).await?;
    let duration = start.elapsed();

    if verify {
        let tikv_sql = sql.replace("INDEX_NAME", "primary");
        let tikv_rows: Vec<DexSwapTxSolana> = sqlx::query_as(&tikv_sql).fetch_all(pool).await?;
        assert_eq!(tici_rows, tikv_rows);
    }

    if verbose {
        println!("---------------------------------------------------");
        println!(
            "[Q2] Time: {:?} | Rows: {}",
            duration.as_millis(),
            tici_rows.len()
        );
        println!("{}", tici_sql);
    }
    Ok(duration)
}
