use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use sqlx::mysql::{MySqlPoolOptions, MySqlConnectOptions};
use sqlx::{Pool, MySql, FromRow};
use sqlx::types::BigDecimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::fs;
use std::path::Path;
use chrono::{DateTime, Utc, Duration as ChronoDuration};
use serde::{Deserialize, Serialize};

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

struct ThreadStats {
    query_latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            query_latencies: Vec::with_capacity(1000),
            errors: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Token0Sample {
    token0_address: String,
    #[serde(with = "chrono::serde::ts_seconds")]
    ts: DateTime<Utc>,
    volume: Option<f64>,
    maker: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Token1Sample {
    token1_address: String,
    #[serde(with = "chrono::serde::ts_seconds")]
    ts: DateTime<Utc>,
    volume: Option<f64>,
    maker: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SampleData {
    token0_samples: Vec<Token0Sample>,
    token1_samples: Vec<Token1Sample>,
    makers: Vec<String>,
}

const CACHE_FILE: &str = "sample_data_cache.json";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.days_back < 1 {
        panic!("❌ --days-back must be at least 1");
    }

    println!("--- Starting Solana DEX Benchmark (UNION ALL Query) ---");
    println!("Concurrency: {}", args.concurrency);
    println!("Sample Size: {}", args.sample_size);
    println!("Max Days Back: {}", args.days_back);
    println!("Logging: {}", if args.verbose { "ENABLED (Full SQL)" } else { "DISABLED" });
    
    let opts = MySqlConnectOptions::from_str(&args.url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    println!("> Checking for cached sample data (required: {})...", args.sample_size);
    let sampled_data = fetch_sample_data(&pool, args.sample_size).await?;
    
    if sampled_data.token0_samples.is_empty() || sampled_data.token1_samples.is_empty() || sampled_data.makers.is_empty() {
        eprintln!("❌ Error: Failed to sample data from tables.");
        return Ok(());
    }
    println!("> Successfully loaded {} token0_samples, {} token1_samples, {} makers.", 
        sampled_data.token0_samples.len(), 
        sampled_data.token1_samples.len(), 
        sampled_data.makers.len());

    let shared_data = Arc::new(sampled_data);
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    
    let mut handles = vec![];

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let data_pool = shared_data.clone(); 
        let verbose = args.verbose;
        let verify = args.verify;
        let max_days_back = args.days_back;
        
        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();
            
            while start_time.elapsed() < run_duration {
                let result = run_union_query(&pool, &mut rng, &data_pool, max_days_back, verbose, verify).await;

                match result {
                    Ok(duration) => {
                        let micros = duration.as_micros();
                        stats.query_latencies.push(micros);
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
    let mut all_latencies = Vec::new();

    for handle in handles {
        let stats = handle.await?;
        total_errors += stats.errors;
        all_latencies.extend(stats.query_latencies);
    }

    let elapsed = start_time.elapsed();
    let total_requests = all_latencies.len();
    let qps = total_requests as f64 / elapsed.as_secs_f64();

    println!("\n--- 📊 Benchmark Summary ---");
    println!("Total Time: {:.2?}", elapsed);
    println!("Total Requests: {}", total_requests);
    println!("Total Errors: {}", total_errors);
    println!("QPS: {:.2}", qps);

    println!("\n--- ⏱️  Latency Statistics (ms) ---");
    print_percentiles("Union Query", &mut all_latencies);

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

#[derive(FromRow)]
struct Token0Row {
    token0_address: String,
    ts: DateTime<Utc>,
    volume: Option<BigDecimal>,
    maker: Option<String>,
}

#[derive(FromRow)]
struct Token1Row {
    token1_address: String,
    ts: DateTime<Utc>,
    volume: Option<BigDecimal>,
    maker: Option<String>,
}

#[derive(FromRow)]
struct Maker {
    maker: String,
}

/// Result structure for UNION ALL query
#[derive(Debug, Clone, FromRow, Eq, PartialEq)]
struct UnionQueryResult {
    ts: DateTime<Utc>,
    #[sqlx(rename = "type")]
    tx_type: i8,
    token0_address: String,
    token1_address: Option<String>,
    token0_symbol: Option<String>,
    token1_symbol: Option<String>,
    token0_volume: Option<BigDecimal>,
    token1_volume: Option<BigDecimal>,
    token0_price_usd: Option<BigDecimal>,
    token1_price_usd: Option<BigDecimal>,
    quote: Option<BigDecimal>,
    volume: Option<BigDecimal>,
    quote_index: Option<i8>,
    maker: Option<String>,
    exclude: Option<i8>,
    factory: Option<String>,
    tx_hash: String,
    height: i64,
    tx_id: Option<i64>,
    log_id: i64,
    #[sqlx(rename = "token0TopPools")]
    token0_top_pools: i8,
    #[sqlx(rename = "token1TopPools")]
    token1_top_pools: i8,
}

fn load_sample_data_from_cache(required_size: usize) -> Option<SampleData> {
    if !Path::new(CACHE_FILE).exists() {
        return None;
    }
    
    match fs::read_to_string(CACHE_FILE) {
        Ok(content) => {
            match serde_json::from_str::<SampleData>(&content) {
                Ok(data) => {
                    // Check if cached data meets the requirements
                    if data.token0_samples.len() >= required_size 
                        && data.token1_samples.len() >= required_size 
                        && data.makers.len() >= required_size {
                        println!("> Loaded {} token0_samples, {} token1_samples, {} makers from cache.", 
                            data.token0_samples.len(), 
                            data.token1_samples.len(), 
                            data.makers.len());
                        Some(data)
                    } else {
                        println!("> Cache exists but insufficient (need {}): token0={}, token1={}, makers={}", 
                            required_size,
                            data.token0_samples.len(),
                            data.token1_samples.len(),
                            data.makers.len());
                        None
                    }
                }
                Err(e) => {
                    eprintln!("> Warning: Failed to parse cache file: {}", e);
                    None
                }
            }
        }
        Err(e) => {
            eprintln!("> Warning: Failed to read cache file: {}", e);
            None
        }
    }
}

fn save_sample_data_to_cache(data: &SampleData) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(data)?;
    fs::write(CACHE_FILE, json)?;
    println!("> Saved sample data to cache file: {}", CACHE_FILE);
    Ok(())
}

async fn fetch_sample_data_from_db(pool: &Pool<MySql>, limit: usize) -> Result<SampleData, sqlx::Error> {
    // Sample token0_address with ts, volume, maker from dquery_dex.dex_swap_tx_solana_1206
    println!("> [1/3] Sampling token0_addresses with ts, volume, maker from dquery_dex.dex_swap_tx_solana_1206...");
    let query0 = format!("SELECT token0_address, ts, volume, maker FROM dquery_dex.dex_swap_tx_solana_1206 TABLESAMPLE REGIONS() WHERE token0_address IS NOT NULL LIMIT {}", limit);
    println!("  SQL: {}", query0);
    let token0_rows: Vec<Token0Row> = sqlx::query_as(&query0).fetch_all(pool).await?;
    let token0_samples: Vec<Token0Sample> = token0_rows.into_iter().map(|r| Token0Sample {
        token0_address: r.token0_address,
        ts: r.ts,
        volume: r.volume.as_ref().and_then(|v| v.to_string().parse::<f64>().ok()),
        maker: r.maker,
    }).collect();
    println!("  Result: {} token0_samples", token0_samples.len());
    
    // Sample token1_address with ts, volume, maker from dquery_dex_new.dex_swap_tx_solana
    println!("> [2/3] Sampling token1_addresses with ts, volume, maker from dquery_dex_new.dex_swap_tx_solana...");
    let query1 = format!("SELECT token1_address, ts, volume, maker FROM dquery_dex_new.dex_swap_tx_solana TABLESAMPLE REGIONS() WHERE token1_address IS NOT NULL LIMIT {}", limit);
    println!("  SQL: {}", query1);
    let token1_rows: Vec<Token1Row> = sqlx::query_as(&query1).fetch_all(pool).await?;
    let token1_samples: Vec<Token1Sample> = token1_rows.into_iter().map(|r| Token1Sample {
        token1_address: r.token1_address,
        ts: r.ts,
        volume: r.volume.as_ref().and_then(|v| v.to_string().parse::<f64>().ok()),
        maker: r.maker,
    }).collect();
    println!("  Result: {} token1_samples", token1_samples.len());
    
    // Collect all makers from samples and additional sampling to reach 1000
    println!("> [3/3] Collecting makers from samples and additional sampling...");
    let mut maker_set = std::collections::HashSet::new();
    
    // Collect makers from token0_samples
    for sample in &token0_samples {
        if let Some(ref m) = sample.maker {
            if !m.is_empty() {
                maker_set.insert(m.clone());
            }
        }
    }
    
    // Collect makers from token1_samples
    for sample in &token1_samples {
        if let Some(ref m) = sample.maker {
            if !m.is_empty() {
                maker_set.insert(m.clone());
            }
        }
    }
    
    // If we don't have enough makers, sample more
    if maker_set.len() < 1000 {
        let needed = 1000 - maker_set.len();
        let query_maker = format!("SELECT DISTINCT maker FROM dquery_dex.dex_swap_tx_solana_1206 TABLESAMPLE REGIONS() WHERE maker IS NOT NULL AND maker != '' LIMIT {}", needed);
        println!("  Additional SQL: {}", query_maker);
        let maker_rows: Vec<Maker> = sqlx::query_as(&query_maker).fetch_all(pool).await?;
        for row in maker_rows {
            maker_set.insert(row.maker);
        }
    }
    
    let makers: Vec<String> = maker_set.into_iter().collect();
    println!("  Result: {} total makers", makers.len());
    
    Ok(SampleData {
        token0_samples,
        token1_samples,
        makers,
    })
}

async fn fetch_sample_data(pool: &Pool<MySql>, limit: usize) -> Result<SampleData, Box<dyn std::error::Error>> {
    // Try to load from cache first
    if let Some(cached_data) = load_sample_data_from_cache(limit) {
        return Ok(cached_data);
    }
    
    // Cache not available or insufficient, fetch from database
    println!("> Fetching sample data from database...");
    let data = fetch_sample_data_from_db(pool, limit).await?;
    
    // Save to cache
    if let Err(e) = save_sample_data_to_cache(&data) {
        eprintln!("> Warning: Failed to save cache: {}", e);
    }
    
    Ok(data)
}


// --- SQL Queries ---

async fn run_union_query(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    data_pool: &SampleData,
    max_days_back: i64,
    verbose: bool,
    verify: bool
) -> Result<Duration, sqlx::Error> {
    // Select random token0_sample and token1_sample
    let token0_sample = data_pool.token0_samples.choose(rng).expect("No token0_samples");
    let token1_sample = data_pool.token1_samples.choose(rng).expect("No token1_samples");
    
    let token0_addr = &token0_sample.token0_address;
    let token1_addr = &token1_sample.token1_address;
    
    // Collect makers: from samples + random selection to reach 1000
    let mut selected_makers = std::collections::HashSet::new();
    
    // Add maker from token0_sample if exists
    if let Some(ref m) = token0_sample.maker {
        if !m.is_empty() {
            selected_makers.insert(m.clone());
        }
    }
    
    // Add maker from token1_sample if exists
    if let Some(ref m) = token1_sample.maker {
        if !m.is_empty() {
            selected_makers.insert(m.clone());
        }
    }
    
    // Add random makers to reach 1000
    let target_count: usize = 1000;
    let needed = target_count.saturating_sub(selected_makers.len());
    if needed > 0 {
        let additional_makers: Vec<String> = data_pool.makers
            .choose_multiple(rng, needed.min(data_pool.makers.len()))
            .cloned()
            .collect();
        for m in additional_makers {
            selected_makers.insert(m);
        }
    }
    
    let selected_makers: Vec<String> = selected_makers.into_iter().collect();
    
    // Fixed conditions
    let platform = 16;
    let no_anchor: i8 = 0; // false
    
    // Random time range using days_back, based on sampled ts
    let base_ts = token0_sample.ts; // Use token0's ts as base
    let window_days = rng.gen_range(1..=max_days_back);
    let window_seconds = window_days * 86400;
    let offset_seconds = rng.gen_range(0..window_seconds);
    let start_limit = base_ts - ChronoDuration::seconds(offset_seconds);
    let end_limit = start_limit + ChronoDuration::seconds(window_seconds);
    
    // Random volume range, based on sampled volume
    let base_volume = token0_sample.volume.unwrap_or(1000.0);
    let volume_variance = base_volume * 0.5; // 50% variance
    let volume_min = (base_volume - volume_variance).max(1.0);
    let volume_max = base_volume + volume_variance + rng.gen_range(1.0..10000.0);
    
    // Random sort direction (DESC or ASC)
    let use_desc = rng.gen_bool(0.5);
    let index_name = if use_desc { "idx_desc" } else { "idx_asc" };
    let sort_direction = if use_desc { "DESC" } else { "ASC" };
    
    // Build maker IN clause with actual values
    let makers_str = selected_makers.iter()
        .map(|m| format!("'{}'", m.replace("'", "''")))  // Escape single quotes
        .collect::<Vec<_>>()
        .join(", ");
    
    let start = Instant::now();
    
    // Build the base UNION ALL query template with actual values (using {} as index placeholder)
    let base_sql = format!(
        r#"
        SELECT 
            ts, type, token0_address, token1_address, 
            token0_symbol, token1_symbol, 
            token0_volume, token1_volume, 
            token0_price_usd, token1_price_usd, 
            quote, volume, quote_index, maker, exclude, 
            factory, tx_hash, height, tx_id, log_id, 
            t0top AS token0TopPools, 
            t1top AS token1TopPools 
        FROM 
            dquery_dex_new.dex_swap_tx_solana
            USE INDEX ({})
        WHERE 
            token0_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND type = 0 
            AND maker IN ({}) 
            AND ts >= '{}' 
            AND ts <= '{}' 
            AND volume >= {} 
            AND volume <= {} 
        UNION ALL
        SELECT 
            ts, type, token0_address, token1_address, 
            token0_symbol, token1_symbol, 
            token0_volume, token1_volume, 
            token0_price_usd, token1_price_usd, 
            quote, volume, quote_index, maker, exclude, 
            factory, tx_hash, height, tx_id, log_id, 
            t0top AS token0TopPools, 
            t1top AS token1TopPools 
        FROM 
            dquery_dex.dex_swap_tx_solana_1206
            USE INDEX ({})
        WHERE 
            token1_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND type = 1 
            AND maker IN ({}) 
            AND ts >= '{}' 
            AND ts <= '{}' 
            AND volume >= {} 
            AND volume <= {} 
        ORDER BY 
            ts {}, height {}, tx_id {}, log_id {} 
        LIMIT 100
        "#,
        "{}", // index placeholder for first table
        token0_addr.replace("'", "''"), platform, no_anchor, makers_str,
        start_limit.format("%Y-%m-%d %H:%M:%S%.3f"),
        end_limit.format("%Y-%m-%d %H:%M:%S%.3f"),
        volume_min, volume_max,
        "{}", // index placeholder for second table
        token1_addr.replace("'", "''"), platform, no_anchor, makers_str,
        start_limit.format("%Y-%m-%d %H:%M:%S%.3f"),
        end_limit.format("%Y-%m-%d %H:%M:%S%.3f"),
        volume_min, volume_max,
        sort_direction, sort_direction, sort_direction, sort_direction
    );
    
    // Build target SQL with actual index
    let target_sql = base_sql.replace("{}", index_name);
    
    // Build verify SQL with primary index if verify is enabled
    let verify_sql = if verify {
        Some(base_sql.replace("{}", "primary"))
    } else {
        None
    };
    
    // Execute target query
    let target_rows: Vec<UnionQueryResult> = sqlx::query_as(&target_sql).fetch_all(pool).await?;
    let duration = start.elapsed();

    // Verify logic: compare with primary index query if verify is enabled
    if verify {
        if let Some(v_sql) = verify_sql {
            let verify_rows: Vec<UnionQueryResult> = sqlx::query_as(&v_sql).fetch_all(pool).await?;
            
            // Compare results: both length and content
            if target_rows.len() != verify_rows.len() {
                eprintln!(
                    "❌ Verify Failed for [Union Query]: Target Rows {}, Verify Rows {}",
                    target_rows.len(),
                    verify_rows.len()
                );
                eprintln!("Target SQL (using {} index):\n{}", index_name, target_sql);
                eprintln!("Verify SQL (using primary index):\n{}", v_sql);
            } else if target_rows != verify_rows {
                eprintln!("❌ Verify Failed for [Union Query]: Content Mismatch (Row count matches: {})", target_rows.len());
                eprintln!("Target SQL (using {} index):\n{}", index_name, target_sql);
                eprintln!("Verify SQL (using primary index):\n{}", v_sql);
            }
        }
    }

    if verbose {
        println!("---------------------------------------------------");
        println!("[Union Query] Time: {:?} | Rows: {}", duration.as_millis(), target_rows.len());
        println!("Token0: {}, Token1: {}, Makers: {}, Platform: {}, NoAnchor: {}", 
            token0_addr, token1_addr, selected_makers.len(), platform, no_anchor);
        println!("TS Range: {} to {}", 
            start_limit.format("%Y-%m-%d %H:%M:%S"), 
            end_limit.format("%Y-%m-%d %H:%M:%S"));
        println!("Volume Range: {} to {}", volume_min, volume_max);
        println!("Sort Direction: {}, Index: {}", sort_direction, index_name);
    }
    Ok(duration)
}
