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

    /// Number of makers to use in query IN clause
    #[arg(long, default_value_t = 500)]
    maker_count: usize,

    /// Query type to run (1-10)
    #[arg(long, default_value_t = 1)]
    query: u8,
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
struct TokenSample {
    token_address: String,
    #[serde(with = "chrono::serde::ts_seconds")]
    ts: DateTime<Utc>,
    volume: Option<f64>,
    maker: Option<String>,
    height: i64,
    tx_id: Option<i64>,
    log_id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SampleData {
    token_samples: Vec<TokenSample>,
    makers: Vec<String>,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.days_back < 1 {
        panic!("❌ --days-back must be at least 1");
    }

    if args.query < 1 || args.query > 10 {
        panic!("❌ --query must be between 1 and 10");
    }

    println!("--- Starting Solana DEX Benchmark (UNION ALL Query {}) ---", args.query);
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

    println!("> Fetching sample data from database (required: {})...", args.sample_size);
    let sampled_data = fetch_sample_data_from_db(&pool, args.sample_size).await?;
    
    if sampled_data.token_samples.is_empty() || sampled_data.makers.is_empty() {
        eprintln!("❌ Error: Failed to sample data from tables.");
        return Ok(());
    }
    println!("> Successfully loaded {} token_samples, {} makers.", 
        sampled_data.token_samples.len(), 
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
        let maker_count = args.maker_count;
        let query_type = args.query;
        
        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();
            
            while start_time.elapsed() < run_duration {
                let result = if query_type == 1 {
                    // Query 1: use original function
                    run_union_query(&pool, &mut rng, &data_pool, max_days_back, verbose, verify, maker_count).await
                } else {
                    // Query 2-10: use new function
                    run_union_query_v2(&pool, &mut rng, &data_pool, max_days_back, verbose, verify, query_type).await
                };

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
    print_percentiles(&format!("Union Query {}", args.query), &mut all_latencies);

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
    height: i64,
    tx_id: Option<i64>,
    log_id: i64,
}

#[derive(FromRow)]
struct Token1Row {
    token1_address: String,
    ts: DateTime<Utc>,
    volume: Option<BigDecimal>,
    maker: Option<String>,
    height: i64,
    tx_id: Option<i64>,
    log_id: i64,
}

#[derive(FromRow)]
struct Maker {
    maker: String,
}

/// Result structure for UNION ALL query (Query 1 - without position_type)
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

/// Result structure for UNION ALL query (Query 2-10 - with position_type)
#[derive(Debug, Clone, FromRow, Eq, PartialEq)]
struct UnionQueryResultWithPosition {
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
    #[sqlx(rename = "token0toppools")]
    token0_top_pools: i8,
    #[sqlx(rename = "token1toppools")]
    token1_top_pools: i8,
    token0_position_type: Option<i8>,
    token1_position_type: Option<i8>,
}

async fn fetch_sample_data_from_db(pool: &Pool<MySql>, limit: usize) -> Result<SampleData, sqlx::Error> {
    // Sample token0_address with ts, volume, maker, height, tx_id, log_id from dquery_dex.dex_swap_tx_solana_1206
    println!("> [1/3] Sampling token0_addresses with ts, volume, maker, height, tx_id, log_id from dquery_dex.dex_swap_tx_solana_1206...");
    let sample_limit = limit * 3; // Sample 3x to account for duplicates
    let query0 = format!("SELECT token0_address, ts, volume, maker, height, tx_id, log_id FROM dquery_dex.dex_swap_tx_solana_1206 TABLESAMPLE REGIONS() WHERE token0_address IS NOT NULL LIMIT {}", sample_limit);
    println!("  SQL: {}", query0);
    let token0_rows: Vec<Token0Row> = sqlx::query_as(&query0).fetch_all(pool).await?;
    
    // Deduplicate by token0_address, keeping first occurrence
    let mut seen = std::collections::HashSet::new();
    let token0_samples: Vec<TokenSample> = token0_rows.into_iter()
        .filter_map(|r| {
            if seen.insert(r.token0_address.clone()) {
                Some(TokenSample {
                    token_address: r.token0_address,
                    ts: r.ts,
                    volume: r.volume.as_ref().and_then(|v| v.to_string().parse::<f64>().ok()),
                    maker: r.maker,
                    height: r.height,
                    tx_id: r.tx_id,
                    log_id: r.log_id,
                })
            } else {
                None
            }
        })
        .take(limit)
        .collect();
    println!("  Result: {} distinct token0_samples (after deduplication)", token0_samples.len());
    
    // Sample token1_address with ts, volume, maker, height, tx_id, log_id from dquery_dex_new.dex_swap_tx_solana
    println!("> [2/3] Sampling token1_addresses with ts, volume, maker, height, tx_id, log_id from dquery_dex_new.dex_swap_tx_solana...");
    let query1 = format!("SELECT token1_address, ts, volume, maker, height, tx_id, log_id FROM dquery_dex_new.dex_swap_tx_solana TABLESAMPLE REGIONS() WHERE token1_address IS NOT NULL LIMIT {}", sample_limit);
    println!("  SQL: {}", query1);
    let token1_rows: Vec<Token1Row> = sqlx::query_as(&query1).fetch_all(pool).await?;
    
    // Deduplicate by token1_address, keeping first occurrence
    let mut seen = std::collections::HashSet::new();
    let token1_samples: Vec<TokenSample> = token1_rows.into_iter()
        .filter_map(|r| {
            if seen.insert(r.token1_address.clone()) {
                Some(TokenSample {
                    token_address: r.token1_address,
                    ts: r.ts,
                    volume: r.volume.as_ref().and_then(|v| v.to_string().parse::<f64>().ok()),
                    maker: r.maker,
                    height: r.height,
                    tx_id: r.tx_id,
                    log_id: r.log_id,
                })
            } else {
                None
            }
        })
        .take(limit)
        .collect();
    println!("  Result: {} distinct token1_samples (after deduplication)", token1_samples.len());
    
    // Combine token0_samples and token1_samples into a single list
    let mut token_samples = token0_samples;
    token_samples.extend(token1_samples);
    println!("  Combined: {} total token_samples", token_samples.len());
    
    // Collect all makers from samples and additional sampling to reach 1000
    println!("> [3/3] Collecting makers from samples and additional sampling...");
    let mut maker_set = std::collections::HashSet::new();
    
    // Collect makers from token_samples
    for sample in &token_samples {
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
        token_samples,
        makers,
    })
}



// --- SQL Queries ---

/// Generate cursor condition for range queries
/// Based on sampled token data, generate (ts, height, tx_id, log_id) with random offset
fn generate_cursor_condition(
    token_sample: &TokenSample,
    rng: &mut impl Rng,
    use_less_than: bool, // true for <, false for >
) -> String {
    // Generate random offsets (left/right random)
    let ts_offset_seconds = rng.gen_range(-86400..86400); // ±1 day
    let height_offset = rng.gen_range(-1000..1000);
    let tx_id_offset = rng.gen_range(-100..100);
    let log_id_offset = rng.gen_range(-50..50);
    
    let cursor_ts = if use_less_than {
        token_sample.ts - ChronoDuration::seconds(ts_offset_seconds.abs())
    } else {
        token_sample.ts + ChronoDuration::seconds(ts_offset_seconds.abs())
    };
    
    let cursor_height = if use_less_than {
        token_sample.height.saturating_sub(height_offset.abs())
    } else {
        token_sample.height.saturating_add(height_offset.abs())
    };
    
    let cursor_tx_id = if use_less_than {
        token_sample.tx_id.map(|id| id.saturating_sub(tx_id_offset.abs()))
    } else {
        token_sample.tx_id.map(|id| id.saturating_add(tx_id_offset.abs()))
    };
    
    let cursor_log_id = if use_less_than {
        token_sample.log_id.saturating_sub(log_id_offset.abs())
    } else {
        token_sample.log_id.saturating_add(log_id_offset.abs())
    };
    
    let tx_id_str = if let Some(tx_id) = cursor_tx_id {
        tx_id.to_string()
    } else {
        "NULL".to_string()
    };
    
    format!(
        "('{}', {}, {}, {})",
        cursor_ts.format("%Y-%m-%d %H:%M:%S%.3f"),
        cursor_height,
        tx_id_str,
        cursor_log_id
    )
}

async fn run_union_query(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    data_pool: &SampleData,
    max_days_back: i64,
    verbose: bool,
    verify: bool,
    maker_count: usize,
) -> Result<Duration, sqlx::Error> {
    // Select random token sample - same address will be used for both token0 and token1
    let token_sample = data_pool.token_samples.choose(rng).expect("No token_samples");
    let token_addr = &token_sample.token_address;
    
    // Collect makers: from sample + random selection to reach target count
    let mut selected_makers = std::collections::HashSet::new();
    
    // Add maker from token_sample if exists
    if let Some(ref m) = token_sample.maker {
        if !m.is_empty() {
            selected_makers.insert(m.clone());
        }
    }
    
    // Add random makers to reach target count
    let target_count: usize = maker_count;
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
    
    // Fixed conditions (same for both UNION parts)
    let platform = 16;
    let no_anchor: i8 = 0; // false
    
    // Random time range using days_back, based on sampled ts (same for both UNION parts)
    let base_ts = token_sample.ts;
    let window_days = rng.gen_range(1..=max_days_back);
    let window_seconds = window_days * 86400;
    let offset_seconds = rng.gen_range(0..window_seconds);
    let start_limit = base_ts - ChronoDuration::seconds(offset_seconds);
    let end_limit = start_limit + ChronoDuration::seconds(window_seconds);
    
    // Random volume range, based on sampled volume (same for both UNION parts)
    let base_volume = token_sample.volume.unwrap_or(1000.0);
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
        token_addr.replace("'", "''"), platform, no_anchor, makers_str,
        start_limit.format("%Y-%m-%d %H:%M:%S%.3f"),
        end_limit.format("%Y-%m-%d %H:%M:%S%.3f"),
        volume_min, volume_max,
        "{}", // index placeholder for second table
        token_addr.replace("'", "''"), platform, no_anchor, makers_str,
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
    
    // Print SQL if query takes more than 1 second
    if duration.as_millis() > 1000 {
        println!("⚠️  Slow query ({}ms):\n{}", duration.as_millis(), target_sql);
    }

    // Verify logic: compare with primary index query if verify is enabled
    if verify {
        if let Some(v_sql) = verify_sql {
            let verify_start = Instant::now();
            let verify_rows: Vec<UnionQueryResult> = sqlx::query_as(&v_sql).fetch_all(pool).await?;
            let verify_duration = verify_start.elapsed();
            
            // Print verify query log
            println!("[Verify Query] Latency: {}ms | Rows: {} | Index: primary", 
                verify_duration.as_millis(),
                verify_rows.len()
            );
            
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
            } else {
                println!("✅ Verify Passed: Rows match ({}), Content match", verify_rows.len());
            }
        }
    }

    // Always print query log (not just in verbose mode)
    println!("[Union Query] Latency: {}ms | Rows: {} | Token: {} | Makers: {} | Platform: {} | NoAnchor: {} | TS: {} to {} | Volume: {} to {} | Sort: {} | Index: {}", 
        duration.as_millis(),
        target_rows.len(),
        token_addr,
        selected_makers.len(),
        platform,
        no_anchor,
        start_limit.format("%Y-%m-%d %H:%M:%S"),
        end_limit.format("%Y-%m-%d %H:%M:%S"),
        volume_min,
        volume_max,
        sort_direction,
        index_name
    );
    
    // Print full SQL in verbose mode
    if verbose {
        println!("  Full SQL:\n{}", target_sql);
    }
    Ok(duration)
}

/// Run UNION ALL query for Query 2-10 (with position_type fields)
async fn run_union_query_v2(
    pool: &Pool<MySql>, 
    rng: &mut impl Rng, 
    data_pool: &SampleData,
    max_days_back: i64,
    verbose: bool,
    verify: bool,
    query_type: u8,
) -> Result<Duration, sqlx::Error> {
    // Select random token sample - same address will be used for both token0 and token1
    let token_sample = data_pool.token_samples.choose(rng).expect("No token_samples");
    let token_addr = &token_sample.token_address;
    
    // Fixed conditions (same for both UNION parts)
    let platform = 16;
    let no_anchor: i8 = 0; // false
    
    // Random time range using days_back, based on sampled ts (same for both UNION parts)
    let base_ts = token_sample.ts;
    let window_days = rng.gen_range(1..=max_days_back);
    let window_seconds = window_days * 86400;
    let offset_seconds = rng.gen_range(0..window_seconds);
    let start_limit = base_ts - ChronoDuration::seconds(offset_seconds);
    let end_limit = start_limit + ChronoDuration::seconds(window_seconds);
    
    // Random volume range, based on sampled volume (same for both UNION parts)
    let base_volume = token_sample.volume.unwrap_or(1000.0);
    let volume_variance = base_volume * 0.5; // 50% variance
    let volume_min = (base_volume - volume_variance).max(1.0);
    let volume_max = base_volume + volume_variance + rng.gen_range(1.0..10000.0);
    
    // Determine ORDER BY direction based on query type
    let (use_desc, sort_direction) = match query_type {
        5 | 8 => (false, "ASC"),
        _ => (true, "DESC"),
    };
    let index_name = if use_desc { "idx_desc" } else { "idx_asc" };
    
    // Select single maker for queries that need it (4, 6, 10)
    let single_maker = if matches!(query_type, 4 | 6 | 10) {
        // Use maker from token_sample if exists, otherwise random from pool
        if let Some(ref m) = token_sample.maker {
            if !m.is_empty() {
                Some(m.clone())
            } else {
                data_pool.makers.choose(rng).cloned()
            }
        } else {
            data_pool.makers.choose(rng).cloned()
        }
    } else {
        None
    };
    
    // Generate cursor condition for queries that need it (3, 5, 6, 9, 10)
    let cursor_condition = match query_type {
        3 | 6 | 9 | 10 => Some(generate_cursor_condition(token_sample, rng, true)), // <
        5 => Some(generate_cursor_condition(token_sample, rng, false)), // >
        _ => None,
    };
    
    let start = Instant::now();
    
    // Build WHERE conditions for first SELECT (token0_address)
    let mut where0_conditions = vec![
        format!("`token0_address` = '{}'", token_addr.replace("'", "''")),
        format!("`platform` = {}", platform),
        format!("`no_anchor` = false"),
        format!("`ts` >= '{}'", start_limit.format("%Y-%m-%d %H:%M:%S%.3f")),
        format!("`ts` <= '{}'", end_limit.format("%Y-%m-%d %H:%M:%S%.3f")),
    ];
    
    // Add maker condition for queries 4, 6, 10
    if let Some(ref maker) = single_maker {
        where0_conditions.push(format!("`maker` = '{}'", maker.replace("'", "''")));
    }
    
    // Add volume condition for queries 7, 9, 10
    if matches!(query_type, 7 | 9 | 10) {
        where0_conditions.push(format!("`volume` >= {}", volume_min));
    }
    
    // Add cursor condition for queries 3, 5, 6, 9, 10
    if let Some(ref cursor) = cursor_condition {
        let operator = if query_type == 5 { ">" } else { "<" };
        where0_conditions.push(format!("(`ts`, `height`, `tx_id`, `log_id`) {} {}", operator, cursor));
    }
    
    // Build WHERE conditions for second SELECT (token1_address) - same as first
    let where1_conditions = where0_conditions.clone()
        .into_iter()
        .map(|c| c.replace("`token0_address`", "`token1_address`"))
        .collect::<Vec<_>>();
    
    let where0_clause = where0_conditions.join("\n            AND ");
    let where1_clause = where1_conditions.join("\n            AND ");
    
    // Build the UNION ALL query
    let base_sql = format!(
        r#"
        SELECT
          `ts`,
          TYPE,
          `token0_address`,
          `token1_address`,
          `token0_symbol`,
          `token1_symbol`,
          `token0_volume`,
          `token1_volume`,
          `token0_price_usd`,
          `token1_price_usd`,
          `quote`,
          `volume`,
          `quote_index`,
          `maker`,
          `exclude`,
          `factory`,
          `tx_hash`,
          `height`,
          `tx_id`,
          `log_id`,
          `t0top` AS `token0toppools`,
          `t1top` AS `token1toppools`,
          `token0_position_type`,
          `token1_position_type`
        FROM
          `dquery_dex_new`.`dex_swap_tx_solana`
          USE INDEX ({})
        WHERE
          {}
        UNION
        ALL
        SELECT
          `ts`,
          TYPE,
          `token0_address`,
          `token1_address`,
          `token0_symbol`,
          `token1_symbol`,
          `token0_volume`,
          `token1_volume`,
          `token0_price_usd`,
          `token1_price_usd`,
          `quote`,
          `volume`,
          `quote_index`,
          `maker`,
          `exclude`,
          `factory`,
          `tx_hash`,
          `height`,
          `tx_id`,
          `log_id`,
          `t0top` AS `token0toppools`,
          `t1top` AS `token1toppools`,
          `token0_position_type`,
          `token1_position_type`
        FROM
          `dquery_dex`.`dex_swap_tx_solana_1206`
          USE INDEX ({})
        WHERE
          {}
        ORDER BY
          `ts` {},
          `height` {},
          `tx_id` {},
          `log_id` {}
        LIMIT
          100
        "#,
        "{}", // index placeholder for first table
        where0_clause,
        "{}", // index placeholder for second table
        where1_clause,
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
    let target_rows: Vec<UnionQueryResultWithPosition> = sqlx::query_as(&target_sql).fetch_all(pool).await?;
    let duration = start.elapsed();
    
    // Print SQL if query takes more than 1 second
    if duration.as_millis() > 1000 {
        println!("⚠️  Slow query ({}ms):\n{}", duration.as_millis(), target_sql);
    }

    // Verify logic: compare with primary index query if verify is enabled
    if verify {
        if let Some(v_sql) = verify_sql {
            let verify_start = Instant::now();
            let verify_rows: Vec<UnionQueryResultWithPosition> = sqlx::query_as(&v_sql).fetch_all(pool).await?;
            let verify_duration = verify_start.elapsed();
            
            // Print verify query log
            println!("[Verify Query] Latency: {}ms | Rows: {} | Index: primary", 
                verify_duration.as_millis(),
                verify_rows.len()
            );
            
            // Compare results: both length and content
            if target_rows.len() != verify_rows.len() {
                eprintln!(
                    "❌ Verify Failed for [Union Query {}]: Target Rows {}, Verify Rows {}",
                    query_type,
                    target_rows.len(),
                    verify_rows.len()
                );
                eprintln!("Target SQL (using {} index):\n{}", index_name, target_sql);
                eprintln!("Verify SQL (using primary index):\n{}", v_sql);
            } else if target_rows != verify_rows {
                eprintln!("❌ Verify Failed for [Union Query {}]: Content Mismatch (Row count matches: {})", query_type, target_rows.len());
                eprintln!("Target SQL (using {} index):\n{}", index_name, target_sql);
                eprintln!("Verify SQL (using primary index):\n{}", v_sql);
            } else {
                println!("✅ Verify Passed: Rows match ({}), Content match", verify_rows.len());
            }
        }
    }

    // Always print query log (not just in verbose mode)
    let maker_info = if let Some(ref m) = single_maker {
        format!("Maker: {}", m)
    } else {
        "No maker filter".to_string()
    };
    let volume_info = if matches!(query_type, 7 | 9 | 10) {
        format!("Volume >= {}", volume_min)
    } else {
        "No volume filter".to_string()
    };
    let cursor_info = if cursor_condition.is_some() {
        format!("Cursor: {}", cursor_condition.as_ref().unwrap())
    } else {
        "No cursor".to_string()
    };
    
    println!("[Union Query {}] Latency: {}ms | Rows: {} | Token: {} | Platform: {} | NoAnchor: {} | TS: {} to {} | {} | {} | {} | Sort: {} | Index: {}", 
        query_type,
        duration.as_millis(),
        target_rows.len(),
        token_addr,
        platform,
        no_anchor,
        start_limit.format("%Y-%m-%d %H:%M:%S"),
        end_limit.format("%Y-%m-%d %H:%M:%S"),
        maker_info,
        volume_info,
        cursor_info,
        sort_direction,
        index_name
    );
    
    // Print full SQL in verbose mode
    if verbose {
        println!("  Full SQL:\n{}", target_sql);
    }
    
    Ok(duration)
}
