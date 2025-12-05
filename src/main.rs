use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use clap::Parser;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::types::BigDecimal;
use sqlx::{FromRow, MySql, Pool};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ==========================================
// 1. 数据结构定义
// ==========================================

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
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
pub struct SampleData {
    token0_address: String,
    ts: DateTime<Utc>,
    maker: Option<String>,
}

/// 对应数据库表: `dex_swap_tx_solana`
#[derive(Debug, Clone, FromRow, Eq, PartialEq)]
pub struct DexSwapTxSolana {
    pub tx_hash: String,
    pub ts: DateTime<Utc>,
    pub platform: i32,
    pub height: i64,
    pub tx_id: Option<i64>,
    #[sqlx(rename = "type")]
    pub tx_type: Option<i8>,
    pub log_id: i64,
    pub factory: Option<String>,
    pub address: Option<String>,
    pub maker: Option<String>,
    pub token0_account: Option<String>,
    pub token1_account: Option<String>,
    pub token0_address: String,
    pub token1_address: Option<String>,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub token0_ts: Option<String>,
    pub token1_ts: Option<String>,
    pub base_address: Option<String>,
    pub quote: Option<BigDecimal>,
    pub token0_price_usd: Option<BigDecimal>,
    pub token1_price_usd: Option<BigDecimal>,
    pub native_price_usd: Option<BigDecimal>,
    pub amount_in: Option<BigDecimal>,
    pub amount_out: Option<BigDecimal>,
    pub token0_volume: Option<BigDecimal>,
    pub token1_volume: Option<BigDecimal>,
    pub reserve0: Option<BigDecimal>,
    pub reserve1: Option<BigDecimal>,
    pub volume: Option<BigDecimal>,
    pub buy_volume_usd: Option<BigDecimal>,
    pub sell_volume_usd: Option<BigDecimal>,
    pub liquidity_usd: Option<BigDecimal>,
    pub fee_amount: Option<BigDecimal>,
    pub quote_index: Option<i8>,
    pub token_in_index: Option<i8>,
    pub exclude: Option<i8>,
    pub exclude_token_price: Option<i8>,
    pub db_create_time: NaiveDateTime,
    pub db_modify_time: NaiveDateTime,
    pub no_anchor: i8,
    pub top: i8,
    pub t0top: i8,
    pub t1top: i8,
    pub fee: Option<String>,
    pub priority_fee: Option<String>,
    pub protocol_code: Option<i32>,
    pub token0_position_type: Option<i8>,
    pub token1_position_type: Option<i8>,
}

// ==========================================
// 2. 核心抽象层 (Trait & Context)
// ==========================================

/// 运行时的上下文，包含所有查询可能需要的公共资源
pub struct BenchmarkContext {
    pub pool: Pool<MySql>,
    pub args: Args,
    pub samples: Arc<Vec<SampleData>>,
}

/// 任何一个新的查询只需要实现这个 Trait
#[async_trait]
pub trait BenchmarkScenario: Send + Sync {
    /// 查询的唯一名称，用于统计输出
    fn name(&self) -> &str;

    /// 执行逻辑
    async fn execute(
        &self,
        ctx: &BenchmarkContext,
        rng: &mut StdRng,
    ) -> Result<Duration, sqlx::Error>;
}

/// 统计收集器
struct StatsCollector {
    // Key: Scenario Name, Value: Latencies (microseconds)
    latencies: HashMap<String, Vec<u128>>,
    errors: usize,
}

impl StatsCollector {
    fn new() -> Self {
        Self {
            latencies: HashMap::new(),
            errors: 0,
        }
    }

    fn record(&mut self, name: &str, micros: u128) {
        self.latencies
            .entry(name.to_string())
            .or_insert_with(|| Vec::with_capacity(1000))
            .push(micros);
    }
}

// ==========================================
// 3. 公共辅助函数
// ==========================================

/// 核心执行器：处理计时、Verify 校验和日志打印
///
/// - `target_sql`: 实际要测试的 SQL
/// - `verify_sql`: (可选) 用于结果校验的 SQL (通常是 USE INDEX (PRIMARY))
async fn run_sql_measure_verify(
    ctx: &BenchmarkContext,
    target_sql: String,
    verify_sql: Option<String>,
    query_name: &str,
) -> Result<Duration, sqlx::Error> {
    let start = Instant::now();

    // 执行目标查询
    let target_rows: Vec<DexSwapTxSolana> =
        sqlx::query_as(&target_sql).fetch_all(&ctx.pool).await?;

    let duration = start.elapsed();

    // 校验逻辑
    if ctx.args.verify {
        if let Some(v_sql) = verify_sql {
            let verify_rows: Vec<DexSwapTxSolana> =
                sqlx::query_as(&v_sql).fetch_all(&ctx.pool).await?;

            // 简单长度校验或全量校验
            if target_rows.len() != verify_rows.len() {
                eprintln!(
                    "❌ Verify Failed for [{}]: Target Rows {}, Verify Rows {}",
                    query_name,
                    target_rows.len(),
                    verify_rows.len()
                );
            } else if target_rows != verify_rows {
                eprintln!("❌ Verify Failed for [{}]: Content Mismatch", query_name);
            }
        }
    }

    // 日志逻辑
    if ctx.args.verbose {
        println!("---------------------------------------------------");
        println!(
            "[{}] Time: {:?} | Rows: {}",
            query_name,
            duration.as_millis(),
            target_rows.len()
        );
        println!("{}", target_sql);
    }

    Ok(duration)
}

fn get_test_sample<'a>(rng: &mut impl Rng, pool: &'a [SampleData]) -> &'a SampleData {
    pool.choose(rng).expect("Pool empty")
}

// ==========================================
// 4. 具体查询实现 (新增查询只需添加 Struct 实现 Trait)
// ==========================================

// --- Scenario 1: Descending Scan ---
struct Q1DescQuery;

#[async_trait]
impl BenchmarkScenario for Q1DescQuery {
    fn name(&self) -> &str {
        "Q1_Desc_Scan"
    }

    async fn execute(
        &self,
        ctx: &BenchmarkContext,
        rng: &mut StdRng,
    ) -> Result<Duration, sqlx::Error> {
        let sample = get_test_sample(rng, &ctx.samples);
        let platform = 16;
        let no_anchor = 0;

        let window_days = rng.gen_range(1..=ctx.args.days_back);
        let window_seconds = window_days * 86400;
        let offset_seconds = rng.gen_range(0..window_seconds);

        let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
        let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

        let base_sql = format!(
            r#"
            SELECT * FROM dex_swap_tx_solana USE INDEX ({})
            WHERE token0_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND ts >= '{}' AND ts <= '{}' 
            ORDER BY ts desc, height desc, tx_id desc, log_id desc 
            LIMIT 10
            "#,
            "{}", // index placeholder
            sample.token0_address,
            platform,
            no_anchor,
            start_limit,
            end_limit
        );

        let target_sql = base_sql.replace("{}", "idx_desc");
        let verify_sql = if ctx.args.verify {
            Some(base_sql.replace("{}", "primary"))
        } else {
            None
        };

        run_sql_measure_verify(ctx, target_sql, verify_sql, self.name()).await
    }
}

// --- Scenario 2: Ascending Scan ---
struct Q2AscQuery;

#[async_trait]
impl BenchmarkScenario for Q2AscQuery {
    fn name(&self) -> &str {
        "Q2_Asc_Scan"
    }

    async fn execute(
        &self,
        ctx: &BenchmarkContext,
        rng: &mut StdRng,
    ) -> Result<Duration, sqlx::Error> {
        let sample = get_test_sample(rng, &ctx.samples);
        let platform = 16;
        let no_anchor = 0;

        let window_days = rng.gen_range(1..=ctx.args.days_back);
        let window_seconds = window_days * 86400;
        let offset_seconds = rng.gen_range(0..window_seconds);

        let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
        let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

        let base_sql = format!(
            r#"
            SELECT * FROM dex_swap_tx_solana USE INDEX ({})
            WHERE token0_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND ts >= '{}' AND ts <= '{}' 
            ORDER BY ts asc, height asc, tx_id asc, log_id asc 
            LIMIT 10
            "#,
            "{}", sample.token0_address, platform, no_anchor, start_limit, end_limit
        );

        let target_sql = base_sql.replace("{}", "idx_asc");
        let verify_sql = if ctx.args.verify {
            Some(base_sql.replace("{}", "primary"))
        } else {
            None
        };

        run_sql_measure_verify(ctx, target_sql, verify_sql, self.name()).await
    }
}

// --- Scenario 3: Type Filter Query (Random 0, 1, or both) ---
struct Q3TypeQuery;

#[async_trait]
impl BenchmarkScenario for Q3TypeQuery {
    fn name(&self) -> &str {
        "Q3_Type_Filter"
    }

    async fn execute(
        &self,
        ctx: &BenchmarkContext,
        rng: &mut StdRng,
    ) -> Result<Duration, sqlx::Error> {
        let sample = get_test_sample(rng, &ctx.samples);
        let platform = 16;
        let no_anchor = 0;

        // --- 核心修改：随机选择 type 的取值范围 ---
        // 随机生成 0, 1, 或 2
        // 0 -> 只查 type=0
        // 1 -> 只查 type=1
        // 2 -> 查 type IN (0, 1)
        let type_option = rng.gen_range(0..3);
        let type_values = match type_option {
            0 => "0",
            1 => "1",
            _ => "0, 1",
        };

        let window_days = rng.gen_range(1..=ctx.args.days_back);
        let window_seconds = window_days * 86400;
        let offset_seconds = rng.gen_range(0..window_seconds);

        let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
        let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

        let base_sql = format!(
            r#"
            SELECT * FROM dex_swap_tx_solana USE INDEX ({})
            WHERE token0_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND type IN ({}) 
            AND ts >= '{}' AND ts <= '{}' 
            ORDER BY ts asc, height asc, tx_id asc, log_id asc 
            LIMIT 10
            "#,
            "{}", // index placeholder
            sample.token0_address,
            platform,
            no_anchor,
            type_values, // 动态插入 "0", "1" 或 "0, 1"
            start_limit,
            end_limit
        );

        let target_sql = base_sql.replace("{}", "idx_asc");

        let verify_sql = if ctx.args.verify {
            Some(base_sql.replace("{}", "primary"))
        } else {
            None
        };

        run_sql_measure_verify(ctx, target_sql, verify_sql, self.name()).await
    }
}

// --- Scenario 4: Maker IN (...) Query ---
struct Q4MakerQuery;

#[async_trait]
impl BenchmarkScenario for Q4MakerQuery {
    fn name(&self) -> &str {
        "Q4_Maker_Filter"
    }

    async fn execute(
        &self,
        ctx: &BenchmarkContext,
        rng: &mut StdRng,
    ) -> Result<Duration, sqlx::Error> {
        let sample = get_test_sample(rng, &ctx.samples);
        let platform = 16;
        let no_anchor = 0;

        // --- 核心逻辑：构建 Maker 列表 ---
        // 1. 决定要取几个 maker (1-5个)
        let count = rng.gen_range(1..=5);

        // 2. 收集 maker。
        // 为了提高命中率，我们首先尝试把当前 sample 的 maker 放进去（如果非空），
        // 然后再从全局 samples 中随机补足剩下的数量。
        let mut selected_makers = HashSet::new();

        if let Some(m) = &sample.maker {
            selected_makers.insert(m.clone());
        }

        // 尝试补足到 count 个，为了防止死循环设置一个最大尝试次数
        let mut attempts = 0;
        while selected_makers.len() < count && attempts < 20 {
            if let Some(random_sample) = ctx.samples.choose(rng) {
                if let Some(m) = &random_sample.maker {
                    selected_makers.insert(m.clone());
                }
            }
            attempts += 1;
        }

        // 3. 格式化为 SQL 字符串: 'addr1', 'addr2'
        // 如果集合为空（比如数据里全是 NULL），填入一个不存在的地址防止 SQL 语法错误
        let makers_sql = if selected_makers.is_empty() {
            "'00000000000000000000000000000000000000000000'".to_string()
        } else {
            selected_makers
                .iter()
                .map(|m| format!("'{}'", m))
                .collect::<Vec<_>>()
                .join(",")
        };

        let window_days = rng.gen_range(1..=ctx.args.days_back);
        let window_seconds = window_days * 86400;
        let offset_seconds = rng.gen_range(0..window_seconds);

        let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
        let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

        let base_sql = format!(
            r#"
            SELECT * FROM dex_swap_tx_solana USE INDEX ({})
            WHERE token0_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND maker IN ({}) 
            AND ts >= '{}' AND ts <= '{}' 
            ORDER BY ts asc, height asc, tx_id asc, log_id asc 
            LIMIT 10
            "#,
            "{}", // index placeholder
            sample.token0_address,
            platform,
            no_anchor,
            makers_sql, // 填入 'a', 'b', 'c'
            start_limit,
            end_limit
        );

        let target_sql = base_sql.replace("{}", "idx_asc");

        let verify_sql = if ctx.args.verify {
            Some(base_sql.replace("{}", "primary"))
        } else {
            None
        };

        run_sql_measure_verify(ctx, target_sql, verify_sql, self.name()).await
    }
}

// --- Scenario 5: Q2 + Volume Range Query ---
struct Q5VolumeQuery;

#[async_trait]
impl BenchmarkScenario for Q5VolumeQuery {
    fn name(&self) -> &str {
        "Q5_Volume_Range"
    }

    async fn execute(
        &self,
        ctx: &BenchmarkContext,
        rng: &mut StdRng,
    ) -> Result<Duration, sqlx::Error> {
        let sample = get_test_sample(rng, &ctx.samples);
        let platform = 16;
        let no_anchor = 0;

        // --- 核心逻辑：生成 Volume 范围 ---
        // 数据库 Min: ~0, Max: ~114,677,690
        // 为了保证查询能命中数据，我们采用以下策略：
        // 1. 下限 (min_vol): 在 0 到 10,000 之间随机 (大多数交易都在这个区间之上)
        // 2. 上限 (max_vol): 在 10,000 到 120,000,000 之间随机 (覆盖中等到最大交易)
        // 这样可以模拟查询 "所有大于微小金额" 的交易，或者 "特定中低金额区间" 的交易

        let min_vol = rng.gen_range(0.0..1000.0); // 随机取一个较小的起步值
        // 确保 max 肯定大于 min，且范围足够大
        let max_vol = min_vol + rng.gen_range(10000.0..100_000_000.0);

        let window_days = rng.gen_range(1..=ctx.args.days_back);
        let window_seconds = window_days * 86400;
        let offset_seconds = rng.gen_range(0..window_seconds);

        let start_limit = sample.ts - ChronoDuration::seconds(offset_seconds);
        let end_limit = start_limit + ChronoDuration::seconds(window_seconds);

        // 基于 Q2 (ASC) 修改，增加 volume 范围
        let base_sql = format!(
            r#"
            SELECT * FROM dex_swap_tx_solana USE INDEX ({})
            WHERE token0_address = '{}' 
            AND platform = {} 
            AND no_anchor = {} 
            AND volume >= {:.6} AND volume <= {:.6}
            AND ts >= '{}' AND ts <= '{}' 
            ORDER BY ts asc, height asc, tx_id asc, log_id asc 
            LIMIT 10
            "#,
            "{}", // index placeholder
            sample.token0_address,
            platform,
            no_anchor,
            min_vol,
            max_vol, // 填入 volume 范围，保留6位小数防止科学计数法导致 SQL 格式问题
            start_limit,
            end_limit
        );

        let target_sql = base_sql.replace("{}", "idx_asc");

        let verify_sql = if ctx.args.verify {
            Some(base_sql.replace("{}", "primary"))
        } else {
            None
        };

        run_sql_measure_verify(ctx, target_sql, verify_sql, self.name()).await
    }
}

// ==========================================
// 5. 主程序逻辑
// ==========================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.days_back < 1 {
        panic!("❌ --days-back must be at least 1");
    }

    println!("--- Starting Solana DEX Benchmark (Refactored) ---");
    println!("Concurrency: {}", args.concurrency);
    println!("Max Days Back: {}", args.days_back);

    // 1. 初始化数据库连接
    let opts = MySqlConnectOptions::from_str(&args.url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 10)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    // 2. 获取采样数据
    println!(
        "> Sampling {} rows (address + ts + maker) from database...",
        args.sample_size
    );
    let sampled_data = fetch_sample_data(&pool, args.sample_size).await?;
    if sampled_data.is_empty() {
        eprintln!("❌ Error: Table seems empty or no data found.");
        return Ok(());
    }
    println!(
        "> Successfully loaded {} unique sample points.",
        sampled_data.len()
    );

    // 3. 构建共享上下文
    let ctx = Arc::new(BenchmarkContext {
        pool: pool.clone(),
        args: args.clone(),
        samples: Arc::new(sampled_data),
    });

    // 4. 注册场景 (在此处添加新查询)
    let scenarios: Vec<Box<dyn BenchmarkScenario>> = vec![
        Box::new(Q1DescQuery),
        Box::new(Q2AscQuery),
        Box::new(Q3TypeQuery),
        Box::new(Q4MakerQuery),
        Box::new(Q5VolumeQuery),
    ];
    let scenarios = Arc::new(scenarios);

    // 5. 启动并发任务
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    let mut handles = vec![];

    println!("> Running benchmark for {} seconds...", args.duration);

    for _ in 0..args.concurrency {
        let ctx = ctx.clone();
        let scenarios = scenarios.clone();

        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = StatsCollector::new();

            while start_time.elapsed() < run_duration {
                // 随机选择一个 Scenario 执行
                if let Some(scenario) = scenarios.choose(&mut rng) {
                    match scenario.execute(&ctx, &mut rng).await {
                        Ok(duration) => {
                            stats.record(scenario.name(), duration.as_micros());
                        }
                        Err(e) => {
                            eprintln!("Error in [{}]: {}", scenario.name(), e);
                            stats.errors += 1;
                        }
                    }
                }
            }
            stats
        });
        handles.push(handle);
    }

    // 6. 汇总结果
    let mut total_errors = 0;
    let mut merged_latencies: HashMap<String, Vec<u128>> = HashMap::new();

    for handle in handles {
        let stats = handle.await?;
        total_errors += stats.errors;

        for (name, mut lats) in stats.latencies {
            merged_latencies.entry(name).or_default().append(&mut lats);
        }
    }

    // 7. 打印统计报告
    let elapsed = start_time.elapsed();
    let total_requests: usize = merged_latencies.values().map(|v| v.len()).sum();
    let qps = total_requests as f64 / elapsed.as_secs_f64();

    println!("\n--- 📊 Benchmark Summary ---");
    println!("Total Time: {:.2?}", elapsed);
    println!("Total Requests: {}", total_requests);
    println!("Total Errors: {}", total_errors);
    println!("QPS: {:.2}", qps);

    println!("\n--- ⏱️  Latency Statistics (ms) ---");

    // 排序 Key 以便输出整洁
    let mut sorted_keys: Vec<_> = merged_latencies.keys().cloned().collect();
    sorted_keys.sort();

    for name in sorted_keys {
        if let Some(lats) = merged_latencies.get_mut(&name) {
            print_percentiles(&name, lats);
        }
    }

    Ok(())
}

fn print_percentiles(name: &str, latencies: &mut Vec<u128>) {
    if latencies.is_empty() {
        println!("{:<25} | No data", name);
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
        "{:<25} | Cnt:{:<6} | P50:{:.2}ms | P95:{:.2}ms | P99:{:.2}ms | Max:{:.2}ms",
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
    let fetch_size = limit * 3;

    // Use dquery_dex database.
    let query = format!(
        "SELECT token0_address, ts, maker FROM dquery_dex.dex_swap_tx_solana LIMIT {}",
        fetch_size
    );
    let rows: Vec<SampleData> = sqlx::query_as(&query).fetch_all(pool).await?;
    let mut seen = HashSet::new();
    let mut unique_rows = Vec::with_capacity(limit);

    for row in rows {
        if seen.insert(row.token0_address.clone()) {
            unique_rows.push(row);
            if unique_rows.len() >= limit {
                break;
            }
        }
    }

    if unique_rows.len() < limit {
        eprintln!(
            "⚠️ Warning: Only found {} unique tokens (requested {})",
            unique_rows.len(),
            limit
        );
    }

    Ok(unique_rows)
}
