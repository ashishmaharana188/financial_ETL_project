import polars as pl
from scripts.database import engine
from typing import cast
from scriptsTemp.profiling_utils import (
    log_duckdb_profile,
    log_data_quality,
    log_std,
    log_duckdb_operators,
    log_arrow_profile,
)
from scriptsTemp.preProcessing import (
    clean_and_transform,
    compute_safe_corr,
)
import pyarrow as pa

# definations

query_txd = """
    SELECT 
        m.ticker, m.close, m.date::DATE AS ReportDate, meta.Industry,
        m.volume, m.delivery_percentage, m.daily_hl_spread, m.daily_vwap_dev,
        m.oi_pcr, m.delta_oi_pcr, m.futures_basis, m.net_block_volume, m.avg_block_premium
    FROM mv_unified_market_matrix m
    INNER JOIN market_metadata meta ON m.ticker = meta.Ticker
    WHERE m.ticker = ?
"""

features = [
    "close",
    "volume",
    "delivery_percentage",
    "daily_hl_spread",
    "daily_vwap_dev",
    "oi_pcr",
    "delta_oi_pcr",
    "futures_basis",
    "net_block_volume",
    "avg_block_premium",
]

query_dxd = """
    SELECT
        m.ticker, m.date::DATE AS ReportDate, meta.Industry,
        m.volume, m.delivery_percentage, m.daily_hl_spread, m.daily_vwap_dev,
        m.oi_pcr, m.delta_oi_pcr, m.futures_basis, m.net_block_volume, m.avg_block_premium
    FROM mv_unified_market_matrix m
    INNER JOIN market_metadata meta ON m.ticker = meta.Ticker
"""

features_dxd = [
    "volume",
    "delivery_percentage",
    "daily_hl_spread",
    "daily_vwap_dev",
    "oi_pcr",
    "delta_oi_pcr",
    "futures_basis",
    "net_block_volume",
    "avg_block_premium",
]

other_features = [f for f in features if f != "close"]

target_industry = "Oil & Gas Refining & Marketing"
target_ticker = "RELIANCE"
txd_stationarization = ["close", "volume"]
dxd_stationarization = ["volume"]

leading_features = [
    "delta_oi_pcr",
    "net_block_volume",
    "avg_block_premium",
    "futures_basis",
]
lagging_targets = ["volume", "delivery_percentage", "daily_hl_spread", "daily_vwap_dev"]

# ______ target x datapoint correaltio

# TXD CHECK 1

with engine.stream_lazy(query_txd, params=[target_ticker]) as microOne:

    batches = list(microOne.reader)
    # arrow_profile_logs
    log_arrow_profile(batches, "ARROW_STREAM")

    table = pa.Table.from_batches(batches)

    prepared = clean_and_transform(
        table,
        txd_stationarization,
    )

    profile_txd = prepared.lf
    valid_cols = prepared.valid_cols
    stats_check = prepared.stats

    microCheck1 = profile_txd.select(valid_cols).collect().corr()

    pass

duckdb_profile = cast(dict, microOne.duckdb_profile)

# Profiling duck_db
print("DUCK_DB PROFILING ENTER\n")
print("TARGET x DATA POINT SECTION START --------------------\n")
log_data_quality(profile_txd)
log_std(stats_check)
log_duckdb_profile(duckdb_profile, "DUCK_DB STREAM")
log_duckdb_operators(duckdb_profile)
print("TARGET x DATA POINT SECTION END --------------------\n")


# ________ datapoint x datapoint correlation


# --- Check 1: Industry Beta + Ticker Alpha ---

with engine.stream_lazy(query_dxd) as stream_1:

    batches = list(stream_1.reader)
    # arrow_profile_logs
    log_arrow_profile(batches, "ARROW_STREAM")

    table = pa.Table.from_batches(batches)

    prepared = clean_and_transform(table, dxd_stationarization)
    profile_dxd = prepared.lf

    # Check 1
    check_1 = compute_safe_corr(
        profile_dxd.filter(pl.col("Industry") == target_industry),
        profile_dxd.collect_schema().names(),
    )

    # Check 2
    check_2 = compute_safe_corr(
        profile_dxd.filter(pl.col("ticker") == target_ticker), features_dxd
    )

    # Check 3
    base_lf = profile_dxd.filter(pl.col("Industry") == target_industry)
    industry_factor_lf = base_lf.group_by("ReportDate").agg(
        [
            pl.col(f).mean().alias(f"{f}_industry")
            for f in profile_dxd.collect_schema().names()
        ]
    )

    ticker_vs_industry = base_lf.filter(pl.col("ticker") == target_ticker).join(
        industry_factor_lf, on="ReportDate", how="inner"
    )

    check_3_features = features_dxd + [
        f"{f}_industry" for f in profile_dxd.collect_schema().names()
    ]
    check_3 = compute_safe_corr(ticker_vs_industry, check_3_features)

    # Check 4
    lead_lag_lf = (
        profile_dxd.filter(pl.col("Industry") == target_industry)
        .sort(["ticker", "ReportDate"])
        .with_columns(
            [
                pl.col(f).shift(1).over("ticker").alias(f"{f}_t-1")
                for f in leading_features
            ]
        )
        .drop_nulls(subset=[f"{f}_t-1" for f in leading_features])
    )

    matrix_cols = lagging_targets + [f"{f}_t-1" for f in leading_features]
    check_4 = compute_safe_corr(lead_lag_lf, matrix_cols)

duckdb_profile = cast(dict, stream_1.duckdb_profile)

print("DATA POINT x DATA POINT SECTION START--------------------------\n")
log_data_quality(profile_dxd)
log_std(stats_check)
log_duckdb_profile(duckdb_profile, "DUCK_DB STREAM")
log_duckdb_operators(duckdb_profile)
print("DATA POINT x DATA POINT SECTION END --------------------\n")

print("DUCK_DB PROFILING EXIT\n")


print("CORRELATION SECTION START --------------------\n")
print("TARGET x DATA POINT CHECKS\n")
print("Check 1 (Industry Fixed Effects):\n", microCheck1)
print("DATA POINT x DATA POINT CHECKS\n")
print("Check 1 (Industry Fixed Effects):\n", check_1)
print("Check 2 (Pure Ticker):\n", check_2)
print(f"Check 3 {target_ticker} vs {target_industry} Baseline:\n", check_3)
print("Check 4 Lead-Lag Micro Matrix:\n", check_4)
print("CORRELATION SECTION END --------------------\n")
