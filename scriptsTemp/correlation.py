import polars as pl
from scripts.database import engine
from typing import cast
from rich.table import Table
from rich.console import Console
from dataclasses import dataclass
import polars.selectors as cs

console = Console()
MB = 1024 * 1024


@dataclass
class PreparedData:

    lf: pl.LazyFrame
    stats: pl.DataFrame
    valid_cols: list[str]
    profile: dict | None = None


def log_profile(profile: dict, title: str):

    table = Table(title=title)

    table.add_column("Metric")
    table.add_column("Value")

    latency = profile.get("latency", profile.get("timing", 0.0))
    table.add_row("Query Time", f"{latency:.4f}s")

    table.add_row("Rows Read", str(profile.get("cumulative_rows_scanned", "-")))
    table.add_row("Rows Output", str(profile.get("rows_returned", "-")))

    table.add_row("Peak Memory", f"{profile['system_peak_buffer_memory']/MB:.2f} MB")

    table.add_row("Read Size", f"{profile['total_bytes_read']/MB:.2f} MB")

    table.add_row("Output Size", f"{profile['result_set_size']/MB:.2f} MB")

    console.print(table)


def log_data_quality(lf):

    schema = lf.collect_schema()

    table = Table(title="Data Quality")

    table.add_column("Column")
    table.add_column("Type")
    table.add_column("Null")
    table.add_column("NaN")
    table.add_column("Inf")

    for c, dt in schema.items():

        nulls = lf.select(pl.col(c).null_count()).collect().item()

        if dt in (pl.Float32, pl.Float64):

            nans = lf.select(pl.col(c).is_nan().sum()).collect().item()

            infs = lf.select(pl.col(c).is_infinite().sum()).collect().item()

        else:
            nans = "-"
            infs = "-"

        table.add_row(
            c,
            str(dt),
            str(nulls),
            str(nans),
            str(infs),
        )

    console.print(table)


def log_std(stats_check):

    table = Table(title="Standard Deviation")

    table.add_column("Column")
    table.add_column("Std")

    for c in stats_check.columns:

        table.add_row(c, f"{stats_check[c][0]:.6f}")

    console.print(table)


def log_operators(profile):

    table = Table(title="DuckDB Operators")

    table.add_column("Operator")
    table.add_column("Time")
    table.add_column("Rows")
    table.add_column("Size MB")

    def walk(node):

        name = (
            node["extra_info"]["Table"]
            if node["operator_name"] == "SEQ_SCAN"
            else node["operator_name"]
        )

        table.add_row(
            name,
            f"{node['operator_timing']:.4f}",
            str(node["operator_cardinality"]),
            f"{node['result_set_size']/MB:.2f}",
        )

        for child in node["children"]:
            walk(child)

    walk(profile["children"][0])

    console.print(table)


def prepare_features(
    reader,
    stationarization_columns,
    features,
):
    df = cast(pl.DataFrame, pl.from_arrow(reader))

    lf = (
        df.lazy()
        .sort(["ticker", "ReportDate"])
        .with_columns(
            [
                (pl.col(c) / pl.col(c).shift(1).over("ticker") - 1).alias(c)
                for c in stationarization_columns
            ]
        )
        # FIX: Restrict the evaluation strictly to floating-point types
        .with_columns(
            pl.when(cs.float().is_infinite() | cs.float().is_nan())
            .then(None)
            .otherwise(cs.float())
            .name.keep()
        )
        .drop_nulls(subset=stationarization_columns)
    )

    stats = lf.select([pl.col(c).std().alias(c) for c in features]).collect()

    valid_cols = [c for c in features if stats[c][0] is not None and stats[c][0] > 0]

    return PreparedData(
        lf=lf,
        stats=stats,
        valid_cols=valid_cols,
    )


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

    prepared = prepare_features(
        microOne.reader,
        txd_stationarization,
        features,
    )

    stationarization = prepared.lf
    valid_cols = prepared.valid_cols
    stats_check = prepared.stats
    profile = prepared.lf

    microCheck1 = stationarization.select(valid_cols).collect().corr()

    pass

profile = cast(dict, microOne.profile)

# Profiling

log_data_quality(stationarization)
log_std(stats_check)
log_profile(profile, "TXDCheck 1")
log_operators(profile)
print("END TXD CHECK_1")


# ________ datapoint x datapoint correlation


def compute_safe_corr(lf: pl.LazyFrame, columns: list[str]) -> pl.DataFrame:
    """Materializes a slice, dynamically prunes flatlines in one pass, and computes correlation."""
    df = lf.select(columns).collect()

    if df.is_empty():
        return pl.DataFrame()

    std_df = df.std()

    moving_cols = [c for c in columns if std_df[c][0] is not None and std_df[c][0] > 0]

    if len(moving_cols) > 1:
        return df.select(moving_cols).corr()

    return pl.DataFrame()


# --- Check 1: Industry Beta + Ticker Alpha ---

with engine.stream_lazy(query_dxd) as stream_1:
    prepared = prepare_features(stream_1.reader, dxd_stationarization, features_dxd)
    stationarization = prepared.lf

    # Check 1
    check_1 = compute_safe_corr(
        stationarization.filter(pl.col("Industry") == target_industry), features_dxd
    )

    # Check 2
    check_2 = compute_safe_corr(
        stationarization.filter(pl.col("ticker") == target_ticker), features_dxd
    )

    # Check 3
    base_lf = stationarization.filter(pl.col("Industry") == target_industry)
    industry_factor_lf = base_lf.group_by("ReportDate").agg(
        [pl.col(f).mean().alias(f"{f}_industry") for f in features_dxd]
    )

    ticker_vs_industry = base_lf.filter(pl.col("ticker") == target_ticker).join(
        industry_factor_lf, on="ReportDate", how="inner"
    )

    check_3_features = features_dxd + [f"{f}_industry" for f in features_dxd]
    check_3 = compute_safe_corr(ticker_vs_industry, check_3_features)

    # Check 4
    lead_lag_lf = (
        stationarization.filter(pl.col("Industry") == target_industry)
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

profile = cast(dict, stream_1.profile)

log_data_quality(stationarization)
log_profile(profile, "DXDCheck 1")
log_operators(profile)

print("END CHECK 4")
print("Check 1 (Industry Fixed Effects):\n", check_1)
print("Check 2 (Pure Ticker):\n", check_2)
print(f"Check 3 {target_ticker} vs {target_industry} Baseline:\n", check_3)
print("Check 4 Lead-Lag Micro Matrix:\n", check_4)
