import polars as pl
from scripts.database import engine
from typing import cast

MB = 1024 * 1024

# ______ target x datapoint correaltion

query = """
    SELECT 
        m.ticker, m.close, m.date::DATE AS ReportDate, meta.Industry,
        m.Volume, m.Delivery_Percentage, m.daily_hl_spread, m.daily_vwap_dev,
        m.oi_pcr, m.delta_oi_pcr, m.futures_basis, m.net_block_volume, m.avg_block_premium
    FROM mv_unified_market_matrix m
    INNER JOIN market_metadata meta ON m.ticker = meta.Ticker
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
other_features = [f for f in features if f != "close"]

target_industry = "Aerospace & Defense"
target_ticker = "COCHINSHIP"

with engine.stream_lazy(query) as microOne:

    lf = cast(pl.DataFrame, pl.from_arrow(microOne.reader))
    microCheck1 = (
        lf.lazy()
        .filter(pl.col("ticker") == target_ticker)
        .select([pl.corr("close", f) for f in other_features])
    )
    pass

profile = cast(dict, microOne.profile)

# Profiling
print(
    f"TXDCheck 1 | "
    f"QUERY | "
    f"time={profile['latency']:.4f}s | "
    f"rows_read={profile['cumulative_rows_scanned']} | "
    f"rows_output={profile['rows_returned']} | "
    f"peak_mem={profile['system_peak_buffer_memory']/MB:.2f}MB | "
    f"read_size={profile['total_bytes_read']/MB:.2f}MB | "
    f"output_size={profile['result_set_size']/MB:.2f}MB\n"
    f"_____________________ \n"
)


def log_operator(node):
    if node["operator_name"] != "SEQ_SCAN":

        print(
            f"{node['operator_name']} | "
            f"time={node['operator_timing']:.4f}s | "
            f"rows={node['operator_cardinality']} | "
            f"size={node['result_set_size']/MB:.2f}MB\n"
        )

    if node["operator_name"] == "SEQ_SCAN":
        print(
            f"{node['extra_info']['Table']} | "
            f"time={node['operator_timing']:.4f}s | "
            f"rows={node['operator_cardinality']} | "
            f"size={node['result_set_size']/MB:.2f}MB\n"
        )

    for child in node["children"]:
        log_operator(child)


log_operator(profile["children"][0])


# ________ datapoint x datapoint correlation

query = """
    SELECT 
        m.ticker, m.date::DATE AS ReportDate, meta.Industry,
        m.Volume, m.Delivery_Percentage, m.daily_hl_spread, m.daily_vwap_dev,
        m.oi_pcr, m.delta_oi_pcr, m.futures_basis, m.net_block_volume, m.avg_block_premium
    FROM mv_unified_market_matrix m
    INNER JOIN market_metadata meta ON m.ticker = meta.Ticker
"""

features = [
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
target_industry = "Aerospace & Defense"
target_ticker = "COCHINSHIP"

# --- Check 1: Industry Beta + Ticker Alpha ---
with engine.stream_lazy(query) as stream_1:

    check_1 = (
        cast(pl.DataFrame, pl.from_arrow(stream_1.reader))
        .lazy()
        .filter(pl.col("Industry") == target_industry)
        .with_columns(
            [
                (
                    (pl.col(f) - pl.col(f).mean().over("ticker"))
                    / pl.when(pl.col(f).std().over("ticker") == 0)
                    .then(1.0)
                    .otherwise(pl.col(f).std().over("ticker"))
                ).alias(f)
                for f in features
            ]
        )
        .select(features)
        .collect(engine="streaming")
        .corr()
    )
    pass


profile = cast(dict, stream_1.profile)

# Profiling
print(
    f"DXDCheck 1 | "
    f"QUERY | "
    f"time={profile['latency']:.4f}s | "
    f"rows_read={profile['cumulative_rows_scanned']} | "
    f"rows_output={profile['rows_returned']} | "
    f"peak_mem={profile['system_peak_buffer_memory']/MB:.2f}MB | "
    f"read_size={profile['total_bytes_read']/MB:.2f}MB | "
    f"output_size={profile['result_set_size']/MB:.2f}MB\n"
    f"_____________________ \n"
)

log_operator(profile["children"][0])

# --- Check 2: Ticker Beta + Ticker Alpha ---
with engine.stream_lazy(query) as stream_2:

    check_2 = (
        cast(pl.DataFrame, pl.from_arrow(stream_2.reader))
        .lazy()
        .filter(pl.col("ticker") == target_ticker)
        .select(features)
        .collect(engine="streaming")
        .corr()
    )
    pass

profile = cast(dict, stream_2.profile)

# Profiling
print(
    f"DXDCheck 2 | "
    f"QUERY | "
    f"time={profile['latency']:.4f}s | "
    f"rows_read={profile['cumulative_rows_scanned']} | "
    f"rows_output={profile['rows_returned']} | "
    f"peak_mem={profile['system_peak_buffer_memory']/MB:.2f}MB | "
    f"read_size={profile['total_bytes_read']/MB:.2f}MB | "
    f"output_size={profile['result_set_size']/MB:.2f}MB\n"
    f"_____________________ \n"
)

log_operator(profile["children"][0])


# --- Check 3: Ticker Beta + Ticker Alpha ---
with engine.stream_lazy(query) as stream_3:

    base_lf = (
        cast(pl.DataFrame, pl.from_arrow(stream_3.reader))
        .lazy()
        .filter(pl.col("Industry") == target_industry)
    )

    industry_factor_lf = base_lf.group_by("ReportDate").agg(
        [pl.col(f).mean().alias(f"{f}_industry") for f in features]
    )

    ticker_vs_industry = (
        base_lf.filter(pl.col("ticker") == target_ticker)
        .join(industry_factor_lf, on="ReportDate", how="inner")
        .collect(engine="streaming")
    )

    check_3 = ticker_vs_industry.select(
        features + [f"{f}_industry" for f in features]
    ).corr()
    pass

profile = cast(dict, stream_3.profile)

# Profiling
print(
    f"DXDCheck 3 | "
    f"QUERY | "
    f"time={profile['latency']:.4f}s | "
    f"rows_read={profile['cumulative_rows_scanned']} | "
    f"rows_output={profile['rows_returned']} | "
    f"peak_mem={profile['system_peak_buffer_memory']/MB:.2f}MB | "
    f"read_size={profile['total_bytes_read']/MB:.2f}MB | "
    f"output_size={profile['result_set_size']/MB:.2f}MB\n"
    f"_____________________ \n"
)

log_operator(profile["children"][0])

# --- Check 4: LAGGED t-1 --- update this to include the above three checks each as lagged

leading_features = [
    "delta_oi_pcr",
    "net_block_volume",
    "avg_block_premium",
    "futures_basis",
]
lagging_targets = ["volume", "delivery_percentage", "daily_hl_spread", "daily_vwap_dev"]


with engine.stream_lazy(query) as stream_4:
    lead_lag_lf = (
        cast(pl.DataFrame, pl.from_arrow(stream_4.reader))
        .lazy()
        .filter(pl.col("Industry") == target_industry)
        .sort(["ticker", "ReportDate"])
        .with_columns(
            [
                pl.col(f).shift(1).over("ticker").alias(f"{f}_t-1")
                for f in leading_features
            ]
        )
        # Drop rows where shifted data is null due to boundary alignment
        .drop_nulls(subset=[f"{f}_t-1" for f in leading_features])
    )
    pass

    # Materialize and run cross-correlation
    matrix_cols = lagging_targets + [f"{f}_t-1" for f in leading_features]
    check_4 = lead_lag_lf.select(matrix_cols).collect(engine="streaming").corr()
    pass

profile = cast(dict, stream_4.profile)

# Profiling
print(
    f"DXDCheck 4 | "
    f"QUERY | "
    f"time={profile['latency']:.4f}s | "
    f"rows_read={profile['cumulative_rows_scanned']} | "
    f"rows_output={profile['rows_returned']} | "
    f"peak_mem={profile['system_peak_buffer_memory']/MB:.2f}MB | "
    f"read_size={profile['total_bytes_read']/MB:.2f}MB | "
    f"output_size={profile['result_set_size']/MB:.2f}MB\n"
    f"_____________________\n "
)

log_operator(profile["children"][0])

print("Check 1 (Industry Fixed Effects):\n", check_1)
print("Check 2 (Pure Ticker):\n", check_2)
print(f"Check 3 {target_ticker} vs {target_industry} Baseline:\n", check_3)
print("Check 4 Lead-Lag Micro Matrix:\n", check_4)
