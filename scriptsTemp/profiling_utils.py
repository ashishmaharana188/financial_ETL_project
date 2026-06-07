import polars as pl

from rich.table import Table
from rich.console import Console

console = Console()
MB = 1024 * 1024


def log_duckdb_profile(profile: dict, title: str):
    # DUCK_DB
    table = Table(title=title)
    if title == "DUCKDB_STREAM":

        table.add_column("Metric")
        table.add_column("Value")

        latency = profile.get("latency", profile.get("timing", 0.0))
        table.add_row("Query Time", f"{latency:.4f}s")

        table.add_row("Rows Read", str(profile.get("cumulative_rows_scanned", "-")))
        table.add_row("Rows Output", str(profile.get("rows_returned", "-")))

        table.add_row(
            "Peak Memory", f"{profile['system_peak_buffer_memory']/MB:.2f} MB"
        )

        table.add_row("Read Size", f"{profile['total_bytes_read']/MB:.2f} MB")

        table.add_row("Output Size", f"{profile['result_set_size']/MB:.2f} MB")

        console.print(table)

    # ARROW_BATCH


def log_arrow_profile(profile, title: str):

    table = Table(title=title)
    table.add_column("Metric")
    table.add_column("Value")

    for batch in profile:

        table.add_row("Rows", str(batch.num_rows))
        table.add_row("Columns", str(batch.num_columns))
        table.add_row("Size", f"{batch.nbytes / MB:.2f} MB")

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


def log_duckdb_operators(profile):

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
