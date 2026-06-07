from typing import cast
import polars.selectors as cs
from dataclasses import dataclass
import polars as pl


@dataclass
class PreparedData:

    lf: pl.LazyFrame
    stats: pl.DataFrame
    valid_cols: list[str]
    profile: dict | None = None


def clean_and_transform(
    reader,
    stationarization_columns,
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

    stats = lf.select(
        [
            pl.col(c).std().alias(c)
            for c, dt in lf.collect_schema().items()
            if dt.is_numeric()
        ]
    ).collect()

    valid_cols = [
        c
        for c in lf.collect_schema().names()
        if c in stats.columns and stats[c][0] is not None and stats[c][0] > 0
    ]

    return PreparedData(
        lf=lf,
        stats=stats,
        valid_cols=valid_cols,
    )


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
