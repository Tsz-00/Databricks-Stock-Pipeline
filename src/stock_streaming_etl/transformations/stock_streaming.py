from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, max_by, lag, struct

VOLUME_PATH = "/Volumes/stock_dev/dev_lamtszhong2014_yfinance/landing/*-*-*/*.parquet"

@dp.table(
    comment="Bronze table for data ingestion from yfinance.landing volumes",
    table_properties={"quality": "bronze"}
)
def bronze_stock():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(VOLUME_PATH)
        .withColumns({
            "_file_path": col("_metadata.file_path"),
            "_ingestion_timestamp": current_timestamp()
        })
    )
    return df.select(*[col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])


@dp.table(
    comment="Silver table for clean and expect",
    table_properties={"quality": "silver"}
)
@dp.expect_all_or_drop({
    "valid_stock_price": 
        """
        close IS NOT NULL
        AND high IS NOT NULL
        AND low IS NOT NULL
        AND open IS NOT NULL
        AND volume IS NOT NULL
        """
})
def silver_stock():
    return (
        spark.table("LIVE.bronze_stock")
        .groupBy("datetime", "ticker")
        .agg(max_by(struct("*"), "_ingestion_timestamp").alias("row"))
        .select("row.*")
        .select("datetime", "ticker", "close", "high", "low", "open", "volume")
    )


@dp.materialized_view(
    comment="Gold table for top gainer and loser by date",
    table_properties={"quality": "gold"}
)
def gold_daily_pct_change():
    date = col("datetime").cast("date").alias("date")
    window = Window.partitionBy("ticker").orderBy(date)
    prev_close = lag("close", 1).over(window)

    return (
        spark.table("LIVE.silver_stock")
        .groupBy(date, "ticker")
        .agg(max_by(struct("*"), "datetime").alias("row"))
        .select("date", "row.*")
        .withColumn("pct_change", (col("close") - prev_close) / prev_close)
    )
