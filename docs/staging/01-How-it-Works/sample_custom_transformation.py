import polars as pl


def batch_read_snapshot_data_file(
    db_snapshot_manifest_file,
):
    # how you plan to read
    df = db_snapshot_manifest_file.read_csv()
    # df = db_snapshot_manifest_file.read_json()
    # df = db_snapshot_manifest_file.read_parquet()
    # df = db_snapshot_manifest_file.read_avro()

    # do any custom transformation
    df = (
        df.with_column(
            pl.col("OrderDate").str.to_datetime(),
        )
        .filter(
            pl.col("Category") != "Internal Order",
        )
        .drop(["Col1", "Col2"])
    )

    return df
