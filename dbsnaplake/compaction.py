# -*- coding: utf-8 -*-

import typing as T
import polars as pl


def get_merged_schema(
    dfs: T.List[pl.DataFrame],
) -> T.Dict[str, pl.DataType]:
    merged_schema = dict()
    for df in dfs:
        schema = dict(df.schema)
        merged_schema.update(schema)
    return merged_schema


def harmonize_schemas(
    dfs: T.List[pl.DataFrame],
    schema: T.Dict[str, pl.DataType],
) -> T.List[pl.DataFrame]:
    new_dfs = list()
    for df in dfs:
        this_schema = set(df.schema)
        merged_schema = dict(schema)
        for k in this_schema:
            merged_schema.pop(k)
        new_columns = [pl.lit(None, dtype=v).alias(k) for k, v in merged_schema.items()]
        df = df.with_columns(*new_columns)
        new_dfs.append(df)
    return new_dfs
