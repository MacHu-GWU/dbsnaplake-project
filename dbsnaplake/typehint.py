# -*- coding: utf-8 -*-

import typing as T

if T.TYPE_CHECKING:
    import polars as pl

T_RECORD = T.Dict[str, T.Any]
T_DF_SCHEMA = T.Dict[str, "pl.DataType"]
T_EXTRACTOR = T.Union["pl.Expr", str]
T_OPTIONAL_KWARGS = T.Optional[T.Dict[str, T.Any]]
