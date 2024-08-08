# -*- coding: utf-8 -*-

import typing as T
import dataclasses
import polars as pl
from collections import deque


@dataclasses.dataclass(slots=True)
class File:
    """
    Represent a file.
    """

    id: str
    size: int


@dataclasses.dataclass(slots=True)
class FileGroup:
    """
    Represent a group of files.
    """

    files: T.List[File] = dataclasses.field(default_factory=list)

    @property
    def total_size(self) -> int:
        return sum([f.size for f in self.files])


def calculate_merge_plan(
    files: T.List[File],
    target_size: int,
) -> T.List[FileGroup]:
    """
    Given a list of :class:`File` and a target size, put them into groups,
    so that each group has approximately the same size as the target size.
    """
    half_target_size = target_size // 2

    files = deque(sorted(files, key=lambda x: x.size))
    file_groups = list()
    file_group = FileGroup()
    file_group_size = 0

    while 1:
        # if no files left
        if len(files) == 0:
            if len(file_group.files):
                file_groups.append(file_group)
            break

        remaining_size = target_size - file_group_size
        # take the largest file
        if remaining_size <= half_target_size:
            file = files.popleft()
        # take the smallest file
        else:
            file = files.pop()

        file_group.files.append(file)
        file_group_size += file.size

        if file_group_size >= target_size:
            file_groups.append(file_group)
            file_group = FileGroup()
            file_group_size = 0

    return file_groups


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
