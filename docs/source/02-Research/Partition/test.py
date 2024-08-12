import polars as pl
import numpy as np
from dbsnaplake.vendor.timer import DateTimeTimer

# n_row = 1_000
# n_row = 1_000_000
n_row = 100_000_000
df = pl.DataFrame(
    {
        "year": np.random.randint(2001, 2010, n_row),
        "month": np.random.randint(1, 12, n_row),
        "value": np.random.randn(n_row),
    },
)
pkeys = ["year", "month"]
with DateTimeTimer("Method 1") as timer:
    for sub_df in df.partition_by(by=pkeys):
        kvs = sub_df.select(pkeys).head(1).to_dicts()[0]
        sub_df = sub_df.drop(pkeys)
        # sub_df.count()
        # print(kvs, sub_df)

with DateTimeTimer("Method 2") as timer:
    for pvalues, sub_df in df.group_by(pkeys):
        kvs = dict(zip(pkeys, pvalues))
        sub_df.count()
        # print(kvs, sub_df)

