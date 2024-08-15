Partition By VS Group By
==============================================================================
Test the performance of ``polars.DataFrame.partition_by`` and ``polars.DataFrame.group_by``.

.. dropdown:: test_partition_by_vs_group_by.py

    .. literalinclude:: ./test_partition_by_vs_group_by.py
       :language: python
       :linenos:

**Conclusion**

If you don't do any operation in sub dataframe, and you need the partition key values, group by is faster.

If you just want to split the dataframe into sub dataframes, and you don't need the partition key values, use partition by.
