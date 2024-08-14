# -*- coding: utf-8 -*-

import typing as T
from datetime import datetime

import pynamodb_mate.api as pm

st = pm.patterns.status_tracker


T_TASK = T.TypeVar("T_TASK", bound=st.BaseTask)


class StatusEnum(st.BaseStatusEnum):
    pending = 10
    in_progress = 12
    failed = 14
    succeeded = 16
    ignored = 18


def create_orm_model(
    tracker_table_name: str,
    aws_region: str,
    use_case_id: str,
) -> T.Type[T_TASK]:
    class Task(st.BaseTask):
        class Meta:
            table_name = tracker_table_name
            region = aws_region
            billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

        status_and_update_time_index = st.StatusAndUpdateTimeIndex()

        config = st.TrackerConfig.make(
            use_case_id=use_case_id,
            pending_status=StatusEnum.pending.value,
            in_progress_status=StatusEnum.in_progress.value,
            failed_status=StatusEnum.failed.value,
            succeeded_status=StatusEnum.succeeded.value,
            ignored_status=StatusEnum.ignored.value,
            n_pending_shard=5,
            n_in_progress_shard=5,
            n_failed_shard=5,
            n_succeeded_shard=10,
            n_ignored_shard=5,
            status_zero_pad=3,
            status_shard_zero_pad=3,
            max_retry=3,
            lock_expire_seconds=60,
        )

    return Task
