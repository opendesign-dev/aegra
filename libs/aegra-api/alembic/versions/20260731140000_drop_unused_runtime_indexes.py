"""删除运行时精简后不再使用的索引

Revision ID: f2c8a5e13d94
Revises: e4a9c2d17b83
Create Date: 2026-07-31 14:00:00.000000

``d1f7b3a9c5e2`` 为 multitask 序列化、延迟运行、TTL 清扫和 thread_state 物化建立的
索引，在这些功能移除后全部失去查询方；其中 ``uq_runs_one_running_per_thread`` 不只是
开销 —— double-texting 保护已随 multitask 一起移除，worker 认领时把 status 改为
``running`` 的 UPDATE 不再有任何前置串行化，同一 thread 的第二个并发 run 会在 commit
时撞上该唯一索引，异常冒泡到 worker 循环后该 run 每次重试都会重复失败。

列与表（``runs.metadata``、``cancel_requested``、``scheduled_at``、
``multitask_strategy``、``thread.ttl``、``crons.failure_count``、``thread_state``、
``webhook_deliveries``）刻意保留：ORM 不再声明它们，且都可空或带 DEFAULT，不影响
写入；删除则不可逆，收益仅是整洁。

保留 ``idx_thread_user_created`` 与 ``idx_runs_thread_created``：thread 列表和
``GET /threads/{id}/runs`` 仍按 (owner/thread, created_at DESC) 查询。
"""

from alembic import op

revision = "f2c8a5e13d94"
down_revision = "e4a9c2d17b83"
branch_labels = None
depends_on = None

_UQ_NAME = "uq_runs_one_running_per_thread"

# (名称, 重建语句)：upgrade 删除，downgrade 从同一份定义恢复。
_UNUSED = (
    # run_ttl_sweeper 已移除。
    (
        "idx_runs_ttl_sweep",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_ttl_sweep ON runs (updated_at) "
        "WHERE status IN ('success', 'error', 'interrupted', 'timeout')",
    ),
    # delayed_run_scheduler 已移除，scheduled_at 不再被扫描。
    (
        "idx_runs_scheduled",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_scheduled ON runs (status, scheduled_at)",
    ),
    # thread_state 不再读写。
    (
        "idx_thread_state_values_gin",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_state_values_gin "
        'ON thread_state USING gin ("values" jsonb_path_ops)',
    ),
    # CronSearchRequest 不再暴露 metadata 过滤。
    (
        "idx_cron_metadata_gin",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cron_metadata_gin ON crons USING gin (metadata jsonb_path_ops)",
    ),
    # webhook 投递已移除，表本身保留。
    (
        "idx_webhook_deliveries_due",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_webhook_deliveries_due "
        "ON webhook_deliveries (status, next_attempt_at)",
    ),
)

# downgrade 必须先消除重复才能重建唯一索引：本迁移一旦生效，同 thread 多个 running
# 就是合法状态，直接 CONCURRENTLY 建索引会失败并留下 INVALID 索引。
_DEDUP_RUNNING = """
    UPDATE runs SET
        status = 'error',
        error_message = COALESCE(error_message, 'superseded: duplicate running run per thread'),
        updated_at = now()
    WHERE run_id IN (
        SELECT run_id FROM (
            SELECT run_id,
                   row_number() OVER (PARTITION BY thread_id ORDER BY created_at DESC, run_id DESC) AS rn
            FROM runs
            WHERE status = 'running'
        ) ranked
        WHERE ranked.rn > 1
    )
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_UQ_NAME}")
        for name, _ in _UNUSED:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")


def downgrade() -> None:
    op.execute(_DEDUP_RUNNING)

    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {_UQ_NAME} ON runs (thread_id) WHERE status = 'running'"
        )
        for _, create_stmt in _UNUSED:
            op.execute(create_stmt)
