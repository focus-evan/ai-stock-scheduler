"""
system_endpoints stub for ai-stock-scheduler.

Only provides the get_scheduler_users_from_db function needed by portfolio_scheduler.
"""

import os
import structlog

logger = structlog.get_logger()

# 定时任务用户列表（硬编码兜底，可通过环境变量覆盖）
_DEFAULT_SCHEDULER_USERS = ["admin"]


async def get_scheduler_users_from_db():
    """
    从数据库或环境变量获取定时任务用户列表。
    在 scheduler 独立部署环境下，简化为直接读取环境变量。
    """
    users_env = os.getenv("SCHEDULER_USERS", "")
    if users_env:
        return [u.strip() for u in users_env.split(",") if u.strip()]
    return _DEFAULT_SCHEDULER_USERS
