"""
模拟交易定时任务调度�?(Portfolio Scheduler) �?每战法单次交易制

设计原则:
1. 每个战法每个交易日只�?**一�?* 交易窗口（非强制，LLM判断是否执行�?
2. 每次交易前必须读取前日复盘，LLM综合分析决定是否操作
3. 14:30-15:00 为用户手动操作保护时段，所有自动交易跳�?
4. 每个战法使用个性化LLM提示词，体现各战法交易纪�?

凌晨预生成推荐（不依赖盘中实时数据）:
  趋势动量:   05:30 预生�?
  均线战法:   04:00 预生�?
  量价关系:   02:30 预生�?
  突破战法:   01:00 预生�?

盘中交易窗口（每战法仅一次）:
  趋势动量:   09:45-10:05  开盘即执行，动量信号鲜�?
  事件驱动:   09:55-10:15  新闻消化后第一时间介入
  龙头战法:   10:15-10:40  等涨停板初步确认后评�?
  突破战法:   10:15-10:35  凌晨推荐+盘中突破确认后交�?
  情绪战法:   10:45-11:10  情绪指标积累充分后决�?
  量价关系:   10:50-11:15  错开突破战法窗口
  均线战法:   11:15-11:35  错开量价窗口
  隔夜施工�?  13:00-13:20  综合上午竞价和午后判�?
  北向资金:   13:30-13:50  下午北向数据充分后决�?
  综合战法:   14:10-14:25  等所有战法结论聚合后执行

⚠️ 14:30-15:00 为用户手动刷新保护时段，自动交易暂停

结算与复�?
  15:05 ~ 16:00  更新收盘价并计算收益
  16:00 ~ 16:30  GPT-5.2 复盘（分析操作优劣）
"""

import asyncio
import traceback
from datetime import datetime, time as dtime, timezone, timedelta
from typing import Dict, Any, List

import structlog

logger = structlog.get_logger()

# 统一反爬模块
try:
    from anti_scrape import (
        async_anti_scrape_delay,
        DELAY_SCHEDULER, DELAY_PORTFOLIO, DELAY_REVIEW, DELAY_LIGHT,
    )
except ImportError:
    from app.anti_scrape import (
        async_anti_scrape_delay,
        DELAY_SCHEDULER, DELAY_PORTFOLIO, DELAY_REVIEW, DELAY_LIGHT,
    )

# ==================== 北京时区 ====================
# 确保无论服务器系统时区如何，调度器始终使用北京时�?
try:
    from zoneinfo import ZoneInfo
    _BEIJING_TZ = ZoneInfo("Asia/Shanghai")
except ImportError:
    _BEIJING_TZ = timezone(timedelta(hours=8))


def _beijing_now() -> datetime:
    """获取当前北京时间（不依赖系统时区设置�?""
    return datetime.now(_BEIJING_TZ)


# ==================== 定时任务配置（每战法单次交易制） ====================

# ⚠️ 用户手动刷新保护时段�?4:30-15:00 期间不执行任何自动交�?
USER_REFRESH_START = dtime(14, 30)
USER_REFRESH_END   = dtime(15, 0)

# ==================== 凌晨预生�?====================
BT_PREMARKET_START    = dtime(1, 0)      # 突破战法 01:00
BT_PREMARKET_END      = dtime(1, 30)
VP_PREMARKET_START    = dtime(2, 30)     # 量价关系 02:30
VP_PREMARKET_END      = dtime(3, 0)
MA_PREMARKET_START    = dtime(4, 0)      # 均线战法 04:00
MA_PREMARKET_END      = dtime(4, 30)
TM_PREMARKET_START    = dtime(5, 30)     # 趋势动量 05:30
TM_PREMARKET_END      = dtime(6, 0)

# ==================== 盘中推荐 ====================
# 上午推荐（依赖实时数据的战法�?
ON_RECOMMEND_AM_START = dtime(9, 25)    # 隔夜施工�?集合竞价�?
ON_RECOMMEND_AM_END   = dtime(9, 40)
ED_RECOMMEND_AM_START = dtime(9, 40)    # 事件驱动 开盘消化后
ED_RECOMMEND_AM_END   = dtime(9, 55)
DH_RECOMMEND_AM_START = dtime(10, 0)    # 龙头战法 涨停板初步确�?
DH_RECOMMEND_AM_END   = dtime(10, 15)
NB_RECOMMEND_AM_START = dtime(10, 15)   # 北向资金 上午积累
NB_RECOMMEND_AM_END   = dtime(10, 30)
SE_RECOMMEND_AM_START = dtime(10, 30)   # 情绪战法 情绪指标积累
SE_RECOMMEND_AM_END   = dtime(10, 45)
CB_RECOMMEND_AM_START = dtime(9, 0)     # 综合战法 读凌晨缓�?
CB_RECOMMEND_AM_END   = dtime(9, 20)

# 下午推荐更新（所有战法，为综合战�?3:50聚合提供最新数据）
ED_RECOMMEND_PM_START = dtime(13, 0)    # 事件驱动 午后新闻
ED_RECOMMEND_PM_END   = dtime(13, 10)
TM_RECOMMEND_PM_START = dtime(13, 10)   # 趋势动量 午后动量确认
TM_RECOMMEND_PM_END   = dtime(13, 20)
DH_RECOMMEND_PM_START = dtime(13, 5)    # 龙头战法 午后涨停更新
DH_RECOMMEND_PM_END   = dtime(13, 15)
BT_RECOMMEND_PM_START = dtime(13, 15)   # 突破战法 盘中突破确认
BT_RECOMMEND_PM_END   = dtime(13, 25)
SE_RECOMMEND_PM_START = dtime(13, 10)   # 情绪战法 午后情绪更新
SE_RECOMMEND_PM_END   = dtime(13, 20)
VP_RECOMMEND_PM_START = dtime(13, 20)   # 量价关系 盘中量价更新
VP_RECOMMEND_PM_END   = dtime(13, 30)
ON_RECOMMEND_PM_START = dtime(14, 30)   # 隔夜施工�?14:30 七步筛�?
ON_RECOMMEND_PM_END   = dtime(14, 35)
MA_RECOMMEND_PM_START = dtime(13, 15)   # 均线战法 盘中均线确认
MA_RECOMMEND_PM_END   = dtime(13, 30)
NB_RECOMMEND_PM_START = dtime(13, 30)   # 北向资金 下午更新
NB_RECOMMEND_PM_END   = dtime(13, 45)
CB_RECOMMEND_PM_START = dtime(14, 36)   # 综合战法 下午聚合（隔夜施工法推荐完毕后）
CB_RECOMMEND_PM_END   = dtime(14, 45)

# ==================== 每战法单次交易窗�?====================
# 趋势动量  09:45-10:05  开盘即执行，动量信号鲜�?
TM_TRADE_START   = dtime(9, 45)
TM_TRADE_END     = dtime(10, 5)

# 事件驱动  09:55-10:15  新闻消化后第一时间
ED_TRADE_START   = dtime(9, 55)
ED_TRADE_END     = dtime(10, 20)

# 龙头战法  10:15-10:40  涨停板确认后综合判断
DH_TRADE_START   = dtime(10, 15)
DH_TRADE_END     = dtime(10, 40)

# 突破战法  10:15-10:35  凌晨推荐+盘中突破双重确认（与龙头错开15分）
BT_TRADE_START   = dtime(10, 20)
BT_TRADE_END     = dtime(10, 40)

# 情绪战法  10:45-11:10  情绪指标充分积累�?
SE_TRADE_START   = dtime(10, 45)
SE_TRADE_END     = dtime(11, 10)

# 量价关系  10:50-11:15  错开突破战法
VP_TRADE_START   = dtime(10, 50)
VP_TRADE_END     = dtime(11, 15)

# 均线战法  11:15-11:35  错开量价窗口
MA_TRADE_START   = dtime(11, 15)
MA_TRADE_END     = dtime(11, 35)

# 隔夜施工�?14:45-14:50  尾盘介入（七步筛选完成后�?
ON_TRADE_START   = dtime(14, 45)
ON_TRADE_END     = dtime(14, 50)

# 北向资金  13:35-13:55  下午北向数据充分后决�?
NB_TRADE_START   = dtime(13, 35)
NB_TRADE_END     = dtime(13, 55)

# 综合战法  14:50-14:55  所有战法结论聚合后
CB_TRADE_START   = dtime(14, 50)
CB_TRADE_END     = dtime(14, 55)

# ==================== 跟投建议（交易后�?0分钟�?====================
TM_FOLLOW_START  = dtime(10, 5)
TM_FOLLOW_END    = dtime(10, 25)
ED_FOLLOW_START  = dtime(10, 20)
ED_FOLLOW_END    = dtime(10, 40)
DH_FOLLOW_START  = dtime(10, 40)
DH_FOLLOW_END    = dtime(11, 0)
BT_FOLLOW_START  = dtime(10, 40)
BT_FOLLOW_END    = dtime(11, 0)
SE_FOLLOW_START  = dtime(11, 10)
SE_FOLLOW_END    = dtime(11, 30)
VP_FOLLOW_START  = dtime(11, 15)
VP_FOLLOW_END    = dtime(11, 35)
MA_FOLLOW_START  = dtime(11, 35)
MA_FOLLOW_END    = dtime(11, 55)
ON_FOLLOW_START  = dtime(14, 50)
ON_FOLLOW_END    = dtime(14, 53)
NB_FOLLOW_START  = dtime(13, 55)
NB_FOLLOW_END    = dtime(14, 15)
CB_FOLLOW_START  = dtime(14, 55)
CB_FOLLOW_END    = dtime(14, 58)

# ==================== 自选盯盘操作指导（�?小时�?====================
WATCHLIST_GUIDANCE_TIMES = [
    (dtime(10, 0), dtime(10, 10), "10:00"),
    (dtime(11, 0), dtime(11, 10), "11:00"),
    (dtime(13, 30), dtime(13, 40), "13:30"),
    (dtime(14, 30), dtime(14, 40), "14:30"),
]

# ==================== 结算 & 复盘 ====================
SETTLE_START = dtime(15, 5)
SETTLE_END   = dtime(16, 0)
REVIEW_START = dtime(16, 0)
REVIEW_END   = dtime(16, 30)

# 推荐数量 & 持仓上限
RECOMMEND_LIMIT = 10
MAX_POSITIONS = 5

# ==================== 策略赛马配置 ====================
ACTIVE_STRATEGIES = {
    "dragon_head", "sentiment", "event_driven",
    "breakthrough", "volume_price", "overnight",
    "moving_average", "combined",
    "northbound", "trend_momentum",
}

AFTERNOON_TRADING_ENABLED = True
POLL_INTERVAL = 30


# ==================== 定时任务用户配置 ====================
# 指定哪些用户自动执行每日交易、跟投建议、每日复�?
# 调度器启动时会自动为这些用户创建所需的策略组�?
# 注意：evan 共享 admin 的组合数据，不需要独立创�?
SCHEDULER_USERS = ["admin"]


def _is_trading_day() -> bool:
    """判断当前是否为交易日（简化版：周一至周五）"""
    return _beijing_now().weekday() < 5


class PortfolioScheduler:
    """模拟交易定时任务调度�?�?推荐后各战法独立交易"""

    def __init__(self):
        self._running = False
        self._task = None
        # 用日期字符串记录已执行的任务，避免重复执�?
        # 推荐
        self._last_dh_rec_am = None
        self._last_dh_rec_pm = None
        self._last_se_rec_am = None
        self._last_se_rec_pm = None
        self._last_ed_rec_am = None
        self._last_ed_rec_pm = None
        # 独立交易
        self._last_dh_trade_am = None
        self._last_dh_trade_pm = None
        self._last_se_trade_am = None
        self._last_se_trade_pm = None
        self._last_ed_trade_am = None
        self._last_ed_trade_pm = None
        # 跟投建议
        self._last_dh_follow_am = None
        self._last_dh_follow_pm = None
        self._last_se_follow_am = None
        self._last_se_follow_pm = None
        self._last_ed_follow_am = None
        self._last_ed_follow_pm = None
        # 新战法推荐（凌晨预生�?+ 下午更新推荐�?
        self._last_bt_premarket = None
        self._last_vp_premarket = None
        self._last_ma_premarket = None
        self._last_bt_rec_pm = None
        self._last_vp_rec_pm = None
        self._last_ma_rec_pm = None
        # 隔夜施工法（盘中推荐�?
        self._last_on_rec_am = None
        self._last_on_rec_pm = None
        # 新战法独立交�?
        self._last_bt_trade_am = None
        self._last_bt_trade_pm = None
        self._last_vp_trade_am = None
        self._last_vp_trade_pm = None
        self._last_on_trade_am = None
        self._last_on_trade_pm = None
        self._last_ma_trade_am = None
        self._last_ma_trade_pm = None
        # 新战法跟投建�?
        self._last_bt_follow_am = None
        self._last_bt_follow_pm = None
        self._last_vp_follow_am = None
        self._last_vp_follow_pm = None
        self._last_on_follow_am = None
        self._last_on_follow_pm = None
        self._last_ma_follow_am = None
        self._last_ma_follow_pm = None
        # 综合战法
        self._last_cb_rec_am = None
        self._last_cb_rec_pm = None
        self._last_cb_trade_am = None
        self._last_cb_trade_pm = None
        self._last_cb_follow_am = None
        self._last_cb_follow_pm = None
        # 趋势动量
        self._last_tm_premarket = None
        self._last_tm_rec_pm = None
        self._last_tm_trade_am = None
        self._last_tm_trade_pm = None
        self._last_tm_follow_am = None
        self._last_tm_follow_pm = None
        # 北向资金
        self._last_nb_rec_am = None
        self._last_nb_rec_pm = None
        self._last_nb_trade_am = None
        self._last_nb_trade_pm = None
        self._last_nb_follow_am = None
        self._last_nb_follow_pm = None
        # 结算 & 复盘
        self._last_settle_date = None
        self._last_review_date = None

    async def start(self):
        """启动调度�?""
        if self._running:
            logger.warning("Portfolio scheduler already running")
            return

        self._running = True
        try:
            await self._ensure_portfolios()
        except Exception as e:
            logger.error("Scheduler _ensure_portfolios failed (non-fatal)", error=str(e),
                         traceback=traceback.format_exc())
        try:
            await self._recover_today_state()
        except Exception as e:
            logger.error("Scheduler _recover_today_state failed (non-fatal)", error=str(e),
                         traceback=traceback.format_exc())
        self._task = asyncio.create_task(self._safe_scheduler_loop())
        logger.info("Portfolio scheduler started (pre-market + trading mode)",
                     scheduler_users=SCHEDULER_USERS,
                     bt_premarket="01:00", bt_trade="09:35/13:05",
                     vp_premarket="02:30", vp_trade="10:00/13:30",
                     ma_premarket="04:00", ma_trade="10:35/13:35",
                     tm_premarket="05:30", tm_trade="09:30/14:05",
                     dh_rec="09:00/12:30", dh_trade="09:10/12:40",
                     ed_rec="09:05/12:35", ed_trade="09:15/12:45",
                     au_rec="09:25/14:30", au_trade="09:35/14:40",
                     nb_rec="10:15/14:00", nb_trade="10:30/14:15",
                     se_rec="11:00/14:00", se_trade="11:10/14:10",
                     settle=f"{SETTLE_START.strftime('%H:%M')}~{SETTLE_END.strftime('%H:%M')}",
                     review=f"{REVIEW_START.strftime('%H:%M')}~{REVIEW_END.strftime('%H:%M')}",
                     poll_interval_s=POLL_INTERVAL)

    async def _recover_today_state(self):
        """
        重启后恢复今天的执行状态�?
        检查数据库中今天是否已有交�?跟投记录，恢复内存标志位�?
        避免重启后跟投等后续任务因前置条件丢失而无法触发�?
        """
        try:
            from portfolio_repository import portfolio_repo
        except ImportError:
            from app.portfolio_repository import portfolio_repo

        today = _beijing_now().strftime("%Y-%m-%d")
        now_time = _beijing_now().time()
        logger.info("Recovering today's scheduler state...", date=today)

        try:
            portfolios = await portfolio_repo.list_portfolios(status="active")
            strategy_map = {}
            for p in portfolios:
                strategy_map[p["strategy_type"]] = p

            for strategy_type, portfolio in strategy_map.items():
                pid = portfolio["id"]

                # 检查今天是否有交易记录
                all_trades = await portfolio_repo.get_trades_paginated(pid, limit=50, offset=0)
                today_trades = [
                    t for t in all_trades
                    if str(t.get("trade_date", "")).startswith(today)
                ]

                has_trades = len(today_trades) > 0

                # 检查今天是否有跟投记录
                follow_list = await portfolio_repo.get_follow_recommendations(
                    portfolio_id=pid, limit=5
                )
                today_follows = [
                    f for f in follow_list
                    if str(f.get("trading_date", "")).startswith(today)
                ]
                follow_sessions = {f.get("session_type") for f in today_follows}

                if strategy_type == "dragon_head":
                    if has_trades:
                        # 根据当前时间判断是上午还是下午的交易
                        if now_time >= DH_TRADE_AM_START:
                            self._last_dh_rec_am = today
                            self._last_dh_trade_am = today
                        if now_time >= DH_TRADE_PM_START:
                            self._last_dh_rec_pm = today
                            self._last_dh_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_dh_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_dh_follow_pm = today

                elif strategy_type == "sentiment":
                    if has_trades:
                        if now_time >= SE_TRADE_AM_START:
                            self._last_se_rec_am = today
                            self._last_se_trade_am = today
                        if now_time >= SE_TRADE_PM_START:
                            self._last_se_rec_pm = today
                            self._last_se_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_se_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_se_follow_pm = today

                elif strategy_type == "event_driven":
                    if has_trades:
                        if now_time >= ED_TRADE_AM_START:
                            self._last_ed_rec_am = today
                            self._last_ed_trade_am = today
                        if now_time >= ED_TRADE_PM_START:
                            self._last_ed_rec_pm = today
                            self._last_ed_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_ed_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_ed_follow_pm = today

                elif strategy_type == "breakthrough":
                    # 凌晨预生成的推荐，盘中有交易记录说明预生成已完成
                    if has_trades:
                        self._last_bt_premarket = today
                        if now_time >= BT_TRADE_AM_START:
                            self._last_bt_trade_am = today
                        if now_time >= BT_TRADE_PM_START:
                            self._last_bt_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_bt_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_bt_follow_pm = today

                elif strategy_type == "volume_price":
                    if has_trades:
                        self._last_vp_premarket = today
                        if now_time >= VP_TRADE_AM_START:
                            self._last_vp_trade_am = today
                        if now_time >= VP_TRADE_PM_START:
                            self._last_vp_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_vp_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_vp_follow_pm = today

                elif strategy_type == "overnight":
                    if has_trades:
                        if now_time >= ON_RECOMMEND_AM_END:
                            self._last_on_rec_am = today
                            self._last_on_trade_am = today
                        if now_time >= ON_TRADE_END:
                            self._last_on_rec_pm = today
                            self._last_on_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_on_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_on_follow_pm = today

                elif strategy_type == "moving_average":
                    if has_trades:
                        self._last_ma_premarket = today
                        if now_time >= MA_TRADE_AM_START:
                            self._last_ma_trade_am = today
                        if now_time >= MA_TRADE_PM_START:
                            self._last_ma_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_ma_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_ma_follow_pm = today

                elif strategy_type == "combined":
                    if has_trades:
                        if now_time >= CB_TRADE_AM_START:
                            self._last_cb_rec_am = today
                            self._last_cb_trade_am = today
                        if now_time >= CB_TRADE_PM_START:
                            self._last_cb_rec_pm = today
                            self._last_cb_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_cb_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_cb_follow_pm = today

                elif strategy_type == "trend_momentum":
                    if has_trades:
                        self._last_tm_premarket = today
                        if now_time >= TM_TRADE_AM_START:
                            self._last_tm_trade_am = today
                        if now_time >= TM_TRADE_PM_START:
                            self._last_tm_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_tm_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_tm_follow_pm = today

                elif strategy_type == "northbound":
                    if has_trades:
                        if now_time >= NB_TRADE_AM_START:
                            self._last_nb_rec_am = today
                            self._last_nb_trade_am = today
                        if now_time >= NB_TRADE_PM_START:
                            self._last_nb_rec_pm = today
                            self._last_nb_trade_pm = today
                    if "morning" in follow_sessions:
                        self._last_nb_follow_am = today
                    if "afternoon" in follow_sessions:
                        self._last_nb_follow_pm = today

                if has_trades:
                    logger.info("Recovered trade state",
                                strategy=strategy_type,
                                trade_count=len(today_trades),
                                follow_sessions=list(follow_sessions))

            # 注意：不恢复结算和复盘状态，让它们在重启后有机会重新执行
            # 结算是幂等操作（更新收盘�?计算收益），重复执行无害
            # 这样确保重启后收益数据能正确计算

            logger.info("Scheduler state recovery completed", date=today)

        except Exception as e:
            logger.warning("State recovery failed (non-critical)",
                           error=str(e), traceback=traceback.format_exc())

    async def _resolve_user_ids(self) -> Dict[str, int]:
        """从数据库配置或硬编码列表查找定时任务用户 ID"""
        try:
            # 优先从数据库读取配置
            try:
                from system_endpoints import get_scheduler_users_from_db
            except ImportError:
                from app.system_endpoints import get_scheduler_users_from_db

            db_users = await get_scheduler_users_from_db()
            user_list = db_users if db_users else SCHEDULER_USERS

            try:
                from portfolio_repository import portfolio_repo
            except ImportError:
                from app.portfolio_repository import portfolio_repo

            pool = await portfolio_repo._get_pool()
            import aiomysql
            user_map = {}
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    placeholders = ",".join(["%s"] * len(user_list))
                    await cur.execute(
                        f"SELECT id, username FROM sys_user WHERE username IN ({placeholders})",
                        user_list
                    )
                    rows = await cur.fetchall()
                    for row in rows:
                        user_map[row["username"]] = row["id"]

            logger.info("Resolved scheduler users",
                         source="db" if db_users else "hardcoded",
                         users=list(user_map.keys()))
            return user_map
        except Exception as e:
            logger.warning("Failed to resolve scheduler user IDs", error=str(e))
            return {}

    async def _ensure_portfolios(self):
        """确保所有配置用户的三个策略组合都存在（不存在则自动创建�?""
        try:
            from portfolio_repository import portfolio_repo
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_repository import portfolio_repo
            from app.portfolio_manager import portfolio_manager

        # 解析用户 ID
        user_map = await self._resolve_user_ids()
        if not user_map:
            logger.warning("No scheduler users found in sys_user, "
                           "falling back to legacy mode (no user_id binding)",
                           configured_users=SCHEDULER_USERS)
            # 兼容旧逻辑：无 user_id 的组�?
            required = {
                "dragon_head": "龙头战法_组合",
                "sentiment": "情绪战法_组合",
                "event_driven": "事件驱动_组合",
                "breakthrough": "突破战法_组合",
                "volume_price": "量价关系_组合",
                "overnight": "隔夜施工_组合",
                "moving_average": "均线战法_组合",
            }
            existing = await portfolio_repo.list_portfolios(status="active")
            existing_types = {p["strategy_type"] for p in existing}
            for strategy_type, name in required.items():
                if strategy_type not in existing_types:
                    logger.info("Auto-creating missing portfolio",
                                 strategy=strategy_type, name=name)
                    await portfolio_manager.create_portfolio(
                        strategy_type=strategy_type, name=name,
                        initial_capital=1000000,
                    )
            return

        required_strategies = {
            "dragon_head": "龙头战法_组合",
            "sentiment": "情绪战法_组合",
            "event_driven": "事件驱动_组合",
            "breakthrough": "突破战法_组合",
            "volume_price": "量价关系_组合",
            "overnight": "隔夜施工_组合",
            "moving_average": "均线战法_组合",
            "combined": "综合战法_组合",
            "northbound": "北向资金_组合",
            "trend_momentum": "趋势动量_组合",
        }

        # 查出所有已有组�?
        existing = await portfolio_repo.list_portfolios(status="active")

        for username, uid in user_map.items():
            # 查找该用户已有的策略组合
            user_portfolios = [p for p in existing if p.get("user_id") == uid]
            user_existing_types = {p["strategy_type"] for p in user_portfolios}

            for strategy_type, base_name in required_strategies.items():
                if strategy_type not in user_existing_types:
                    name = f"{base_name}_{username}"
                    logger.info("Auto-creating portfolio for user",
                                 username=username, user_id=uid,
                                 strategy=strategy_type, name=name)
                    result = await portfolio_manager.create_portfolio(
                        strategy_type=strategy_type,
                        name=name,
                        initial_capital=1000000,
                        user_id=uid,
                    )
                    logger.info("Portfolio auto-created",
                                 username=username,
                                 strategy=strategy_type,
                                 portfolio_id=result.get("portfolio_id"))

        # 同时为没�?user_id 的旧组合绑定到第一个配置用�?
        unbound = [p for p in existing if p.get("user_id") is None]
        if unbound and user_map:
            first_user = list(user_map.keys())[0]
            first_uid = user_map[first_user]
            import aiomysql
            pool = await portfolio_repo._get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for p in unbound:
                        await cur.execute(
                            "UPDATE portfolio_config SET user_id = %s WHERE id = %s AND user_id IS NULL",
                            (first_uid, p["id"])
                        )
                        logger.info("Bound orphan portfolio to user",
                                     portfolio_id=p["id"],
                                     username=first_user, user_id=first_uid)

    async def stop(self):
        """停止调度�?""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Portfolio scheduler stopped")

    async def _safe_scheduler_loop(self):
        """包装层：确保调度循环永不静默退�?""
        try:
            await self._scheduler_loop()
        except asyncio.CancelledError:
            logger.info("Scheduler task cancelled")
        except Exception as e:
            logger.error("💀 SCHEDULER LOOP CRASHED �?this should never happen!",
                         error=str(e), traceback=traceback.format_exc())
            self._running = False

    async def _scheduler_loop(self):
        """调度主循�?�?每战法单次交易制"""
        _heartbeat_counter = 0
        logger.info("🚀 Scheduler loop entered and running")
        while self._running:
            try:
                now = _beijing_now()
                today = now.strftime("%Y-%m-%d")
                ct = now.time()

                # 心跳日志：每 ~60s 输出一次，证明调度器活着
                _heartbeat_counter += 1
                if _heartbeat_counter % 2 == 0:  # 30s * 2 = 60s
                    done_count = sum(1 for attr in dir(self) if attr.startswith('_last_') and getattr(self, attr, None) == today)
                    logger.info("💓 Scheduler heartbeat",
                                time=ct.strftime("%H:%M:%S"),
                                is_trading_day=_is_trading_day(),
                                tasks_done_today=done_count)

                # ===================== 凌晨预生成（历史K线战法，不依赖实时数据） =====================
                # 01:00 突破战法
                if (BT_PREMARKET_START <= ct <= BT_PREMARKET_END
                        and self._last_bt_premarket != today
                        and _is_trading_day()):
                    logger.info("=== 突破战法凌晨预生�?===", date=today)
                    await self._run_recommendation("breakthrough", "premarket")
                    self._last_bt_premarket = today

                # 02:30 量价关系
                if (VP_PREMARKET_START <= ct <= VP_PREMARKET_END
                        and self._last_vp_premarket != today
                        and _is_trading_day()):
                    logger.info("=== 量价关系凌晨预生�?===", date=today)
                    await self._run_recommendation("volume_price", "premarket")
                    self._last_vp_premarket = today

                # 04:00 均线战法
                if (MA_PREMARKET_START <= ct <= MA_PREMARKET_END
                        and self._last_ma_premarket != today
                        and _is_trading_day()):
                    logger.info("=== 均线战法凌晨预生�?===", date=today)
                    await self._run_recommendation("moving_average", "premarket")
                    self._last_ma_premarket = today

                # 05:30 趋势动量
                if (TM_PREMARKET_START <= ct <= TM_PREMARKET_END
                        and self._last_tm_premarket != today
                        and _is_trading_day()):
                    logger.info("=== 趋势动量凌晨预生�?===", date=today)
                    await self._run_recommendation("trend_momentum", "premarket")
                    self._last_tm_premarket = today

                if _is_trading_day():

                    # ⚠️ 已移�?14:30-15:00 手动刷新互斥保护，确保尾盘自动战法（隔夜、综合）能如期触�?

                    # ===================== 隔夜施工法推荐（盘前扫描已移除，仅保留下午推荐） =====================
                    if "overnight" in ACTIVE_STRATEGIES:
                        if (ON_RECOMMEND_PM_START <= ct <= ON_RECOMMEND_PM_END
                                and self._last_on_rec_pm != today):
                            logger.info("=== 隔夜施工法下午推�?===", date=today)
                            await self._run_recommendation("overnight", "afternoon")
                            self._last_on_rec_pm = today

                    # ===================== 09:00 综合战法上午推荐 =====================
                    if "combined" in ACTIVE_STRATEGIES:
                        if (CB_RECOMMEND_AM_START <= ct <= CB_RECOMMEND_AM_END
                                and self._last_cb_rec_am != today):
                            logger.info("=== 综合战法上午推荐 ===", date=today)
                            await self._run_recommendation("combined", "morning")
                            self._last_cb_rec_am = today

                    # ===================== 09:40 事件驱动推荐 =====================
                    if "event_driven" in ACTIVE_STRATEGIES:
                        if (ED_RECOMMEND_AM_START <= ct <= ED_RECOMMEND_AM_END
                                and self._last_ed_rec_am != today):
                            logger.info("=== 事件驱动上午推荐 ===", date=today)
                            await self._run_recommendation("event_driven", "morning")
                            self._last_ed_rec_am = today

                    # ===================== 09:45 趋势动量 �?单次交易 =====================
                    if "trend_momentum" in ACTIVE_STRATEGIES:
                        if (TM_TRADE_START <= ct <= TM_TRADE_END
                                and self._last_tm_trade_am != today
                                and self._last_tm_premarket == today):
                            logger.info("=== 趋势动量交易（凌晨推�?开盘确认）===", date=today)
                            await self._run_strategy_trade("trend_momentum", "morning")
                            self._last_tm_trade_am = today

                        if (TM_FOLLOW_START <= ct <= TM_FOLLOW_END
                                and self._last_tm_follow_am != today
                                and self._last_tm_trade_am == today):
                            logger.info("=== 趋势动量跟投建议 ===", date=today)
                            await self._run_follow_recommendation("trend_momentum", "morning")
                            self._last_tm_follow_am = today

                    # ===================== 09:55 事件驱动 �?单次交易 =====================
                    if "event_driven" in ACTIVE_STRATEGIES:
                        if (ED_TRADE_START <= ct <= ED_TRADE_END
                                and self._last_ed_trade_am != today
                                and self._last_ed_rec_am == today):
                            logger.info("=== 事件驱动交易（新闻消化后�?==", date=today)
                            await self._run_strategy_trade("event_driven", "morning")
                            self._last_ed_trade_am = today

                        if (ED_FOLLOW_START <= ct <= ED_FOLLOW_END
                                and self._last_ed_follow_am != today
                                and self._last_ed_trade_am == today):
                            logger.info("=== 事件驱动跟投建议 ===", date=today)
                            await self._run_follow_recommendation("event_driven", "morning")
                            self._last_ed_follow_am = today

                    # ===================== 10:00 龙头战法推荐 =====================
                    if "dragon_head" in ACTIVE_STRATEGIES:
                        if (DH_RECOMMEND_AM_START <= ct <= DH_RECOMMEND_AM_END
                                and self._last_dh_rec_am != today):
                            logger.info("=== 龙头战法上午推荐 ===", date=today)
                            await self._run_recommendation("dragon_head", "morning")
                            self._last_dh_rec_am = today

                    # ===================== 10:15 龙头战法 �?单次交易 =====================
                    if "dragon_head" in ACTIVE_STRATEGIES:
                        if (DH_TRADE_START <= ct <= DH_TRADE_END
                                and self._last_dh_trade_am != today
                                and self._last_dh_rec_am == today):
                            logger.info("=== 龙头战法交易（涨停板确认后）===", date=today)
                            await self._run_strategy_trade("dragon_head", "morning")
                            self._last_dh_trade_am = today

                        if (DH_FOLLOW_START <= ct <= DH_FOLLOW_END
                                and self._last_dh_follow_am != today
                                and self._last_dh_trade_am == today):
                            logger.info("=== 龙头战法跟投建议 ===", date=today)
                            await self._run_follow_recommendation("dragon_head", "morning")
                            self._last_dh_follow_am = today

                    # ===================== 10:15 北向资金推荐（上午推荐已移除�?=====================

                    # ===================== 10:20 突破战法 �?单次交易 =====================
                    if "breakthrough" in ACTIVE_STRATEGIES:
                        if (BT_TRADE_START <= ct <= BT_TRADE_END
                                and self._last_bt_trade_am != today
                                and self._last_bt_premarket == today):
                            logger.info("=== 突破战法交易（凌晨推�?盘中量能确认�?==", date=today)
                            await self._run_strategy_trade("breakthrough", "morning")
                            self._last_bt_trade_am = today

                        if (BT_FOLLOW_START <= ct <= BT_FOLLOW_END
                                and self._last_bt_follow_am != today
                                and self._last_bt_trade_am == today):
                            logger.info("=== 突破战法跟投建议 ===", date=today)
                            await self._run_follow_recommendation("breakthrough", "morning")
                            self._last_bt_follow_am = today

                    # ===================== 10:30 情绪战法推荐 =====================
                    if "sentiment" in ACTIVE_STRATEGIES:
                        if (SE_RECOMMEND_AM_START <= ct <= SE_RECOMMEND_AM_END
                                and self._last_se_rec_am != today):
                            logger.info("=== 情绪战法上午推荐 ===", date=today)
                            await self._run_recommendation("sentiment", "morning")
                            self._last_se_rec_am = today

                    # ===================== 10:45 情绪战法 �?单次交易 =====================
                    if "sentiment" in ACTIVE_STRATEGIES:
                        if (SE_TRADE_START <= ct <= SE_TRADE_END
                                and self._last_se_trade_am != today
                                and self._last_se_rec_am == today):
                            logger.info("=== 情绪战法交易（情绪指标积累后�?==", date=today)
                            await self._run_strategy_trade("sentiment", "morning")
                            self._last_se_trade_am = today

                        if (SE_FOLLOW_START <= ct <= SE_FOLLOW_END
                                and self._last_se_follow_am != today
                                and self._last_se_trade_am == today):
                            logger.info("=== 情绪战法跟投建议 ===", date=today)
                            await self._run_follow_recommendation("sentiment", "morning")
                            self._last_se_follow_am = today

                    # ===================== 10:50 量价关系 �?单次交易 =====================
                    if "volume_price" in ACTIVE_STRATEGIES:
                        if (VP_TRADE_START <= ct <= VP_TRADE_END
                                and self._last_vp_trade_am != today
                                and self._last_vp_premarket == today):
                            logger.info("=== 量价关系交易（凌晨推�?盘中确认�?==", date=today)
                            await self._run_strategy_trade("volume_price", "morning")
                            self._last_vp_trade_am = today

                        if (VP_FOLLOW_START <= ct <= VP_FOLLOW_END
                                and self._last_vp_follow_am != today
                                and self._last_vp_trade_am == today):
                            logger.info("=== 量价关系跟投建议 ===", date=today)
                            await self._run_follow_recommendation("volume_price", "morning")
                            self._last_vp_follow_am = today

                    # ===================== 11:15 均线战法 �?单次交易 =====================
                    if "moving_average" in ACTIVE_STRATEGIES:
                        if (MA_TRADE_START <= ct <= MA_TRADE_END
                                and self._last_ma_trade_am != today
                                and self._last_ma_premarket == today):
                            logger.info("=== 均线战法交易（凌晨推�?均线确认�?==", date=today)
                            await self._run_strategy_trade("moving_average", "morning")
                            self._last_ma_trade_am = today

                        if (MA_FOLLOW_START <= ct <= MA_FOLLOW_END
                                and self._last_ma_follow_am != today
                                and self._last_ma_trade_am == today):
                            logger.info("=== 均线战法跟投建议 ===", date=today)
                            await self._run_follow_recommendation("moving_average", "morning")
                            self._last_ma_follow_am = today

                    # ===================== 14:45 隔夜施工�?�?单次交易 =====================
                    if "overnight" in ACTIVE_STRATEGIES:
                        if (ON_TRADE_START <= ct <= ON_TRADE_END
                                and self._last_on_trade_pm != today
                                and self._last_on_rec_pm == today):
                            logger.info("=== 隔夜施工法交易（尾盘选股�?==", date=today)
                            await self._run_strategy_trade("overnight", "afternoon")
                            self._last_on_trade_pm = today

                        if (ON_FOLLOW_START <= ct <= ON_FOLLOW_END
                                and self._last_on_follow_pm != today
                                and self._last_on_trade_pm == today):
                            logger.info("=== 隔夜施工法跟投建�?===", date=today)
                            await self._run_follow_recommendation("overnight", "afternoon")
                            self._last_on_follow_pm = today

                    # ===================== 13:00~13:45 所有子战法下午推荐更新 =====================
                    # 为综合战�?13:50 聚合提供盘中最新数�?

                    # 13:00 事件驱动下午推荐（已移除�?

                    # 13:00 趋势动量下午推荐
                    if "trend_momentum" in ACTIVE_STRATEGIES:
                        if (TM_RECOMMEND_PM_START <= ct <= TM_RECOMMEND_PM_END
                                and self._last_tm_rec_pm != today):
                            logger.info("=== 趋势动量下午推荐 ===", date=today)
                            await self._run_recommendation("trend_momentum", "afternoon")
                            self._last_tm_rec_pm = today

                    # 13:05 龙头战法下午推荐（已移除�?

                    # 13:05 突破战法下午推荐
                    if "breakthrough" in ACTIVE_STRATEGIES:
                        if (BT_RECOMMEND_PM_START <= ct <= BT_RECOMMEND_PM_END
                                and self._last_bt_rec_pm != today):
                            logger.info("=== 突破战法下午推荐 ===", date=today)
                            await self._run_recommendation("breakthrough", "afternoon")
                            self._last_bt_rec_pm = today

                    # 13:10 情绪战法下午推荐（已移除�?

                    # 13:10 量价关系下午推荐
                    if "volume_price" in ACTIVE_STRATEGIES:
                        if (VP_RECOMMEND_PM_START <= ct <= VP_RECOMMEND_PM_END
                                and self._last_vp_rec_pm != today):
                            logger.info("=== 量价关系下午推荐 ===", date=today)
                            await self._run_recommendation("volume_price", "afternoon")
                            self._last_vp_rec_pm = today

                    # 13:15 隔夜施工法下午推�?
                    if "overnight" in ACTIVE_STRATEGIES:
                        if (ON_RECOMMEND_PM_START <= ct <= ON_RECOMMEND_PM_END
                                and self._last_on_rec_pm != today):
                            logger.info("=== 隔夜施工法下午推�?===", date=today)
                            await self._run_recommendation("overnight", "afternoon")
                            self._last_on_rec_pm = today

                    # 13:15 均线战法下午推荐
                    if "moving_average" in ACTIVE_STRATEGIES:
                        if (MA_RECOMMEND_PM_START <= ct <= MA_RECOMMEND_PM_END
                                and self._last_ma_rec_pm != today):
                            logger.info("=== 均线战法下午推荐 ===", date=today)
                            await self._run_recommendation("moving_average", "afternoon")
                            self._last_ma_rec_pm = today

                    # 13:30 北向资金下午推荐
                    if "northbound" in ACTIVE_STRATEGIES:
                        if (NB_RECOMMEND_PM_START <= ct <= NB_RECOMMEND_PM_END
                                and self._last_nb_rec_pm != today):
                            logger.info("=== 北向资金下午推荐 ===", date=today)
                            await self._run_recommendation("northbound", "afternoon")
                            self._last_nb_rec_pm = today

                    # ===================== 13:35 北向资金 �?单次交易 =====================
                    if "northbound" in ACTIVE_STRATEGIES:
                        if (NB_TRADE_START <= ct <= NB_TRADE_END
                                and self._last_nb_trade_am != today
                                and (self._last_nb_rec_pm == today or self._last_nb_rec_am == today)):
                            logger.info("=== 北向资金交易（下午外资数据充分）===", date=today)
                            await self._run_strategy_trade("northbound", "afternoon")
                            self._last_nb_trade_am = today

                        if (NB_FOLLOW_START <= ct <= NB_FOLLOW_END
                                and self._last_nb_follow_am != today
                                and self._last_nb_trade_am == today):
                            logger.info("=== 北向资金跟投建议 ===", date=today)
                            await self._run_follow_recommendation("northbound", "afternoon")
                            self._last_nb_follow_am = today

                    # ===================== 13:50 综合战法下午推荐 =====================
                    if "combined" in ACTIVE_STRATEGIES:
                        if (CB_RECOMMEND_PM_START <= ct <= CB_RECOMMEND_PM_END
                                and self._last_cb_rec_pm != today):
                            logger.info("=== 综合战法下午推荐（聚合所有战法）===", date=today)
                            await self._run_recommendation("combined", "afternoon")
                            self._last_cb_rec_pm = today

                    # ===================== 14:10 综合战法 �?单次交易（最后窗口）=====================
                    if "combined" in ACTIVE_STRATEGIES:
                        if (CB_TRADE_START <= ct <= CB_TRADE_END
                                and self._last_cb_trade_am != today
                                and (self._last_cb_rec_pm == today or self._last_cb_rec_am == today)):
                            logger.info("=== 综合战法交易（所有战法聚合，14:30前结束）===", date=today)
                            await self._run_strategy_trade("combined", "afternoon")
                            self._last_cb_trade_am = today

                        if (CB_FOLLOW_START <= ct <= CB_FOLLOW_END
                                and self._last_cb_follow_am != today
                                and self._last_cb_trade_am == today):
                            logger.info("=== 综合战法跟投建议 ===", date=today)
                            await self._run_follow_recommendation("combined", "afternoon")
                            self._last_cb_follow_am = today

                    # ===================== 自选盯盘操作指导（每小时） =====================
                    for wg_start, wg_end, wg_label in WATCHLIST_GUIDANCE_TIMES:
                        flag_key = f"guidance_{wg_label.replace(':', '')}"
                        if (wg_start <= ct <= wg_end
                                and getattr(self, f'_last_{flag_key}', '') != today):
                            logger.info(f"=== 自选盯盘操作指�?{wg_label} ===", date=today)
                            try:
                                try:
                                    from combined_watchlist_guidance import watchlist_guidance
                                except ImportError:
                                    from app.combined_watchlist_guidance import watchlist_guidance
                                count = await watchlist_guidance.generate_guidance_for_all(
                                    trading_session=wg_label
                                )
                                logger.info(f"Watchlist guidance {wg_label} done", count=count)
                            except Exception as e:
                                logger.error(f"Watchlist guidance {wg_label} failed", error=str(e))
                            setattr(self, f'_last_{flag_key}', today)

                    # ===================== 结算 & 复盘 =====================
                    # 15:05 ~ 16:00 结算
                    if (SETTLE_START <= ct <= SETTLE_END
                            and self._last_settle_date != today):
                        logger.info("=== 收盘结算 ===", date=today)
                        await self._run_all_settlement()
                        self._last_settle_date = today

                    # 16:00 ~ 16:30 每日复盘
                    if (REVIEW_START <= ct <= REVIEW_END
                            and self._last_review_date != today):
                        logger.info("=== 每日复盘 ===", date=today)
                        await self._run_daily_review()
                        self._last_review_date = today

                await asyncio.sleep(POLL_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Scheduler loop error", error=str(e),
                             traceback=traceback.format_exc())
                await asyncio.sleep(POLL_INTERVAL)

    # ==================== 推荐生成 ====================

    async def _run_recommendation(self, strategy_type: str, session_type: str):
        """为指定策略生成推荐并写入缓存（带随机延迟防反爬）"""
        try:
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_manager import portfolio_manager

        try:
            # 反爬随机延迟（策略间错开�?
            await async_anti_scrape_delay(
                f"recommendation_{strategy_type}", *DELAY_SCHEDULER
            )
            logger.info("Anti-scraping delay before recommendation",
                        strategy=strategy_type)

            stocks = await portfolio_manager.generate_recommendations(
                strategy_type=strategy_type,
                session_type=session_type,
                limit=RECOMMEND_LIMIT,
            )
            logger.info("Recommendation done",
                        strategy=strategy_type, session=session_type,
                        count=len(stocks))
        except Exception as e:
            logger.error("Recommendation failed",
                         strategy=strategy_type, session=session_type,
                         error=str(e), traceback=traceback.format_exc())

    # ==================== 独立交易（核心变更） ====================

    async def _run_strategy_trade(self, strategy_type: str, session_type: str):
        """
        单个战法的独立交易决策：

        在推荐生�?0分钟后触发，GPT根据以下信息综合判断是否交易�?
        1. 当前持仓情况（已有持仓、浮盈浮亏）
        2. 最新推荐股�?
        3. 市场整体环境
        4. 前一日复盘经�?
        5. 当日是否已经交易过（上午交易�?�?下午再判断）

        GPT可以决定"不交�?（全�?hold），这是合理的�?
        """
        try:
            from portfolio_repository import portfolio_repo
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_repository import portfolio_repo
            from app.portfolio_manager import portfolio_manager

        try:
            # 找到该策略对应的所有组合（admin + evan 各有一个）
            portfolios = await portfolio_repo.list_portfolios(status="active")
            targets = [p for p in portfolios if p.get("strategy_type") == strategy_type]

            if not targets:
                logger.warning("No portfolio found for strategy", strategy=strategy_type)
                return

            import random
            for i, target_portfolio in enumerate(targets):
                pid = target_portfolio["id"]

                if target_portfolio.get("auto_trade") != 1:
                    logger.info("Auto trade disabled, skip",
                                strategy=strategy_type, portfolio_id=pid)
                    continue

                if i > 0:
                    await async_anti_scrape_delay(
                        f"trade_{strategy_type}_portfolio_{pid}", *DELAY_PORTFOLIO
                    )

                # 执行调仓（portfolio_manager.run_daily_rebalance 会让 GPT 判断交易或不交易�?
                result = await portfolio_manager.run_daily_rebalance(pid)
                trade_count = result.get("trade_count", 0)
                status = result.get("status", "unknown")

                logger.info("Strategy independent trade completed",
                            strategy=strategy_type, session=session_type,
                            portfolio_id=pid, trade_count=trade_count,
                            status=status,
                            decision_summary=result.get("decision_summary", "")[:200])

        except Exception as e:
            logger.error("Strategy independent trade failed",
                         strategy=strategy_type, session=session_type,
                         error=str(e), traceback=traceback.format_exc())

    # ==================== 跟投建议 ====================

    async def _run_follow_recommendation(self, strategy_type: str, session_type: str):
        """在交易完成后5分钟生成跟投建议（组合间错开防反爬）"""
        try:
            from portfolio_repository import portfolio_repo
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_repository import portfolio_repo
            from app.portfolio_manager import portfolio_manager

        try:
            import random
            portfolios = await portfolio_repo.list_portfolios(status="active")
            targets = [p for p in portfolios if p.get("strategy_type") == strategy_type]

            if not targets:
                logger.warning("No portfolio for follow recommendation", strategy=strategy_type)
                return

            for i, target in enumerate(targets):
                pid = target["id"]

                if i > 0:
                    await async_anti_scrape_delay(
                        f"follow_{strategy_type}_portfolio_{pid}", *DELAY_LIGHT
                    )

                result = await portfolio_manager.generate_follow_recommendations(
                    pid, session_type=session_type
                )
                logger.info("Follow recommendation completed",
                            strategy=strategy_type, session=session_type,
                            portfolio_id=pid,
                            stock_count=result.get("stock_count", 0))
        except Exception as e:
            logger.error("Follow recommendation failed",
                         strategy=strategy_type, session=session_type,
                         error=str(e), traceback=traceback.format_exc())

    # ==================== 收盘结算 ====================

    async def _run_all_settlement(self):
        """对所有活跃组合执行完整结算（共享一份行情数据，错开执行防反爬）"""
        try:
            from portfolio_repository import portfolio_repo
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_repository import portfolio_repo
            from app.portfolio_manager import portfolio_manager

        try:
            import random

            # 1. 一次性获取全市场行情（所有组合共用，避免重复调API�?
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes

            import asyncio
            logger.info("Fetching realtime quotes for settlement (once for all)")
            realtime_df = await asyncio.to_thread(get_realtime_quotes)

            portfolios = await portfolio_repo.list_portfolios(status="active")

            for i, p in enumerate(portfolios):
                pid = p["id"]
                try:
                    if i > 0:
                        await async_anti_scrape_delay(
                            f"settlement_portfolio_{pid}", *DELAY_LIGHT
                        )

                    result = await portfolio_manager.settle_daily(
                        pid, realtime_df=realtime_df,
                    )
                    logger.info("Settlement done", portfolio_id=pid,
                                result_status=result.get("status"),
                                total_asset=result.get("total_asset"),
                                daily_profit=result.get("daily_profit"))
                except Exception as e:
                    logger.error("Settlement failed", portfolio_id=pid, error=str(e))
        except Exception as e:
            logger.error("Run all settlement failed", error=str(e))

    # ==================== 每日复盘 ====================

    async def _run_daily_review(self):
        """对所有活跃组合进�?GPT-5.2 复盘（组合间错开�?API 限流�?""
        try:
            from portfolio_repository import portfolio_repo
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_repository import portfolio_repo
            from app.portfolio_manager import portfolio_manager

        try:
            import random
            portfolios = await portfolio_repo.list_portfolios(status="active")
            for i, p in enumerate(portfolios):
                pid = p["id"]
                strategy = p.get("strategy_type", "unknown")
                try:
                    if i > 0:
                        await async_anti_scrape_delay(
                            f"review_portfolio_{pid}", *DELAY_REVIEW
                        )

                    result = await portfolio_manager.generate_daily_review(pid)
                    logger.info("Daily review done", portfolio_id=pid,
                                strategy=strategy,
                                status=result.get("status"))
                except Exception as e:
                    logger.error("Daily review failed", portfolio_id=pid,
                                 strategy=strategy, error=str(e))
        except Exception as e:
            logger.error("Run daily review failed", error=str(e))

    # ==================== 手动触发 ====================

    async def trigger_rebalance(self, portfolio_id: int) -> Dict[str, Any]:
        """手动触发某个组合的调�?""
        try:
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_manager import portfolio_manager
        return await portfolio_manager.run_daily_rebalance(portfolio_id)

    async def trigger_recommendations(self, session_type: str = "manual") -> Dict[str, Any]:
        """手动触发所有策略的推荐生成（策略间错开防反爬）"""
        try:
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_manager import portfolio_manager

        import random
        results = {}
        strategies = ["dragon_head", "sentiment", "event_driven",
                       "breakthrough", "volume_price", "overnight", "moving_average",
                       "northbound", "trend_momentum"]
        for i, strategy in enumerate(strategies):
            try:
                if i > 0:
                    await async_anti_scrape_delay(
                        f"manual_rec_{strategy}", *DELAY_PORTFOLIO
                    )

                stocks = await portfolio_manager.generate_recommendations(
                    strategy_type=strategy,
                    session_type=session_type,
                    limit=RECOMMEND_LIMIT,
                )
                results[strategy] = {"status": "success", "count": len(stocks)}
            except Exception as e:
                results[strategy] = {"status": "error", "error": str(e)}
        return {"status": "success", "results": results}

    async def trigger_strategy_trade(self, strategy_type: str) -> Dict[str, Any]:
        """手动触发某个战法的独立交�?""
        await self._run_strategy_trade(strategy_type, "manual")
        return {"status": "triggered", "strategy": strategy_type}

    async def trigger_review(self, portfolio_id: int) -> Dict[str, Any]:
        """手动触发某个组合的复�?""
        try:
            from portfolio_manager import portfolio_manager
        except ImportError:
            from app.portfolio_manager import portfolio_manager
        return await portfolio_manager.generate_daily_review(portfolio_id)


# 全局单例
portfolio_scheduler = PortfolioScheduler()
