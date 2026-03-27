"""
情绪战法（Sentiment Strategy）核心逻辑模块

通过三大情绪指标量化全市场情绪：
1. 昨日涨停溢价率 - 衡量短线资金赚钱效应
2. 涨跌停家数比   - 衡量市场整体广度
3. 连板晋级率     - 衡量资金接力意愿

综合指标经 Z-Score 标准化后合成综合情绪曲线，生成冰点/高潮信号。

数据源：AkShare (免费开源金融数据接口)
"""

import os
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import structlog

# 统一反爬模块
try:
    from anti_scrape import anti_scrape_delay, DELAY_HEAVY
except ImportError:
    from app.anti_scrape import anti_scrape_delay, DELAY_HEAVY

logger = structlog.get_logger()

# 滚动窗口常量
Z_WINDOW = 20       # Z-Score 滚动窗口（约一个月交易日）
RANK_WINDOW = 250   # 历史分位数滚动窗口（约一年交易日）


class SentimentStrategy:
    """情绪战法核心策略类"""

    def __init__(self):
        self._ak = None
        self._cache: Dict[str, Any] = {}
        self._cache_time: Optional[datetime] = None
        # 当日涨停/跌停数据缓存（同一天不重复请求，防止被反爬封IP）
        self._daily_limit_up_cache: Optional[pd.DataFrame] = None
        self._daily_limit_down_cache: Optional[pd.DataFrame] = None
        self._daily_cache_date: Optional[str] = None
        # 当日实时行情缓存（_fetch_today_data 和 _fetch_yesterday_data 共享，避免重复拉取5000股）
        self._realtime_df_cache: Optional[pd.DataFrame] = None
        self._realtime_df_date: Optional[str] = None

    @property
    def ak(self):
        """延迟导入 akshare"""
        if self._ak is None:
            from app.akshare_client import ak_client as ak
            self._ak = ak
        return self._ak


    def _is_daily_cache_valid(self) -> bool:
        """判断当日涨停/跌停缓存是否有效"""
        today_str = datetime.now().strftime("%Y-%m-%d")
        return self._daily_cache_date == today_str

    def _is_cache_valid(self) -> bool:
        """结果缓存 10 分钟有效"""
        if self._cache_time is None:
            return False
        return (datetime.now() - self._cache_time).total_seconds() < 600

    def _get_realtime_df(self) -> Optional[pd.DataFrame]:
        """
        获取全市场实时行情 DataFrame（当日缓存，避免重复拉取 5000 股）

        _fetch_today_data 和 _fetch_yesterday_data 都需要用到实时行情，
        通过这个方法共享同一份数据，只拉取一次。
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        if self._realtime_df_date == today_str and self._realtime_df_cache is not None:
            return self._realtime_df_cache

        try:
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes

            df = get_realtime_quotes()
            if df is not None and not df.empty:
                self._realtime_df_cache = df
                self._realtime_df_date = today_str
                logger.info("Realtime quotes fetched and cached", rows=len(df))
                return df
        except Exception as e:
            logger.warning("Failed to fetch realtime quotes", error=str(e))

        return None

    # ========================= 公开方法 =========================

    async def get_sentiment(self, days: int = 30) -> Dict[str, Any]:
        """
        获取情绪战法数据

        Args:
            days: 返回最近多少个交易日的历史数据，默认 30

        Returns:
            {
                "status": "success",
                "data": {
                    "today": { ... },          # 今日情绪快照
                    "history": [ ... ],        # 历史 N 日情绪序列
                    "signal": "冰点转折" | "高潮退潮" | "正常",
                    "generated_at": "...",
                    "trading_date": "..."
                }
            }
        """
        import asyncio

        cache_key = f"sentiment_{days}"
        if self._is_cache_valid() and cache_key in self._cache:
            logger.info("Returning cached sentiment data")
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            # Step 1: 获取今日涨停/跌停数据
            logger.info("Fetching limit up/down data for sentiment")
            today_data = await loop.run_in_executor(None, self._fetch_today_data)

            # Step 2: 获取昨日涨停股信息（用于计算溢价率和晋级率）
            yesterday_data = await loop.run_in_executor(None, self._fetch_yesterday_data)

            # Step 3: 计算三大情绪指标
            indicators = self._calculate_indicators(today_data, yesterday_data)

            # Step 4: 尝试从数据库获取历史数据计算 Z-Score
            try:
                try:
                    from sentiment_repository import sentiment_repo
                except ImportError:
                    from app.sentiment_repository import sentiment_repo

                history_raw = await sentiment_repo.get_history(days=max(days, RANK_WINDOW + 10))
            except Exception as e:
                logger.warning("Failed to get history from DB, using indicators only", error=str(e))
                history_raw = []

            # Step 5: 计算 Z-Score 和综合情绪
            today_snapshot = self._compute_composite(indicators, history_raw)

            # ==================== Step 5.5: GPT-5.2 情绪深度分析 ====================
            llm_analysis = None
            try:
                try:
                    from sentiment_llm import sentiment_llm
                except ImportError:
                    from app.sentiment_llm import sentiment_llm

                # 取最近 5 日历史给 LLM 做趋势判断
                recent_for_llm = history_raw[-5:] if history_raw else []

                logger.info("Starting GPT-5.2 sentiment analysis")
                llm_analysis = await sentiment_llm.enhance_sentiment(
                    today_snapshot=today_snapshot,
                    recent_history=recent_for_llm,
                )

                if llm_analysis:
                    # 将 LLM 分析结果附加到 snapshot
                    today_snapshot["llm_analysis"] = llm_analysis
                    today_snapshot["llm_enhanced"] = True
                    logger.info("GPT-5.2 sentiment enhancement applied",
                                phase=llm_analysis.get("emotion_phase", ""))
                else:
                    today_snapshot["llm_enhanced"] = False
                    logger.warning("GPT-5.2 returned empty, using rule-based only")

            except Exception as llm_e:
                logger.error("GPT-5.2 sentiment enhancement failed",
                             error=str(llm_e))
                today_snapshot["llm_enhanced"] = False

            # ==================== Step 5.6: 情绪选股 ====================
            stock_picks_result = {"pick_strategy": "none", "stocks": [], "pick_count": 0}
            try:
                try:
                    from sentiment_stock_picker import sentiment_stock_picker
                except ImportError:
                    from app.sentiment_stock_picker import sentiment_stock_picker

                # 确定情绪阶段（优先用 LLM 判断，其次用规则）
                emotion_phase = "修复"
                if llm_analysis and llm_analysis.get("emotion_phase"):
                    emotion_phase = llm_analysis["emotion_phase"]
                elif today_snapshot.get("is_ice_point"):
                    emotion_phase = "冰点"
                elif today_snapshot.get("is_climax_retreat"):
                    emotion_phase = "退潮"
                elif today_snapshot.get("sentiment_percentile", 0.5) >= 0.8:
                    emotion_phase = "高潮"
                elif today_snapshot.get("sentiment_percentile", 0.5) >= 0.5:
                    emotion_phase = "升温"

                # 获取涨停股 DataFrame
                limit_up_df = today_data.get("limit_up_stocks")

                logger.info("Starting sentiment stock picking",
                             phase=emotion_phase)

                stock_picks_result = await sentiment_stock_picker.pick_stocks(
                    emotion_phase=emotion_phase,
                    today_snapshot=today_snapshot,
                    limit_up_df=limit_up_df,
                )

                # GPT-5.2 增强推荐
                if stock_picks_result.get("stocks"):
                    try:
                        try:
                            from sentiment_llm import sentiment_llm as slm
                        except ImportError:
                            from app.sentiment_llm import sentiment_llm as slm

                        enhanced_stocks = await slm.enhance_stock_picks(
                            stocks=stock_picks_result["stocks"],
                            emotion_phase=emotion_phase,
                            today_snapshot=today_snapshot,
                        )
                        stock_picks_result["stocks"] = enhanced_stocks
                        stock_picks_result["llm_enhanced"] = True
                        logger.info("Stock picks LLM enhanced",
                                     count=len(enhanced_stocks))
                    except Exception as llm_err:
                        logger.warning("Stock pick LLM enhancement failed",
                                       error=str(llm_err))
                        stock_picks_result["llm_enhanced"] = False

                # 持久化推荐
                try:
                    try:
                        from sentiment_repository import sentiment_repo as sr
                    except ImportError:
                        from app.sentiment_repository import sentiment_repo as sr

                    batch_data = {
                        "trading_date": today_snapshot.get("trading_date",
                                                           datetime.now().strftime("%Y-%m-%d")),
                        "batch_no": datetime.now().strftime("%Y%m%d_%H%M%S"),
                        "emotion_phase": emotion_phase,
                        "signal_text": today_snapshot.get("signal_text", "正常"),
                        "composite_sentiment": today_snapshot.get("composite_sentiment", 0),
                        "sentiment_percentile": today_snapshot.get("sentiment_percentile", 0),
                        "limit_up_count": today_snapshot.get("limit_up_count", 0),
                        "limit_down_count": today_snapshot.get("limit_down_count", 0),
                        "pick_strategy": stock_picks_result.get("pick_strategy"),
                        "llm_enhanced": stock_picks_result.get("llm_enhanced", False),
                        "llm_model": os.getenv("DRAGON_LLM_MODEL", "gpt-5.2"),
                        "market_overview": llm_analysis.get("market_analysis", "") if llm_analysis else "",
                        "pick_logic": f"情绪阶段:{emotion_phase} 策略:{stock_picks_result.get('pick_strategy','')}",
                        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    await sr.save_stock_picks(batch_data, stock_picks_result.get("stocks", []))
                    logger.info("Stock picks saved to DB")
                except Exception as save_err:
                    logger.warning("Failed to save stock picks", error=str(save_err))

            except Exception as pick_e:
                logger.error("Sentiment stock picking failed",
                             error=str(pick_e),
                             traceback=traceback.format_exc())

            # Step 6: 持久化今日数据
            try:
                try:
                    from sentiment_repository import sentiment_repo
                except ImportError:
                    from app.sentiment_repository import sentiment_repo
                await sentiment_repo.save_daily_snapshot(today_snapshot)
                logger.info("Sentiment snapshot saved to DB")
            except Exception as e:
                logger.warning("Failed to save snapshot", error=str(e))

            # Step 7: 构建历史序列（含今日）
            history_for_response = self._build_history(today_snapshot, history_raw, days)

            result = {
                "status": "success",
                "data": {
                    "today": today_snapshot,
                    "history": history_for_response,
                    "signal": today_snapshot.get("signal_text", "正常"),
                    "llm_analysis": llm_analysis,
                    "llm_enhanced": today_snapshot.get("llm_enhanced", False),
                    "stock_picks": {
                        "pick_strategy": stock_picks_result.get("pick_strategy", "none"),
                        "emotion_phase": stock_picks_result.get("pick_strategy") and
                            (llm_analysis.get("emotion_phase", "") if llm_analysis else ""),
                        "stocks": stock_picks_result.get("stocks", []),
                        "pick_count": stock_picks_result.get("pick_count", 0),
                        "llm_enhanced": stock_picks_result.get("llm_enhanced", False),
                    },
                    "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "trading_date": today_snapshot.get("trading_date",
                                                        datetime.now().strftime("%Y-%m-%d")),
                },
            }

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Sentiment strategy failed", error=str(e),
                         traceback=traceback.format_exc())
            raise

    # ========================= 数据获取 =========================

    def _fetch_today_data(self) -> Dict[str, Any]:
        """
        获取今日涨停/跌停数据（复用 dragon_data_provider 多源降级体系）

        涨停池数据源（通过 dragon_data_provider 共享）：
          1. akshare-gateway（独立微服务，不同IP）
          2. 新浪财经（筛选涨幅≥9.8%模拟）
          3. akshare 本地调用
          4. 历史日期回退

        跌停池数据源（情绪战法独有，龙头战法不需要）：
          1. akshare 本地调用（带反爬延迟）

        Returns:
            {
                "trading_date": "2026-03-04",
                "limit_up_stocks": DataFrame,
                "limit_down_stocks": DataFrame,
                "limit_up_count": int,
                "limit_down_count": int,
            }
        """
        result = {
            "trading_date": datetime.now().strftime("%Y-%m-%d"),
            "limit_up_stocks": pd.DataFrame(),
            "limit_down_stocks": pd.DataFrame(),
            "limit_up_count": 0,
            "limit_down_count": 0,
        }

        # ---- 检查当日缓存（同一天不重复请求，防止被封） ----
        if self._is_daily_cache_valid():
            if self._daily_limit_up_cache is not None and not self._daily_limit_up_cache.empty:
                result["limit_up_stocks"] = self._daily_limit_up_cache
                result["limit_up_count"] = len(self._daily_limit_up_cache)
                logger.info("Using daily cached limit_up data", count=result["limit_up_count"])
            if self._daily_limit_down_cache is not None and not self._daily_limit_down_cache.empty:
                result["limit_down_stocks"] = self._daily_limit_down_cache
                result["limit_down_count"] = len(self._daily_limit_down_cache)
            return result

        # ---- 涨停池：复用 dragon_data_provider（共享4级降级 + gateway + UA池） ----
        try:
            try:
                from dragon_data_provider import get_limit_up_stocks
            except ImportError:
                from app.dragon_data_provider import get_limit_up_stocks

            df_up = get_limit_up_stocks()
            if df_up is not None and not df_up.empty:
                result["limit_up_stocks"] = df_up
                result["limit_up_count"] = len(df_up)
                logger.info("Got limit up stocks via dragon_data_provider",
                            count=len(df_up))
        except Exception as e:
            logger.warning("dragon_data_provider limit_up failed, using fallback",
                           error=str(e))
            # 最终兜底：直接调 akshare（带反爬延迟）
            for days_back in range(5):
                try:
                    date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
                    anti_scrape_delay(f"limit_up_fallback_{date_str}", *DELAY_HEAVY)
                    df_up = self.ak.stock_zt_pool_em(date=date_str)
                    if df_up is not None and not df_up.empty:
                        result["limit_up_stocks"] = df_up
                        result["limit_up_count"] = len(df_up)
                        result["trading_date"] = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
                        logger.info("Got limit up stocks from akshare fallback",
                                    date=date_str, count=len(df_up))
                        break
                except Exception:
                    continue

        # ---- 跌停池：dragon_data_provider 没有跌停接口，仍需自行获取 ----
        # 保留跌停板股票列表（仅用于后续选股参考，家数不用此值）
        for days_back in range(5):
            try:
                date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
                anti_scrape_delay(f"limit_down_{date_str}", *DELAY_HEAVY)
                df_down = self.ak.stock_zt_pool_dtgc_em(date=date_str)
                if df_down is not None and not df_down.empty:
                    result["limit_down_stocks"] = df_down
                    # 注意：这里只存 DataFrame，家数由下方实时行情来计算
                    logger.info("Got limit down pool (sealed only)",
                                date=date_str, sealed_count=len(df_down))
                    break
            except Exception:
                continue

        # ---- 方案一：用实时行情统计真实跌停家数（涨跌幅 <= -9.5%）----
        # 这样能计算出"打开跌停但曾经跌停"的股票，避免只看封板池导致数据失真
        realtime_df = self._get_realtime_df()
        if realtime_df is not None and not realtime_df.empty and "涨跌幅" in realtime_df.columns:
            pct_col = pd.to_numeric(realtime_df["涨跌幅"], errors="coerce")
            # <= -9.5% 视为跌停（兼容 科创板/创业板 -20% 和普通 -10%）
            real_limit_down_count = int((pct_col <= -9.5).sum())
            result["limit_down_count"] = real_limit_down_count
            logger.info("Real limit down count from realtime quotes",
                        real_count=real_limit_down_count,
                        sealed_pool=len(result["limit_down_stocks"]) if not result["limit_down_stocks"].empty else 0)
        else:
            # 实时行情获取失败，回退到封板池家数
            result["limit_down_count"] = len(result["limit_down_stocks"]) if not result["limit_down_stocks"].empty else 0
            logger.warning("Realtime quotes unavailable, fallback to sealed pool count",
                           count=result["limit_down_count"])

        # ---- 更新当日缓存 ----
        self._daily_cache_date = datetime.now().strftime("%Y-%m-%d")
        if result["limit_up_count"] > 0:
            self._daily_limit_up_cache = result["limit_up_stocks"]
        if result["limit_down_count"] > 0:
            self._daily_limit_down_cache = result["limit_down_stocks"]

        return result

    def _fetch_yesterday_data(self) -> Dict[str, Any]:
        """
        获取昨日涨停股数据（用于计算溢价率和晋级率）

        涨停池复用 dragon_data_provider（共享降级体系）
        实时行情复用 market_data_provider（共享新浪降级）
        """
        result = {
            "pre_limit_up_codes": set(),
            "pre_limit_up_count": 0,
            "today_pct_of_pre_limit_up": [],   # 昨日涨停股今日涨跌幅列表
            "continuous_limit_up_count": 0,     # 连板（昨今都涨停）
        }

        # 获取昨日涨停股列表（复用 dragon_data_provider）
        yesterday_df = None
        try:
            try:
                from dragon_data_provider import get_limit_up_stocks
            except ImportError:
                from app.dragon_data_provider import get_limit_up_stocks

            # 从昨天开始逐日回退查找
            for days_back in range(1, 7):
                date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
                df = get_limit_up_stocks(date_str=date_str)
                if df is not None and not df.empty:
                    yesterday_df = df
                    logger.info("Got yesterday limit up stocks via dragon_data_provider",
                                date=date_str, count=len(df))
                    break
        except Exception as e:
            logger.warning("dragon_data_provider yesterday failed, using akshare fallback",
                           error=str(e))
            # 兜底：直接调 akshare
            for days_back in range(1, 7):
                try:
                    date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
                    anti_scrape_delay(f"yesterday_limit_up_{date_str}", *DELAY_HEAVY)
                    yesterday_df = self.ak.stock_zt_pool_em(date=date_str)
                    if yesterday_df is not None and not yesterday_df.empty:
                        logger.info("Got yesterday limit up stocks from akshare",
                                    date=date_str, count=len(yesterday_df))
                        break
                except Exception:
                    continue

        if yesterday_df is None or yesterday_df.empty:
            return result

        # 提取昨日涨停股代码
        code_col = None
        for col in yesterday_df.columns:
            if '代码' in col:
                code_col = col
                break
        if code_col is None:
            return result

        pre_codes = set(yesterday_df[code_col].astype(str).tolist())
        result["pre_limit_up_codes"] = pre_codes
        result["pre_limit_up_count"] = len(pre_codes)

        # 获取这些股票今日的涨跌幅（复用 _get_realtime_df 当日缓存，避免重复拉取）
        try:
            realtime_df = self._get_realtime_df()
            if realtime_df is not None and not realtime_df.empty:
                rt_code_col = None
                rt_pct_col = None
                for col in realtime_df.columns:
                    if '代码' in col and rt_code_col is None:
                        rt_code_col = col
                    if '涨跌幅' in col and rt_pct_col is None:
                        rt_pct_col = col

                if rt_code_col and rt_pct_col:
                    # 筛选昨日涨停的股票
                    mask = realtime_df[rt_code_col].astype(str).isin(pre_codes)
                    matched = realtime_df[mask]

                    pct_values = pd.to_numeric(matched[rt_pct_col], errors='coerce').dropna()
                    result["today_pct_of_pre_limit_up"] = pct_values.tolist()

                    # 计算连板数  — 今日涨跌幅 >= 9.5% 近似视为涨停
                    # （严格判断需区分板块，这里用宽松阈值）
                    continuous = pct_values[pct_values >= 9.5].count()
                    result["continuous_limit_up_count"] = int(continuous)

        except Exception as e:
            logger.warning("Failed to get realtime data for premium calc", error=str(e))

        return result

    # ========================= 指标计算 =========================

    def _calculate_indicators(self, today: Dict, yesterday: Dict) -> Dict[str, Any]:
        """
        计算三大情绪指标

        Returns:
            {
                "trading_date": str,
                "limit_up_count": int,
                "limit_down_count": int,
                "pre_limit_up_count": int,
                "continuous_limit_up_count": int,
                "limit_up_premium": float,   # 涨停溢价率
                "up_down_ratio": float,      # 涨跌停比
                "promotion_rate": float,     # 连板晋级率
            }
        """
        limit_up_count = today["limit_up_count"]
        limit_down_count = today["limit_down_count"]
        pre_limit_up_count = yesterday["pre_limit_up_count"]
        continuous_count = yesterday["continuous_limit_up_count"]

        # 指标 1：涨停溢价率 = 昨日涨停股今日平均涨跌幅
        pct_list = yesterday["today_pct_of_pre_limit_up"]
        if pct_list:
            limit_up_premium = float(np.mean(pct_list))
        else:
            limit_up_premium = 0.0

        # 指标 2：涨跌停比 = 涨停 / (涨停 + 跌停)
        denominator = limit_up_count + limit_down_count
        if denominator > 0:
            up_down_ratio = limit_up_count / denominator
        else:
            up_down_ratio = 0.5  # 无涨停无跌停视为中性

        # 指标 3：连板晋级率 = 连板数 / 昨日涨停数
        if pre_limit_up_count > 0:
            promotion_rate = continuous_count / pre_limit_up_count
        else:
            promotion_rate = 0.0

        indicators = {
            "trading_date": today["trading_date"],
            "limit_up_count": limit_up_count,
            "limit_down_count": limit_down_count,
            "pre_limit_up_count": pre_limit_up_count,
            "continuous_limit_up_count": continuous_count,
            "limit_up_premium": round(limit_up_premium, 4),
            "up_down_ratio": round(up_down_ratio, 4),
            "promotion_rate": round(promotion_rate, 4),
        }

        logger.info("Sentiment indicators calculated",
                     premium=indicators["limit_up_premium"],
                     ratio=indicators["up_down_ratio"],
                     promotion=indicators["promotion_rate"])

        return indicators

    def _compute_composite(self, today_indicators: Dict,
                           history: List[Dict]) -> Dict[str, Any]:
        """
        计算 Z-Score 标准化 + 综合情绪 + 交易信号

        使用滚动窗口避免未来函数
        """
        # 将历史数据构建成 DataFrame
        all_records = list(history) + [today_indicators]

        if len(all_records) < 2:
            # 历史不够，只保存原始指标，Z-Score 为 0
            today_indicators.update({
                "z_premium": 0.0,
                "z_ratio": 0.0,
                "z_promotion": 0.0,
                "composite_sentiment": 0.0,
                "sentiment_percentile": 0.5,
                "is_ice_point": False,
                "is_climax_retreat": False,
                "signal_text": "数据积累中",
            })
            return today_indicators

        df = pd.DataFrame(all_records)

        for col in ["limit_up_premium", "up_down_ratio", "promotion_rate"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 滚动 Z-Score
        def rolling_z(series, window=Z_WINDOW):
            mu = series.rolling(window=window, min_periods=3).mean()
            sigma = series.rolling(window=window, min_periods=3).std()
            return (series - mu) / (sigma + 1e-8)

        df["z_premium"] = rolling_z(df["limit_up_premium"])
        df["z_ratio"] = rolling_z(df["up_down_ratio"])
        df["z_promotion"] = rolling_z(df["promotion_rate"])

        # 综合情绪 = 三个 Z 之和
        df["composite_sentiment"] = df["z_premium"] + df["z_ratio"] + df["z_promotion"]

        # 历史分位数（滚动 250 日）
        df["sentiment_percentile"] = df["composite_sentiment"].rolling(
            window=RANK_WINDOW, min_periods=10
        ).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1] if len(x) > 0 else 0.5)

        # 填充 NaN
        df = df.fillna(0)

        # 取最后一行（今日）
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last

        z_premium = round(float(last["z_premium"]), 4)
        z_ratio = round(float(last["z_ratio"]), 4)
        z_promotion = round(float(last["z_promotion"]), 4)
        composite = round(float(last["composite_sentiment"]), 4)
        percentile = round(float(last["sentiment_percentile"]), 4)

        prev_percentile = float(prev.get("sentiment_percentile", 0.5))
        prev_composite = float(prev.get("composite_sentiment", 0))
        prev_promotion = float(prev.get("promotion_rate", 0))

        # 信号判定
        is_ice = (prev_percentile < 0.05) and (composite > prev_composite)
        is_climax = (prev_percentile > 0.95) and (
            today_indicators["promotion_rate"] < prev_promotion - 0.20
        )

        if is_ice:
            signal_text = "冰点转折"
        elif is_climax:
            signal_text = "高潮退潮"
        elif percentile > 0.8:
            signal_text = "情绪过热"
        elif percentile < 0.2:
            signal_text = "情绪低迷"
        else:
            signal_text = "正常"

        today_indicators.update({
            "z_premium": z_premium,
            "z_ratio": z_ratio,
            "z_promotion": z_promotion,
            "composite_sentiment": composite,
            "sentiment_percentile": percentile,
            "is_ice_point": is_ice,
            "is_climax_retreat": is_climax,
            "signal_text": signal_text,
        })

        logger.info("Composite sentiment computed",
                     composite=composite, percentile=percentile, signal=signal_text)

        return today_indicators

    def _build_history(self, today: Dict, history_raw: List[Dict],
                       days: int) -> List[Dict[str, Any]]:
        """构建返回给前端的历史序列"""
        # 历史中过滤掉今日（避免重复）
        today_date = today.get("trading_date")
        history = [
            h for h in history_raw
            if h.get("trading_date") != today_date
        ]

        # 按日期排序，取后 days-1 条
        history.sort(key=lambda x: x.get("trading_date", ""))
        history = history[-(days - 1):]

        # 追加今日
        history.append(today)

        return history


# 全局单例
sentiment_strategy = SentimentStrategy()
