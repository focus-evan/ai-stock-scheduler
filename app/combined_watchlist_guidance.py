"""
综合战法自选盯盘 — 操作指导生成模块

核心逻辑：
1. 遍历用户自选列表
2. 获取每只股票的实时行情
3. 按命中的每个战法，用战法专项 Prompt 调用 LLM 分析
4. 汇总各战法分析，生成综合操作决策
5. 全部入库
"""

import asyncio
import json
import os
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger()

# ===================== 分战法差异化阈值 =====================

STRATEGY_THRESHOLDS = {
    "dragon_head":     {"stop": -0.03, "target": 0.08, "max_days": 3},
    "sentiment":       {"stop": -0.04, "target": 0.06, "max_days": 4},
    "event_driven":    {"stop": -0.04, "target": 0.07, "max_days": 7},
    "breakthrough":    {"stop": -0.03, "target": 0.08, "max_days": 5},
    "volume_price":    {"stop": -0.04, "target": 0.07, "max_days": 5},
    "overnight":       {"stop": -0.02, "target": 0.05, "max_days": 1},
    "moving_average":  {"stop": -0.05, "target": 0.10, "max_days": 10},
    "northbound":      {"stop": -0.04, "target": 0.08, "max_days": 15},
    "trend_momentum":  {"stop": -0.04, "target": 0.10, "max_days": 15},
}

# 加权投票权重（短线战法信号优先级更高）
VOTE_WEIGHTS = {
    "dragon_head": 1.5, "overnight": 1.5, "sentiment": 1.2,
    "event_driven": 1.0, "breakthrough": 1.0, "volume_price": 1.0,
    "moving_average": 0.8, "northbound": 0.8, "trend_momentum": 0.8,
}

# 扩展操作关键词
STOP_KEYWORDS = ["止损", "离场", "清仓", "割肉", "立刻卖出", "必须卖出"]
SELL_KEYWORDS = ["止盈", "减仓", "兑现", "锁定利润", "分批卖出", "获利了结", "高位兑现"]
BUY_KEYWORDS = ["加仓", "加码", "补仓", "增持", "逢低买入"]
HOLD_KEYWORDS = ["持有", "观察", "等待", "耐心", "观望"]

# ===================== 战法专项 Prompt 模板 =====================

STRATEGY_PROMPT_TEMPLATES = {
    "dragon_head": {
        "name": "龙头战法",
        "icon": "🐉",
        "prompt": (
            "你是龙头战法专家。请从以下维度分析：\n"
            "1. 该股在板块/题材中是否仍占据龙头地位？连板高度和空间？\n"
            "2. 封板资金是否充沛？是否有分歧迹象（炸板、封板延迟）？\n"
            "3. 涨停梯队中同题材已有多少连板，当前处于什么阶段？\n"
            "4. 操作建议：打板跟进、锁仓惜售、还是高位兑现？\n"
            "重点关注：连板天花板、板块情绪、核心催化剂是否还在"
        ),
    },
    "sentiment": {
        "name": "情绪战法",
        "icon": "💹",
        "prompt": (
            "你是情绪战法专家。请从以下维度分析：\n"
            "1. 当前市场整体情绪（涨停/跌停比、涨跌家数比、炸板率）？\n"
            "2. 该股所处的情绪周期位置（冰点/恢复/高潮/退潮）？\n"
            "3. 人气排名是否靠前？资金是否持续流入？\n"
            "4. 操作建议：情绪高潮减仓锁利润？冰点可逆势加仓？\n"
            "重点关注：市场情绪拐点信号、该股相对市场的强弱"
        ),
    },
    "event_driven": {
        "name": "事件驱动",
        "icon": "📰",
        "prompt": (
            "你是事件驱动专家。请从以下维度分析：\n"
            "1. 驱动该股的核心事件/催化剂是什么？时效性和影响力？\n"
            "2. 事件是否已充分反映（price in）还是仍有后续催化？\n"
            "3. 同类事件驱动的历史持续时间参考？\n"
            "4. 操作建议：第一波已过应兑现？还是等二次催化？\n"
            "重点关注：政策/消息面的持续性、预期差"
        ),
    },
    "breakthrough": {
        "name": "突破战法",
        "icon": "🚀",
        "prompt": (
            "你是突破战法专家。请从以下维度分析：\n"
            "1. 突破的关键阻力位是多少？是否有效突破（站稳3日）？\n"
            "2. 突破时量能是否达标（换手率>5%或放量50%以上）？\n"
            "3. 突破后是正常回踩确认还是假突破回落？\n"
            "4. 操作建议：回踩支撑位加仓？跌破突破位止损？\n"
            "重点关注：前阻力变支撑的关键价位、量能持续性"
        ),
    },
    "volume_price": {
        "name": "量价关系",
        "icon": "📊",
        "prompt": (
            "你是量价关系专家。请从以下维度分析：\n"
            "1. 量价关系是否健康（量增价涨/量缩价稳/量增价滞）？\n"
            "2. 主力资金动向：近3日是否有大单净流入/流出？\n"
            "3. 换手率水平是否异常？是否有异动大单？\n"
            "4. 操作建议：出货信号减仓？量能萎缩需离场？\n"
            "重点关注：天量见天价信号、缩量回调的支撑位"
        ),
    },
    "overnight": {
        "name": "隔夜施工法",
        "icon": "🌙",
        "prompt": (
            "你是隔夜施工法专家。请从以下维度分析：\n"
            "1. 竞价阶段的量比和资金方向？集合竞价是否超预期？\n"
            "2. 是否仍具备次日高开溢价条件（尾盘资金介入力度）？\n"
            "3. 次日开盘预判：高开几个点？冲高回落风险？\n"
            "4. 操作建议：竞价应加仓/减仓/清仓？溢价兑现点？\n"
            "重点关注：集合竞价资金流向、T+1溢价空间"
        ),
    },
    "moving_average": {
        "name": "均线战法",
        "icon": "📈",
        "prompt": (
            "你是趋势均线专家。请从以下维度分析：\n"
            "1. 当前股价与5日、10日、20日均线的位置关系？\n"
            "2. 均线系统是否多头排列？MACD/KDJ副图指标状态？\n"
            "3. 最近一次金叉/死叉发生在何时？趋势持续性？\n"
            "4. 操作建议：回踩哪根均线可加仓？跌破哪根止损？\n"
            "重点关注：趋势拐点信号、均线粘合后的方向选择"
        ),
    },
    "northbound": {
        "name": "北向资金",
        "icon": "🏦",
        "prompt": (
            "你是北向资金分析专家。请从以下维度分析：\n"
            "1. 近5日北向资金对该股增减持情况（净买入、持股占比）？\n"
            "2. 北向的操作风格暗示（长期增仓 vs 短期波段）？\n"
            "3. 与北向整体资金的一致性（大盘净流入时同步增持）？\n"
            "4. 操作建议：北向减持应跟随卖出？增持值得加仓？\n"
            "重点关注：外资持股比例变化趋势、与大盘同步性"
        ),
    },
    "trend_momentum": {
        "name": "趋势动量",
        "icon": "⚡",
        "prompt": (
            "你是趋势动量专家。请从以下维度分析：\n"
            "1. 当前动量指标（RSI/DMI/布林）是否支持趋势延续？\n"
            "2. 短期动量是否有衰减（涨幅递减、成交量递减）？\n"
            "3. 与大盘动量的对比：该股的相对强弱？\n"
            "4. 操作建议：动量衰减止盈？动量加速追加？\n"
            "重点关注：RSI超买超卖、动量背离信号"
        ),
    },
}


class WatchlistGuidanceGenerator:
    """自选盯盘操作指导生成器"""

    def __init__(self):
        self._llm_base = os.getenv("DRAGON_LLM_BASE_URL", "https://api.wxznb.cn")
        self._llm_model = os.getenv("DRAGON_LLM_MODEL", "gpt-5.5")
        self._llm_key = os.getenv("DRAGON_LLM_API_KEY", "")

    async def generate_guidance_for_all(self, trading_session: str = "") -> int:
        """
        为所有自选股票生成操作指导

        Returns:
            生成的指导数量
        """
        try:
            try:
                from portfolio_repository import portfolio_repo
            except ImportError:
                from api.portfolio_repository import portfolio_repo

            watchlist = await portfolio_repo.get_watchlist()
            if not watchlist:
                logger.info("No watchlist items, skipping guidance generation")
                return 0

            logger.info(
                "Generating watchlist guidance",
                count=len(watchlist),
                session=trading_session,
            )

            generated = 0
            for item in watchlist:
                try:
                    result = await self.generate_guidance_for_stock(
                        item, trading_session
                    )
                    if result:
                        generated += 1
                except Exception as e:
                    logger.error(
                        "Guidance generation failed for stock",
                        code=item.get("stock_code"),
                        error=str(e),
                    )

            logger.info(
                "Watchlist guidance generation done",
                total=len(watchlist),
                generated=generated,
                session=trading_session,
            )
            return generated

        except Exception as e:
            logger.error(
                "generate_guidance_for_all failed",
                error=str(e),
                traceback=traceback.format_exc(),
            )
            return 0

    async def _fetch_market_env(self) -> Dict:
        """获取大盘环境数据"""
        try:
            try:
                from portfolio_manager import portfolio_manager
            except ImportError:
                from api.portfolio_manager import portfolio_manager
            # _extract_market_context 是同步方法，传 None 时仅返回时间上下文
            return portfolio_manager._extract_market_context(None)
        except Exception as e:
            logger.warning("Failed to fetch market env for guidance", error=str(e))
            return {}

    def _calc_holding_days(self, watchlist_item: Dict) -> int:
        """计算持有天数"""
        try:
            added_at = watchlist_item.get("created_at") or watchlist_item.get("added_at")
            if added_at:
                if isinstance(added_at, str):
                    added_at = datetime.fromisoformat(added_at.replace("Z", "+00:00"))
                delta = datetime.now() - added_at.replace(tzinfo=None)
                return max(delta.days, 0)
        except Exception:
            pass
        return 0

    async def generate_guidance_for_stock(
        self, watchlist_item: Dict, trading_session: str = ""
    ) -> Optional[int]:
        """
        为单只自选股票生成操作指导

        1. 获取实时行情 + 大盘环境
        2. 按命中的每个战法并发调用 LLM（含差异化阈值+大盘+持有天数）
        3. 加权投票汇总综合决策
        4. 入库
        """
        try:
            from portfolio_repository import portfolio_repo
        except ImportError:
            from api.portfolio_repository import portfolio_repo

        stock_code = watchlist_item["stock_code"]
        stock_name = watchlist_item["stock_name"]
        buy_price = float(watchlist_item["buy_price"])
        buy_shares = int(watchlist_item["buy_shares"])
        strategies = watchlist_item.get("strategies", [])
        if isinstance(strategies, str):
            strategies = json.loads(strategies)

        # 计算持有天数
        holding_days = self._calc_holding_days(watchlist_item)

        # 1. 获取实时行情 + 大盘环境
        market = await self._fetch_realtime(stock_code, stock_name)
        market_env = await self._fetch_market_env()
        current_price = market.get("current_price", 0)
        change_pct = market.get("change_pct", 0)

        # 1.5 盘前情绪预警
        sentiment_warning_text = ""
        try:
            try:
                from premarket_sentiment import get_latest_sentiment_report
            except ImportError:
                from api.premarket_sentiment import get_latest_sentiment_report

            sentiment_report = await get_latest_sentiment_report(
                trading_date=datetime.now().strftime("%Y-%m-%d")
            )
            if sentiment_report:
                # 检查该持仓是否命中暴雷或板块风险
                sr_bombs = sentiment_report.get("earnings_bombs", [])
                bomb_codes = {b.get("code", "") for b in sr_bombs}
                bomb_sectors = {b.get("sector", "") for b in sr_bombs if b.get("sector")}

                # 持仓预警
                portfolio_warnings = sentiment_report.get("portfolio_warnings", [])
                for pw in portfolio_warnings:
                    if pw.get("code") == stock_code:
                        sentiment_warning_text += f"\n🚨 {pw.get('warning', '')}"

                # 直接暴雷
                if stock_code in bomb_codes:
                    bomb = next((b for b in sr_bombs if b.get("code") == stock_code), {})
                    sentiment_warning_text += (
                        f"\n🚨 该股出现业绩暴雷: {bomb.get('event', '')}，"
                        f"建议在操作建议中优先考虑止损或减仓！"
                    )

                # 同板块暴雷风险
                if bomb_sectors:
                    for sa in strategies:
                        # 用战法名做板块匹配的近似（不完美但有用）
                        pass
                    # 尝试从 LLM 分析中获取行业
                    stock_industry = watchlist_item.get("industry", "")
                    if stock_industry:
                        for bs in bomb_sectors:
                            if bs and bs in stock_industry:
                                sentiment_warning_text += (
                                    f"\n⚠️ 同板块({bs})有暴雷风险({next((b.get('name','') for b in sr_bombs if b.get('sector')==bs), '')})，"
                                    f"注意板块联动下跌风险！"
                                )
                                break

                # 整体情绪
                sr_advice = sentiment_report.get("trading_advice", "")
                sr_risk = sentiment_report.get("risk_level", "")
                if sr_risk in ("high", "extreme"):
                    sentiment_warning_text += f"\n⚠️ 盘前情绪: {sr_advice}，整体风险偏高"
                elif sr_risk == "medium_high":
                    sentiment_warning_text += f"\n📊 盘前提示: {sr_advice}"

        except Exception as se:
            logger.warning("Sentiment check for guidance failed (non-fatal)", error=str(se))

        # 计算盈亏
        if current_price > 0:
            buy_amount = buy_price * buy_shares
            current_value = current_price * buy_shares
            pnl_amount = round(current_value - buy_amount, 2)
            pnl_pct = round((pnl_amount / buy_amount) * 100, 2) if buy_amount > 0 else 0
        else:
            pnl_amount = 0
            pnl_pct = 0

        # 2. 按战法并发调用 LLM（注入差异化阈值+大盘环境+持有天数）
        tasks = []
        for strategy_key in strategies:
            tmpl = STRATEGY_PROMPT_TEMPLATES.get(strategy_key)
            if tmpl:
                tasks.append(
                    self._analyze_by_strategy(
                        strategy_key, tmpl, stock_code, stock_name,
                        buy_price, buy_shares, current_price, change_pct, market,
                        pnl_amount, pnl_pct,
                        market_env=market_env,
                        holding_days=holding_days,
                        sentiment_warning=sentiment_warning_text,
                    )
                )

        if not tasks:
            logger.warning(
                "No valid strategies, falling back to combined analysis",
                code=stock_code, strategies=strategies,
            )
            # 兜底：任何战法为空或不匹配，使用通用综合分析模板
            _fallback_tmpl = {
                "name": "综合分析",
                "icon": "📊",
                "prompt": (
                    "你是A股综合分析师。请从以下维度分析：\n"
                    "1. 技术面：均线系统（5/10/20日）、MACD金叉死叉、RSI超买超卖状态\n"
                    "2. 量价关系：近期成交量能与价格配合是否健康，有无主力出货迹象\n"
                    "3. 持仓评估：当前浮盈/浮亏情况，是否已超出合理持有周期\n"
                    "4. 操作建议：持有/止损/止盈的明确判断，给出具体关键价位\n"
                    "重点关注：止损纪律，防止小亏变大亏"
                ),
            }
            tasks.append(
                self._analyze_by_strategy(
                    "combined", _fallback_tmpl, stock_code, stock_name,
                    buy_price, buy_shares, current_price, change_pct, market,
                    pnl_amount, pnl_pct,
                    market_env=market_env,
                    holding_days=holding_days,
                    sentiment_warning=sentiment_warning_text,
                )
            )

        strategy_results = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤异常
        strategy_analyses = []
        for r in strategy_results:
            if isinstance(r, dict):
                strategy_analyses.append(r)
            elif isinstance(r, Exception):
                logger.warning("Strategy analysis exception", error=str(r))

        if not strategy_analyses:
            logger.warning("All strategy analyses failed", code=stock_code)
            return None

        # 3. 加权投票汇总综合决策（含主导战法阈值）
        primary_strat = strategies[0] if strategies else "combined"
        thresholds = STRATEGY_THRESHOLDS.get(primary_strat, {"stop": -0.03, "target": 0.05, "max_days": 5})
        overall = self._aggregate_decision(
            strategy_analyses, current_price, buy_price, pnl_pct,
            holding_days=holding_days, thresholds=thresholds,
        )

        # ===== 关键修复：综合决策和子战法 action 一致性同步 =====
        # 防止外层"止损"内层"持有观察"的矛盾
        overall_decision = overall["decision"]
        if overall_decision in ("止损", "止盈减仓"):
            for sa in strategy_analyses:
                sa_action = sa.get("action", "")
                # 如果子战法是兜底的"持有观察"或空，强制同步
                is_fallback = ("暂不可用" in sa.get("analysis", "") or
                               sa_action in ("持有观察", "观察", ""))
                if is_fallback:
                    sa["action"] = overall_decision
                    sa["analysis"] = sa.get("analysis", "").replace(
                        "持有观察",
                        f"{overall_decision}（综合决策）"
                    )

        # 4. 入库
        record_id = await portfolio_repo.save_watchlist_guidance(
            watchlist_id=watchlist_item["id"],
            user_id=watchlist_item.get("user_id", "admin"),
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            change_pct=change_pct,
            pnl_amount=pnl_amount,
            pnl_pct=pnl_pct,
            strategy_analyses=strategy_analyses,
            overall_decision=overall["decision"],
            overall_summary=overall["summary"],
            trading_session=trading_session,
        )

        logger.info(
            "Guidance generated",
            code=stock_code,
            strategies=len(strategy_analyses),
            decision=overall["decision"],
            record_id=record_id,
        )
        return record_id

    async def _fetch_realtime(self, code: str, stock_name: str = "") -> Dict:
        """获取单只股票实时行情及技术指标（深度数据）"""
        try:
            try:
                from stock_data_fetcher import stock_data_fetcher, detect_market
            except ImportError:
                from app.stock_data_fetcher import stock_data_fetcher, detect_market

            market_type = detect_market(code)
            data = await stock_data_fetcher.fetch_comprehensive_data(code, stock_name, market_type)

            kline = data.get("kline_analysis") or {}
            metrics = data.get("key_metrics") or {}

            # 优先从 metrics 获取，其次 kline
            current_price = metrics.get("current_price") or kline.get("current_price", 0)
            change_pct = metrics.get("change_pct") or kline.get("change_pct", 0)

            return {
                "current_price": current_price,
                "change_pct": change_pct,
                "volume": metrics.get("volume", 0) or kline.get("avg_vol_5", 0),
                "turnover": metrics.get("turnover_rate", 0),
                "ma5": kline.get("ma5", 0),
                "ma10": kline.get("ma10", 0),
                "ma20": kline.get("ma20", 0),
                "ma60": kline.get("ma60"),
                "macd": kline.get("macd"),
                "macd_signal": kline.get("macd_signal"),
                "macd_histogram": kline.get("macd_histogram"),
                "rsi_14": kline.get("rsi_14"),
                "rsi": kline.get("rsi_14"),
                "is_bull_aligned": kline.get("is_bull_aligned"),
                "vol_ratio": kline.get("vol_ratio"),
                "change_5d": kline.get("change_5d"),
                "change_20d": kline.get("change_20d"),
                "support_1": kline.get("support_1"),
                "resistance_1": kline.get("resistance_1"),
                "high_20d": kline.get("high_20d"),
                "low_20d": kline.get("low_20d"),
            }
        except Exception as e:
            logger.warning("Failed to fetch realtime for watchlist", code=code, error=str(e))
        return {}

    async def _analyze_by_strategy(
        self,
        strategy_key: str,
        tmpl: Dict,
        stock_code: str,
        stock_name: str,
        buy_price: float,
        buy_shares: int,
        current_price: float,
        change_pct: float,
        market: Dict,
        pnl_amount: float,
        pnl_pct: float,
        market_env: Dict = None,
        holding_days: int = 0,
        sentiment_warning: str = "",
    ) -> Dict:
        """按单个战法调用 LLM 分析（含差异化阈值+大盘环境+持有天数）"""
        strategy_name = tmpl["name"]
        strategy_prompt = tmpl["prompt"]

        buy_amount = round(buy_price * buy_shares, 2)
        # 分战法差异化止损止盈（P0核心优化）
        thresh = STRATEGY_THRESHOLDS.get(strategy_key, {"stop": -0.03, "target": 0.05, "max_days": 5})
        stop_loss = round(buy_price * (1 + thresh["stop"]), 2)
        target_price = round(buy_price * (1 + thresh["target"]), 2)
        max_days = thresh["max_days"]

        price_block = (
            f"现价:{current_price:.2f}元 涨跌:{change_pct:+.2f}% "
            f"成交量:{market.get('volume', 0):.0f}手 "
            f"换手率:{market.get('turnover', 0):.2f}%\n"
            f"[技术面] 5日均线:{market.get('ma5', 0):.2f} 10日均线:{market.get('ma10', 0):.2f} "
            f"20日均线:{market.get('ma20', 0):.2f} "
            f"MACD:{market.get('macd') or '暂无'} "
            f"RSI(14):{market.get('rsi') or '暂无'}"
            if current_price > 0 else "实时行情缺失，请保守预估"
        )

        # 技术指标数据块 —— 关键修复：将 K线分析数据注入 LLM prompt
        tech_block = ""
        ma5 = market.get("ma5")
        ma10 = market.get("ma10")
        ma20 = market.get("ma20")
        macd_val = market.get("macd")
        macd_hist = market.get("macd_histogram")
        rsi_val = market.get("rsi_14")
        vol_ratio = market.get("vol_ratio")

        if any(v is not None for v in [ma5, ma10, ma20, macd_val, rsi_val]):
            tech_block = "\n【技术指标】\n"
            if ma5 is not None:
                ma60 = market.get("ma60")
                bull = market.get("is_bull_aligned")
                tech_block += f"5日均线:{ma5:.2f} 10日均线:{ma10:.2f} 20日均线:{ma20:.2f}"
                if ma60 is not None:
                    tech_block += f" 60日均线:{ma60:.2f}"
                if bull is not None:
                    tech_block += f" {'多头排列✅' if bull else '非多头❌'}"
                tech_block += "\n"
            if macd_val is not None:
                macd_sig = market.get("macd_signal")
                tech_block += f"MACD:{macd_val:.4f} Signal:{macd_sig:.4f}" if macd_sig is not None else f"MACD:{macd_val:.4f}"
                if macd_hist is not None:
                    tech_block += f" 柱状:{macd_hist:+.4f}{'(金叉)' if macd_hist > 0 else '(死叉)'}"
                tech_block += "\n"
            if rsi_val is not None:
                rsi_label = "超买⚠️" if rsi_val > 70 else ("超卖⚠️" if rsi_val < 30 else "正常")
                tech_block += f"RSI(14):{rsi_val:.1f}({rsi_label})\n"
            if vol_ratio is not None:
                tech_block += f"量比:{vol_ratio:.2f}\n"
            # 支撑/阻力
            s1 = market.get("support_1")
            r1 = market.get("resistance_1")
            if s1 is not None and r1 is not None:
                tech_block += f"20日支撑:{s1:.2f} 20日阻力:{r1:.2f}\n"
            # 近期涨跌
            c5 = market.get("change_5d")
            c20 = market.get("change_20d")
            if c5 is not None:
                tech_block += f"近5日涨跌:{c5:+.2f}%"
                if c20 is not None:
                    tech_block += f" 近20日涨跌:{c20:+.2f}%"
                tech_block += "\n"
        else:
            tech_block = "\n【技术指标】当前数据不足，请基于已有信息分析\n"

        # 大盘环境段落（P1优化）
        market_block = ""
        if market_env:
            sse = market_env.get("sse_change", 0)
            up_cnt = market_env.get("market_up_count", 0)
            down_cnt = market_env.get("market_down_count", 0)
            market_block = (
                f"\n【大盘环境】\n"
                f"上证指数: {sse:+.2f}% | 上涨{up_cnt}家 / 下跌{down_cnt}家\n"
            )
            if sse < -1.5:
                market_block += "⚠️ 大盘明显弱势，操作需更谨慎\n"
            elif sse > 1.5:
                market_block += "🚀 大盘强势，可适当乐观\n"

        # 持有天数段落（P2优化）
        holding_block = ""
        if holding_days > 0:
            holding_block = (
                f"\n【持仓状态】\n"
                f"已持有: {holding_days}天 | {strategy_name}建议最长持有: {max_days}天\n"
            )
            if holding_days > max_days:
                holding_block += f"⚠️ 已超出{strategy_name}建议持有周期({max_days}天)，请重点评估是否应该离场！\n"
            elif holding_days >= max_days * 0.8:
                holding_block += f"⏰ 接近{strategy_name}建议持有上限，关注离场时机\n"

        prompt = f"""你正在以"{strategy_name}"视角分析用户持仓股票，请深度结合{strategy_name}的核心逻辑给出操作指导。

【持仓信息】
股票：{stock_name}({stock_code})
买入价：{buy_price:.2f}元/股，买入{buy_shares}股，总成本{buy_amount:.2f}元
浮动盈亏：{pnl_amount:+.2f}元（{pnl_pct:+.2f}%）
{strategy_name}止损线：{stop_loss}元({thresh['stop']*100:.0f}%)  止盈目标：{target_price}元({thresh['target']*100:.0f}%)

【实时行情】
{price_block}
{tech_block}{market_block}{holding_block}{'\n【盘前风险预警】' + sentiment_warning + '\n' if sentiment_warning else ''}
【{strategy_name}分析要求】
{strategy_prompt}

请返回纯JSON（不要markdown标记），格式：
{{
  "analysis": "从{strategy_name}角度的深度分析（100-150字，必须包含具体数据和技术指标数值，引用均线/MACD/RSI等具体读数作为论据）",
  "action": "操作建议（30字以内，明确持有/加仓/减仓/止损/止盈/离场）",
  "action_reason": "操作理由（50字，必须引用至少2个具体技术指标或价格数据作为依据）",
  "key_metrics": {{"指标名": "值"}},
  "risk_level": "低/中/高",
  "trigger_prices": {{
    "stop_loss": 止损价数字,
    "stop_loss_basis": "止损价依据（如：跌破20日均线XX.XX元 / 跌破前低支撑位XX.XX元）",
    "take_profit": 止盈价数字,
    "take_profit_basis": "目标价依据（如：前高压力位XX.XX元 / 上方缺口XX.XX元 / 布林上轨XX.XX元）",
    "add_position": 加仓价数字
  }}
}}"""

        try:
            payload = {
                "model": self._llm_model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            f"你是{strategy_name}资深分析师，擅长从{strategy_name}的独特视角分析股票走势。"
                            "你的分析必须紧扣该战法的核心指标和方法论，数据驱动，结论明确。"
                            "【强制要求】"
                            "1. 止损价和目标价必须有明确的技术面依据（如均线、支撑/阻力位、前高前低），"
                            "不能简单用买入价乘以一个百分比！"
                            "2. 每个操作建议都要引用至少2个具体的技术指标数值作为论据。"
                            "3. 若持有天数已超出建议周期，必须给出明确的离场或继续持有判断。"
                            "4. action_reason 必须包含具体价格和指标数据，不能是空洞的描述。"
                            "请只返回 JSON，不要任何其他内容。"
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.3,
                "max_tokens": 2000,
            }

            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{self._llm_base}/chat/completions",
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self._llm_key}",
                    },
                )
                if resp.status_code == 200:
                    content = (
                        resp.json()
                        .get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                    )
                    parsed = self._parse_json(content)
                    if parsed:
                        parsed["strategy"] = strategy_key
                        parsed["strategy_name"] = strategy_name
                        parsed["icon"] = tmpl.get("icon", "")
                        return parsed

        except Exception as e:
            logger.warning(
                "LLM call failed for strategy",
                strategy=strategy_name,
                code=stock_code,
                error=str(e),
            )

        # 兜底（也使用差异化阈值）
        return {
            "strategy": strategy_key,
            "strategy_name": strategy_name,
            "icon": tmpl.get("icon", ""),
            "analysis": f"（{strategy_name}分析暂不可用，请结合实时行情自行判断）",
            "action": "持有观察",
            "key_metrics": {},
            "risk_level": "中",
            "trigger_prices": {
                "stop_loss": stop_loss,
                "take_profit": target_price,
                "add_position": round(buy_price * 0.98, 2),
            },
        }

    def _parse_json(self, content: str) -> Optional[Dict]:
        """从 LLM 响应中解析 JSON"""
        cleaned = content.strip()
        for prefix in ["```json", "```"]:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
        try:
            return json.loads(cleaned)
        except Exception:
            for suffix in ["}", "]}", "]}}"]:
                try:
                    return json.loads(cleaned + suffix)
                except Exception:
                    continue
        return None

    def _aggregate_decision(
        self,
        analyses: List[Dict],
        current_price: float,
        buy_price: float,
        pnl_pct: float,
        holding_days: int = 0,
        thresholds: Dict = None,
    ) -> Dict:
        """加权投票汇总各战法分析，生成综合决策（含超期预警）"""
        if thresholds is None:
            thresholds = {"stop": -0.03, "target": 0.05, "max_days": 5}

        # 加权统计各战法的建议（P1核心优化）
        stop_score = 0.0
        sell_score = 0.0
        hold_score = 0.0
        buy_score = 0.0
        high_risk_score = 0.0
        total_weight = 0.0

        for a in analyses:
            action = a.get("action", "")
            risk = a.get("risk_level", "中")
            strat = a.get("strategy", "")
            w = VOTE_WEIGHTS.get(strat, 1.0)
            total_weight += w

            if any(kw in action for kw in STOP_KEYWORDS):
                stop_score += w
            elif any(kw in action for kw in SELL_KEYWORDS):
                sell_score += w
            elif any(kw in action for kw in BUY_KEYWORDS):
                buy_score += w
            elif any(kw in action for kw in HOLD_KEYWORDS):
                hold_score += w

            if risk == "高":
                high_risk_score += w

        total = len(analyses)
        stop_ratio = stop_score / total_weight if total_weight > 0 else 0
        sell_ratio = sell_score / total_weight if total_weight > 0 else 0
        buy_ratio = buy_score / total_weight if total_weight > 0 else 0
        risk_ratio = high_risk_score / total_weight if total_weight > 0 else 0

        # 分战法差异化止损线（P0核心优化）
        stop_price = buy_price * (1 + thresholds["stop"])
        max_days = thresholds["max_days"]

        # 决策逻辑（含超期预警）
        if current_price > 0 and current_price <= stop_price:
            decision = "止损"
            summary = f"现价已跌破止损线({thresholds['stop']*100:.0f}%)，建议立即止损离场"
        elif stop_ratio >= 0.45:
            decision = "止损"
            summary = f"加权投票{stop_ratio:.0%}战法建议止损，风险较大，建议止损离场"
        elif sell_ratio >= 0.45:
            decision = "止盈减仓"
            summary = f"加权投票{sell_ratio:.0%}战法建议止盈/减仓，建议分批锁定利润"
        elif holding_days > max_days and pnl_pct > 0:
            decision = "止盈减仓"
            summary = f"已持有{holding_days}天超出建议周期({max_days}天)且浮盈{pnl_pct:+.1f}%，建议止盈"
        elif holding_days > max_days and pnl_pct <= 0:
            decision = "减仓观望"
            summary = f"已持有{holding_days}天超出建议周期({max_days}天)且浮亏{pnl_pct:+.1f}%，建议减仓"
        elif buy_ratio >= 0.45 and risk_ratio < 0.3:
            decision = "加仓"
            summary = f"加权投票{buy_ratio:.0%}战法建议加仓，且风险可控，可适度加仓"
        elif risk_ratio >= 0.45:
            decision = "减仓观望"
            summary = f"加权投票{risk_ratio:.0%}战法显示高风险，建议减仓观望"
        else:
            decision = "持有观察"
            days_info = f"持有{holding_days}天, " if holding_days > 0 else ""
            summary = f"各战法建议不一，{days_info}浮盈{pnl_pct:+.1f}%，建议持有并关注关键价位"

        return {"decision": decision, "summary": summary}


# 模块级单例
watchlist_guidance = WatchlistGuidanceGenerator()
