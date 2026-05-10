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
        self._llm_model = os.getenv("DRAGON_LLM_MODEL", "gpt-5.4")
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
                from app.portfolio_repository import portfolio_repo

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

    async def generate_guidance_for_stock(
        self, watchlist_item: Dict, trading_session: str = ""
    ) -> Optional[int]:
        """
        为单只自选股票生成操作指导

        1. 获取实时行情
        2. 按命中的每个战法并发调用 LLM
        3. 汇总生成综合决策
        4. 入库
        """
        try:
            from portfolio_repository import portfolio_repo
        except ImportError:
            from app.portfolio_repository import portfolio_repo

        stock_code = watchlist_item["stock_code"]
        stock_name = watchlist_item["stock_name"]
        buy_price = float(watchlist_item["buy_price"])
        buy_shares = int(watchlist_item["buy_shares"])
        strategies = watchlist_item.get("strategies", [])
        if isinstance(strategies, str):
            strategies = json.loads(strategies)

        # 1. 获取实时行情（改为深层次的个股详情，而不是整个市场行情）
        market = await self._fetch_realtime(stock_code, stock_name)
        current_price = market.get("current_price", 0)
        change_pct = market.get("change_pct", 0)

        # 计算盈亏
        if current_price > 0:
            buy_amount = buy_price * buy_shares
            current_value = current_price * buy_shares
            pnl_amount = round(current_value - buy_amount, 2)
            pnl_pct = round((pnl_amount / buy_amount) * 100, 2) if buy_amount > 0 else 0
        else:
            pnl_amount = 0
            pnl_pct = 0

        # 2. 按战法并发调用 LLM
        tasks = []
        for strategy_key in strategies:
            tmpl = STRATEGY_PROMPT_TEMPLATES.get(strategy_key)
            if tmpl:
                tasks.append(
                    self._analyze_by_strategy(
                        strategy_key, tmpl, stock_code, stock_name,
                        buy_price, buy_shares, current_price, change_pct, market,
                        pnl_amount, pnl_pct,
                    )
                )

        if not tasks:
            logger.warning("No valid strategies for guidance", code=stock_code)
            return None

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

        # 3. 汇总综合决策
        overall = self._aggregate_decision(
            strategy_analyses, current_price, buy_price, pnl_pct
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

    async def _fetch_realtime(self, code: str, stock_name: str) -> Dict:
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
                "macd": kline.get("macd"),
                "rsi": kline.get("rsi_14"),
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
    ) -> Dict:
        """按单个战法调用 LLM 分析"""
        strategy_name = tmpl["name"]
        strategy_prompt = tmpl["prompt"]

        buy_amount = round(buy_price * buy_shares, 2)
        stop_loss = round(buy_price * 0.97, 2)
        target_price = round(buy_price * 1.05, 2)

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

        prompt = f"""你正在以"{strategy_name}"视角分析用户持仓股票，请深度结合{strategy_name}的核心逻辑给出操作指导。

【持仓信息】
股票：{stock_name}({stock_code})
买入价：{buy_price:.2f}元/股，买入{buy_shares}股，总成本{buy_amount:.2f}元
浮动盈亏：{pnl_amount:+.2f}元（{pnl_pct:+.2f}%）
参考止损：{stop_loss}元  参考目标：{target_price}元

【实时行情】
{price_block}

【{strategy_name}分析要求】
{strategy_prompt}

请返回纯JSON（不要markdown标记），格式：
{{
  "analysis": "从{strategy_name}角度的深度分析（80-120字，必须包含具体数据和指标）",
  "action": "操作建议（30字以内，明确持有/加仓/减仓/止损/止盈）",
  "key_metrics": {{"指标名": "值"}},
  "risk_level": "低/中/高",
  "trigger_prices": {{
    "stop_loss": 止损价数字,
    "take_profit": 止盈价数字,
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
                    f"{self._llm_base}/v1/chat/completions",
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

        # 兜底
        return {
            "strategy": strategy_key,
            "strategy_name": strategy_name,
            "icon": tmpl.get("icon", ""),
            "analysis": f"（{strategy_name}分析暂不可用，请结合实时行情自行判断）",
            "action": "持有观察",
            "key_metrics": {},
            "risk_level": "中",
            "trigger_prices": {
                "stop_loss": round(buy_price * 0.97, 2),
                "take_profit": round(buy_price * 1.05, 2),
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
    ) -> Dict:
        """汇总各战法分析，生成综合决策"""
        # 统计各战法的建议
        actions = [a.get("action", "") for a in analyses]
        risk_levels = [a.get("risk_level", "中") for a in analyses]

        # 统计关键词
        stop_count = sum(1 for a in actions if "止损" in a)
        sell_count = sum(1 for a in actions if "止盈" in a or "减仓" in a or "兑现" in a)
        hold_count = sum(1 for a in actions if "持有" in a)
        buy_count = sum(1 for a in actions if "加仓" in a)
        high_risk = sum(1 for r in risk_levels if r == "高")

        total = len(analyses)

        # 决策逻辑
        if current_price > 0 and current_price <= buy_price * 0.97:
            decision = "止损"
            summary = f"现价已跌破止损线，{stop_count}/{total}个战法建议止损，建议立即执行"
        elif stop_count >= total * 0.5:
            decision = "止损"
            summary = f"{stop_count}/{total}个战法建议止损，风险较大，建议止损离场"
        elif sell_count >= total * 0.5:
            decision = "止盈减仓"
            summary = f"{sell_count}/{total}个战法建议止盈或减仓，建议分批锁定利润"
        elif buy_count >= total * 0.5 and high_risk == 0:
            decision = "加仓"
            summary = f"{buy_count}/{total}个战法建议加仓，且无高风险信号，可适度加仓"
        elif high_risk >= total * 0.5:
            decision = "减仓观望"
            summary = f"{high_risk}/{total}个战法显示高风险，建议减仓观望"
        else:
            decision = "持有观察"
            summary = f"各战法建议不一，浮盈{pnl_pct:+.1f}%，建议持有并关注关键价位"

        return {"decision": decision, "summary": summary}


# 模块级单例
watchlist_guidance = WatchlistGuidanceGenerator()
