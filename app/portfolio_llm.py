"""
模拟交易 LLM 决策模块 (Portfolio LLM)

通过 GPT-5.2 对推荐股票进行智能交易决策：
1. 初始建仓 - 根据推荐股票综合分析，自动分配初始资金比例
2. 动态调仓 - 根据持仓和最新行情，决定买入/卖出/持有
3. 风险控制 - 止盈止损建议
"""

import json
import os
import traceback
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger()

# 每个组合最多持有的股票数量
MAX_POSITIONS = 5

# 复用龙头战法的 GPT-5.2 配置
PORTFOLIO_LLM_BASE_URL = os.getenv("DRAGON_LLM_BASE_URL", "https://api.wxznb.cn")
PORTFOLIO_LLM_MODEL = os.getenv("DRAGON_LLM_MODEL", "gpt-5.4")
PORTFOLIO_LLM_PROVIDER = os.getenv("DRAGON_LLM_PROVIDER", "openai")
DRAGON_LLM_API_KEY = os.getenv("DRAGON_LLM_API_KEY", "")


class PortfolioLLM:
    """模拟交易 GPT-5.2 决策客户端"""

    def __init__(self):
        self.base_url = PORTFOLIO_LLM_BASE_URL
        self.model = PORTFOLIO_LLM_MODEL
        self.provider = PORTFOLIO_LLM_PROVIDER
        self.api_key = DRAGON_LLM_API_KEY
        self.timeout = 90

    async def _chat(self, messages: List[Dict[str, str]],
                    temperature: float = 0.3,
                    max_tokens: int = 3000) -> Optional[str]:
        """调用 GPT-5.2 API"""
        try:
            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/v1/chat/completions",
                    json=payload,
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"},
                )

                if response.status_code == 200:
                    data = response.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    return content
                else:
                    logger.error("Portfolio LLM API error",
                                 status=response.status_code,
                                 body=response.text[:500])
                    return None

        except httpx.TimeoutException:
            logger.error("Portfolio LLM call timed out", timeout=self.timeout)
            return None
        except Exception as e:
            logger.error("Portfolio LLM call failed", error=str(e))
            return None

    # ==================== 1. 初始建仓决策 ====================

    async def allocate_initial_capital(
        self,
        stocks: List[Dict],
        initial_capital: float,
        strategy_type: str,
    ) -> Dict[str, Any]:
        """
        GPT-5.2 根据推荐股票综合分析，自动分配初始资金

        Args:
            stocks: 推荐股票列表（来自龙头战法或情绪战法）
            initial_capital: 初始资金
            strategy_type: 策略类型 dragon_head / sentiment

        Returns:
            {
                "allocations": [
                    {
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "weight": 15.0,          # 仓位百分比
                        "amount": 15000.0,        # 分配金额
                        "quantity": 1000,          # 建议股数(按手取整)
                        "reason": "理由"
                    }, ...
                ],
                "cash_reserve": 10.0,  # 现金保留比例
                "summary": "整体分配思路"
            }
        """
        prompt = self._build_allocation_prompt(stocks, initial_capital, strategy_type)

        for attempt in range(1, 3):
            try:
                response = await self._chat(
                    messages=[
                        {"role": "system", "content": self._get_allocation_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.3,
                    max_tokens=4000,
                )

                if not response:
                    logger.warning("Portfolio LLM returned no response", attempt=attempt)
                    continue

                result = self._parse_json_response(response)
                if result and result.get("allocations"):
                    # 验证并修正分配
                    result = self._validate_allocations(result, initial_capital, stocks)
                    logger.info("Capital allocation completed",
                                stock_count=len(result["allocations"]),
                                cash_reserve=result.get("cash_reserve", 0))
                    return result

            except Exception as e:
                logger.error("Allocation attempt failed", attempt=attempt, error=str(e))

        # 降级：均匀分配
        return self._fallback_allocation(stocks, initial_capital)

    # ==================== 2. 动态调仓决策 ====================

    async def make_trading_decisions(
        self,
        positions: List[Dict],
        new_recommendations: List[Dict],
        portfolio_info: Dict,
        strategy_type: str,
        max_positions: int = MAX_POSITIONS,
        last_review: Dict = None,
        today_bought_codes: set = None,
    ) -> Dict[str, Any]:
        """
        GPT-5.2 根据持仓和最新推荐，决定买卖操作

        Args:
            positions: 当前持仓列表
            new_recommendations: 最新推荐股票列表
            portfolio_info: 组合信息（总资产、可用现金等）
            strategy_type: 策略类型
            last_review: 前一日复盘信息（用于改进交易决策）
            today_bought_codes: 当天已买入的股票代码集合（T+1约束）

        Returns:
            {
                "decisions": [...],
                "summary": "整体决策思路"
            }
        """
        prompt = self._build_trading_prompt(positions, new_recommendations,
                                             portfolio_info, strategy_type,
                                             max_positions, last_review,
                                             today_bought_codes)

        system_prompt = self.get_strategy_system_prompt(strategy_type)

        for attempt in range(1, 3):
            try:
                response = await self._chat(
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.3,
                    max_tokens=4000,
                )

                if not response:
                    continue

                result = self._parse_json_response(response)
                if result and result.get("decisions") is not None:
                    trade_today = result.get("trade_today", True)
                    if not trade_today:
                        logger.info("LLM decided not to trade today",
                                    strategy=strategy_type,
                                    summary=result.get("summary", "")[:100])
                        return {
                            "decisions": [],
                            "summary": result.get("summary", "LLM判断今日不宜操作，观望等待"),
                            "trade_today": False,
                        }
                    logger.info("Trading decisions made",
                                decision_count=len(result["decisions"]),
                                strategy=strategy_type)
                    return result

            except Exception as e:
                logger.error("Trading decision attempt failed",
                             attempt=attempt, error=str(e))

        return {"decisions": [], "summary": "LLM 决策失败，本次不操作", "trade_today": False}


    # ==================== Prompt 构建 ====================

    def get_strategy_system_prompt(self, strategy_type: str) -> str:
        """根据战法类型返回个性化 System Prompt（含前日复盘整合要求）"""
        base = (
            f"你是A股模拟交易操盘手，精通{strategy_type}策略。\n"
            f"每次交易决策前你已阅读前日复盘报告，必须结合复盘的得失和'今日应执行操作'来优化今日决策。\n"
            f"【T+1规则】当天买入的股票当天不能卖出。\n"
            f"【硬性止损】亏损≥5%已由系统强制执行，LLM无需重复设置止损操作。\n"
            f"只返回JSON，不要markdown标记。\n\n"
        )
        strategy_rules = {
            "dragon_head": (
                "【龙头战法纪律】\n"
                "1. 不追涨停！涨停当日不买，等次日回踩5日线附近才介入\n"
                "2. 高位多板大概率高开低走，爆量分歧必须减仓\n"
                "3. 退潮期（连板股频繁大阴线）必须空仓，不抄底\n"
                "4. 止盈：盈利8%卖出50%仓位，盈利15%全部清仓\n"
                "5. 持仓周期：1-3天，龙头股最多5天\n"
            ),
            "sentiment": (
                "【情绪战法纪律】\n"
                "1. 情绪过热（涨停>200家）只做波段不追高\n"
                "2. 情绪极度悲观（跌停>300家）逆势建仓但仓位≤30%\n"
                "3. 情绪由热转冷时立即减仓50%\n"
                "4. 止盈：情绪指数高位回落时清仓\n"
                "5. 持仓周期：2-4天，随情绪变化灵活调整\n"
            ),
            "event_driven": (
                "【事件驱动纪律】\n"
                "1. 事件兑现原则：利好公告当天或次日是最佳卖点\n"
                "2. 利好低于预期立即清仓，不设等待期\n"
                "3. 事件被证伪立即止损\n"
                "4. 止盈：事件兑现后盈利6-10%离场\n"
                "5. 持仓周期：按事件发酵期，政策≤7天，业绩≤5天\n"
            ),
            "breakthrough": (
                "【突破战法纪律】\n"
                "1. 真突破条件：成交量必须放大150%以上，缩量突破为假信号不追\n"
                "2. 突破后回踩支撑位不破=加仓机会；跌破突破位立刻止损\n"
                "3. 止损位：突破价下方3%\n"
                "4. 止盈：第1目标=前期高点，第2目标=突破幅度100%延伸\n"
                "5. 持仓周期：强势突破2-3天，震荡突破5-7天\n"
            ),
            "volume_price": (
                "【量价关系纪律】\n"
                "1. 天量上涨=主力出货信号，必须减仓\n"
                "2. 缩量回调+价格不跌=强势整理，加仓机会\n"
                "3. 放量阴线=主力出货，立即减仓\n"
                "4. 止盈：天量次日放量阴线立即全部卖出\n"
                "5. 持仓周期：放量突破1-3天，缩量整理3-7天\n"
            ),
            "moving_average": (
                "【均线战法纪律】\n"
                "1. 满仓条件：5日>10日>20日>60日均线全部多头排列\n"
                "2. 跌破20日均线减仓50%；跌破60日均线清仓\n"
                "3. 5日线死叉10日线=减仓信号\n"
                "4. 止盈：价格偏离均线乖离率>15%时分批止盈\n"
                "5. 持仓周期：均线多头期间5-10天\n"
            ),
            "overnight": (
                "【隔夜施工法则】\n"
                "1. 尾盘(14:50后)择机买入，次日集合竞价或开盘冲高卖出\n"
                "2. 集合竞价高开3%+卖出；平开或冲高5分钟内卖出；低开>2%立即止损\n"
                "3. 不论盈亏只持有一夜，绝不隔第二夜\n"
                "4. 强信号个股(如均线全多头/量能放大)可稍微多观察开盘前5分钟\n"
                "5. 持仓周期：严格1天\n"
            ),
            "northbound": (
                "【北向资金纪律】\n"
                "1. 仅跟踪连续净买入≥3天且单日净买入>1亿的个股\n"
                "2. 北向全市场单日净卖出>5亿时，所有仓位缩减50%\n"
                "3. 某股北向净卖出连续3天=主动离场\n"
                "4. 止盈：北向开始减仓而价格仍涨=外资获利了结，跟随卖出\n"
                "5. 持仓周期：外资持续净买期间持有，通常5-15天\n"
            ),
            "trend_momentum": (
                "【趋势动量纪律】\n"
                "1. 强动量条件：ADX>25 + MACD金叉 + RSI区间50-70\n"
                "2. RSI>80=超买减仓50%；ADX<20=趋势减弱，开始分批离场\n"
                "3. RSI高位背离（价格新高但RSI不新高）=减仓信号\n"
                "4. 止盈：动量指标全面走弱时清仓\n"
                "5. 持仓周期：动量强时7-15天，动量减弱立刻减仓\n"
            ),
            "combined": (
                "【综合战法纪律】\n"
                "1. 共振强度决定仓位：1-2个战法=10%；3个=25%；4个以上=40%\n"
                "2. 多个子战法同时发出止损信号=必须清仓\n"
                "3. 置信度<60%的推荐不介入\n"
                "4. 综合战法作为最后交易窗口，等上午各战法完成后执行\n"
                "5. 持仓周期：2-7天，跟随主导战法调整\n"
            ),
        }
        return base + strategy_rules.get(strategy_type, (
            "【通用纪律】\n"
            "1. 亏损超过5%必须止损\n"
            "2. 盈利超过10%卖出1/3，超过20%全部卖出\n"
            "3. 持仓最多5只，买新必先卖旧\n"
        ))

    def _get_allocation_system_prompt(self) -> str:
        return (
            f"你是A股模拟交易资金分配专家。根据推荐股票的质量和风险，"
            f"合理分配资金比例。必须保留至少10%现金。"
            f"持仓上限为{MAX_POSITIONS}只股票，从推荐列表中选出最匹配的{MAX_POSITIONS}只。"
            f"只返回JSON，不要markdown标记。"
        )

    def _get_trading_system_prompt(self) -> str:
        """保留兼容性，新代码请使用 get_strategy_system_prompt(strategy_type)"""
        return self.get_strategy_system_prompt("dragon_head")

    def _build_allocation_prompt(self, stocks: List[Dict],
                                  capital: float, strategy: str) -> str:
        # 取前 MAX_POSITIONS 只
        top_stocks = stocks[:MAX_POSITIONS]
        stocks_text = []
        for i, s in enumerate(top_stocks, 1):
            line = (
                f"{i}. {s.get('name', s.get('stock_name', '?'))}"
                f"({s.get('code', s.get('stock_code', '?'))}) "
                f"价格:{s.get('price', s.get('current_price', 0))}元 "
                f"涨幅:{s.get('change_pct', 0):.2f}% "
                f"推荐等级:{s.get('recommendation_level', '关注')}"
            )
            stocks_text.append(line)

        strategy_name = "龙头战法" if strategy == "dragon_head" else "情绪战法"

        prompt = (
            f"策略:{strategy_name}，初始资金:{capital:.0f}元，持仓上限:{MAX_POSITIONS}只\n"
            f"推荐股票:\n" + "\n".join(stocks_text) + "\n\n"
            f"请从上述股票中选出最匹配的{MAX_POSITIONS}只（或更少）分配资金，返回JSON（不要```标记）：\n"
            f'{{"allocations":[{{"stock_code":"代码","stock_name":"名称",'
            f'"weight":仓位百分比,"amount":金额,"quantity":股数(100的整数倍),'
            f'"reason":"分配理由"}}],'
            f'"cash_reserve":现金保留百分比,'
            f'"summary":"整体分配思路(50字)"}}'
        )
        return prompt

    def _build_trading_prompt(self, positions: List[Dict],
                               new_recs: List[Dict],
                               portfolio: Dict, strategy: str,
                               max_positions: int = MAX_POSITIONS,
                               last_review: Dict = None,
                               today_bought_codes: set = None) -> str:
        # 持仓信息（展示更多有用指标）
        pos_lines = []
        for p in positions:
            pnl_pct = float(p.get('profit_pct', 0))
            alert = "🔴" if pnl_pct <= -5 else ("🟢" if pnl_pct >= 10 else "")
            line = (
                f"- {p.get('stock_name','?')}({p.get('stock_code','?')}) "
                f"持{p.get('quantity',0)}股 "
                f"成本:{float(p.get('avg_cost',0)):.2f} "
                f"现价:{float(p.get('current_price',0)):.2f} "
                f"盈亏:{pnl_pct:.2f}% {alert}"
            )
            pos_lines.append(line)
        pos_text = "\n".join(pos_lines) if pos_lines else "空仓"
        current_count = len([p for p in positions if p.get('quantity', 0) > 0])

        # 新推荐（带级别和理由）
        rec_lines = []
        for i, r in enumerate(new_recs[:max_positions], 1):
            code = r.get('code', r.get('stock_code', '?'))
            name = r.get('name', r.get('stock_name', '?'))
            price = r.get('price', r.get('current_price', 0))
            level = r.get('recommendation_level', r.get('level', ''))
            score = r.get('score', r.get('total_score', r.get('signal_score', '')))
            reasons = r.get('reasons', [])
            reason_str = "；".join(reasons[:2]) if reasons else ""
            rec_lines.append(
                f"{i}. {name}({code}) 价:{price} 级别:{level} 评分:{score}"
                + (f" [{reason_str}]" if reason_str else "")
            )
        rec_text = "\n".join(rec_lines) if rec_lines else "暂无新推荐"

        strategy_names = {
            "dragon_head": "龙头战法", "sentiment": "情绪战法",
            "event_driven": "事件驱动", "breakthrough": "突破战法",
            "volume_price": "量价关系", "moving_average": "均线战法",
            "overnight": "隔夜施工法", "northbound": "北向资金",
            "trend_momentum": "趋势动量", "combined": "综合战法",
        }
        strategy_name = strategy_names.get(strategy, strategy)

        # 深度提取前日复盘（mistakes / improvements / next_day_actions）
        review_section = ""
        if last_review:
            review_date = last_review.get('trading_date', '')
            overall_score = last_review.get('overall_score', '?')
            next_day_actions = last_review.get('next_day_actions', [])
            mistakes = (
                last_review.get('mistakes') or
                last_review.get('issues') or
                last_review.get('problems') or []
            )
            improvements = (
                last_review.get('improvements') or
                last_review.get('suggestions') or
                last_review.get('lessons') or []
            )
            review_summary = (
                last_review.get('summary') or
                last_review.get('review_summary') or ''
            )

            review_section = (
                f"\n【前日复盘 {review_date} | 综合评分:{overall_score}/100】\n"
            )
            if review_summary:
                review_section += f"  摘要: {str(review_summary)[:200]}\n"
            if mistakes:
                items = mistakes if isinstance(mistakes, list) else [mistakes]
                review_section += "  ⚠️ 昨日失误:\n"
                for m in items[:3]:
                    review_section += f"    - {m}\n"
            if improvements:
                items = improvements if isinstance(improvements, list) else [improvements]
                review_section += "  💡 改进方向:\n"
                for m in items[:3]:
                    review_section += f"    - {m}\n"
            if next_day_actions:
                items = next_day_actions if isinstance(next_day_actions, list) else [next_day_actions]
                review_section += "  ✅ 今日应执行操作（优先级最高）:\n"
                for a in items[:5]:
                    review_section += f"    - {a}\n"

        prompt = (
            f"战法:{strategy_name} | 持仓上限:{max_positions}只\n"
            f"总资产:{float(portfolio.get('total_asset',0)):.0f}元 "
            f"可用:{float(portfolio.get('available_cash',0)):.0f}元 "
            f"累计收益:{float(portfolio.get('total_profit_pct',0)):.2f}%\n"
            f"持仓({current_count}/{max_positions}):\n{pos_text}\n"
            f"{review_section}\n"
            f"今日推荐:\n{rec_text}\n\n"
            f"决策要点:\n"
            f"1. 【优先】执行前日复盘'今日应执行操作'（如有）\n"
            f"2. 避免重复昨日失误\n"
            f"3. 持仓上限{max_positions}只，买新必先卖旧\n"
            f"4. 亏损≥5%系统已硬性止损，LLM无需重复设置\n"
            f"5. T+1规则：当天买入当天不能卖出\n"
            f"6. 若大盘环境差或复盘建议观望，可以选择不操作（trade_today=false）\n"
        )
        if today_bought_codes:
            codes_str = ', '.join(today_bought_codes)
            prompt += f"⚠️ T+1约束: 今天已买入以下股票今天不能卖出: {codes_str}\n"
        prompt += (
            "\n请给出交易决策，返回JSON（不要```标记）：\n"
            '{"decisions":[{"stock_code":"代码","stock_name":"名称",'
            '"action":"buy/sell/hold","quantity":股数(100整数倍),'
            '"reason":"决策理由，基于复盘的请标注"}],'
            '"summary":"整体决策思路（80字），是否参考复盘及如何改进",'
            '"trade_today":true或false}'
            "\n说明: trade_today=false表示今日不宜操作，所有action=hold"
        )
        return prompt

    # ==================== 解析与校验 ====================

    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """解析 LLM JSON 响应"""
        if not response:
            return None
        try:
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse LLM JSON", error=str(e),
                           response=response[:200])
            return None

    def _validate_allocations(self, result: Dict, capital: float,
                               stocks: List[Dict]) -> Dict:
        """验证并修正资金分配"""
        allocations = result.get("allocations", [])
        total_weight = sum(a.get("weight", 0) for a in allocations)
        cash_reserve = result.get("cash_reserve", 10)

        if total_weight + cash_reserve > 100:
            # 按比例调整
            factor = (100 - cash_reserve) / total_weight if total_weight > 0 else 0
            for a in allocations:
                a["weight"] = round(a["weight"] * factor, 2)

        # 确保 quantity 是 100 的整数倍
        for a in allocations:
            price = 0
            for s in stocks:
                code = s.get("code", s.get("stock_code", ""))
                if code == a.get("stock_code"):
                    price = float(s.get("price", s.get("current_price", 0)))
                    break

            amount = capital * a["weight"] / 100
            a["amount"] = round(amount, 2)

            if price > 0:
                qty = int(amount / price / 100) * 100
                a["quantity"] = max(qty, 100)
            else:
                a["quantity"] = a.get("quantity", 100)

        result["allocations"] = allocations
        return result

    def _fallback_allocation(self, stocks: List[Dict],
                              capital: float) -> Dict:
        """降级方案：均匀分配（最多 MAX_POSITIONS 只）"""
        top = stocks[:MAX_POSITIONS]
        if not top:
            return {"allocations": [], "cash_reserve": 100, "summary": "无可分配股票"}

        cash_pct = 20
        per_stock = (100 - cash_pct) / len(top)
        allocations = []

        for s in top:
            code = s.get("code", s.get("stock_code", ""))
            name = s.get("name", s.get("stock_name", ""))
            price = float(s.get("price", s.get("current_price", 0)))
            amount = capital * per_stock / 100
            qty = int(amount / price / 100) * 100 if price > 0 else 100

            allocations.append({
                "stock_code": code,
                "stock_name": name,
                "weight": round(per_stock, 2),
                "amount": round(amount, 2),
                "quantity": max(qty, 100),
                "reason": "均匀分配(LLM降级方案)"
            })

        return {
            "allocations": allocations,
            "cash_reserve": cash_pct,
            "summary": f"LLM不可用，均匀分配到{len(top)}只股票，保留{cash_pct}%现金"
        }

    # ==================== 跟投建议生成 ====================

    async def generate_follow_recommendations(
        self,
        positions: List[Dict],
        recent_trades: List[Dict],
        new_recommendations: List[Dict],
        portfolio_info: Dict,
        strategy_type: str,
        session_type: str = "morning",
    ) -> Dict[str, Any]:
        """
        根据模拟交易结果，生成5只跟投建议股票

        Args:
            positions: 当前持仓
            recent_trades: 当日最新交易记录
            new_recommendations: 最新推荐列表
            portfolio_info: 组合信息
            strategy_type: 策略类型
            session_type: morning/afternoon
        """
        strategy_names = {
            "dragon_head": "龙头战法",
            "sentiment": "情绪战法",
            "event_driven": "事件驱动",
            "breakthrough": "突破战法",
            "volume_price": "量价关系",
            "overnight": "隔夜施工法",
            "moving_average": "均线战法",
            "combined": "综合战法",
        }
        # 每种战法的典型持有周期特征
        strategy_holding_hints = {
            "dragon_head": "龙头战法属于超短线，持有周期通常1-3天，快进快出",
            "sentiment": "情绪战法以市场情绪为主，队情绪高涨1-2天即可，情绪低迷可持有3-5天等待反转",
            "event_driven": "事件驱动根据事件发酵周期而定，短期事件2-3天，重大政策事件5-10天",
            "breakthrough": "突破战法要看突破后的跟进力度，强势突破持2-3天，弱势突破可持5-7天",
            "volume_price": "量价战法根据放量程度，缩量上涨持3-5天，放量突破持1-3天",
            "overnight": "隔夜施工法是超短线，当天尾盘买入，次日开盘卖出，持有一夜",
            "moving_average": "均线战法偏中线，跑在均线上可持有5-10天，跌破均线则离场",
            "combined": "综合战法多战法共振，根据主导战法决定持有周期2-7天",
        }
        strategy_cn = strategy_names.get(strategy_type, strategy_type)
        holding_hint = strategy_holding_hints.get(strategy_type, "持有周期根据个股情况动态调整")

        # 构建持仓摘要
        pos_text = "无持仓"
        if positions:
            pos_lines = []
            for p in positions:
                if p.get("quantity", 0) > 0:
                    pct = p.get("profit_pct", 0)
                    pos_lines.append(
                        f"  {p.get('stock_code','')} {p.get('stock_name','')} "
                        f"持仓{p.get('quantity',0)}股 均价{p.get('avg_cost',0):.2f} "
                        f"现价{p.get('current_price',0):.2f} 盈亏{pct:.2f}%"
                    )
            pos_text = "\n".join(pos_lines) if pos_lines else "无持仓"

        # 构建当日交易摘要
        trade_text = "今日无交易"
        if recent_trades:
            trade_lines = []
            for t in recent_trades:
                direction = "买入" if t.get("direction") == "buy" else "卖出"
                trade_lines.append(
                    f"  {direction} {t.get('stock_code','')} {t.get('stock_name','')} "
                    f"{t.get('quantity',0)}股 @{t.get('price',0):.2f} "
                    f"理由: {(t.get('reason','') or '')[:60]}"
                )
            trade_text = "\n".join(trade_lines)

        # 构建推荐摘要
        rec_text = "无推荐"
        if new_recommendations:
            rec_lines = []
            for r in new_recommendations[:10]:
                code = r.get("code", r.get("stock_code", ""))
                name = r.get("name", r.get("stock_name", ""))
                price = r.get("price", r.get("current_price", 0))
                score = r.get("score", r.get("total_score", ""))
                rec_lines.append(f"  {code} {name} 价格{price} 评分{score}")
            rec_text = "\n".join(rec_lines)

        total_asset = portfolio_info.get("total_asset", 0)
        total_profit_pct = portfolio_info.get("total_profit_pct", 0)

        prompt = (
            f"策略:{strategy_cn} 总资产:{total_asset:.0f}元 收益率:{total_profit_pct:.2f}%\n"
            f"持仓:\n{pos_text}\n"
            f"今日交易:\n{trade_text}\n"
            f"推荐池:\n{rec_text}\n\n"
            f"【战法持有周期特征】{holding_hint}\n\n"
            f"从持仓和推荐中选5只最佳跟投股，返回JSON(不要```标记):\n"
            f'{{"stocks":[{{"stock_code":"代码","stock_name":"名称","current_price":价格,'
            f'"target_price":目标价,"stop_loss_price":止损价,"position_pct":仓位占比,'
            f'"reason":"50字跟投理由","risk_level":"低/中/高",'
            f'"expected_return":"如8.5%","holding_period":"根据战法和个股动态配置如1-2天/3-5天/5-10天"}}],'
            f'"market_overview":"30字大盘概述",'
            f'"strategy_summary":"30字策略总结",'
            f'"risk_warning":"风险提示","confidence_score":1-100}}\n'
            f"注意:\n"
            f"1. 优先选当日有交易的股票和盈利持仓，给出具体目标价和止损价\n"
            f"2. 【次日盈利导向】所有推荐必须是次日有上涨空间的，不推荐已大涨透支的股票\n"
            f"3. 【T+1规则】当天买入的股票当天不能卖出，建议持有期至少1天以上\n"
            f"4. target_price应反映次日或短期内的合理目标价位\n"
            f"5. 【重要】holding_period必须根据{strategy_cn}的特点和每只股票的具体情况动态配置，不要全部写成3-5天"
        )

        messages = [
            {"role": "system", "content": f"A股{strategy_cn}投资顾问，从模拟交易结果提炼5只跟投建议，只返回JSON。"},
            {"role": "user", "content": prompt},
        ]

        try:
            response = await self._chat(messages, temperature=0.3, max_tokens=4000)
            if not response:
                logger.warning("Follow recommendation LLM returned empty")
                return self._fallback_follow(positions, new_recommendations, strategy_type)

            result = self._parse_json(response)
            if result and "stocks" in result:
                logger.info("Follow recommendations generated",
                           strategy=strategy_type, count=len(result["stocks"]))
                return result
            else:
                logger.warning("Follow recommendation parse failed")
                return self._fallback_follow(positions, new_recommendations, strategy_type)

        except Exception as e:
            logger.error("Follow recommendation LLM failed", error=str(e))
            return self._fallback_follow(positions, new_recommendations, strategy_type)

    def _fallback_follow(self, positions: List[Dict],
                         recommendations: List[Dict],
                         strategy_type: str = "") -> Dict:
        """降级方案: 从持仓和推荐中直接选取"""
        # 根据战法类型配置默认持有周期
        default_periods = {
            "dragon_head": "1-3天",
            "sentiment": "2-4天",
            "event_driven": "3-7天",
            "breakthrough": "2-5天",
            "volume_price": "2-5天",
            "overnight": "1天",
            "moving_average": "5-10天",
            "combined": "2-5天",
        }
        default_period = default_periods.get(strategy_type, "2-5天")

        stocks = []

        # 优先选择盈利的持仓
        for p in sorted(positions, key=lambda x: x.get("profit_pct", 0), reverse=True):
            if p.get("quantity", 0) > 0 and len(stocks) < 5:
                stocks.append({
                    "stock_code": p.get("stock_code", ""),
                    "stock_name": p.get("stock_name", ""),
                    "current_price": float(p.get("current_price", 0)),
                    "target_price": float(p.get("current_price", 0)) * 1.08,
                    "stop_loss_price": float(p.get("current_price", 0)) * 0.95,
                    "position_pct": 20,
                    "reason": f"模拟组合持仓标的，当前盈亏{p.get('profit_pct', 0):.2f}%（LLM降级方案）",
                    "risk_level": "中",
                    "expected_return": "5-8%",
                    "holding_period": default_period,
                })

        # 不够5只则从推荐中补充
        for r in recommendations:
            if len(stocks) >= 5:
                break
            code = r.get("code", r.get("stock_code", ""))
            if code and code not in [s["stock_code"] for s in stocks]:
                price = float(r.get("price", r.get("current_price", 0)))
                stocks.append({
                    "stock_code": code,
                    "stock_name": r.get("name", r.get("stock_name", "")),
                    "current_price": price,
                    "target_price": price * 1.08,
                    "stop_loss_price": price * 0.95,
                    "position_pct": 20,
                    "reason": f"策略推荐标的（LLM降级方案）",
                    "risk_level": "中",
                    "expected_return": "5-8%",
                    "holding_period": default_period,
                })

        return {
            "stocks": stocks[:5],
            "market_overview": "LLM不可用，请自行判断大盘行情",
            "strategy_summary": "LLM降级方案：从持仓和推荐中直接选取",
            "risk_warning": "此为降级方案，建议谨慎跟投",
            "confidence_score": 30,
        }


# 全局单例
portfolio_llm = PortfolioLLM()
