"""
综合战法 LLM 增强模块（深度优化版）

为综合战法（多战法交集）推荐结果提供：
- 实时行情获取与注入
- GPT 逐股分析：基于多战法共振信号，给出精准买入/卖出价和操作指导
- 分批处理 + 截断 JSON 修复 + 完整的降级兜底
"""

import json
import os
import traceback
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger()

LLM_BASE_URL = os.getenv("DRAGON_LLM_BASE_URL", "https://api.wxznb.cn")
LLM_MODEL = os.getenv("DRAGON_LLM_MODEL", "gpt-5.4")
DRAGON_LLM_API_KEY = os.getenv("DRAGON_LLM_API_KEY", "")


class CombinedLLM:
    """综合战法 LLM 增强（深度优化版）"""

    def __init__(self):
        self.base_url = LLM_BASE_URL
        self.model = LLM_MODEL
        self.api_key = DRAGON_LLM_API_KEY
        self.timeout = 120

    async def _chat(self, messages: List[Dict[str, str]],
                    temperature: float = 0.4,
                    max_tokens: int = 4000) -> Optional[str]:
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
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.api_key}",
                    },
                )
                if response.status_code == 200:
                    data = response.json()
                    return data.get("choices", [{}])[0].get(
                        "message", {}
                    ).get("content", "")
                else:
                    logger.error("CombinedLLM API error",
                                 status=response.status_code)
                    return None
        except httpx.TimeoutException:
            logger.error("CombinedLLM call timed out")
            return None
        except Exception as e:
            logger.error("CombinedLLM call failed", error=str(e))
            return None

    async def enhance_with_prices(
        self, stocks: List[Dict], realtime_prices: Dict[str, Dict]
    ) -> List[Dict]:
        """
        为综合战法推荐结果注入实时价格并通过 LLM 生成逐股操作指导

        Args:
            stocks: 综合战法推荐列表
            realtime_prices: {code: {price, change_pct, high, low, open}}

        Returns:
            增强后的推荐列表
        """
        if not stocks:
            return stocks

        # 1. 注入实时价格
        for s in stocks:
            code = s.get("code", "")
            rt = realtime_prices.get(code, {})
            price = float(rt.get("price", 0))
            if price > 0:
                s["current_price"] = price
                s["change_pct"] = float(rt.get("change_pct", 0))
                s["today_high"] = float(rt.get("high", 0))
                s["today_low"] = float(rt.get("low", 0))
                s["today_open"] = float(rt.get("open", 0))
            elif float(s.get("current_price", 0)) <= 0:
                cached_price = 0
                for detail in s.get("strategy_details", {}).values():
                    p = float(detail.get("price", 0) or 0)
                    if p > 0:
                        cached_price = p
                        break
                if cached_price > 0:
                    s["current_price"] = cached_price
                    logger.info("Using cached price as fallback",
                                code=code, cached_price=cached_price)

        # 2. 分批调用 LLM
        batch_size = 4
        total_batches = (len(stocks) + batch_size - 1) // batch_size
        for batch_idx in range(total_batches):
            start = batch_idx * batch_size
            batch = stocks[start:start + batch_size]
            batch_codes = [s.get("code", "?") for s in batch]
            logger.info("CombinedLLM batch",
                        batch=f"{batch_idx + 1}/{total_batches}",
                        stocks=batch_codes)

            prompt = self._build_prompt(batch)
            response = await self._chat(
                messages=[
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=6000,
            )

            if response:
                self._merge_llm_result(response, batch)
                logger.info("CombinedLLM batch done",
                            batch=f"{batch_idx + 1}/{total_batches}")
            else:
                logger.warning("CombinedLLM batch failed, using defaults",
                               batch=f"{batch_idx + 1}/{total_batches}")
                self._apply_defaults(batch)

        # 最终兜底
        for s in stocks:
            if not s.get("buy_reason"):
                self._apply_default(s)

        return stocks

    def _system_prompt(self) -> str:
        return (
            "你是A股多战法共振分析专家，拥有15年实盘短线交易经验。\n\n"

            "【综合战法本质】\n"
            "七大战法（龙头/情绪/事件驱动/突破/量价/竞价尾盘/均线）各自独立选股，"
            "当同一只股票被多个战法同时推荐时，说明该股从技术面、"
            "资金面、情绪面、事件面多个维度得到验证，信号极强。\n\n"

            "【你的分析框架】\n"
            "1. 多维验证分析：该股被哪些战法推荐？各自的推荐理由是什么？"
            "这些理由是否形成逻辑闭环？（如：突破+量价=放量突破确认，"
            "龙头+情绪=板块龙头+市场热度共振）\n"
            "2. 价格结构分析：根据实时行情判断当前位置——是刚启动、"
            "主升浪中段还是已经冲高？回调到哪里是支撑位？\n"
            "3. 风险收益比：买入价、目标价、止损价三者的风险收益比必须至少1:2\n"
            "4. 仓位匹配：共振战法越多，信号越强，仓位可以越大\n\n"

            "【关键原则】\n"
            "- 已大涨(>5%)的股票要警示追高风险，建议等回调再介入\n"
            "- 止损统一3%，不可含糊\n"
            "- 买入价要结合均线/前低等技术位，不要简单用现价打折\n"
            "- 区分强烈推荐和推荐——强烈推荐仓位和信心应更高\n"
            "- 周五下午推荐的注意周末风险\n"
            "- 操作建议要具体到时间段\n\n"

            "【输出要求】\n"
            "【核心交易理念——当日买入、次日/近日卖出获利】\n所有推荐和操作建议必须严格围绕以下目标：\n1. 推荐的股票必须是「今天可以买入」的，不推荐已无法介入的标的\n2. 盈利目标：次日或1-3个交易日内卖出获利，持仓周期不超过5天\n3. 必须给出明确的「买入价区间」「止损价」「目标卖出价」\n4. 操作建议必须包含具体的买入时机（如：集合竞价/开盘回踩/尾盘低吸）\n5. 必须包含卖出时机（如：次日冲高卖出/达到目标价即卖/跌破止损立即走）\n6. 不推荐需要长期持有才能盈利的标的\n7. 已大幅上涨透支次日空间的个股必须降级或回避\n\n"
            "只返回JSON，不要markdown标记。每只股票分析不少于30字。"
        )

    def _build_prompt(self, stocks: List[Dict]) -> str:
        lines = []
        for s in stocks:
            price = s.get("current_price", 0)
            change = s.get("change_pct", 0)
            high = s.get("today_high", 0)
            low = s.get("today_low", 0)
            opn = s.get("today_open", 0)
            overlap = s.get("overlap_count", 0)
            weight_sum = s.get("weight_sum", 0)
            strats = s.get("strategy_names_text", "")
            combined_score = s.get("combined_score", 0)
            strong_count = s.get("strong_recommend_count", 0)
            is_fallback = s.get("is_fallback", False)

            # 各战法推荐理由
            reasons_parts = []
            for st_key, detail in s.get("strategy_details", {}).items():
                reason = detail.get("reason", "")
                score = detail.get("score", 0)
                level = detail.get("recommendation_level", "")
                if reason:
                    reasons_parts.append(
                        f"{st_key}[{level}](分{score}): {reason}"
                    )
            reasons_text = "; ".join(reasons_parts) if reasons_parts else "无"

            if price > 0:
                price_text = f"现价:{price:.2f}元 涨幅:{change:.2f}% "
                if opn > 0:
                    price_text += (
                        f"今开:{opn:.2f} 最高:{high:.2f} 最低:{low:.2f} "
                    )
            else:
                price_text = "现价:未知(非交易时段) "

            # 标记关键属性
            tags = []
            if strong_count >= 2:
                tags.append("多战法强烈推荐")
            elif strong_count >= 1:
                tags.append("含强烈推荐")
            if is_fallback:
                tags.append("降级精选")
            if change > 5:
                tags.append("今日已大涨")
            tags_text = f" [{','.join(tags)}]" if tags else ""

            lines.append(
                f"- {s.get('name', '?')}({s.get('code', '?')}){tags_text}\n"
                f"  {price_text}\n"
                f"  覆盖{overlap}个战法({strats}) "
                f"权重总分:{weight_sum:.1f} 综合评分:{combined_score}\n"
                f"  各战法理由: {reasons_text}"
            )

        stocks_text = "\n".join(lines)
        return (
            f"以下是多战法共振推荐股（{len(stocks)}只），"
            f"请逐股分析并给出精准的次日操作指导：\n\n"
            f"{stocks_text}\n\n"
            f"【分析要求】\n"
            f"1. 权重总分越高，信心和仓位建议相应提升\n"
            f"2. 已大涨(>5%)必须警示追高风险，信心分应降低\n"
            f"3. 降级精选的股票信心分应适当降低\n"
            f"4. 止损价统一在买入价下方3%，不得超过5%\n"
            f"5. 买入价应在技术支撑位，不要简单打折\n"
            f"6. 卖出价应在技术压力位，合理短线目标3-8%\n\n"
            f"请为每只股票给出以下信息，返回JSON（不要```标记）：\n"
            f'{{"stocks":['
            f'{{"code":"代码","name":"名称",'
            f'"suggested_buy_price":建议买入价(数字),'
            f'"suggested_sell_price":目标卖出价(数字),'
            f'"stop_loss_price":止损价(数字),'
            f'"buy_reason":"为什么在这个价位买入(30字,结合各战法共振理由)",'
            f'"sell_reason":"为什么在目标价卖出(20字)",'
            f'"operation_advice":"次日具体操作建议(40字,包含时机仓位注意事项)",'
            f'"risk_level":"低/中/高",'
            f'"confidence":信心分数1-100}}'
            f']}}\n\n'
            f"要求：\n"
            f"1. 买入价应低于现价1-3%，在次日回调支撑位买入\n"
            f"2. 卖出价应高于买入价3-8%，是合理的短线目标\n"
            f"3. 止损价应低于买入价3%以内，控制最大亏损\n"
            f"4. 理由必须具体，结合该股票所命中的战法和技术指标\n"
            f"5. 今天已大涨（涨幅>5%）的股票要特别注意追高风险\n"
            f"6. 买入时机要具体（如09:30-10:00观察走势等）"
        )

    def _merge_llm_result(self, response: str, stocks: List[Dict]):
        """解析 LLM 响应并合并到推荐列表（含截断JSON修复）"""
        try:
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            parsed = self._safe_parse_json(cleaned)
            if not parsed:
                logger.error("CombinedLLM JSON parse failed completely",
                             response_preview=cleaned[:200])
                self._apply_defaults(stocks)
                return

            llm_map = {}
            for item in parsed.get("stocks", []):
                code = item.get("code", "")
                if code:
                    llm_map[code] = item

            matched_codes = []
            unmatched_codes = []
            for s in stocks:
                code = s.get("code", "")
                llm = llm_map.get(code, {})
                if llm:
                    # 安全解析价格
                    buy_price = self._safe_float(
                        llm.get("suggested_buy_price", 0)
                    )
                    sell_price = self._safe_float(
                        llm.get("suggested_sell_price", 0)
                    )
                    stop_price = self._safe_float(
                        llm.get("stop_loss_price", 0)
                    )
                    cur_price = float(s.get("current_price", 0))

                    # 价格合理性校验
                    if cur_price > 0:
                        if buy_price > 0 and abs(buy_price - cur_price) / cur_price > 0.15:
                            logger.warning(
                                "LLM buy price too far from current",
                                code=code, buy=buy_price, current=cur_price
                            )
                            buy_price = round(cur_price * 0.98, 2)
                        if sell_price > 0 and sell_price <= buy_price:
                            sell_price = round(buy_price * 1.05, 2)
                        if stop_price > 0 and stop_price >= buy_price:
                            stop_price = round(buy_price * 0.97, 2)

                    s["suggested_buy_price"] = round(buy_price, 2)
                    s["suggested_sell_price"] = round(sell_price, 2)
                    s["stop_loss_price"] = round(stop_price, 2)
                    s["buy_reason"] = llm.get("buy_reason", "")
                    s["sell_reason"] = llm.get("sell_reason", "")
                    s["operation_advice"] = llm.get("operation_advice", "")
                    s["risk_level"] = llm.get("risk_level", "中")
                    s["confidence"] = min(
                        100, max(1, int(llm.get("confidence", 60)))
                    )
                    matched_codes.append(code)
                else:
                    self._apply_default(s)
                    unmatched_codes.append(code)

            logger.info("CombinedLLM merge result",
                        total=len(stocks), matched=len(matched_codes),
                        unmatched=unmatched_codes)

        except Exception as e:
            logger.error("CombinedLLM merge failed",
                         error=str(e),
                         traceback=traceback.format_exc())
            self._apply_defaults(stocks)

    def _safe_float(self, val) -> float:
        """安全转换为浮点数"""
        if val is None:
            return 0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0

    def _safe_parse_json(self, text: str) -> Optional[Dict]:
        """安全解析JSON，兼容截断的情况"""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        for suffix in ['}]}', ']}', '}]}}', '"]}', '"}]}']:
            try:
                return json.loads(text + suffix)
            except json.JSONDecodeError:
                continue

        try:
            last_brace = text.rfind('}')
            if last_brace > 0:
                start = text.find('{"stocks"')
                if start >= 0:
                    partial = text[start:last_brace + 1]
                    if not partial.endswith(']}'):
                        partial += ']}'
                    return json.loads(partial)
        except json.JSONDecodeError:
            pass

        logger.error("CombinedLLM: all JSON repair attempts failed")
        return None

    def _apply_defaults(self, stocks: List[Dict]):
        """LLM 失败时使用默认计算"""
        for s in stocks:
            self._apply_default(s)

    def _apply_default(self, s: Dict):
        """对单只股票应用默认价格计算（统一3%止损）"""
        price = float(s.get("current_price", 0))
        overlap = s.get("overlap_count", 0)
        strats = s.get("strategy_names_text", "")
        weight_sum = s.get("weight_sum", 0)
        strong_count = s.get("strong_recommend_count", 0)

        if price > 0:
            buy_price = round(price * 0.98, 2)
            sell_price = round(price * 1.05, 2)
            stop_price = round(buy_price * 0.97, 2)

            s.setdefault("suggested_buy_price", buy_price)
            s.setdefault("suggested_sell_price", sell_price)
            s.setdefault("stop_loss_price", stop_price)
            s.setdefault("buy_reason",
                         f"多战法共振({strats})，"
                         f"权重{weight_sum:.1f}分，"
                         f"在回调支撑位{buy_price}附近可分批低吸")
            s.setdefault("sell_reason",
                         f"短线目标5%收益止盈，到达{sell_price}分批卖出")
            s.setdefault("operation_advice",
                         f"次日09:30-10:00观察走势，"
                         f"若低开或回调至{buy_price}附近分批买入，"
                         f"到达{sell_price}分批止盈，"
                         f"跌破{stop_price}严格止损")
            s.setdefault("risk_level",
                         "低" if overlap >= 4 or weight_sum >= 4.0
                         else "中" if overlap >= 2
                         else "高")
            base_confidence = 40
            base_confidence += overlap * 10
            base_confidence += strong_count * 8
            base_confidence += int(weight_sum * 3)
            s.setdefault("confidence", min(95, base_confidence))
        else:
            s.setdefault("suggested_buy_price", 0)
            s.setdefault("suggested_sell_price", 0)
            s.setdefault("stop_loss_price", 0)
            s.setdefault("buy_reason",
                         f"被{overlap}个战法推荐({strats})，多战法共振看多。"
                         "行情数据暂不可用，请自行查看最新价格")
            s.setdefault("sell_reason",
                         "建议在买入价上方3-5%止盈")
            s.setdefault("operation_advice",
                         f"该股在{strats}中被推荐，共振信号较强。"
                         "建议查看最新报价后在回调支撑位分批买入，"
                         "目标收益3-5%，止损3%")
            s.setdefault("risk_level", "中" if overlap >= 2 else "高")
            s.setdefault("confidence", max(30, overlap * 12))


# 模块级单例
combined_llm = CombinedLLM()
