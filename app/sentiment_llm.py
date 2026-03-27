"""
情绪战法 LLM 增强模块

通过 GPT-5.2 对量化情绪指标进行深度解读：
1. 情绪周期判断   - 判断当前处于哪个阶段（冰点/修复/升温/高潮/退潮）
2. 市场解读报告   - 结合指标数据给出专业的市场情绪分析
3. 操作建议       - 根据情绪阶段给出短线仓位和操作建议
4. 风险预警       - 识别极端情绪状态下的风险信号
"""

import json
import os
import traceback
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger()

# GPT-5.2 API 配置（复用龙头战法的配置）
SENTIMENT_LLM_BASE_URL = os.getenv("DRAGON_LLM_BASE_URL", "https://api.wxznb.cn")
SENTIMENT_LLM_MODEL = os.getenv("DRAGON_LLM_MODEL", "gpt-5.4")
SENTIMENT_LLM_PROVIDER = os.getenv("DRAGON_LLM_PROVIDER", "openai")
DRAGON_LLM_API_KEY = os.getenv("DRAGON_LLM_API_KEY", "")


class SentimentLLM:
    """情绪战法 LLM 增强客户端"""

    def __init__(self):
        self.base_url = SENTIMENT_LLM_BASE_URL
        self.model = SENTIMENT_LLM_MODEL
        self.provider = SENTIMENT_LLM_PROVIDER
        self.api_key = DRAGON_LLM_API_KEY
        self.timeout = 60  # 60秒超时（情绪分析数据量不大，应较快返回）

    async def _chat(self, messages: List[Dict[str, str]], temperature: float = 0.5,
                    max_tokens: int = 1500) -> Optional[str]:
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
                    logger.error("Sentiment LLM API error",
                                 status=response.status_code,
                                 body=response.text[:500])
                    return None

        except httpx.TimeoutException:
            logger.error("Sentiment LLM call timed out", timeout=self.timeout)
            return None
        except Exception as e:
            logger.error("Sentiment LLM call failed", error=str(e))
            return None

    async def enhance_sentiment(
        self,
        today_snapshot: Dict[str, Any],
        recent_history: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        GPT-5.2 情绪深度分析

        Args:
            today_snapshot: 今日情绪快照（含三大指标 + Z-Score + 综合情绪）
            recent_history: 最近 5-7 日的历史情绪数据（用于趋势判断）

        Returns:
            LLM 增强结果字典，失败返回 None
        """
        max_retry = 2

        for attempt in range(1, max_retry + 1):
            try:
                prompt = self._build_prompt(today_snapshot, recent_history)

                logger.info("Calling GPT-5.2 for sentiment analysis", attempt=attempt)

                response = await self._chat(
                    messages=[
                        {"role": "system", "content": self._get_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.4,
                    max_tokens=2500,
                )

                if not response:
                    logger.warning("GPT-5.2 returned empty response", attempt=attempt)
                    continue

                result = self._parse_response(response)
                if result:
                    logger.info("Sentiment LLM enhancement completed",
                                phase=result.get("emotion_phase", ""))
                    return result

            except Exception as e:
                logger.error("Sentiment LLM attempt failed",
                             attempt=attempt, error=str(e))

        logger.warning("All sentiment LLM attempts failed")
        return None

    async def enhance_stock_picks(
        self,
        stocks: List[Dict[str, Any]],
        emotion_phase: str,
        today_snapshot: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        GPT-5.2 增强股票推荐

        给每只候选股生成推荐理由、评级、风险提示

        Args:
            stocks: 候选股票列表
            emotion_phase: 情绪阶段
            today_snapshot: 今日情绪快照

        Returns:
            增强后的股票列表
        """
        if not stocks:
            return stocks

        try:
            prompt = self._build_stock_prompt(stocks, emotion_phase, today_snapshot)

            logger.info("Calling GPT-5.2 for stock pick enhancement",
                         count=len(stocks))

            response = await self._chat(
                messages=[
                    {"role": "system", "content": (
                        "你是A股短线情绪分析师，精通情绪周期理论。\n"
                        "根据情绪阶段和个股数据给出推荐评级和理由。\n"
                        "核心原则：\n"
                        "1. 推荐必须以「当天买入、次日或1-3天内卖出获利」为唯一目标\n"
                        "2. 封板质量（封单比=封单金额/成交额）>50%的才视为强封\n"
                        "3. 退潮期只推荐'关注'级别，不给'强烈推荐'\n"
                        "4. 冰点期重点推荐超跌反弹且放量的个股\n"
                        "只返回JSON数组。"
                    )},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.4,
                max_tokens=3000,
            )

            if not response:
                logger.warning("GPT-5.2 stock enhancement returned empty")
                return stocks

            enhanced = self._parse_stock_response(response, stocks)
            if enhanced:
                logger.info("Stock pick enhancement completed",
                             enhanced_count=len(enhanced))
                return enhanced

        except Exception as e:
            logger.error("Stock pick LLM enhancement failed", error=str(e))

        return stocks

    # ========================= 情绪分析 prompt =========================

    def _get_system_prompt(self) -> str:
        """专业情绪分析系统提示词"""
        return (
            "你是一名经验丰富的A股短线情绪分析师，专注于通过量化情绪指标判断市场情绪周期。\n\n"
            "【情绪周期理论】\n"
            "A股短线情绪遵循五阶段循环：冰点→修复→升温→高潮→退潮→冰点\n"
            "- 冰点：涨停家数骤降、溢价率转负、晋级率<10%，恐慌出清；\n"
            "- 修复：溢价率由负转正、涨停数回升但未放量，试探性买入；\n"
            "- 升温：涨停持续增加、晋级率>25%、连板高度提升，量价齐升；\n"
            "- 高潮：涨停>80家、晋级率>40%、多只5板+，群体亢奋但分歧将现；\n"
            "- 退潮：头部连板断板、晋级率骤降、跌停增加，主力出货。\n\n"
            "【判断原则】\n"
            "1. 阶段判断必须综合三大指标趋势，不能只看单一数值\n"
            "2. 关注指标的变化速度（加速/减速），而非绝对值\n"
            "3. 冰点出手要果断（左侧交易），高潮退出要坚决（不贪最后一个板）\n"
            "4. 仓位建议必须与情绪阶段严格匹配\n\n"
            "【核心交易理念——当日买入、次日/近日卖出获利】\n所有推荐和操作建议必须严格围绕以下目标：\n1. 推荐的股票必须是「今天可以买入」的，不推荐已无法介入的标的\n2. 盈利目标：次日或1-3个交易日内卖出获利，持仓周期不超过5天\n3. 必须给出明确的「买入价区间」「止损价」「目标卖出价」\n4. 操作建议必须包含具体的买入时机（如：集合竞价/开盘回踩/尾盘低吸）\n5. 必须包含卖出时机（如：次日冲高卖出/达到目标价即卖/跌破止损立即走）\n6. 不推荐需要长期持有才能盈利的标的\n7. 已大幅上涨透支次日空间的个股必须降级或回避\n\n"
            "只返回JSON，不加```标记。"
        )

    def _build_prompt(self, today: Dict, history: List[Dict]) -> str:
        """构建分析提示词（增强版 - 包含变化速度和连板高度）"""
        today_text = (
            f"涨停{today.get('limit_up_count', 0)}家, "
            f"跌停{today.get('limit_down_count', 0)}家, "
            f"溢价率{today.get('limit_up_premium', 0):.2f}%, "
            f"涨跌停比{(today.get('up_down_ratio', 0) * 100):.1f}%, "
            f"晋级率{(today.get('promotion_rate', 0) * 100):.1f}%, "
            f"综合情绪{today.get('composite_sentiment', 0):.2f}, "
            f"分位数{(today.get('sentiment_percentile', 0) * 100):.0f}%"
        )

        # 计算情绪变化速度（与昨日对比）
        change_text = ""
        if history and len(history) >= 1:
            last = history[-1]
            prev_composite = float(last.get('composite_sentiment', 0))
            curr_composite = float(today.get('composite_sentiment', 0))
            delta = curr_composite - prev_composite
            direction = "↑加速" if delta > 0.3 else "↓减速" if delta < -0.3 else "→持平"
            prev_promotion = float(last.get('promotion_rate', 0)) * 100
            curr_promotion = float(today.get('promotion_rate', 0)) * 100
            promo_delta = curr_promotion - prev_promotion
            change_text = (
                f"\n情绪变化: 综合情绪{direction}(Δ{delta:+.2f}), "
                f"晋级率变化{promo_delta:+.1f}%"
            )

        trend_lines = []
        for h in history[-5:]:
            trend_lines.append(
                f"{h.get('trading_date', '?')}: "
                f"涨停{h.get('limit_up_count', 0)} "
                f"跌停{h.get('limit_down_count', 0)} "
                f"溢价{h.get('limit_up_premium', 0):.1f}% "
                f"晋级{(h.get('promotion_rate', 0) * 100):.0f}% "
                f"情绪{h.get('composite_sentiment', 0):.2f}"
            )
        trend_text = "\n".join(trend_lines) if trend_lines else "历史数据不足"

        signal = today.get("signal_text", "正常")
        # 连板最高高度
        continuous = today.get('continuous_limit_up_count', 0)

        prompt = (
            f"今日:{today_text}\n"
            f"量化信号:{signal}{change_text}\n"
            f"连板最高高度:{continuous}板\n"
            f"近5日趋势:\n{trend_text}\n\n"
            f"请综合判断当前处于情绪周期的哪个阶段，重点关注指标的变化趋势而非绝对值。\n\n"
            f"返回JSON（不要```标记）：\n"
            f'{{"emotion_phase":"冰点/修复/升温/高潮/退潮 五选一",'
            f'"phase_description":"30字以内描述当前情绪阶段特征和转换方向",'
            f'"market_analysis":"80-120字市场情绪深度分析，必须引用具体数据佐证",'
            f'"position_advice":"建议仓位如20%/50%/80%等",'
            f'"operation_advice":"40字以内具体操作建议（明确买/卖/持有及条件）",'
            f'"risk_points":["风险点1","风险点2"],'
            f'"opportunity_points":["机会点1","机会点2"],'
            f'"next_day_outlook":"20字以内次日展望，含概率判断"}}'
        )

        return prompt

    # ========================= 选股增强 prompt =========================

    def _build_stock_prompt(self, stocks: List[Dict], phase: str,
                            snapshot: Dict) -> str:
        """构建选股增强提示词（增强版 - 含封板质量和次日预期）"""
        lines = []
        for s in stocks[:13]:
            seal = s.get('seal_amount', 0)
            amount = s.get('amount', 0)
            seal_ratio = (seal / amount * 100) if amount > 0 else 0
            line = (
                f"{s.get('stock_code','')} {s.get('stock_name','')} "
                f"涨{s.get('change_pct', 0):.1f}% "
                f"额{s.get('amount', 0) / 1e8:.1f}亿 "
                f"换{s.get('turnover_rate', 0):.1f}% "
                f"连板{s.get('limit_up_days', 0)} "
                f"封单比{seal_ratio:.0f}% "
                f"标签:{s.get('pick_reason_tag', '')}"
            )
            lines.append(line)

        stock_text = "\n".join(lines)

        # 情绪阶段对应的选股原则
        phase_guide = {
            "冰点": "冰点阶段注重超跌反弹潜力，优先选近期跌幅大但今日放量反弹的个股，不追涨停",
            "修复": "修复阶段注重量价配合，优先选率先放量突破的个股，控制仓位",
            "升温": "升温阶段可适当追强，优先选板块龙头和连板股，但要注意不追高位放量滞涨",
            "高潮": "高潮阶段只做最强龙头，封单比>50%的才值得关注，其余观望",
            "退潮": "退潮阶段以防御为主，只选总龙头或大盘蓝筹抗跌股，严控仓位≤30%",
        }
        guide = phase_guide.get(phase, "根据市场情况灵活选股")

        prompt = (
            f"情绪阶段:{phase} 综合情绪:{snapshot.get('composite_sentiment', 0):.2f} "
            f"分位:{(snapshot.get('sentiment_percentile', 0) * 100):.0f}%\n"
            f"选股原则:{guide}\n"
            f"候选股:\n{stock_text}\n\n"
            f"【重要】以次日盈利为导向评级，已涨停透支的个股降级处理。\n"
            f"对每只股返回JSON数组（不要```标记），每只股格式：\n"
            f'{{"code":"股票代码","level":"强烈推荐/推荐/关注 三选一",'
            f'"reason":"25字推荐理由（必须说明次日盈利逻辑和封板质量）",'
            f'"risk":"15字风险提示",'
            f'"operation":"30字操作建议：含今日买入时机+买入价+次日卖出策略+止损价"}}'
        )

        return prompt

    def _parse_stock_response(self, response: str,
                              original: List[Dict]) -> Optional[List[Dict]]:
        """解析选股增强 LLM 返回"""
        try:
            cleaned = self._clean_json(response)
            parsed = json.loads(cleaned)

            if not isinstance(parsed, list):
                parsed = [parsed]

            # 构建 code -> LLM 结果的映射
            llm_map = {}
            for item in parsed:
                code = item.get("code", "")
                if code:
                    llm_map[code] = item

            # 合并到原始数据
            for stock in original:
                code = stock.get("stock_code", "")
                if code in llm_map:
                    llm_item = llm_map[code]
                    stock["recommendation_level"] = llm_item.get("level", "关注")
                    stock["llm_reason"] = llm_item.get("reason", "")
                    stock["llm_risk_warning"] = llm_item.get("risk", "")
                    stock["llm_operation"] = llm_item.get("operation", "")
                else:
                    stock.setdefault("recommendation_level", "关注")
                    stock.setdefault("llm_reason", "")
                    stock.setdefault("llm_risk_warning", "")
                    stock.setdefault("llm_operation", "")

            return original

        except json.JSONDecodeError as e:
            logger.warning("Failed to parse stock LLM JSON", error=str(e))
            return None
        except Exception as e:
            logger.error("Failed to process stock LLM response", error=str(e))
            return None

    # ========================= 通用解析 =========================

    def _clean_json(self, text: str) -> str:
        """清理 LLM 返回的 JSON 文本"""
        cleaned = text.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()

    def _parse_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析情绪分析 LLM 返回的 JSON"""
        try:
            cleaned = self._clean_json(response)
            parsed = json.loads(cleaned)

            result = {
                "emotion_phase": parsed.get("emotion_phase", ""),
                "phase_description": parsed.get("phase_description", ""),
                "market_analysis": parsed.get("market_analysis", ""),
                "position_advice": parsed.get("position_advice", "50%"),
                "operation_advice": parsed.get("operation_advice", ""),
                "risk_points": parsed.get("risk_points", []),
                "opportunity_points": parsed.get("opportunity_points", []),
                "next_day_outlook": parsed.get("next_day_outlook", ""),
            }

            valid_phases = {"冰点", "修复", "升温", "高潮", "退潮"}
            if result["emotion_phase"] not in valid_phases:
                result["emotion_phase"] = "修复"

            return result

        except json.JSONDecodeError as e:
            logger.warning("Failed to parse sentiment LLM JSON", error=str(e))
            return None
        except Exception as e:
            logger.error("Failed to process sentiment LLM response", error=str(e))
            return None


# 全局单例
sentiment_llm = SentimentLLM()
