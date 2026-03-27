"""
四大新战法统一 LLM 增强模块

通过 GPT-5.2 对突破/量价/竞价尾盘/均线战法的推荐结果进行深度分析增强。
共用同一套 LLM 调用逻辑，通过 strategy_name 区分上下文。
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
LLM_PROVIDER = os.getenv("DRAGON_LLM_PROVIDER", "openai")
DRAGON_LLM_API_KEY = os.getenv("DRAGON_LLM_API_KEY", "")


class StrategyLLM:
    """通用战法 LLM 增强客户端"""

    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.base_url = LLM_BASE_URL
        self.model = LLM_MODEL
        self.provider = LLM_PROVIDER
        self.api_key = DRAGON_LLM_API_KEY
        self.timeout = 90

    async def _chat(self, messages: List[Dict[str, str]], temperature: float = 0.7,
                    max_tokens: int = 3000) -> Optional[str]:
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
                    return data.get("choices", [{}])[0].get("message", {}).get("content", "")
                else:
                    logger.error("LLM API error", status=response.status_code)
                    return None
        except httpx.TimeoutException:
            logger.error("LLM call timed out", strategy=self.strategy_name)
            return None
        except Exception as e:
            logger.error("LLM call failed", error=str(e))
            return None

    async def enhance_recommendations(self, stocks: List[Dict], **kwargs) -> Dict[str, Any]:
        """增强推荐结果"""
        if not stocks:
            return {}

        try:
            stocks_info = []
            for s in stocks:
                vol_tag = f" 5日量比:{s.get('vol_ratio_5', 0):.1f}x"
                sustained_tag = f" 持续{s.get('vol_sustained_days', 0)}日" if s.get('vol_sustained_days') else ""
                extreme_tag = " ⚠️天量风险" if s.get('is_extreme_volume') else ""
                score_detail = s.get('score_detail', {})
                detail_str = ""
                if score_detail:
                    detail_str = f" [信号{score_detail.get('signal_base',0)}持续{score_detail.get('sustain',0)}趋势{score_detail.get('trend',0)}]"

                info = (
                    f"排名{s.get('rank', '?')}: {s.get('name', '?')}({s.get('code', '?')}) "
                    f"| 价:{s.get('price', 0)}元 | 涨幅:{s.get('change_pct', 0):.2f}% "
                    f"| 信号:{s.get('signal_type', '未知')}"
                    f"{vol_tag}{sustained_tag}{extreme_tag}"
                    f" | 总分:{s.get('signal_score', 0)}{detail_str}"
                )
                stocks_info.append(info)

            extra_context = ""
            if kwargs.get("strategy_mode"):
                extra_context = f"\n当前模式: {kwargs['strategy_mode']}"

            prompt = (
                f"量价关系战法候选股（已按多维度评分排序）：\n" + "\n".join(stocks_info) + extra_context +
                "\n\n【分析要求】\n"
                "1. 判断量价信号是否真实可靠（量能放大是主力建仓还是出货）\n"
                "2. 带⚠️天量标记的需要评估是否为主力出货，必须降级\n"
                "3. '量价背离'信号必须标记为'回避'\n"
                "4. 给出次日具体介入价格区间和止损位\n\n"
                "返回JSON(不要```标记):"
                '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                '"reasons":["理由1","理由2","理由3"],'
                '"risk_warning":"具体风险提示",'
                '"operation_suggestion":"次日操作：介入区间+止损位+目标位+仓位"}],'
                '"strategy_report":"150-250字量价分析报告，重点分析量能含义和次日操作策略"}'
            )

            response = await self._chat(
                messages=[
                    {"role": "system", "content": (
                        "你是A股量价关系分析专家，拥有15年量价分析经验。\n\n"
                        "【量价战法本质】\n"
                        "成交量是价格的先行指标，通过量能变化可以提前发现主力动向。\n\n"
                        "【信号判断标准】\n"
                        "量价齐升：价格上涨+量能放大 = 趋势确认，最健康的上涨形态\n"
                        "底部放量：长期下跌后突然放量 = 可能是主力建仓，但也可能是换手\n"
                        "缩量回调后放量：洗盘结束重新启动 = 主升浪开始\n"
                        "量价背离：价升量缩 = 动能衰竭，必须回避\n\n"
                        "【天量警示】\n"
                        "成交量达到60日均量3倍以上 = 天量天价，\n"
                        "可能是主力对倒出货，必须警惕\n\n"
                        "【操作原则】\n"
                        "- 量价齐升：可半仓参与，止损设在均线下方3%\n"
                        "- 底部放量：轻仓试探，等确认后加仓\n"
                        "- 缩量反转：可较重仓位，止损在回调低点\n\n"
                        "【核心交易理念——当日买入、次日/近日卖出获利】\n所有推荐和操作建议必须严格围绕以下目标：\n1. 推荐的股票必须是「今天可以买入」的，不推荐已无法介入的标的\n2. 盈利目标：次日或1-3个交易日内卖出获利，持仓周期不超过5天\n3. 必须给出明确的「买入价区间」「止损价」「目标卖出价」\n4. 操作建议必须包含具体的买入时机（如：集合竞价/开盘回踩/尾盘低吸）\n5. 必须包含卖出时机（如：次日冲高卖出/达到目标价即卖/跌破止损立即走）\n6. 不推荐需要长期持有才能盈利的标的\n7. 已大幅上涨透支次日空间的个股必须降级或回避\n\n"
            "只返回JSON，不要markdown标记。"
                    )},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.5,
                max_tokens=3000,
            )

            if not response:
                return {}

            return self._parse_response(response, stocks)

        except Exception as e:
            logger.error("LLM enhancement failed", strategy=self.strategy_name, error=str(e))
            return {}

    def _parse_response(self, response: str, original_stocks: List[Dict]) -> Dict[str, Any]:
        result = {"enhanced_stocks": [], "strategy_report": ""}
        try:
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            parsed = json.loads(cleaned)
            result["strategy_report"] = parsed.get("strategy_report", "")

            enhanced_map = {es.get("code", ""): es for es in parsed.get("enhanced_stocks", [])}
            for stock in original_stocks:
                code = stock.get("code", "")
                if code in enhanced_map:
                    llm_data = enhanced_map[code]
                    stock["reasons"] = llm_data.get("reasons", stock.get("reasons", []))
                    stock["recommendation_level"] = llm_data.get("recommendation_level", stock.get("recommendation_level", "关注"))
                    stock["risk_warning"] = llm_data.get("risk_warning", "")
                    stock["operation_suggestion"] = llm_data.get("operation_suggestion", "")
            result["enhanced_stocks"] = original_stocks

        except json.JSONDecodeError:
            result["strategy_report"] = response
            result["enhanced_stocks"] = original_stocks
        except Exception as e:
            logger.error("Failed to parse LLM response", error=str(e))
            result["enhanced_stocks"] = original_stocks

        return result


# 全局单例
breakthrough_llm = StrategyLLM("突破战法")
volume_price_llm = StrategyLLM("量价关系战法")
auction_llm = StrategyLLM("竞价尾盘战法")
moving_average_llm = StrategyLLM("均线战法")
