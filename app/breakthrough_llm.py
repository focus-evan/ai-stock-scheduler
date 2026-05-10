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
                # 【优化】包含更多维度数据供GPT判断
                fake_tag = " ⚠️假突破风险" if s.get('is_fake_risk') else ""
                bull_tag = " 🟢多头排列" if s.get('is_bull_aligned') else ""
                rsi_tag = f" RSI:{s.get('rsi_14', '?')}" if s.get('rsi_14') else ""
                vol_tag = f" 量比:{s.get('volume_ratio', 0):.1f}x"
                if s.get('is_volume_confirmed'):
                    vol_tag += "✅放量"
                score_detail = s.get('score_detail', {})
                detail_str = ""
                if score_detail:
                    detail_str = f" [突破{score_detail.get('bt_level',0)}量能{score_detail.get('volume',0)}均线{score_detail.get('ma_form',0)}技术{score_detail.get('tech',0)}]"

                info = (
                    f"排名{s.get('rank', '?')}: {s.get('name', '?')}({s.get('code', '?')}) "
                    f"| 价:{s.get('price', 0)}元 | 涨幅:{s.get('change_pct', 0):.2f}% "
                    f"| {s.get('breakthrough_type', '未知')} 突破价:{s.get('breakthrough_price',0)}元 "
                    f"| 突破幅度:{s.get('breakthrough_pct',0):.1f}%"
                    f"{vol_tag}{rsi_tag}{bull_tag}{fake_tag}"
                    f" | 总分:{s.get('breakthrough_score', 0)}{detail_str}"
                )
                stocks_info.append(info)

            prompt = (
                f"突破战法候选股（已按六维度百分制评分排序）：\n" + "\n".join(stocks_info) +
                "\n\n【分析要求】\n"
                "1. 判断每只股票的突破是否为真突破（量价配合+均线支撑+无长上影线）\n"
                "2. 带⚠️标记的有假突破风险，必须降级为'关注'或'回避'\n"
                "3. 给出次日具体介入价格区间、止损位（突破价下方3%）和目标位\n"
                "4. RSI>80的超买股需要特别提示风险\n\n"
                "返回JSON(不要```标记):"
                '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                '"reasons":["理由1","理由2","理由3"],'
                '"risk_warning":"具体风险提示",'
                '"operation_suggestion":"次日操作：介入区间+止损位+目标位+仓位"}],'
                '"strategy_report":"150-250字突破战法分析报告，重点分析真假突破和次日操作策略"}'
            )

            response = await self._chat(
                messages=[
                    {"role": "system", "content": (
                        "你是A股技术突破战法专家，拥有15年突破交易经验。\n\n"
                        "【突破战法本质】\n"
                        "突破战法的核心是捕捉股价突破关键阻力位后的加速上涨段。\n"
                        "关键在于区分真突破和假突破：\n\n"
                        "【真突破标志】\n"
                        "1. 量价配合：突破日成交量显著放大（量比≥1.5x）\n"
                        "2. 实体突破：收盘价站稳在阻力位之上（而非仅上影线突破）\n"
                        "3. 均线支撑：多头排列(MA5>MA10>MA20)为最佳\n"
                        "4. RSI健康：RSI在50-70区间说明动能充足且未超买\n\n"
                        "【假突破警示】\n"
                        "- 上影线超过实体的50%：抛压重，假突破风险高\n"
                        "- 未放量突破：缺乏资金认可，可能是诱多\n"
                        "- RSI>80: 超买区，突破后可能立即回调\n\n"
                        "【操作原则】\n"
                        "- 止损位：突破价下方3%（跌破则突破失败）\n"
                        "- 目标位：突破幅度的1.5-2倍\n"
                        "- 仓位：真突破半仓，可疑突破轻仓\n\n"
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
