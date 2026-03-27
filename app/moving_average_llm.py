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
                bull_tag = " 🟢多头排列" if s.get('is_bull_aligned') else ("🟡部分多头" if s.get('is_partial_bull') else "")
                macd_tag = " MACD刚金叉✨" if s.get('is_macd_just_golden') else (" MACD正" if s.get('is_macd_golden') else " MACD负")
                slope_tag = f" 斜率:{s.get('ma20_slope', 0):.1f}%" if s.get('ma20_slope') else ""
                vol_tag = f" 量比:{s.get('vol_ratio', 0):.1f}x" if s.get('vol_ratio') else ""
                score_detail = s.get('score_detail', {})
                detail_str = ""
                if score_detail:
                    detail_str = f" [信号{score_detail.get('signal_base',0)}形态{score_detail.get('form',0)}MACD{score_detail.get('macd',0)}量能{score_detail.get('volume',0)}]"

                info = (
                    f"排名{s.get('rank', '?')}: {s.get('name', '?')}({s.get('code', '?')}) "
                    f"| 价:{s.get('price', 0)}元 | 涨幅:{s.get('change_pct', 0):.2f}% "
                    f"| 信号:{s.get('signal_type', '未知')}"
                    f"{bull_tag}{macd_tag}{slope_tag}{vol_tag}"
                    f" | 总分:{s.get('signal_score', 0)}{detail_str}"
                )
                stocks_info.append(info)

            prompt = (
                f"均线战法候选股（已按五维度百分制评分排序）：\n" + "\n".join(stocks_info) +
                "\n\n【分析要求】\n"
                "1. 判断均线信号的可靠性（多头排列是否稳固，金叉是否有效）\n"
                "2. MACD负的股票动能不足，需要提示风险\n"
                "3. 回踩支撑信号需判断是稳健回调还是趋势破位\n"
                "4. 给出次日具体介入价格区间和止损位（均线下方2-3%）\n\n"
                "返回JSON(不要```标记):"
                '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                '"reasons":["理由1","理由2","理由3"],'
                '"risk_warning":"具体风险提示",'
                '"operation_suggestion":"次日操作：介入区间+止损位+目标位+仓位"}],'
                '"strategy_report":"150-250字均线分析报告，重点分析趋势状态和次日操作策略"}'
            )

            response = await self._chat(
                messages=[
                    {"role": "system", "content": (
                        "你是A股均线战法专家，拥有15年趋势交易经验。\n\n"
                        "【均线战法本质】\n"
                        "均线是趋势的数学表征，通过别均线排列和交叉判断趋势方向。\n\n"
                        "【四大信号判定】\n"
                        "1. 多头排列(MA5>MA10>MA20>MA60): 上升趋势最强确认，可较重仓位\n"
                        "2. 金叉信号: 趋势转换起点，在20日线上方金叉更可靠\n"
                        "3. 回踩20日线: 低风险介入点，止损明确(均线下方2-3%)\n"
                        "4. 均线粘合突破: 方向选择，突破后常有加速\n\n"
                        "【核心验证】\n"
                        "- MACD共振: 均线信号+MACD金叉 = 双重确认，可靠性最高\n"
                        "- 均线斜率: 20日线上升斜率>3% = 趋势加速\n"
                        "- 量能配合: 放量突破均线更可靠\n\n"
                        "【操作原则】\n"
                        "- 止损位: 均线下方2-3%（跌破均线则趋势可能结束）\n"
                        "- 目标位: 下一压力位或均线发散度\n"
                        "- 仓位: 多头排列半仓，金叉轻仓，回踩支撑可较重\n\n"
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
