"""
北向资金跟踪战法 LLM 增强模块

通过 GPT 对北向资金推荐结果进行深度分析增强。
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


class NorthboundLLM:
    """北向资金跟踪 LLM 增强客户端"""

    def __init__(self):
        self.base_url = LLM_BASE_URL
        self.model = LLM_MODEL
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
            logger.error("LLM call timed out", strategy="northbound")
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
                nb_tag = " 🏦北向持仓" if s.get('in_northbound') else ""
                contrarian_tag = " 🔥逆势加仓" if s.get('is_contrarian') else ""
                inflow = s.get('main_net_inflow', 0)
                inflow_str = f"{inflow / 1e8:.2f}亿" if abs(inflow) >= 1e8 else f"{inflow / 1e4:.0f}万"

                # 连续增持信号
                cd = s.get('consecutive_days', 0)
                cd_tag = f" 📈连续增持{cd}天" if cd >= 3 else ""
                accel_tag = " ⚡增持加速" if s.get('is_accelerating') else ""
                r5d = s.get('ratio_change_5d', 0)
                trend_tag = f" 近5日占比变化:{r5d:+.3f}%" if r5d != 0 else ""

                info = (
                    f"排名{s.get('rank', '?')}: {s.get('name', '?')}({s.get('code', '?')}) "
                    f"| 价:{s.get('price', 0)}元 | 涨幅:{s.get('change_pct', 0):.2f}% "
                    f"| 北向占比:{s.get('hold_ratio', 0):.2f}% | 增持:{s.get('increase', 0):.0f}股"
                    f" | 主力净流入:{inflow_str}"
                    f"{nb_tag}{contrarian_tag}{cd_tag}{accel_tag}{trend_tag}"
                    f" | 总分:{s.get('northbound_score', 0)}"
                )
                stocks_info.append(info)

            prompt = (
                f"北向资金跟踪候选股（已按评分排序）：\n" + "\n".join(stocks_info) +
                "\n\n【分析要求】\n"
                "1. 判断每只股票北向资金增持的可持续性（是价值投资还是短期套利）\n"
                "2. 标注🔥逆势加仓的股票是否为真正的价值洼地\n"
                "3. 给出次日具体介入价格区间、止损位和目标位\n"
                "4. 分析外资持仓占比变化趋势（是加速增持还是减速）\n\n"
                "返回JSON(不要```标记):"
                '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                '"reasons":["理由1","理由2","理由3"],'
                '"risk_warning":"具体风险提示",'
                '"operation_suggestion":"次日操作：介入区间+止损位+目标位+仓位"}],'
                '"strategy_report":"150-250字北向资金分析报告，重点分析外资动向和操作策略"}'
            )

            response = await self._chat(
                messages=[
                    {"role": "system", "content": (
                        "你是A股北向资金研究专家，拥有15年跟踪沪深港通的经验。\n\n"
                        "【北向资金本质】\n"
                        "北向资金（沪深港通）被称为'聪明钱'，代表海外机构的投资偏好。\n"
                        "连续大额净买入的标的，后续1-3个月平均跑赢大盘5-8%。\n\n"
                        "【关键判断维度】\n"
                        "1. 持仓占比>5%: 说明外资深度介入，看好中长期\n"
                        "2. 连续增持: 比单日大额更有意义，代表持续看好\n"
                        "3. 逆势加仓: 大盘跌但外资买入，最珍贵的信号\n"
                        "4. 主力净流入: 外资+国内机构共振效果最佳\n\n"
                        "【风险提示】\n"
                        "- '假外资'：内地资金借道香港回流，需结合基本面判断\n"
                        "- 汇率风险：人民币贬值可能导致外资阶段性撤出\n"
                        "- 持仓上限：部分个股接近28%外资持股上限\n\n"
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
            logger.error("Northbound LLM enhancement failed", error=str(e))
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
northbound_llm = NorthboundLLM()
