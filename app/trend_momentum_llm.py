"""
趋势动量战法 LLM 增强模块

通过 GPT 对趋势动量推荐结果进行深度分析增强。
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


class TrendMomentumLLM:
    """趋势动量 LLM 增强客户端"""

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
            logger.error("LLM call timed out", strategy="trend_momentum")
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
                golden_tag = " 🔥MACD金叉" if s.get('macd_golden') else ""
                bull_tag = " 🟢多头排列" if s.get('is_bull_aligned') else ""
                adx_tag = f" ADX:{s.get('adx', '?')}" if s.get('adx') else ""
                rsi_tag = f" RSI:{s.get('rsi_14', '?')}" if s.get('rsi_14') else ""
                high_tag = " 📈60日新高" if s.get('is_60d_high') else (" 📈20日新高" if s.get('is_20d_high') else "")

                # 衰竭预警
                exhaustion_tags = ""
                for w in s.get('exhaustion_warnings', []):
                    exhaustion_tags += f" ⚠️{w}"
                if s.get('macd_bar_divergence'):
                    exhaustion_tags += " ⚠️MACD柱缩短"
                if s.get('rsi_divergence'):
                    exhaustion_tags += " ⚠️RSI顶背离"

                info = (
                    f"排名{s.get('rank', '?')}: {s.get('name', '?')}({s.get('code', '?')}) "
                    f"| 价:{s.get('price', 0)}元 | 涨幅:{s.get('change_pct', 0):.2f}% "
                    f"| {s.get('signal_type', '?')} "
                    f"| 5日动量:{s.get('momentum_5d', 0):+.1f}% "
                    f"| 20日动量:+{s.get('momentum_20d', 0):.1f}% "
                    f"| 60日动量:+{s.get('momentum_60d', 0):.1f}%"
                    f"{golden_tag}{bull_tag}{adx_tag}{rsi_tag}{high_tag}{exhaustion_tags}"
                    f" | 总分:{s.get('momentum_score', 0)}"
                )
                stocks_info.append(info)

            prompt = (
                f"趋势动量候选股（已按六维度百分制评分+衰竭扣分排序）：\n" + "\n".join(stocks_info) +
                "\n\n【分析要求】\n"
                "1. 判断每只股票的趋势阶段（启动/加速/高潮/衰竭）\n"
                "2. MACD金叉+ADX>25为最强信号，重点分析这类股票\n"
                "3. 给出次日具体介入价格区间、止损位（前低或MA20下方3%）和目标位\n"
                "4. RSI>80的超买股必须提示追高风险\n"
                "5. 带⚠️衰竭预警的股票：判断是否应等待回调后再介入\n"
                "6. 5日动量为负但20日动量仍高 = 动量减速信号，需特别提示\n\n"
                "返回JSON(不要```标记):"
                '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                '"reasons":["理由1","理由2","理由3"],'
                '"risk_warning":"具体风险提示",'
                '"operation_suggestion":"次日操作：介入区间+止损位+目标位+仓位"}],'
                '"strategy_report":"150-250字趋势动量分析报告，重点分析动量延续性、衰竭风险和操作策略"}'
            )

            response = await self._chat(
                messages=[
                    {"role": "system", "content": (
                        "你是A股趋势交易专家，拥有15年趋势动量交易经验。\n\n"
                        "【趋势动量本质】\n"
                        "动量效应是全球股市最稳健的因子之一。\n"
                        "过去3个月涨幅前20%的股票，未来1-3个月大概率继续跑赢。\n"
                        "核心在于识别趋势的启动和加速阶段，避免趋势衰竭阶段追高。\n\n"
                        "【关键技术标志】\n"
                        "1. MACD金叉+ADX>25: 趋势确立且强度够，最佳介入时机\n"
                        "2. 多头排列(MA5>MA10>MA20): 短中期趋势一致向上\n"
                        "3. 创新高+放量: 突破后的动量延续\n"
                        "4. 20日动量>10%: 短期加速上涨，但要警惕过热\n\n"
                        "【趋势阶段判断】\n"
                        "- 启动期: MACD刚金叉，ADX从低位回升，最佳介入\n"
                        "- 加速期: 动量持续增大，MACD柱状体放大，可加仓\n"
                        "- 高潮期: RSI>80，换手率飙升，应止盈\n"
                        "- 衰竭期: MACD顶背离，动量减弱，应离场\n\n"
                        "【操作原则】\n"
                        "- 止损位：MA20下方3%或前低\n"
                        "- 动量衰竭时果断止盈\n"
                        "- 仓位：趋势确立半仓，加速不追仓\n\n"
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
            logger.error("Trend Momentum LLM enhancement failed", error=str(e))
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
trend_momentum_llm = TrendMomentumLLM()
