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
                signal = s.get('signal_type', '未知')
                extra_tags = ""
                if '竞价' in signal:
                    open_pct = s.get('open_pct', 0)
                    strength = s.get('strength', 0)
                    drop_tag = " ⚠️高开低走" if s.get('is_high_open_drop') else ""
                    extra_tags = f" 高开:{open_pct:.1f}% 强度:{strength:+.1f}%{drop_tag}"
                elif '尾盘' in signal:
                    near_high = s.get('near_high_pct', 100)
                    extra_tags = f" 距最高:{near_high:.1f}%"

                info = (
                    f"排名{s.get('rank', '?')}: {s.get('name', '?')}({s.get('code', '?')}) "
                    f"| 价:{s.get('price', 0)}元 | 涨幅:{s.get('change_pct', 0):.2f}% "
                    f"| 信号:{signal}{extra_tags}"
                    f" | 评分:{s.get('signal_score', 0)}"
                )
                stocks_info.append(info)

            extra_context = ""
            if kwargs.get("strategy_mode"):
                extra_context = f"\n当前模式: {kwargs['strategy_mode']}"

            # 根据策略类型选择不同的prompt
            if self.strategy_name == "隔夜施工法":
                prompt = (
                    f"隔夜施工法候选股（尾盘买入、次日卖出策略）：\n" + "\n".join(stocks_info) + extra_context +
                    "\n\n【分析要求】\n"
                    "1. 判断该股次日高开概率（结合均线排列+量能+涨停记忆）\n"
                    "2. 评估隔夜风险（板块是否过热、是否有利空预期）\n"
                    "3. 给出精确的尾盘买入价位（回踩均价线买入）\n"
                    "4. 给出次日卖出策略（高开3%+集合竞价卖/平开5分钟内卖/低开止损）\n"
                    "5. 止损线统一-2%\n\n"
                    "返回JSON(不要```标记):"
                    '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                    '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                    '"reasons":["理由1","理由2","理由3"],'
                    '"risk_warning":"具体风险提示",'
                    '"operation_suggestion":"尾盘买入价+次日卖出策略+止损位"}],'
                    '"strategy_report":"150-250字隔夜施工法分析报告"}'
                )
            else:
                prompt = (
                    f"竞价尾盘战法候选股：\n" + "\n".join(stocks_info) + extra_context +
                    "\n\n【分析要求】\n"
                    "1. 判断竞价高开是资金抢筹还是诱多\n"
                    "2. 带⚠️高开低走标记的必须降级\n"
                    "3. 尾盘拉升需判断是主力拉升还是对倒\n"
                    "4. 给出次日操作建议（竞价股关注开盘前10分钟，尾盘股关注次日开盘溢价）\n\n"
                    "返回JSON(不要```标记):"
                    '{"enhanced_stocks":[{"code":"代码","name":"名称",'
                    '"recommendation_level":"强烈推荐/推荐/关注/回避",'
                    '"reasons":["理由1","理由2","理由3"],'
                    '"risk_warning":"具体风险提示",'
                    '"operation_suggestion":"次日操作：介入时机+价格区间+止损位+仓位"}],'
                    '"strategy_report":"150-250字竞价尾盘分析报告"}'
                )

            # 根据策略类型选择不同的system prompt
            if self.strategy_name == "隔夜施工法":
                sys_prompt = (
                    "你是A股隔夜施工法（尾盘买入次日卖出）交易专家，拥有15年短线交易经验。\n\n"
                    "【隔夜施工法本质】\n"
                    "14:30后七步筛选强势股，尾盘介入，次日集合竞价或开盘冲高卖出：\n"
                    "- 涨幅3-5%是最佳区间，太低动力不足，太高次日空间有限\n"
                    "- 量比>1+换手率5-10% = 活跃且不过热\n"
                    "- 均线多头排列 = 趋势向上，顺势而为\n"
                    "- 成交量台阶式放大 = 增量资金持续进场\n"
                    "- 涨停记忆 = 短线资金二次关注概率高\n\n"
                    "【买入原则】\n"
                    "- 14:45-14:55 确认走势不回落后买入\n"
                    "- 最佳买点：股价回踩分时均价线不破\n\n"
                    "【卖出铁律】\n"
                    "- 次日高开3%+：集合竞价挂卖\n"
                    "- 次日平开/微高：开盘5分钟内冲高即出\n"
                    "- 次日低开>2%：立即止损\n"
                    "- 绝不隔第二夜！\n\n"
                    "只返回JSON，不要markdown标记。"
                )
            else:
                sys_prompt = (
                    "你是A股竞价尾盘战法专家，拥有15年短线交易经验。\n\n"
                    "【集合竞价战法本质】\n"
                    "9:25竞价结果是当日资金意图的第一次暴露：\n"
                    "- 适度高开(3-5%)+竞价量能匹配 = 资金抢筹，强烈看多\n"
                    "- 过度高开(>5%) = 可能是诱多，高开低走风险大\n"
                    "- 高开后继续走强(强度>0) = 真强势\n"
                    "- 高开后贰幅回落(强度<-1) = 诱多陷阱\n\n"
                    "【尾盘战法本质】\n"
                    "14:30后选股买入，规避日内波动，博弈次日溢价：\n"
                    "- 收盘价接近日高(distance<5%) = 强势不回头的。\n"
                    "- 尾盘缩量拉升 = 可能是主力对倒，要警惕\n"
                    "- 尾盘放量拉升 = 真实买入，次日溢价概率高\n\n"
                    "【操作原则】\n"
                    "- 竞价股：开盘10分钟内观察，不回落再介入\n"
                    "- 尾盘股：关注次日开盘前5分钟竞价，高开即持有\n\n"
                    "【核心交易理念——当日买入、次日/近日卖出获利】\n所有推荐和操作建议必须严格围绕以下目标：\n1. 推荐的股票必须是「今天可以买入」的，不推荐已无法介入的标的\n2. 盈利目标：次日或1-3个交易日内卖出获利，持仓周期不超过5天\n3. 必须给出明确的「买入价区间」「止损价」「目标卖出价」\n4. 操作建议必须包含具体的买入时机（如：集合竞价/开盘回踩/尾盘低吸）\n5. 必须包含卖出时机（如：次日冲高卖出/达到目标价即卖/跌破止损立即走）\n6. 不推荐需要长期持有才能盈利的标的\n7. 已大幅上涨透支次日空间的个股必须降级或回避\n\n"
                    "只返回JSON，不要markdown标记。"
                )

            response = await self._chat(
                messages=[
                    {"role": "system", "content": sys_prompt},
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
overnight_llm = StrategyLLM("隔夜施工法")
