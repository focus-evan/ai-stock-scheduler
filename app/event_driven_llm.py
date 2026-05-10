"""
事件驱动战法 LLM 分析模块 (Event-Driven LLM)

通过 GPT-5.2 大模型将非结构化新闻文本转化为结构化交易信号：
1. 语义拆解 — 提取核心关键词、判定影响等级(1-5)
2. 逻辑映射 — 直接受益 → 产业链上游 → 替代品 → A股行业板块
3. 推荐增强 — 生成策略报告、风险提示、操作建议
"""

import json
import os
import traceback
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger()

# GPT-5.2 API 配置（复用龙头战法的代理配置）
EVENT_LLM_BASE_URL = os.getenv("DRAGON_LLM_BASE_URL", "https://api.wxznb.cn")
EVENT_LLM_MODEL = os.getenv("DRAGON_LLM_MODEL", "gpt-5.4")
EVENT_LLM_PROVIDER = os.getenv("DRAGON_LLM_PROVIDER", "openai")
DRAGON_LLM_API_KEY = os.getenv("DRAGON_LLM_API_KEY", "")


class EventDrivenLLM:
    """事件驱动 LLM 语义分析客户端"""

    def __init__(self):
        self.base_url = EVENT_LLM_BASE_URL
        self.model = EVENT_LLM_MODEL
        self.provider = EVENT_LLM_PROVIDER
        self.api_key = DRAGON_LLM_API_KEY
        self.timeout = 90

    async def _chat(self, messages: List[Dict[str, str]], temperature: float = 0.5,
                    max_tokens: int = 4000) -> Optional[str]:
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
                    logger.error("Event LLM API error",
                                 status=response.status_code,
                                 body=response.text[:500])
                    return None

        except httpx.TimeoutException:
            logger.error("Event LLM call timed out", timeout=self.timeout)
            return None
        except Exception as e:
            logger.error("Event LLM call failed", error=str(e), error_type=type(e).__name__)
            return None

    # ==================== 1. 新闻事件语义分析 ====================

    async def analyze_events(self, news_list: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        批量分析新闻事件，提取关键词、影响等级、关联板块

        Args:
            news_list: [{title, content, source, time}, ...]

        Returns:
            [{keywords, impact_level, sectors, logic_chain, summary}, ...]
        """
        if not news_list:
            return []

        max_retry = 2
        # 限制新闻数量避免 token 过多
        news_for_llm = news_list[:25]

        for attempt in range(1, max_retry + 1):
            try:
                if attempt > 1:
                    news_for_llm = news_list[:15]

                prompt = self._build_event_analysis_prompt(news_for_llm)

                logger.info("Calling GPT-5.2 for event analysis",
                             news_count=len(news_for_llm), attempt=attempt)

                response = await self._chat(
                    messages=[
                        {"role": "system", "content": self._get_event_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.3,
                    max_tokens=3000,
                )

                if not response:
                    logger.warning("Event LLM returned no response", attempt=attempt)
                    continue

                result = self._parse_event_response(response)
                if result:
                    logger.info("Event analysis completed", events_analyzed=len(result))
                    return result

            except Exception as e:
                logger.error("Event analysis attempt failed",
                             attempt=attempt, error=str(e),
                             traceback=traceback.format_exc())
                if attempt >= max_retry:
                    break

        logger.warning("Event analysis all attempts failed, using fallback")
        return self._fallback_event_analysis(news_list)

    # ==================== 2. 推荐增强 ====================

    async def enhance_recommendations(
        self,
        candidates: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        GPT-5.2 增强事件驱动的股票推荐

        Args:
            candidates: 初筛的候选股票列表
            events: 关联事件列表

        Returns:
            增强后的推荐 + 策略报告
        """
        if not candidates:
            return {"enhanced_stocks": [], "strategy_report": "暂无事件驱动推荐"}

        max_retry = 2
        stocks_for_llm = candidates[:8]

        for attempt in range(1, max_retry + 1):
            try:
                if attempt > 1:
                    stocks_for_llm = candidates[:5]

                prompt = self._build_enhance_prompt(stocks_for_llm, events)

                response = await self._chat(
                    messages=[
                        {"role": "system", "content": self._get_enhance_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.5,
                    max_tokens=4000,
                )

                if not response:
                    continue

                result = self._parse_enhance_response(response, candidates)
                if result:
                    return result

            except Exception as e:
                logger.error("Enhance attempt failed", attempt=attempt, error=str(e))
                if attempt >= max_retry:
                    break

        # fallback
        return {
            "enhanced_stocks": candidates,
            "strategy_report": "LLM 不可用，使用算法评分结果",
            "market_assessment": {},
        }

    # ==================== Prompt 构建 ====================

    def _get_event_system_prompt(self) -> str:
        return (
            "你是资深A股事件驱动策略分析师，拥有15年短线事件投资经验。\n\n"
            "【核心方法论】\n"
            "事件驱动投资的本质是捕捉'信息不对称→市场重新定价'的过程。\n"
            "关键不是事件本身，而是事件是否创造了'预期差'：\n"
            "- 超预期事件（市场未充分定价）→ 产生交易机会\n"
            "- 符合预期事件（市场已充分定价）→ 不产生新机会\n"
            "- 低于预期事件 → 可能产生反向交易机会\n\n"
            "【事件分类体系】\n"
            "1. 政策事件：国务院/部委政策、行业规划、补贴/税收变化\n"
            "   - 影响链：政策→直接受益行业→上下游→替代品/竞品\n"
            "2. 产业事件：技术突破、供需变化、价格异动、产能扩张\n"
            "   - 影响链：产业变化→核心环节→配套环节→终端应用\n"
            "3. 公司事件：业绩预告、中标/签约、股权变动、重组\n"
            "   - 影响链：个股→同行业→产业链上下游\n"
            "4. 市场事件：北向资金、融资融券、股指期货、外围市场\n"
            "   - 影响链：资金流向→板块轮动→个股传导\n\n"
            "【影响等级判定标准】\n"
            "5级（重大）：国家级政策/行业颠覆性变化，影响持续数周以上\n"
            "4级（显著）：部委级政策/行业重要变化，影响持续数日\n"
            "3级（中等）：行业普通利好/利空，可能影响1-3日\n"
            "2级（轻微）：常规新闻/跟踪报道，影响有限\n"
            "1级（噪音）：无实质影响的信息\n\n"
            "【'见光死'风险判定】\n"
            "以下情况事件可能已充分定价，标记为'旧闻'：\n"
            "- 该事件已被市场讨论超过24小时\n"
            "- 相关股票已连续涨停\n"
            "- 事件内容与之前传闻一致（靴子落地）\n\n"
            "【核心交易理念——当日买入、次日/近日卖出获利】\n所有推荐和操作建议必须严格围绕以下目标：\n1. 推荐的股票必须是「今天可以买入」的，不推荐已无法介入的标的\n2. 盈利目标：次日或1-3个交易日内卖出获利，持仓周期不超过5天\n3. 必须给出明确的「买入价区间」「止损价」「目标卖出价」\n4. 操作建议必须包含具体的买入时机（如：集合竞价/开盘回踩/尾盘低吸）\n5. 必须包含卖出时机（如：次日冲高卖出/达到目标价即卖/跌破止损立即走）\n6. 不推荐需要长期持有才能盈利的标的\n7. 已大幅上涨透支次日空间的个股必须降级或回避\n\n"
            "只返回JSON，不要markdown标记。"
        )

    def _build_event_analysis_prompt(self, news_list: List[Dict]) -> str:
        news_text = []
        for i, n in enumerate(news_list, 1):
            title = n.get("title", "")
            content = n.get("content", "")[:300]
            source = n.get("source", "未知")
            pub_time = n.get("time", "")
            news_text.append(f"{i}. [{source}] {title}\n   {content}\n   发布时间: {pub_time}")

        prompt = (
            f"请分析以下{len(news_list)}条财经新闻事件：\n\n"
            + "\n\n".join(news_text) + "\n\n"
            "【分析要求】\n"
            "1. 判断每条新闻是否有A股投资价值（无价值的直接跳过）\n"
            "2. 重点判断事件是否存在'预期差'（市场是否已充分定价）\n"
            "3. 区分'首次'（全新事件）、'跟踪'（事件进展）和'旧闻'（已知信息）\n"
            "4. 逻辑链要从'直接受益→产业链上游→替代品/配套'完整推演\n"
            "5. sectors字段必须填写具体的A股概念板块名（如'固态电池'而非'电池'），\n"
            "   这些名称将直接用于匹配东财/同花顺概念板块\n\n"
            "返回JSON（不要```标记）：\n"
            '{"events":['
            '{"news_index":序号,"title":"新闻标题",'
            '"keywords":["关键词1","关键词2","关键词3"],'
            '"impact_level":影响等级1-5,'
            '"impact_duration":"短期/中期/长期",'
            '"logic_chain":"直接受益→产业链→替代品的完整逻辑推演",'
            '"sectors":["A股概念板块名1","概念板块名2"],'
            '"sector_codes":["行业代码或概念名称"],'
            '"freshness":"首次/旧闻/跟踪",'
            '"expectation_gap":"超预期/符合预期/低于预期",'
            '"summary":"30字事件影响总结"}],'
            '"overall_assessment":"50字整体市场事件评估，指出当前最值得关注的主线"}'
        )
        return prompt

    def _get_enhance_system_prompt(self) -> str:
        return (
            "你是A股事件驱动交易策略专家，专注于从事件中挖掘次日盈利机会。\n\n"
            "【核心原则】\n"
            "1. 次日盈利导向：推荐必须基于次日仍有上涨空间的判断\n"
            "2. 预期差优先：优先推荐市场尚未充分定价的标的\n"
            "3. 不追高原则：已经涨停或涨幅超过7%的个股，需要评估次日溢价风险\n"
            "4. 事件持续性：优先选择事件影响将持续多日的标的\n\n"
            "【推荐等级标准】\n"
            "强烈推荐：事件影响4-5级 + 预期差大 + 股价未充分反映\n"
            "推荐：事件影响3-4级 + 有一定预期差\n"
            "关注：事件有价值但存在较大不确定性\n"
            "回避：已充分定价或'见光死'风险高\n\n"
            "【操作建议要求】\n"
            "- 给出次日合理的介入价格区间（如'回调至XX元附近可介入'）\n"
            "- 给出明确止损位（如'跌破XX元止损'）\n"
            "- 给出目标价位和持有周期\n"
            "- 建议仓位比例（如'轻仓试探/半仓参与/重仓出击'）\n\n"
            "只返回JSON，不要markdown标记。"
        )

    def _build_enhance_prompt(self, stocks: List[Dict], events: List[Dict]) -> str:
        # 事件摘要（增加预期差和持续性信息）
        event_lines = []
        for e in events[:5]:
            freshness = e.get('freshness', '未知')
            gap = e.get('expectation_gap', '未知')
            event_lines.append(
                f"- {e.get('title', '?')} (影响:{e.get('impact_level', '?')}级, "
                f"持续:{e.get('impact_duration', '?')}, "
                f"新鲜度:{freshness}, 预期差:{gap}, "
                f"关联:{','.join(e.get('sectors', [])[:3])})"
            )
        events_text = "\n".join(event_lines) if event_lines else "暂无高影响事件"

        # 候选股票（增加成交额信息）
        stock_lines = []
        for i, s in enumerate(stocks, 1):
            amount_str = ""
            if s.get('amount', 0) > 0:
                amount_yi = s['amount'] / 1e8
                amount_str = f"成交额:{amount_yi:.1f}亿 "
            stock_lines.append(
                f"{i}. {s.get('name', '?')}({s.get('code', '?')}) "
                f"价格:{s.get('price', 0):.2f}元 "
                f"涨跌幅:{s.get('change_pct', 0):.2f}% "
                f"换手率:{s.get('turnover_rate', 0):.2f}% "
                f"{amount_str}"
                f"事件评分:{s.get('event_score', 0):.1f} "
                f"关联:{','.join(s.get('related_concepts', [])[:3])} "
                f"事件:{s.get('event_reason', '未知')}"
            )
        stocks_text = "\n".join(stock_lines)

        prompt = (
            f"今日核心事件：\n{events_text}\n\n"
            f"事件驱动候选股（已按算法评分排序）：\n{stocks_text}\n\n"
            "【分析要求】\n"
            "1. 判断每只股票的事件关联是否真实且有持续性\n"
            "2. 评估次日是否仍有上涨空间（是否已'见光死'）\n"
            "3. 给出具体的介入价格区间和止损位\n"
            "4. 事件已充分定价的标的必须降级为'关注'或'回避'\n\n"
            "返回JSON（不要```标记）：\n"
            '{"market_assessment":{"event_type":"政策/产业/技术/市场","heat_level":"高/中/低",'
            '"risk_level":"低/中/高","main_line":"当前最强事件主线",'
            '"description":"50字市场事件评估"},'
            '"enhanced_stocks":[{"code":"代码","name":"名称",'
            '"recommendation_level":"强烈推荐/推荐/关注/回避",'
            '"reasons":["理由1","理由2","理由3"],'
            '"risk_warning":"具体风险提示",'
            '"operation_suggestion":"次日操作建议：介入区间+止损位+目标位+仓位",'
            '"event_sustainability":"事件持续性判断（1-3日/1周/长期）",'
            '"logic_strength":逻辑强度1-5}],'
            '"strategy_report":"150-250字事件驱动策略报告，分析当前事件主线和次日操作策略"}'
        )
        return prompt

    # ==================== 响应解析 ====================

    def _parse_event_response(self, response: str) -> List[Dict]:
        """解析事件分析 JSON"""
        try:
            cleaned = self._clean_json(response)
            parsed = json.loads(cleaned)
            events = parsed.get("events", [])
            # 保存整体评估
            for e in events:
                e["overall_assessment"] = parsed.get("overall_assessment", "")
            return events
        except json.JSONDecodeError:
            logger.warning("Failed to parse event analysis JSON")
            return []

    def _parse_enhance_response(self, response: str, original_stocks: List[Dict]) -> Dict:
        """解析推荐增强 JSON"""
        result = {
            "enhanced_stocks": [],
            "strategy_report": "",
            "market_assessment": {},
        }

        try:
            cleaned = self._clean_json(response)
            parsed = json.loads(cleaned)

            result["market_assessment"] = parsed.get("market_assessment", {})
            result["strategy_report"] = parsed.get("strategy_report", "")

            # 合并 LLM 增强数据到原始股票列表
            enhanced_map = {}
            for es in parsed.get("enhanced_stocks", []):
                code = es.get("code", "")
                if code:
                    enhanced_map[code] = es

            for stock in original_stocks:
                code = stock.get("code", "")
                if code in enhanced_map:
                    llm_data = enhanced_map[code]
                    stock["reasons"] = llm_data.get("reasons", stock.get("reasons", []))
                    stock["recommendation_level"] = llm_data.get(
                        "recommendation_level", stock.get("recommendation_level", "关注"))
                    stock["risk_warning"] = llm_data.get("risk_warning", "")
                    stock["operation_suggestion"] = llm_data.get("operation_suggestion", "")
                    stock["logic_strength"] = llm_data.get("logic_strength", 3)
                    stock["event_sustainability"] = llm_data.get("event_sustainability", "")

            result["enhanced_stocks"] = original_stocks
            return result

        except json.JSONDecodeError:
            logger.warning("Failed to parse enhance JSON, using raw text")
            result["strategy_report"] = response
            result["enhanced_stocks"] = original_stocks
            return result
        except Exception as e:
            logger.error("Failed to process enhance response", error=str(e))
            result["enhanced_stocks"] = original_stocks
            return result

    def _clean_json(self, text: str) -> str:
        """清理 JSON 中的 markdown 标记"""
        cleaned = text.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()

    def _fallback_event_analysis(self, news_list: List[Dict]) -> List[Dict]:
        """LLM 不可用时的降级方案"""
        results = []
        for i, n in enumerate(news_list[:5]):
            title = n.get("title", "")
            results.append({
                "news_index": i + 1,
                "title": title,
                "keywords": title[:20].split(),
                "impact_level": 2,
                "impact_duration": "短期",
                "logic_chain": "暂无(LLM降级)",
                "sectors": [],
                "sector_codes": [],
                "freshness": "未知",
                "summary": title[:30],
            })
        return results


# 全局单例
event_driven_llm = EventDrivenLLM()
