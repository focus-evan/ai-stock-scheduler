"""
龙头战法 LLM 增强模块

通过 GPT-5.2 大模型对龙头战法的算法结果进行深度分析和推荐增强：
1. 主线题材深度解读 - 分析题材的政策驱动力和持续性
2. 个股推荐理由生成 - 基于多因子数据生成个性化推荐理由
3. 新闻情绪深度分析 - 替代简单关键词匹配，语义理解新闻与题材的关联
4. 策略报告生成 - 生成专业级龙头战法分析报告
"""

import json
import os
import traceback
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger()

# GPT-5.2 API 配置
DRAGON_LLM_BASE_URL = os.getenv("DRAGON_LLM_BASE_URL", "https://api.wxznb.cn")
DRAGON_LLM_MODEL = os.getenv("DRAGON_LLM_MODEL", "gpt-5.4")
DRAGON_LLM_PROVIDER = os.getenv("DRAGON_LLM_PROVIDER", "openai")
DRAGON_LLM_API_KEY = os.getenv("DRAGON_LLM_API_KEY", "")


class DragonHeadLLM:
    """龙头战法 LLM 增强客户端"""

    def __init__(self):
        self.base_url = DRAGON_LLM_BASE_URL
        self.model = DRAGON_LLM_MODEL
        self.provider = DRAGON_LLM_PROVIDER
        self.api_key = DRAGON_LLM_API_KEY
        self.timeout = 90  # 90秒超时（上游 OpenAI API 约60-90s就会超时）

    async def _chat(self, messages: List[Dict[str, str]], temperature: float = 0.7,
                    max_tokens: int = 4000) -> Optional[str]:
        """
        调用 GPT-5.2 API

        Args:
            messages: 对话消息列表
            temperature: 温度参数
            max_tokens: 最大生成 token 数

        Returns:
            LLM 返回的文本内容，失败返回 None
        """
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
                    logger.error("GPT-5.2 API error",
                                 status=response.status_code,
                                 body=response.text[:500])
                    return None

        except httpx.TimeoutException:
            logger.error("GPT-5.2 call timed out", timeout=self.timeout)
            return None
        except Exception as e:
            logger.error("GPT-5.2 call failed", error=str(e), error_type=type(e).__name__)
            return None

    async def enhance_recommendations(
        self,
        dragon_heads: List[Dict[str, Any]],
        main_themes: List[str],
        theme_details: Dict[str, Any],
        news_resonance: Dict[str, Any],
        limit_up_count: int,
    ) -> Dict[str, Any]:
        """
        用 GPT-5.2 增强龙头战法推荐结果

        将算法筛选出的原始数据发给大模型，生成：
        1. 每只个股的个性化推荐理由
        2. 主线题材的深度解读
        3. 新闻情绪的语义分析
        4. 完整的策略推荐报告

        Returns:
            增强后的推荐数据
        """
        # 只发送前5只给LLM分析（降低token数，避免上游API超时）
        stocks_for_llm = dragon_heads[:5]
        max_retry = 2

        for attempt in range(1, max_retry + 1):
            try:
                # 重试时进一步减少股票数量
                if attempt > 1:
                    stocks_for_llm = dragon_heads[:3]
                    logger.info("GPT-5.2 retry with reduced stocks", attempt=attempt, count=len(stocks_for_llm))

                prompt = self._build_analysis_prompt(
                    stocks_for_llm, main_themes, theme_details,
                    news_resonance, limit_up_count
                )

                logger.info("Calling GPT-5.2 for dragon head analysis",
                             stocks_count=len(stocks_for_llm),
                             themes_count=len(main_themes),
                             attempt=attempt)

                response = await self._chat(
                    messages=[
                        {"role": "system", "content": self._get_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.5,
                    max_tokens=4000,
                )

                if not response:
                    logger.warning("GPT-5.2 returned no response", attempt=attempt)
                    continue

                # 解析 LLM 返回的结构化结果（传入全部股票以保持原始序）
                result = self._parse_llm_response(response, dragon_heads)

                logger.info("GPT-5.2 enhancement completed",
                             has_report=bool(result.get("strategy_report")),
                             enhanced_stocks=len(result.get("enhanced_stocks", [])))

                return result

            except Exception as e:
                logger.error("LLM enhancement attempt failed",
                             attempt=attempt, error=str(e),
                             traceback=traceback.format_exc())
                if attempt >= max_retry:
                    return {}

        logger.warning("GPT-5.2 all attempts failed, using fallback")
        return {}

    def _get_system_prompt(self) -> str:
        """获取系统提示词（注入龙头战法专业知识）"""
        return (
            "你是A股短线龙头战法资深分析师，精通情绪周期和龙头股操作体系。\n"
            "【龙头战法核心知识】\n"
            "1. 情绪周期四阶段：\n"
            "   - 启动期：市场刚出现赚钱效应，主线题材初现，连板高度2-3板，此时买入龙头确定性最高\n"
            "   - 上升期：龙头股加速上涨，跟风股开始补涨，连板4-6板，可追龙头首次分歧回踩\n"
            "   - 高潮期：涨停家数暴增50+，龙头高度7板以上，此时不应追高，等补涨或低位新龙头\n"
            "   - 退潮期：连板股纷纷炸板，亏钱效应扩大，应空仓观望或只做确定性最高的反包\n"
            "2. 真龙头vs跟风：\n"
            "   - 真龙头：板块内最先涨停、连板最高、换手充分后仍能封住、有辨识度（市值/题材/股性）\n"
            "   - 跟风股：封板晚、跟随龙头涨停、容易炸板、次日大概率低开\n"
            "3. 操作纪律：\n"
            "   - 不追已涨停的股票（次日溢价不确定）\n"
            "   - 买入时机：龙头次日低开低走企稳时低吸，或强势股回踩5日线时介入\n"
            "   - 严格5%止损，打板被砸次日必须走\n"
            "   - 仓位管理：启动期/上升期可重仓(30-50%)，高潮期轻仓(10-20%)，退潮期空仓\n"
            "【核心交易理念——当日买入、次日/近日卖出获利】\n所有推荐和操作建议必须严格围绕以下目标：\n1. 推荐的股票必须是「今天可以买入」的，不推荐已无法介入的标的\n2. 盈利目标：次日或1-3个交易日内卖出获利，持仓周期不超过5天\n3. 必须给出明确的「买入价区间」「止损价」「目标卖出价」\n4. 操作建议必须包含具体的买入时机（如：集合竞价/开盘回踩/尾盘低吸）\n5. 必须包含卖出时机（如：次日冲高卖出/达到目标价即卖/跌破止损立即走）\n6. 不推荐需要长期持有才能盈利的标的\n7. 已大幅上涨透支次日空间的个股必须降级或回避\n\n"
            "【输出要求】只返回JSON，不要markdown标记。"
        )

    def _build_analysis_prompt(
        self,
        dragon_heads: List[Dict],
        main_themes: List[str],
        theme_details: Dict,
        news_resonance: Dict,
        limit_up_count: int,
    ) -> str:
        """构建分析提示词"""

        # 格式化股票数据（caller 已控制数量，无需再截断）
        stocks_info = []
        for s in dragon_heads:
            stock_str = (
                f"排名{s.get('rank', '?')}：{s.get('name', '?')}({s.get('code', '?')}) "
                f"| 最新价:{s.get('price', 0)}元 "
                f"| 涨跌幅:{s.get('change_pct', 0):.2f}% "
                f"| 连板:{s.get('limit_up_days', 1)}天 "
                f"| 封板时间:{s.get('first_limit_time', '未知')} "
                f"| 成交额:{self._format_amount(s.get('amount', 0))} "
                f"| 流通市值:{self._format_amount(s.get('float_market_cap', 0))} "
                f"| 换手率:{s.get('turnover_rate', 0):.2f}% "
                f"| 封单额:{self._format_amount(s.get('seal_amount', 0))}"
            )
            stocks_info.append(stock_str)

        stocks_text = "\n".join(stocks_info)

        # 格式化主线题材
        themes_text = ""
        if main_themes:
            theme_lines = []
            for t in main_themes:
                detail = theme_details.get(t, {})
                theme_lines.append(
                    f"- {t}: 板块涨幅{detail.get('change_pct', 0):.2f}%, "
                    f"上涨{detail.get('up_count', 0)}家, "
                    f"涨停{detail.get('limit_up_count', 0)}家"
                )
            themes_text = "\n".join(theme_lines)
        else:
            themes_text = "今日未识别出明确主线题材"

        # 格式化新闻共振
        news_text = f"分析了{news_resonance.get('news_count', 0)}条财经快讯，"
        if news_resonance.get('matching_themes'):
            news_text += f"与主线共振的题材：{'、'.join(news_resonance['matching_themes'])}，"
            news_text += f"共振评分：{news_resonance.get('resonance_score', 0):.0f}/100"
        else:
            news_text += "未发现与主线题材的明显共振"

        # 新闻关键词
        if news_resonance.get('news_keywords'):
            kw_list = [f"{kw['keyword']}({kw['count']}次)" for kw in news_resonance['news_keywords']]
            news_text += f"\n新闻高频关键词：{'、'.join(kw_list)}"

        prompt = (
            f"【市场数据】今日涨停{limit_up_count}只\n"
            f"【主线题材】\n{themes_text}\n"
            f"【新闻共振】{news_text}\n"
            f"【龙头候选股】\n{stocks_text}\n\n"
            f"请按以下JSON格式返回分析（不要```标记）：\n"
            '{"market_sentiment":{'
            '"phase":"启动期/上升期/高潮期/退潮期",'
            '"description":"当前情绪周期判断依据(80字)",'
            '"risk_level":"低/中/高",'
            '"position_advice":"根据情绪周期给出总仓位建议(如:可重仓30-50%/轻仓10-20%/建议空仓)"'
            '},'
            '"enhanced_stocks":[{'
            '"code":"代码","name":"名称",'
            '"recommendation_level":"强烈推荐/推荐/关注/回避",'
            '"is_true_dragon":true/false,'
            '"reasons":["理由1(结合连板高度/封板时间/板块地位分析)","理由2","理由3"],'
            '"risk_warning":"具体风险(如:连板过高有核按钮风险/跟风股容易炸板等)",'
            '"operation_suggestion":"今日买入建议(含具体买入时机如集合竞价/盘中回踩/尾盘低吸,以及买入价区间) + 次日卖出策略(如冲高卖出/达目标价即卖)",'
            '"buy_price_range":"建议买入价区间(如:12.5-13.0)",'
            '"stop_loss_price":止损价(数字),'
            '"target_price":目标价(数字),'
            '"position_advice":"建议仓位(如:15%/20%)"'
            '}],'
            '"strategy_report":"150-250字分析报告，必须包含：1.当前情绪周期判断 2.主线题材持续性分析 3.龙头股辨识度排序 4.次日操作策略（什么时间点买/卖/观望）"'
            '}'
        )

        return prompt

    def _format_amount(self, val: float) -> str:
        """格式化金额"""
        if not val:
            return "0"
        if val >= 1e12:
            return f"{val / 1e12:.2f}万亿"
        if val >= 1e8:
            return f"{val / 1e8:.2f}亿"
        if val >= 1e4:
            return f"{val / 1e4:.2f}万"
        return f"{val:.2f}"

    def _parse_llm_response(self, response: str, original_stocks: List[Dict]) -> Dict[str, Any]:
        """
        解析 LLM 返回的 JSON 结果

        增加容错处理，确保即使 LLM 返回格式不完美也能正确解析
        """
        result = {
            "market_sentiment": {},
            "theme_analysis": [],
            "enhanced_stocks": [],
            "strategy_report": "",
        }

        try:
            # 清理 response 中可能的 markdown 代码块标记
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            parsed = json.loads(cleaned)

            # 提取各字段
            result["market_sentiment"] = parsed.get("market_sentiment", {})
            result["theme_analysis"] = parsed.get("theme_analysis", [])
            result["strategy_report"] = parsed.get("strategy_report", "")

            # 处理增强的股票数据 - 与原始数据关联
            enhanced_map = {}
            for es in parsed.get("enhanced_stocks", []):
                code = es.get("code", "")
                if code:
                    enhanced_map[code] = es

            # 将 LLM 增强数据合并到原始股票列表
            for stock in original_stocks:
                code = stock.get("code", "")
                if code in enhanced_map:
                    llm_data = enhanced_map[code]
                    stock["reasons"] = llm_data.get("reasons", stock.get("reasons", []))
                    stock["recommendation_level"] = llm_data.get("recommendation_level",
                                                                  stock.get("recommendation_level", "关注"))
                    stock["risk_warning"] = llm_data.get("risk_warning", "")
                    stock["operation_suggestion"] = llm_data.get("operation_suggestion", "")
                    # 新增跟投友好字段
                    stock["is_true_dragon"] = llm_data.get("is_true_dragon", False)
                    stock["buy_price_range"] = llm_data.get("buy_price_range", "")
                    stock["stop_loss_price"] = llm_data.get("stop_loss_price", 0)
                    stock["target_price"] = llm_data.get("target_price", 0)
                    stock["position_advice"] = llm_data.get("position_advice", "")

            result["enhanced_stocks"] = original_stocks

        except json.JSONDecodeError as e:
            logger.warning("Failed to parse LLM JSON response, using raw text",
                           error=str(e))
            # 如果 JSON 解析失败，将整个响应作为策略报告
            result["strategy_report"] = response
            result["enhanced_stocks"] = original_stocks

        except Exception as e:
            logger.error("Failed to process LLM response", error=str(e))
            result["enhanced_stocks"] = original_stocks

        return result


# 全局单例
dragon_head_llm = DragonHeadLLM()
