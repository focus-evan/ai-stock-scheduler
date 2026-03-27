"""
事件驱动战法（Event-Driven Strategy）核心逻辑模块

六大核心模块：
1. 多维度新闻聚合 (Ingestion) — 财联社/同花顺/东财/板块异动/百度热搜
2. LLM 语义拆解与逻辑映射 (Reasoning) — GPT-5.2 提取关键词/影响等级/行业映射
3. 概念-股票映射 (Mapping) — 通过 AkShare 概念板块匹配个股
4. 量化过滤与技术对齐 (Filtering) — 换手率/涨跌幅/跨策略共振
5. 加权打分排序 (Ranking) — 综合逻辑强度/历史胜率/资金认可/稀缺性
6. 新闻整理摘要 (Digest) — 按来源/分类/关联度整理新闻

数据源：AkShare (免费开源金融数据接口)
"""

import asyncio
import re
import traceback
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog

# 统一反爬模块
try:
    from anti_scrape import anti_scrape_delay, DELAY_LIGHT, DELAY_NORMAL
except ImportError:
    from app.anti_scrape import anti_scrape_delay, DELAY_LIGHT, DELAY_NORMAL

logger = structlog.get_logger()


class EventDrivenStrategy:
    """事件驱动战法核心策略类"""

    def __init__(self):
        self._ak = None
        self._cache: Dict[str, Any] = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=15)

    @property
    def ak(self):
        """延迟导入 akshare"""
        if self._ak is None:
            from app.akshare_client import ak_client as ak
            self._ak = ak
        return self._ak

    def _is_cache_valid(self) -> bool:
        if not self._cache or not self._cache_time:
            return False
        return datetime.now() - self._cache_time < self._cache_ttl

    # ==================== 主入口 ====================

    async def get_recommendations(self, limit: int = 13) -> Dict[str, Any]:
        """
        事件驱动推荐主流程

        流程: 抓取新闻 → LLM 分析 → 概念映射 → 量化过滤 → 打分排序 → LLM 增强

        Args:
            limit: 返回推荐数量

        Returns:
            包含推荐列表和策略报告的字典
        """
        cache_key = f"event_driven_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            logger.info("Returning cached event-driven recommendations")
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            # 模块1: 抓取新闻
            logger.info("=== Event-Driven 模块1: 新闻捕获 ===")
            news_list = await loop.run_in_executor(None, self._fetch_news)

            # 模块2: LLM 语义分析
            logger.info("=== Event-Driven 模块2: LLM 语义分析 ===", news_count=len(news_list))
            try:
                from event_driven_llm import event_driven_llm
            except ImportError:
                from app.event_driven_llm import event_driven_llm

            events = await event_driven_llm.analyze_events(news_list)

            # 模块3: 概念-股票映射
            logger.info("=== Event-Driven 模块3: 概念-股票映射 ===", events_count=len(events))
            candidates = await loop.run_in_executor(
                None, self._map_events_to_stocks, events
            )

            # 模块4: 量化过滤
            logger.info("=== Event-Driven 模块4: 量化过滤 ===", candidates_before=len(candidates))
            filtered = await loop.run_in_executor(
                None, self._filter_stocks, candidates
            )

            # 模块5: 加权打分排序
            logger.info("=== Event-Driven 模块5: 打分排序 ===", candidates_after=len(filtered))
            scored = self._score_and_rank(filtered, events, limit)

            # 模块6: 新闻整理
            logger.info("=== Event-Driven 模块6: 新闻整理 ===")
            news_digest = self._build_news_digest(news_list, events)

            # 构建基础结果
            result = self._build_result(scored, events, news_list, news_digest, limit)

            # LLM 增强推荐
            if scored:
                try:
                    llm_result = await event_driven_llm.enhance_recommendations(
                        candidates=scored,
                        events=events,
                    )
                    if llm_result:
                        if llm_result.get("enhanced_stocks"):
                            result["data"]["recommendations"] = llm_result["enhanced_stocks"][:limit]
                        if llm_result.get("strategy_report"):
                            result["data"]["strategy_report"] = llm_result["strategy_report"]
                        if llm_result.get("market_assessment"):
                            result["data"]["market_assessment"] = llm_result["market_assessment"]
                        result["data"]["llm_enhanced"] = True
                except Exception as llm_e:
                    logger.error("Event LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False
            else:
                result["data"]["llm_enhanced"] = False

            # 更新缓存
            self._cache[cache_key] = result
            self._cache_time = datetime.now()

            return result

        except Exception as e:
            logger.error("Event-driven strategy failed", error=str(e),
                         traceback=traceback.format_exc())
            raise

    # ==================== 模块1: 多维度新闻聚合引擎 ====================

    def _fetch_news(self) -> List[Dict[str, str]]:
        """
        多维度新闻聚合引擎（优化版）

        并行聚合5大数据源，质量排序后返回：
        1. 财联社电报 — A股最快速最专业的资讯
        2. 同花顺财经直播 — 独家分析内容
        3. 东方财富全球快讯 — 覆盖面最广
        4. 板块异动信号 — 发现当日突然异动的概念板块
        5. 百度热搜股票 — 市场关注焦点
        """
        all_news = []

        # ===== 源1: 财联社电报（A股最核心资讯源）=====
        cls_news = self._fetch_cls_news()
        all_news.extend(cls_news)

        # 【优化】源之间反爬延迟
        anti_scrape_delay("news_between_cls_ths", *DELAY_LIGHT)

        # ===== 源2: 同花顺财经直播 =====
        ths_news = self._fetch_ths_news()
        all_news.extend(ths_news)

        anti_scrape_delay("news_between_ths_em", *DELAY_LIGHT)

        # ===== 源3: 东方财富全球快讯 =====
        em_news = self._fetch_em_news()
        all_news.extend(em_news)

        anti_scrape_delay("news_between_em_board", *DELAY_LIGHT)

        # ===== 源4: 板块异动信号（转化为新闻条目）=====
        board_news = self._fetch_board_change_news()
        all_news.extend(board_news)

        # ===== 源5: 百度热搜股票（转化为新闻条目）=====
        hot_news = self._fetch_hot_search_news()
        all_news.extend(hot_news)

        # 去重
        all_news = self._dedup_news(all_news)

        # 质量排序：A股相关度高的排前面
        all_news = self._rank_news_quality(all_news)

        logger.info("Multi-source news aggregation completed",
                     total=len(all_news),
                     cls=len(cls_news), ths=len(ths_news),
                     em=len(em_news), board=len(board_news),
                     hot=len(hot_news))
        return all_news

    def _fetch_cls_news(self) -> List[Dict[str, str]]:
        """财联社电报 — A股最快速专业的资讯，复用 dragon_data_provider 多源降级"""
        news = []
        try:
            try:
                from dragon_data_provider import get_financial_news
            except ImportError:
                from app.dragon_data_provider import get_financial_news

            df = get_financial_news()
            if df is not None and not df.empty:
                for _, row in df.head(30).iterrows():
                    title, content, pub_time = "", "", ""
                    for col in df.columns:
                        c = str(col)
                        if "标题" in c or "内容" in c:
                            val = str(row[col]) if pd.notna(row[col]) else ""
                            if not title:
                                title = val
                            else:
                                content = val
                        elif "时间" in c or "发布" in c:
                            pub_time = str(row[col]) if pd.notna(row[col]) else ""
                    if title:
                        news.append({
                            "title": title, "content": content or title,
                            "source": "财联社", "time": pub_time,
                            "category": "A股资讯",
                        })
                logger.info("Fetched news via dragon_data_provider", count=len(news))
        except Exception as e:
            logger.warning("Failed to fetch news via provider", error=str(e))
        return news

    def _fetch_ths_news(self) -> List[Dict[str, str]]:
        """同花顺全球财经直播（带反爬延迟）"""
        news = []
        try:
            anti_scrape_delay("ths_news", *DELAY_LIGHT)
            df = self.ak.stock_info_global_ths()
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    title, content = "", ""
                    for col in df.columns:
                        c = str(col)
                        if "标题" in c:
                            title = str(row[col]) if pd.notna(row[col]) else ""
                        elif "内容" in c or "摘要" in c:
                            content = str(row[col]) if pd.notna(row[col]) else ""
                    if title:
                        news.append({
                            "title": title, "content": content or title,
                            "source": "同花顺", "time": "",
                            "category": "财经直播",
                        })
                logger.info("Fetched THS live", count=len(news))
        except Exception as e:
            logger.warning("Failed to fetch THS news", error=str(e))
        return news

    def _fetch_em_news(self) -> List[Dict[str, str]]:
        """东方财富全球财经快讯（带反爬延迟）"""
        news = []
        try:
            anti_scrape_delay("em_news", *DELAY_NORMAL)
            df = self.ak.stock_info_global_em()
            if df is not None and not df.empty:
                for _, row in df.head(50).iterrows():
                    title, content, pub_time = "", "", ""
                    for col in df.columns:
                        c = str(col).lower()
                        if "标题" in c or "title" in c:
                            title = str(row[col]) if pd.notna(row[col]) else ""
                        elif "内容" in c or "content" in c:
                            content = str(row[col]) if pd.notna(row[col]) else ""
                        elif "时间" in c or "date" in c or "time" in c:
                            pub_time = str(row[col]) if pd.notna(row[col]) else ""
                    if title:
                        news.append({
                            "title": title, "content": content or title,
                            "source": "东方财富", "time": pub_time,
                            "category": "全球快讯",
                        })
                logger.info("Fetched EM global news", count=len(news))
        except Exception as e:
            logger.warning("Failed to fetch EM news", error=str(e))
        return news

    def _fetch_board_change_news(self) -> List[Dict[str, str]]:
        """板块异动信号 — 发现当日异动最大的概念板块（带反爬延迟）"""
        news = []
        try:
            anti_scrape_delay("board_change", *DELAY_NORMAL)
            df = self.ak.stock_board_change_em()
            if df is not None and not df.empty:
                name_col = None
                for col in df.columns:
                    if "板块" in str(col) and "名称" in str(col):
                        name_col = col
                        break
                if not name_col:
                    name_col = df.columns[0]
                # 排除过于宽泛的板块
                skip_boards = {"融资融券", "深股通", "沪股通", "创业板综",
                               "富时罗素", "MSCI", "标普道琼斯"}
                for _, row in df.head(20).iterrows():
                    board = str(row[name_col]) if pd.notna(row[name_col]) else ""
                    if board and len(board) > 1 and board not in skip_boards:
                        news.append({
                            "title": f"【板块异动】{board}板块出现明显异动",
                            "content": f"今日{board}概念板块出现显著盘口异动，"
                                       f"建议关注相关个股投资机会。",
                            "source": "板块异动监控", "time": "",
                            "category": "板块异动",
                        })
                logger.info("Fetched board change signals", count=len(news))
        except Exception as e:
            logger.warning("Failed to fetch board changes", error=str(e))
        return news

    def _fetch_hot_search_news(self) -> List[Dict[str, str]]:
        """百度热搜股票 — 市场关注焦点"""
        news = []
        try:
            today_str = datetime.now().strftime("%Y%m%d")
            anti_scrape_delay("hot_search", *DELAY_NORMAL)
            df = self.ak.stock_hot_search_baidu(
                symbol="A股", date=today_str, time="今日"
            )
            if df is not None and not df.empty:
                for _, row in df.head(15).iterrows():
                    name_code, change, heat = "", "", ""
                    for col in df.columns:
                        c = str(col)
                        if "名称" in c or "代码" in c:
                            name_code = str(row[col]) if pd.notna(row[col]) else ""
                        elif "涨跌" in c:
                            change = str(row[col]) if pd.notna(row[col]) else ""
                        elif "热度" in c:
                            heat = str(row[col]) if pd.notna(row[col]) else ""
                    if name_code:
                        news.append({
                            "title": f"【热搜】{name_code} 涨跌幅{change} 热度{heat}",
                            "content": f"百度热搜股票: {name_code}，今日涨跌幅{change}，"
                                       f"综合热度{heat}，市场高度关注。",
                            "source": "百度热搜", "time": "",
                            "category": "热搜股票",
                        })
                logger.info("Fetched Baidu hot search", count=len(news))
        except Exception as e:
            logger.warning("Failed to fetch hot search", error=str(e))
        return news

    def _dedup_news(self, news_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """新闻去重（基于标题前缀）"""
        seen = set()
        result = []
        for n in news_list:
            title = n.get("title", "").strip()
            key = title[:20] if len(title) > 20 else title
            if key and key not in seen:
                seen.add(key)
                result.append(n)
        logger.info("News dedup", before=len(news_list), after=len(result))
        return result

    def _rank_news_quality(self, news_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        按A股相关度对新闻排序

        评分: A股术语+3, 行业关键词+2, 优质来源+2, 含股票代码+3, 国际噪音-2
        """
        a_kw = {"涨停", "跌停", "板块", "概念", "利好", "利空", "龙头",
                "封板", "连板", "打板", "炸板", "换手", "放量", "缩量",
                "主力", "机构", "游资", "北向", "融资", "回购",
                "公告", "业绩", "预增", "中标", "合同", "订单",
                "政策", "补贴", "规划", "改革"}
        ind_kw = {"碳纤维", "芯片", "半导体", "新能源", "光伏", "锂电",
                  "军工", "航天", "AI", "人工智能", "机器人", "无人驾驶",
                  "医药", "创新药", "生物", "数据", "算力", "存储",
                  "复合材料", "碳中和", "储能", "充电桩", "传感器",
                  "稀土", "黄金", "固态电池", "氢能", "低空经济",
                  "消费", "白酒", "汽车", "化工", "钢铁", "有色"}
        good_src = {"财联社", "板块异动监控", "百度热搜", "同花顺"}
        noise = {"美国总统", "欧洲央行", "英国首相", "以色列", "俄罗斯",
                 "乌克兰", "巴勒斯坦", "北约", "联合国",
                 "委内瑞拉", "阿根廷", "日本央行", "韩国总统"}

        for n in news_list:
            score = 0
            text = n.get("title", "") + n.get("content", "")
            if any(k in text for k in a_kw):
                score += 3
            if any(k in text for k in ind_kw):
                score += 2
            if n.get("source", "") in good_src:
                score += 2
            if re.search(r'\d{6}', text):
                score += 3
            if any(k in text for k in noise):
                score -= 2
            n["_quality_score"] = score

        news_list.sort(key=lambda x: x.get("_quality_score", 0), reverse=True)
        return news_list

    # ==================== 模块3: 概念-股票映射 ====================

    def _map_events_to_stocks(self, events: List[Dict]) -> List[Dict[str, Any]]:
        """
        将事件分析结果映射到具体股票

        策略优先级（连接错误快速中断）：
        1. 东方财富概念板块 stock_board_concept_name_em (1次尝试)
        2. 同花顺概念板块 stock_board_concept_name_ths (不同服务器)
        3. 涨停池 stock_zt_pool_em (已验证服务器可用)
        """
        stock_map: Dict[str, Dict] = {}  # code -> stock_info
        stock_events: Dict[str, List] = {}  # code -> [event_refs]

        if not events:
            return []

        import time

        # 【优化】复用 dragon_data_provider 的多源降级概念板块接口
        try:
            from dragon_data_provider import get_concept_boards, get_limit_up_stocks
        except ImportError:
            from app.dragon_data_provider import get_concept_boards, get_limit_up_stocks

        # 收集所有需要查找的板块关键词
        sector_keywords = set()
        for event in events:
            for sector in event.get("sectors", []):
                sector_keywords.add(sector)
            for kw in event.get("keywords", []):
                sector_keywords.add(kw)

        logger.info("Mapping sectors to stocks", keywords=list(sector_keywords)[:10])

        # 标记东财 _em 接口是否可达
        em_dead = False

        # ===== 策略1: 复用 dragon_data_provider 概念板块（共享多源降级） =====
        concept_map = {}
        try:
            logger.info("Getting concept boards via dragon_data_provider")
            concept_df = get_concept_boards()
            if concept_df is not None and not concept_df.empty:
                name_col = None
                for col in concept_df.columns:
                    if "板块名称" in col or "概念名称" in col or "概念" in col or "名称" in col:
                        name_col = col
                        break
                if name_col:
                    for _, row in concept_df.iterrows():
                        cname = str(row[name_col])
                        concept_map[cname] = cname
                logger.info("Got concept board list", count=len(concept_map))
        except Exception as e:
            logger.warning("Concept board via provider failed", error=str(e))

        # 通过关键词匹配概念板块
        matched_concepts = set()
        if concept_map:
            for kw in sector_keywords:
                for concept_name in concept_map:
                    if kw in concept_name or concept_name in kw:
                        matched_concepts.add(concept_name)

        # 获取成分股（复用 dragon_data_provider）
        if matched_concepts:
            for concept_name in list(matched_concepts)[:5]:
                try:
                    anti_scrape_delay(f"concept_cons_{concept_name}", *DELAY_NORMAL)
                    try:
                        from dragon_data_provider import get_stock_concepts
                    except ImportError:
                        from app.dragon_data_provider import get_stock_concepts
                    # 尝试用 akshare 的成分股接口
                    cons_df = self.ak.stock_board_concept_cons_em(symbol=concept_name)
                    if cons_df is not None and not cons_df.empty:
                        self._extract_stocks_from_df(
                            cons_df, concept_name, stock_map, stock_events
                        )
                        logger.info("Mapped concept stocks",
                                     concept=concept_name, count=min(30, len(cons_df)))
                except Exception as e:
                    err_str = str(e)
                    if "RemoteDisconnected" in err_str or "Connection" in err_str:
                        logger.warning("Concept cons dead, trying THS fallback")
                        break
                    logger.warning("Failed concept lookup",
                                   concept=concept_name, error=err_str)

        if stock_map:
            logger.info("EM strategy succeeded", count=len(stock_map))
        else:
            logger.info("EM strategy yielded 0 stocks, trying THS fallback")

        # ===== 策略2: 同花顺概念板块 (完全不同的服务器) =====
        if not stock_map:
            try:
                logger.info("Trying THS concept board list")
                ths_concept_df = self.ak.stock_board_concept_name_ths()
                if ths_concept_df is not None and not ths_concept_df.empty:
                    # 找概念名列
                    ths_name_col = None
                    for col in ths_concept_df.columns:
                        if "概念" in col or "板块" in col or "名称" in col:
                            ths_name_col = col
                            break
                    if not ths_name_col:
                        ths_name_col = ths_concept_df.columns[1] if len(ths_concept_df.columns) > 1 else ths_concept_df.columns[0]

                    # 匹配关键词
                    ths_matched = set()
                    for _, row in ths_concept_df.iterrows():
                        cname = str(row[ths_name_col])
                        for kw in sector_keywords:
                            if kw in cname or cname in kw:
                                ths_matched.add(cname)

                    logger.info("THS matched concepts",
                                 count=len(ths_matched),
                                 concepts=list(ths_matched)[:5])

                    # 获取 THS 成分股
                    for concept_name in list(ths_matched)[:5]:
                        try:
                            anti_scrape_delay(f"ths_cons_{concept_name}", *DELAY_NORMAL)
                            cons_df = self.ak.stock_board_concept_cons_ths(symbol=concept_name)
                            if cons_df is not None and not cons_df.empty:
                                self._extract_stocks_from_df(
                                    cons_df, concept_name, stock_map, stock_events
                                )
                                logger.info("Mapped THS concept stocks",
                                             concept=concept_name,
                                             count=min(30, len(cons_df)))
                        except Exception as e:
                            logger.warning("Failed THS concept lookup",
                                           concept=concept_name, error=str(e))

                    if stock_map:
                        logger.info("THS strategy succeeded", count=len(stock_map))

            except Exception as e:
                logger.warning("THS concept board failed", error=str(e))

        # ===== 策略3: 涨停池降级 (复用 dragon_data_provider) =====
        if not stock_map:
            logger.info("Concept mapping failed, falling back to limit-up pool")
            try:
                zt_df = get_limit_up_stocks()
                if zt_df is not None and not zt_df.empty:
                    self._extract_zt_pool_stocks(
                        zt_df, sector_keywords, events, stock_map, stock_events
                    )
                    logger.info("Limit-up pool fallback succeeded",
                                 count=len(stock_map))
            except Exception as e:
                logger.warning("Limit-up pool also failed", error=str(e))

        # 合并事件关联信息
        for code, stock in stock_map.items():
            related = stock_events.get(code, [])
            stock["related_concepts"] = list(set(related))
            stock["concept_count"] = len(set(related))
            # 找到最高影响等级的相关事件
            max_impact = 1
            event_reason = ""
            for event in events:
                event_sectors = event.get("sectors", []) + event.get("keywords", [])
                for concept in related:
                    if any(kw in concept or concept in kw for kw in event_sectors):
                        if event.get("impact_level", 1) > max_impact:
                            max_impact = event["impact_level"]
                            event_reason = event.get("summary", event.get("title", ""))
            stock["max_impact_level"] = max_impact
            stock["event_reason"] = event_reason

        candidates = list(stock_map.values())
        logger.info("Total candidate stocks mapped", count=len(candidates))
        return candidates

    def _extract_stocks_from_df(
        self, df: pd.DataFrame, concept_name: str,
        stock_map: Dict[str, Dict], stock_events: Dict[str, List]
    ):
        """从 DataFrame 中提取股票信息到 stock_map（通用：EM/THS 概念板块成分股）"""
        code_col = name_col = price_col = change_col = None
        turnover_col = market_col = amount_col = None

        for col in df.columns:
            if "代码" in col and not code_col:
                code_col = col
            elif "名称" in col and not name_col:
                name_col = col
            elif "最新价" in col and not price_col:
                price_col = col
            elif "涨跌幅" in col and not change_col:
                change_col = col
            elif "换手率" in col and not turnover_col:
                turnover_col = col
            elif "总市值" in col and not market_col:
                market_col = col
            elif "成交额" in col and not amount_col:
                amount_col = col

        if not code_col:
            return

        for _, row in df.head(30).iterrows():
            code = str(row[code_col])
            if code in stock_map:
                stock_events.setdefault(code, []).append(concept_name)
                continue

            stock_info = {
                "code": code,
                "name": str(row[name_col]) if name_col else "",
                "price": float(row[price_col]) if price_col and pd.notna(row[price_col]) else 0,
                "change_pct": float(row[change_col]) if change_col and pd.notna(row[change_col]) else 0,
                "turnover_rate": float(row[turnover_col]) if turnover_col and pd.notna(row[turnover_col]) else 0,
                "total_market_cap": float(row[market_col]) if market_col and pd.notna(row[market_col]) else 0,
                "amount": float(row[amount_col]) if amount_col and pd.notna(row[amount_col]) else 0,
                "related_concepts": [concept_name],
            }
            stock_map[code] = stock_info
            stock_events.setdefault(code, []).append(concept_name)

    def _extract_zt_pool_stocks(
        self, zt_df: pd.DataFrame, keywords: set, events: List[Dict],
        stock_map: Dict[str, Dict], stock_events: Dict[str, List]
    ):
        """
        从涨停池提取股票（终极降级方案）
        涨停池(stock_zt_pool_em)字段: 代码, 名称, 最新价, 涨跌幅, 成交额, 换手率, 连板数, 所属行业
        """
        col_map = {}
        for col in zt_df.columns:
            if '代码' in col and 'code' not in col_map.values():
                col_map[col] = 'code'
            elif '名称' in col and 'name' not in col_map.values():
                col_map[col] = 'name'
            elif '最新价' in col and 'price' not in col_map.values():
                col_map[col] = 'price'
            elif '涨跌幅' in col and '涨跌额' not in col and 'change_pct' not in col_map.values():
                col_map[col] = 'change_pct'
            elif '成交额' in col and 'amount' not in col_map.values():
                col_map[col] = 'amount'
            elif '换手率' in col and 'turnover' not in col_map.values():
                col_map[col] = 'turnover'
            elif '总市值' in col and 'total_mv' not in col_map.values():
                col_map[col] = 'total_mv'
            elif ('连板' in col or '涨停天' in col) and 'limit_days' not in col_map.values():
                col_map[col] = 'limit_days'
            elif '行业' in col and 'industry' not in col_map.values():
                col_map[col] = 'industry'

        df = zt_df.rename(columns=col_map)

        # 过滤 ST
        if 'name' in df.columns:
            df = df[~df['name'].str.contains('ST|退', na=False)]

        # 数值转换
        for c in ['price', 'change_pct', 'amount', 'turnover', 'total_mv', 'limit_days']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

        # 按连板数排序
        if 'limit_days' in df.columns:
            df = df.sort_values('limit_days', ascending=False)

        for _, row in df.head(50).iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            if not code or not name:
                continue

            # 涨停池的股票用 "涨停活跃" 标记
            tag = "涨停活跃"
            industry = str(row.get('industry', ''))

            # 尝试和事件关键词关联
            matched_kw = []
            for kw in keywords:
                if kw in name or kw in industry:
                    matched_kw.append(kw)

            stock_info = {
                "code": code,
                "name": name,
                "price": float(row.get('price', 0)),
                "change_pct": float(row.get('change_pct', 0)),
                "turnover_rate": float(row.get('turnover', 0)),
                "total_market_cap": float(row.get('total_mv', 0)),
                "amount": float(row.get('amount', 0)),
                "related_concepts": matched_kw if matched_kw else [tag],
            }
            stock_map[code] = stock_info
            for mk in (matched_kw if matched_kw else [tag]):
                stock_events.setdefault(code, []).append(mk)

        logger.info("Extracted stocks from limit-up pool", count=len(stock_map))

    # ==================== 模块4: 量化过滤 ====================

    def _filter_stocks(self, candidates: List[Dict]) -> List[Dict]:
        """
        量化过滤（优化版 — 收紧阈值）：
        1. 基本过滤 — 剔除 ST 股、无价格股
        2. 空间过滤 — 剔除涨幅已超 11%（已封板无操作空间）和跌幅超-5%（趋势太差）
        3. 筹码过滤 — 换手率超 20% 剔除（分歧太大）
        4. 【新增】成交额过滤 — 成交额 < 5000万 剔除（无流动性）
        """
        filtered = []
        skip_st = 0
        skip_price = 0
        skip_high = 0
        skip_low = 0
        skip_turnover = 0
        skip_amount = 0

        for stock in candidates:
            code = stock.get("code", "")
            name = stock.get("name", "")
            price = stock.get("price", 0)
            change_pct = stock.get("change_pct", 0)
            turnover = stock.get("turnover_rate", 0)
            amount = stock.get("amount", 0)

            # 基本过滤
            if not code or price <= 0:
                skip_price += 1
                continue
            if "ST" in name.upper() or "退" in name:
                skip_st += 1
                continue

            # 【优化】空间过滤：涨幅>11% 已封板无操作空间
            if change_pct > 11:
                skip_high += 1
                continue
            # 【优化】跌幅超-5% 说明趋势极差
            if change_pct < -5:
                skip_low += 1
                continue

            # 【优化】换手率收紧到20%
            if turnover > 20:
                skip_turnover += 1
                continue

            # 【新增】成交额过滤：< 5000万的无流动性
            if amount > 0 and amount < 5e7:
                skip_amount += 1
                continue

            filtered.append(stock)

        logger.info("Filtered stocks", before=len(candidates), after=len(filtered),
                     skip_st=skip_st, skip_price=skip_price,
                     skip_high=skip_high, skip_low=skip_low,
                     skip_turnover=skip_turnover, skip_amount=skip_amount)
        return filtered

    # ==================== 模块5: 加权打分排序 ====================

    def _score_and_rank(self, candidates: List[Dict], events: List[Dict],
                        limit: int) -> List[Dict]:
        """
        加权打分系统（优化版 — 增加事件新鲜度维度）：
        - 逻辑强度 35%: 影响等级 × 产业链核心程度
        - 资金认可 30%: 涨幅 + 成交额
        - 历史胜率 15%: 概念板块关联数 (越多越好)
        - 事件新鲜度 10%: 【新增】首次事件加分，旧闻降分
        - 稀缺性 10%: 概念唯一性
        """
        if not candidates:
            return []

        # 归一化参数
        max_impact = max((s.get("max_impact_level", 1) for s in candidates), default=5)
        max_concepts = max((s.get("concept_count", 1) for s in candidates), default=1)
        max_change = max((abs(s.get("change_pct", 0)) for s in candidates), default=1)
        max_amount = max((s.get("amount", 0) for s in candidates), default=1)

        # 统计概念出现频率（用于稀缺性）
        concept_freq = Counter()
        for s in candidates:
            for c in s.get("related_concepts", []):
                concept_freq[c] += 1

        # 【新增】事件新鲜度映射
        freshness_map = {"首次": 1.0, "跟踪": 0.5, "旧闻": 0.2, "未知": 0.6}

        for stock in candidates:
            # 逻辑强度 (35%)
            impact = stock.get("max_impact_level", 1) / max(max_impact, 1)
            concept_depth = min(stock.get("concept_count", 1), 5) / 5
            logic_score = (impact * 0.7 + concept_depth * 0.3) * 35

            # 资金认可 (30%) — 正向涨幅 + 成交额
            change_norm = max(stock.get("change_pct", 0), 0) / max(max_change, 1)
            amount_norm = stock.get("amount", 0) / max(max_amount, 1)
            capital_score = (change_norm * 0.5 + amount_norm * 0.5) * 30

            # 历史胜率近似 (15%) — 用概念关联数近似
            history_score = min(stock.get("concept_count", 1), 5) / 5 * 15

            # 【新增】事件新鲜度 (10%)
            # 找到关联事件的新鲜度
            freshness_val = 0.6  # 默认
            for ev in events:
                event_sectors = ev.get("sectors", []) + ev.get("keywords", [])
                related = stock.get("related_concepts", [])
                if any(kw in c or c in kw for c in related for kw in event_sectors):
                    f = freshness_map.get(ev.get("freshness", "未知"), 0.6)
                    freshness_val = max(freshness_val, f)
            freshness_score = freshness_val * 10

            # 稀缺性 (10%) — 概念越少人有越稀缺
            avg_freq = 1
            related = stock.get("related_concepts", [])
            if related:
                avg_freq = sum(concept_freq.get(c, 1) for c in related) / len(related)
            scarcity = max(0, 1 - avg_freq / max(len(candidates), 1))
            scarcity_score = scarcity * 10

            total_score = logic_score + capital_score + history_score + freshness_score + scarcity_score
            stock["event_score"] = round(total_score, 2)
            stock["score_detail"] = {
                "logic": round(logic_score, 1),
                "capital": round(capital_score, 1),
                "history": round(history_score, 1),
                "freshness": round(freshness_score, 1),
                "scarcity": round(scarcity_score, 1),
            }

        # 排序
        candidates.sort(key=lambda x: x.get("event_score", 0), reverse=True)

        # 取前 limit 只
        top_stocks = candidates[:limit]

        # 添加排名
        for i, stock in enumerate(top_stocks, 1):
            stock["rank"] = i

        logger.info("Scored and ranked", total=len(candidates), selected=len(top_stocks),
                     top_score=top_stocks[0]["event_score"] if top_stocks else 0)

        return top_stocks

    # ==================== 模块6: 新闻整理 ====================

    def _build_news_digest(self, news_list: List[Dict], events: List[Dict]) -> Dict[str, Any]:
        """
        整理今日新闻摘要：按来源/分类/关联度分类组织

        输出:
        - by_source: 各来源新闻数量统计
        - by_category: 各分类新闻数量统计
        - high_quality_news: 高质量A股相关新闻
        - board_signals: 板块异动信号列表
        - hot_stocks: 热搜股票列表
        - event_related_news: 与事件分析关联的新闻
        """
        digest = {
            "total_captured": len(news_list),
            "by_source": {},
            "by_category": {},
            "high_quality_news": [],
            "board_signals": [],
            "hot_stocks": [],
            "event_related_news": [],
        }

        # 按来源 & 分类统计
        for n in news_list:
            src = n.get("source", "未知")
            cat = n.get("category", "其他")
            digest["by_source"][src] = digest["by_source"].get(src, 0) + 1
            digest["by_category"][cat] = digest["by_category"].get(cat, 0) + 1

        # 高质量新闻 (评分 >= 3)
        for n in news_list:
            if n.get("_quality_score", 0) >= 3:
                digest["high_quality_news"].append({
                    "title": n["title"],
                    "content": n.get("content", "")[:200],
                    "source": n.get("source", ""),
                    "category": n.get("category", ""),
                    "quality_score": n.get("_quality_score", 0),
                })
                if len(digest["high_quality_news"]) >= 20:
                    break

        # 板块异动
        digest["board_signals"] = [
            n["title"] for n in news_list
            if n.get("category") == "板块异动"
        ]

        # 热搜股票
        digest["hot_stocks"] = [
            n["title"] for n in news_list
            if n.get("category") == "热搜股票"
        ]

        # 与事件分析关联的新闻
        if events:
            event_keywords = set()
            for e in events:
                for kw in e.get("keywords", []):
                    event_keywords.add(kw)
                for s in e.get("sectors", []):
                    event_keywords.add(s)

            for n in news_list:
                text = n.get("title", "") + n.get("content", "")
                matched = [kw for kw in event_keywords if kw in text]
                if matched:
                    digest["event_related_news"].append({
                        "title": n["title"],
                        "source": n.get("source", ""),
                        "matched_keywords": matched[:3],
                    })
                    if len(digest["event_related_news"]) >= 15:
                        break

        logger.info("News digest built",
                     high_quality=len(digest["high_quality_news"]),
                     board_signals=len(digest["board_signals"]),
                     hot_stocks=len(digest["hot_stocks"]),
                     event_related=len(digest["event_related_news"]))
        return digest

    # ==================== 结果构建 ====================

    def _build_result(self, scored: List[Dict], events: List[Dict],
                      news_list: List[Dict], news_digest: Dict[str, Any],
                      limit: int) -> Dict[str, Any]:
        """构建最终返回结果"""
        now = datetime.now()

        # 提取高影响事件
        high_impact_events = [e for e in events if e.get("impact_level", 0) >= 3]

        # 构建策略说明
        explanation = self._generate_explanation(scored, events, news_list, news_digest)

        result = {
            "status": "success",
            "data": {
                "recommendations": scored[:limit],
                "total": len(scored),
                "events": {
                    "total_analyzed": len(news_list),
                    "high_impact_count": len(high_impact_events),
                    "high_impact_events": high_impact_events[:5],
                    "overall_assessment": events[0].get("overall_assessment", "") if events else "",
                },
                "news_digest": news_digest,
                "market_assessment": {},
                "strategy_report": explanation,
                "llm_enhanced": False,
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
            }
        }

        return result

    def _generate_explanation(self, scored: List[Dict], events: List[Dict],
                               news_list: List[Dict],
                               news_digest: Dict[str, Any] = None) -> str:
        """生成策略推荐逻辑说明（非LLM版本）"""
        now = datetime.now()
        lines = [
            f"## 事件驱动战法推荐 ({now.strftime('%Y-%m-%d %H:%M')})",
            "",
        ]

        # 多维度新闻聚合统计
        lines.append("### 📡 多维度新闻聚合")
        if news_digest:
            by_src = news_digest.get("by_source", {})
            src_detail = "、".join([f"{k}{v}条" for k, v in by_src.items()])
            lines.append(
                f"共从 **{len(by_src)}** 个数据源聚合 **{len(news_list)}** 条资讯"
                f"（{src_detail}），"
                f"经质量排序筛选出 **{len(news_digest.get('high_quality_news', []))}** "
                f"条高质量A股相关新闻。"
            )
        else:
            lines.append(f"共捕获 **{len(news_list)}** 条财经新闻。")

        lines.append(
            f"经 LLM 分析识别出 **{len(events)}** 个有投资价值的事件。"
        )
        lines.append("")

        # 板块异动信号
        if news_digest and news_digest.get("board_signals"):
            lines.append("### 📊 板块异动信号")
            for sig in news_digest["board_signals"][:8]:
                lines.append(f"- {sig}")
            lines.append("")

        # 热搜股票
        if news_digest and news_digest.get("hot_stocks"):
            lines.append("### 🔥 市场热搜焦点")
            for hs in news_digest["hot_stocks"][:5]:
                lines.append(f"- {hs}")
            lines.append("")

        # 高影响事件
        high = [e for e in events if e.get("impact_level", 0) >= 3]
        if high:
            lines.append("### 💥 高影响事件")
            for e in high[:5]:
                lines.append(
                    f"- **{e.get('title', '?')}** (影响{e.get('impact_level', '?')}级, "
                    f"{e.get('impact_duration', '未知')})"
                )
                if e.get("logic_chain"):
                    lines.append(f"  逻辑链: {e['logic_chain']}")
            lines.append("")

        # 推荐概览
        if scored:
            lines.append(f"### 🎯 推荐个股 (Top {len(scored)})")
            for s in scored[:5]:
                lines.append(
                    f"- {s.get('name', '?')}({s.get('code', '?')}) "
                    f"评分:{s.get('event_score', 0):.1f} "
                    f"涨跌:{s.get('change_pct', 0):+.2f}%"
                )
            lines.append("")

        lines.append("### ⚠️ 风险提示")
        lines.append("事件驱动策略依赖新闻时效性，存在\"见光死\"风险。建议配合量价验证后入场。")

        return "\n".join(lines)


# 全局单例
event_driven_strategy = EventDrivenStrategy()
