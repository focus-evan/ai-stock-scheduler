"""
综合战法（Combined Strategy）核心逻辑模块 — 适配实盘跟投（深度优化版）

从当前激活的战法推荐缓存中筛选高质量交集：
- **只读取** ACTIVE_STRATEGIES 中的战法推荐缓存
- **只取** recommendation_level 为「推荐」或「强烈推荐」的股票
- 找出在 >=2 个战法中同时被推荐的股票（多战法共振）
- 如果交集为空，降级取各战法排名前3的合格推荐
- 输出跟投友好信息：建议买入时段、仓位建议、操作摘要

优化要点：
- 排序优先考虑权重总分（weight_sum），重要战法共振的权重更高
- 止损统一 3%（综合战法胜率高，止损可以收紧）
- 降级策略优化：优先取强烈推荐 + 多维度评分
- 买入时机建议更细化（竞价/突破/趋势回调等场景）
"""

import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

import structlog

logger = structlog.get_logger()

# 当前激活的策略（赛马模式：所有战法并行运行）
ACTIVE_STRATEGIES = [
    "dragon_head", "sentiment", "event_driven",
    "breakthrough", "volume_price", "overnight",
    "moving_average", "northbound", "trend_momentum",
]

# 所有战法
ALL_STRATEGIES = [
    "dragon_head", "sentiment", "event_driven",
    "breakthrough", "volume_price", "overnight",
    "moving_average", "northbound", "trend_momentum",
]

# 战法中文名称映射
STRATEGY_NAMES = {
    "dragon_head": "龙头战法",
    "sentiment": "情绪战法",
    "event_driven": "事件驱动",
    "breakthrough": "突破战法",
    "volume_price": "量价关系",
    "overnight": "隔夜施工法",
    "moving_average": "均线战法",
    "northbound": "北向资金",
    "trend_momentum": "趋势动量",
}

# 【新增】战法权重（不同战法的共振贡献度不同）
STRATEGY_WEIGHTS = {
    "dragon_head": 1.5,     # 龙头战法经过多年验证，权重最高
    "sentiment": 1.2,       # 情绪战法能捕捉市场热度
    "event_driven": 1.3,    # 事件驱动有明确催化剂
    "breakthrough": 1.4,    # 突破战法信号明确
    "volume_price": 1.3,    # 量价关系能验证主力动向
    "overnight": 1.0,         # 隔夜施工法时间较短，权重稍低
    "moving_average": 1.1,  # 均线有滞后性
    "northbound": 1.3,      # 北向资金"聪明钱"指引
    "trend_momentum": 1.2,  # 趋势动量有延续效应
}

# 最低交集数量
MIN_INTERSECTION = 2

# 每日最少保证推荐数量（给实盘跟投留好操作空间）
MIN_GUARANTEED = 3

# 合格的推荐等级
QUALIFIED_LEVELS = {"推荐", "强烈推荐"}


class CombinedStrategy:
    """综合战法核心策略类（适配实盘跟投）"""

    def __init__(self):
        self._cache: Dict[str, Any] = {}

    async def get_recommendations(
        self, limit: int = 5, min_intersection: int = MIN_INTERSECTION,
        use_active_only: bool = True,
    ) -> Dict[str, Any]:
        """
        获取综合战法推荐列表（适配实盘跟投）

        流程:
        1. 读取激活战法的最新推荐缓存
        2. 按股票代码聚合，统计出现在几个战法中
        3. 筛选出现在 >= min_intersection 个战法中的股票
        4. 如果交集为空，降级取各战法Top精选
        5. 返回 Top N（含跟投友好信息）

        Args:
            limit: 推荐数量上限
            min_intersection: 最少出现在几个战法中才算有效推荐
            use_active_only: 是否只从激活战法中取数据

        Returns:
            推荐结果字典
        """
        try:
            # 1. 读取激活战法的推荐
            strategies = ACTIVE_STRATEGIES if use_active_only else ALL_STRATEGIES
            all_recs = await self._load_all_recommendations(strategies)

            # 2. 聚合交集
            intersection_stocks = self._find_intersection(
                all_recs, min_intersection
            )

            # 2.5 如果交集为空，降级取各战法Top精选
            if not intersection_stocks:
                logger.info("No intersection found, falling back to top picks")
                intersection_stocks = self._fallback_top_picks(all_recs)

            # 2.6 如果降级后仍不足 MIN_GUARANTEED 只，启用紧急降级
            if len(intersection_stocks) < MIN_GUARANTEED:
                logger.warning(
                    "Still below minimum guaranteed, using emergency picks",
                    current=len(intersection_stocks),
                    min_guaranteed=MIN_GUARANTEED,
                )
                emergency = self._emergency_picks(
                    all_recs, existing_codes=set(intersection_stocks.keys())
                )
                intersection_stocks.update(emergency)
                logger.info(
                    "After emergency picks",
                    total=len(intersection_stocks),
                )

            # 3. 构建初步结果
            result = self._build_recommendations(
                intersection_stocks, all_recs, limit
            )

            # 4. 获取实时行情 + LLM 逐股分析
            recs = result.get("data", {}).get("recommendations", [])
            if recs:
                try:
                    rt_prices = await self._fetch_realtime_prices(
                        [r["code"] for r in recs]
                    )
                    try:
                        from combined_llm import combined_llm
                    except ImportError:
                        from app.combined_llm import combined_llm

                    enhanced = await combined_llm.enhance_with_prices(
                        recs, rt_prices
                    )
                    result["data"]["recommendations"] = enhanced
                    result["data"]["llm_enhanced"] = True
                except Exception as e:
                    logger.warning(
                        "Combined LLM enhancement failed, using raw data",
                        error=str(e),
                    )

            return result

        except Exception as e:
            logger.error(
                "Combined strategy failed",
                error=str(e),
                traceback=traceback.format_exc(),
            )
            raise

    async def _load_all_recommendations(
        self, strategies: List[str] = None,
    ) -> Dict[str, List[Dict]]:
        """读取指定战法的最新推荐缓存"""
        try:
            from portfolio_repository import portfolio_repo
        except ImportError:
            from app.portfolio_repository import portfolio_repo

        if strategies is None:
            strategies = ACTIVE_STRATEGIES

        results = {}
        for strategy in strategies:
            try:
                cached = await portfolio_repo.get_recommendation_cache(
                    strategy, include_meta=True
                )
                stocks = cached.get("stocks", []) if cached else []
                results[strategy] = stocks
                logger.debug(
                    "Loaded strategy recommendations",
                    strategy=strategy,
                    count=len(stocks),
                )
            except Exception as e:
                logger.warning(
                    "Failed to load strategy cache",
                    strategy=strategy,
                    error=str(e),
                )
                results[strategy] = []

        total = sum(len(v) for v in results.values())
        logger.info(
            "Strategy recommendations loaded",
            strategies=len(results),
            strategy_names=[STRATEGY_NAMES.get(s, s) for s in strategies],
            total_stocks=total,
        )
        return results

    async def _fetch_realtime_prices(
        self, codes: List[str]
    ) -> Dict[str, Dict]:
        """获取指定股票的实时行情"""
        try:
            import asyncio
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes

            df = await asyncio.to_thread(get_realtime_quotes)
            if df is None or df.empty:
                return {}

            # 筛选目标股票
            if "代码" not in df.columns:
                return {}

            df_target = df[df["代码"].isin(codes)]
            prices = {}
            for _, row in df_target.iterrows():
                code = str(row.get("代码", ""))
                prices[code] = {
                    "price": float(row.get("最新价", 0) or 0),
                    "change_pct": float(row.get("涨跌幅", 0) or 0),
                    "high": float(row.get("最高", 0) or 0),
                    "low": float(row.get("最低", 0) or 0),
                    "open": float(row.get("今开", 0) or 0),
                }

            logger.info("Realtime prices fetched for combined",
                        requested=len(codes), found=len(prices))
            return prices
        except Exception as e:
            logger.warning("Failed to fetch realtime prices",
                           error=str(e))
            return {}

    def _extract_code(self, stock: Dict) -> str:
        """从推荐记录中提取股票代码（兼容不同字段名）"""
        code = (
            stock.get("code")
            or stock.get("stock_code")
            or stock.get("symbol")
            or ""
        )
        return str(code).strip()

    def _extract_name(self, stock: Dict) -> str:
        """从推荐记录中提取股票名称"""
        name = (
            stock.get("name")
            or stock.get("stock_name")
            or stock.get("名称")
            or ""
        )
        return str(name).strip()

    def _is_qualified_recommendation(self, stock: Dict) -> bool:
        """
        判断一只股票是否达到合格推荐等级（推荐 或 强烈推荐）

        兼容不同战法的字段名：
        - recommendation_level: 龙头/突破/量价/竞价/均线/事件驱动
        - level: 情绪战法
        """
        level = (
            stock.get("recommendation_level")
            or stock.get("level")
            or ""
        )
        return level in QUALIFIED_LEVELS

    def _find_intersection(
        self,
        all_recs: Dict[str, List[Dict]],
        min_overlap: int,
    ) -> Dict[str, Dict[str, Any]]:
        """
        从各战法中筛选「推荐」或「强烈推荐」的股票，然后找多战法交集

        核心逻辑：
        1. 遍历每个战法的推荐缓存
        2. 只保留 recommendation_level 为「推荐」或「强烈推荐」的股票
        3. 按股票代码聚合，统计在几个战法中被高质量推荐
        4. 筛选 overlap_count >= min_overlap 的股票

        Returns:
            {code: {name, strategies, strategy_details, overlap_count, ...}}
        """
        # code -> {strategies, name, details}
        stock_map: Dict[str, Dict[str, Any]] = {}

        # 统计各战法的筛选情况
        filter_stats: Dict[str, Dict[str, int]] = {}

        for strategy, stocks in all_recs.items():
            total_count = len(stocks)
            qualified_count = 0

            for stock in stocks:
                code = self._extract_code(stock)
                if not code:
                    continue

                # 核心过滤：只接受「推荐」或「强烈推荐」的股票
                if not self._is_qualified_recommendation(stock):
                    continue

                qualified_count += 1
                rec_level = (
                    stock.get("recommendation_level")
                    or stock.get("level")
                    or "推荐"
                )

                if code not in stock_map:
                    stock_map[code] = {
                        "code": code,
                        "name": self._extract_name(stock),
                        "strategies": [],
                        "strategy_names": [],
                        "strategy_details": {},
                        "overlap_count": 0,
                        "max_score": 0,
                        "avg_score": 0,
                        "scores": [],
                        "prices": [],
                        "strong_recommend_count": 0,
                    }

                entry = stock_map[code]
                if strategy not in entry["strategies"]:
                    entry["strategies"].append(strategy)
                    entry["strategy_names"].append(
                        STRATEGY_NAMES.get(strategy, strategy)
                    )

                # 统计强烈推荐次数
                if rec_level == "强烈推荐":
                    entry["strong_recommend_count"] += 1

                # 保存该战法对该股票的推荐详情
                entry["strategy_details"][strategy] = {
                    "rank": stock.get("rank"),
                    "score": stock.get("score") or stock.get("total_score") or 0,
                    "reason": stock.get("reason") or stock.get("ai_analysis") or "",
                    "recommendation_level": rec_level,
                    "price": stock.get("price") or stock.get("current_price"),
                    "change_pct": stock.get("change_pct") or stock.get("涨跌幅"),
                }

                # 累计评分
                score = (
                    stock.get("score")
                    or stock.get("total_score")
                    or stock.get("signal_score")
                    or 0
                )
                if isinstance(score, (int, float)):
                    entry["scores"].append(score)

                # 累计价格
                price = (
                    stock.get("price")
                    or stock.get("current_price")
                    or 0
                )
                try:
                    price = float(price)
                    if price > 0:
                        entry["prices"].append(price)
                except (ValueError, TypeError):
                    pass

            filter_stats[strategy] = {
                "total": total_count,
                "qualified": qualified_count,
                "filtered_out": total_count - qualified_count,
            }

        # 打印各战法的筛选统计
        for strategy, stats in filter_stats.items():
            logger.info(
                "Strategy qualification filter",
                strategy=STRATEGY_NAMES.get(strategy, strategy),
                total=stats["total"],
                qualified=stats["qualified"],
                filtered_out=stats["filtered_out"],
            )

        # 计算 overlap、权重和聚合评分
        result = {}
        for code, info in stock_map.items():
            info["overlap_count"] = len(info["strategies"])
            # 【新增】计算权重累加
            info["weight_sum"] = round(sum(
                STRATEGY_WEIGHTS.get(s, 1.0) for s in info["strategies"]
            ), 2)
            if info["overlap_count"] >= min_overlap:
                if info["scores"]:
                    info["max_score"] = max(info["scores"])
                    info["avg_score"] = round(
                        sum(info["scores"]) / len(info["scores"]), 2
                    )
                if info["prices"]:
                    avg_price = round(
                        sum(info["prices"]) / len(info["prices"]), 2
                    )
                    buy_price = round(avg_price * 0.98, 2)
                    info["current_price"] = avg_price
                    info["suggested_buy_price"] = buy_price
                    info["suggested_sell_price"] = round(avg_price * 1.05, 2)
                    # 统一 3% 止损（基于买入价）
                    info["stop_loss_price"] = round(buy_price * 0.97, 2)
                else:
                    info["current_price"] = 0
                    info["suggested_buy_price"] = 0
                    info["suggested_sell_price"] = 0
                    info["stop_loss_price"] = 0
                del info["scores"]
                del info["prices"]
                info["is_fallback"] = False
                result[code] = info

        logger.info(
            "Intersection analysis done (qualified only)",
            total_unique_stocks=len(stock_map),
            intersection_count=len(result),
            min_overlap=min_overlap,
            qualified_levels=list(QUALIFIED_LEVELS),
            filter_stats={
                STRATEGY_NAMES.get(s, s): f"{v['qualified']}/{v['total']}"
                for s, v in filter_stats.items()
            },
        )
        return result

    def _fallback_top_picks(
        self, all_recs: Dict[str, List[Dict]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        降级策略（优化版）：当多战法交集为空时，智能筛选精选推荐

        优先取「强烈推荐」的股票，然后按 rank 排序取前3
        确保综合战法不会返回空结果
        """
        result = {}
        # 第一轮：收集所有「强烈推荐」的股票
        for strategy, stocks in all_recs.items():
            for stock in stocks:
                code = self._extract_code(stock)
                if not code:
                    continue

                rec_level = (
                    stock.get("recommendation_level")
                    or stock.get("level")
                    or ""
                )

                # 优先收集强烈推荐
                if rec_level != "强烈推荐":
                    continue

                rank = stock.get("rank", 99)
                if rank <= 3:  # 扩大到前3名
                    self._add_fallback_stock(
                        result, code, stock, strategy, rec_level
                    )

        # 第二轮：如果强烈推荐不足，补充「推荐」中的前2名
        if len(result) < 3:
            for strategy, stocks in all_recs.items():
                for stock in stocks:
                    code = self._extract_code(stock)
                    if not code or code in result:
                        continue
                    if not self._is_qualified_recommendation(stock):
                        continue

                    rec_level = (
                        stock.get("recommendation_level")
                        or stock.get("level")
                        or "推荐"
                    )
                    rank = stock.get("rank", 99)
                    if rank <= 2:
                        self._add_fallback_stock(
                            result, code, stock, strategy, rec_level
                        )

        logger.info("Fallback top picks", count=len(result))
        return result

    def _add_fallback_stock(
        self, result: Dict, code: str, stock: Dict,
        strategy: str, rec_level: str,
    ):
        """向降级结果中添加一只股票（去重+合并）"""
        if code not in result:
            score = stock.get("score") or stock.get("total_score") or 0
            price = float(
                stock.get("price") or stock.get("current_price") or 0
            )
            buy_price = round(price * 0.98, 2) if price > 0 else 0
            result[code] = {
                "code": code,
                "name": self._extract_name(stock),
                "strategies": [strategy],
                "strategy_names": [
                    STRATEGY_NAMES.get(strategy, strategy)
                ],
                "strategy_details": {
                    strategy: {
                        "rank": stock.get("rank", 99),
                        "score": score,
                        "reason": (
                            stock.get("reason")
                            or stock.get("ai_analysis")
                            or ""
                        ),
                        "recommendation_level": rec_level,
                        "price": price,
                    }
                },
                "overlap_count": 1,
                "weight_sum": round(
                    STRATEGY_WEIGHTS.get(strategy, 1.0), 2
                ),
                "max_score": (
                    score if isinstance(score, (int, float)) else 0
                ),
                "avg_score": (
                    score if isinstance(score, (int, float)) else 0
                ),
                "strong_recommend_count": (
                    1 if rec_level == "强烈推荐" else 0
                ),
                "current_price": price,
                "suggested_buy_price": buy_price,
                "suggested_sell_price": (
                    round(price * 1.05, 2) if price > 0 else 0
                ),
                # 统一 3% 止损
                "stop_loss_price": (
                    round(buy_price * 0.97, 2) if buy_price > 0 else 0
                ),
                "is_fallback": True,
            }
        else:
            entry = result[code]
            if strategy not in entry["strategies"]:
                entry["strategies"].append(strategy)
                entry["strategy_names"].append(
                    STRATEGY_NAMES.get(strategy, strategy)
                )
                entry["overlap_count"] = len(entry["strategies"])
                entry["weight_sum"] = round(sum(
                    STRATEGY_WEIGHTS.get(s, 1.0)
                    for s in entry["strategies"]
                ), 2)
                # 多个战法命中就不再算降级
                entry["is_fallback"] = False
                if rec_level == "强烈推荐":
                    entry["strong_recommend_count"] += 1

    def _emergency_picks(
        self, all_recs: Dict[str, List[Dict]],
        existing_codes: set = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        紧急降级：确保每天至少有 MIN_GUARANTEED 只推荐

        不限制推荐等级，从每个战法中取评分最高的第1名。
        标记为 is_fallback=True 和 is_emergency=True。
        用于其他两级降级都无法凑够数量时的兜底。
        """
        if existing_codes is None:
            existing_codes = set()

        result = {}
        need = MIN_GUARANTEED - len(existing_codes)
        if need <= 0:
            return result

        # 按战法权重排序（优先从高权重战法取）
        sorted_strategies = sorted(
            all_recs.keys(),
            key=lambda s: STRATEGY_WEIGHTS.get(s, 1.0),
            reverse=True,
        )

        for strategy in sorted_strategies:
            if len(result) >= need:
                break

            stocks = all_recs.get(strategy, [])
            if not stocks:
                continue

            # 取该战法评分最高的股票（不限推荐等级）
            best = None
            best_score = -1
            for stock in stocks:
                code = self._extract_code(stock)
                if not code or code in existing_codes or code in result:
                    continue
                score = (
                    stock.get("score")
                    or stock.get("total_score")
                    or stock.get("signal_score")
                    or 0
                )
                if isinstance(score, (int, float)) and score > best_score:
                    best = stock
                    best_score = score

            if best:
                code = self._extract_code(best)
                rec_level = (
                    best.get("recommendation_level")
                    or best.get("level")
                    or "关注"
                )
                price = float(
                    best.get("price") or best.get("current_price") or 0
                )
                buy_price = round(price * 0.98, 2) if price > 0 else 0

                result[code] = {
                    "code": code,
                    "name": self._extract_name(best),
                    "strategies": [strategy],
                    "strategy_names": [
                        STRATEGY_NAMES.get(strategy, strategy)
                    ],
                    "strategy_details": {
                        strategy: {
                            "rank": best.get("rank", 1),
                            "score": best_score,
                            "reason": (
                                best.get("reason")
                                or best.get("ai_analysis")
                                or ""
                            ),
                            "recommendation_level": rec_level,
                            "price": price,
                        }
                    },
                    "overlap_count": 1,
                    "weight_sum": round(
                        STRATEGY_WEIGHTS.get(strategy, 1.0), 2
                    ),
                    "max_score": best_score,
                    "avg_score": best_score,
                    "strong_recommend_count": (
                        1 if rec_level == "强烈推荐" else 0
                    ),
                    "current_price": price,
                    "suggested_buy_price": buy_price,
                    "suggested_sell_price": (
                        round(price * 1.05, 2) if price > 0 else 0
                    ),
                    "stop_loss_price": (
                        round(buy_price * 0.97, 2) if buy_price > 0 else 0
                    ),
                    "is_fallback": True,
                    "is_emergency": True,
                }

        logger.info(
            "Emergency picks",
            count=len(result),
            strategies=[
                STRATEGY_NAMES.get(
                    list(v["strategy_details"].keys())[0], "?"
                )
                for v in result.values()
            ],
        )
        return result

    def _build_recommendations(
        self,
        intersection: Dict[str, Dict],
        all_recs: Dict[str, List[Dict]],
        limit: int,
    ) -> Dict[str, Any]:
        """构建推荐结果（含跟投友好信息）"""
        # 排序优化：权重总分优先（重要战法共振得分更高）
        sorted_stocks = sorted(
            intersection.values(),
            key=lambda x: (
                x.get("weight_sum", 0),            # 权重总分最重要
                x["overlap_count"],                 # 覆盖战法数
                x.get("strong_recommend_count", 0),  # 强烈推荐数
                x.get("avg_score", 0),              # 平均得分
                x.get("max_score", 0),              # 最高得分
            ),
            reverse=True,
        )

        recommendations = []
        for idx, stock in enumerate(sorted_stocks[:limit]):
            level_tags = []
            for s_key in stock["strategies"]:
                detail = stock["strategy_details"].get(s_key, {})
                s_name = STRATEGY_NAMES.get(s_key, s_key)
                level = detail.get("recommendation_level", "推荐")
                level_tags.append(f"{s_name}[{level}]")

            strong_count = stock.get("strong_recommend_count", 0)
            overlap = stock["overlap_count"]
            weight_sum = stock.get("weight_sum", 0)
            combined_score = round(
                weight_sum * 15
                + strong_count * 10
                + stock.get("avg_score", 0), 2
            )

            price = stock.get("current_price", 0)
            buy_price = stock.get("suggested_buy_price", 0)
            sell_price = stock.get("suggested_sell_price", 0)
            stop_price = stock.get("stop_loss_price", 0)

            # ==== 跟投友好信息 ====
            is_fallback = stock.get("is_fallback", False)
            if weight_sum >= 4.0 or overlap >= 4:
                confidence_text = "极高共识"
                position_advice = "可用20-30%仓位"
            elif weight_sum >= 2.5 or overlap >= 3:
                confidence_text = "高共识"
                position_advice = "可用15-20%仓位"
            elif overlap >= 2:
                confidence_text = "双战法共振"
                position_advice = "建议10-15%仓位"
            else:
                confidence_text = "单战法精选"
                position_advice = "建议5-10%仓位，轻仓试探"

            # 【优化】根据包含的战法类型精细化买入时机建议
            has_overnight = "overnight" in stock["strategies"]
            has_breakthrough = "breakthrough" in stock["strategies"]
            has_dragon = "dragon_head" in stock["strategies"]
            has_ma = "moving_average" in stock["strategies"]

            if has_overnight:
                buy_timing = (
                    "09:25观察竞价量能，若高开<3%且量能放大可25%仓位介入，"
                    "10分钟内不回落可加仓"
                )
            elif has_breakthrough and has_dragon:
                buy_timing = (
                    "09:30-09:45观察是否放量突破，"
                    "确认突破后分两批介入（30%+20%）"
                )
            elif has_ma:
                buy_timing = (
                    "09:30-10:30观察是否回踩均线支撑，"
                    "在5日/10日均线附近分批买入"
                )
            else:
                buy_timing = (
                    "09:30-10:30开盘后观察，"
                    "若低开或回调至支撑位可分批买入"
                )
            if price > 0 and buy_price > 0:
                action_summary = (
                    f"【{stock['name']}】{confidence_text}，"
                    f"建议{round(buy_price, 2)}附近买入，"
                    f"目标{round(sell_price, 2)}，"
                    f"止损{round(stop_price, 2)}，"
                    f"{position_advice}"
                )
            else:
                action_summary = (
                    f"【{stock['name']}】{confidence_text}，"
                    f"开盘后观察走势再决定，{position_advice}"
                )

            rec = {
                "rank": idx + 1,
                "code": stock["code"],
                "name": stock["name"],
                "overlap_count": overlap,
                "strong_recommend_count": strong_count,
                "strategies": stock["strategies"],
                "strategy_names": stock["strategy_names"],
                "strategy_names_text": " + ".join(stock["strategy_names"]),
                "strategy_level_text": " | ".join(level_tags),
                "strategy_details": stock["strategy_details"],
                "combined_score": combined_score,
                "max_score": stock.get("max_score", 0),
                "avg_score": stock.get("avg_score", 0),
                # 推荐等级
                "recommendation_level": (
                    "强烈推荐" if weight_sum >= 3.5 or strong_count >= 2 or overlap >= 4
                    else "推荐" if overlap >= 2
                    else "关注"
                ),
                # 买卖推荐价格
                "current_price": price,
                "suggested_buy_price": buy_price,
                "suggested_sell_price": sell_price,
                "stop_loss_price": stop_price,
                # ==== 跟投专用字段 ====
                "buy_timing": buy_timing,
                "position_advice": position_advice,
                "action_summary": action_summary,
                "confidence_text": confidence_text,
                "is_fallback": is_fallback,
            }
            recommendations.append(rec)

        # 统计各战法贡献度
        active_strategies = list(all_recs.keys())
        strategy_contribution = {}
        for s in active_strategies:
            count = sum(
                1
                for stock in intersection.values()
                if s in stock["strategies"]
            )
            strategy_contribution[STRATEGY_NAMES.get(s, s)] = count

        qualified_stats = {}
        for s in active_strategies:
            total = len(all_recs.get(s, []))
            qualified = sum(
                1 for stock in all_recs.get(s, [])
                if self._is_qualified_recommendation(stock)
            )
            qualified_stats[STRATEGY_NAMES.get(s, s)] = {
                "total": total,
                "qualified": qualified,
            }

        return {
            "status": "success",
            "data": {
                "recommendations": recommendations,
                "total": len(recommendations),
                "intersection_threshold": MIN_INTERSECTION,
                "qualified_levels": list(QUALIFIED_LEVELS),
                "active_strategies": [
                    STRATEGY_NAMES.get(s, s) for s in active_strategies
                ],
                "strategy_contribution": strategy_contribution,
                "qualified_stats": qualified_stats,
                "source_strategies": {
                    STRATEGY_NAMES.get(s, s): len(all_recs.get(s, []))
                    for s in active_strategies
                },
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "llm_enhanced": False,
                # 跟投提示
                "follow_note": (
                    "本推荐基于多战法共振分析，被多个独立战法同时推荐意味着信号极强。"
                    "建议09:30开盘后观察走势，在回调支撑位分批介入。"
                    "严格执行3%止损纪律，单只股票绝不扛单。"
                    "总仓位不超过60%，单只不超过30%。"
                    "已大涨(>5%)的股票切勿追高，宁可错过不要做错。"
                ),
            },
        }


# 模块级单例
combined_strategy = CombinedStrategy()
