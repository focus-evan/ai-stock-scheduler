"""
北向资金跟踪战法（Northbound Capital Strategy）核心逻辑模块

实现北向资金跟踪战法的核心模块：
1. 获取北向资金（沪深港通）流向数据
2. 获取多日排行数据（今日/3日/5日/10日）判断增持连续性
3. 识别外资连续大额净买入个股
4. 逆势加仓信号检测
5. 持仓占比变化趋势分析
6. 综合打分排序（七维度评分）
7. GPT 深度分析增强

数据源：AkShare（stock_hsgt_hold_stock_em / stock_individual_fund_flow_rank）

优化点：
- 多周期排行对比（今日+5日+10日）判断持仓变化速度
- 连续增持天数估算（通过多周期占比对比）
- 持仓加速/减速指标
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd
import structlog

try:
    from anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT, get_gateway_url, gateway_call
except ImportError:
    from app.anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT, get_gateway_url, gateway_call

logger = structlog.get_logger()


class NorthboundStrategy:
    """北向资金跟踪战法核心策略类"""

    def __init__(self):
        self._ak = None
        self._cache: Dict[str, Any] = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=10)

    @property
    def ak(self):
        if self._ak is None:
            from app.akshare_client import ak_client as ak
            self._ak = ak
        return self._ak

    def _is_cache_valid(self) -> bool:
        if not self._cache or not self._cache_time:
            return False
        return datetime.now() - self._cache_time < self._cache_ttl

    async def get_recommendations(self, limit: int = 13) -> Dict[str, Any]:
        """
        获取北向资金跟踪推荐列表

        流程:
        1. 获取北向资金个股持仓排名（今日+5日+10日多周期）
        2. 获取资金流向排名（大单净流入）
        3. 获取实时行情验证（涨跌幅、活跃度）
        4. 综合评分排序（七维度）
        5. GPT 深度分析增强
        """
        cache_key = f"northbound_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            logger.info("Returning cached northbound recommendations")
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            # 步骤1: 获取北向资金持仓数据（今日）
            hold_data = await loop.run_in_executor(None, self._get_northbound_holdings)

            # 步骤1b: 获取5日排行数据（用于判断连续增持趋势）
            hold_5d = await loop.run_in_executor(None, self._get_northbound_holdings_period, "5日排行")

            # 步骤1c: 获取10日排行数据（用于判断中期趋势）
            hold_10d = await loop.run_in_executor(None, self._get_northbound_holdings_period, "10日排行")

            # 步骤2: 获取个股资金流向
            flow_data = await loop.run_in_executor(None, self._get_fund_flow_rank)

            # 步骤3: 获取实时行情
            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)

            # 步骤4: 合并数据并打分（七维度）
            scored_stocks = await loop.run_in_executor(
                None, self._merge_and_score, hold_data, hold_5d, hold_10d, flow_data, realtime_df
            )

            # 步骤5: 构建推荐结果
            result = self._build_recommendations(scored_stocks, limit)

            # 步骤6: LLM 增强
            if scored_stocks:
                try:
                    try:
                        from northbound_llm import northbound_llm
                    except ImportError:
                        from app.northbound_llm import northbound_llm

                    llm_result = await northbound_llm.enhance_recommendations(
                        stocks=result["data"]["recommendations"][:5],
                    )
                    if llm_result:
                        if llm_result.get("enhanced_stocks"):
                            result["data"]["recommendations"] = llm_result["enhanced_stocks"][:limit]
                        if llm_result.get("strategy_report"):
                            result["data"]["strategy_report"] = llm_result["strategy_report"]
                        result["data"]["llm_enhanced"] = True
                    else:
                        result["data"]["llm_enhanced"] = False
                except Exception as llm_e:
                    logger.error("Northbound LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False
            else:
                result["data"]["llm_enhanced"] = False

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Northbound strategy failed", error=str(e),
                         traceback=traceback.format_exc())
            raise

    def _get_realtime_data(self) -> pd.DataFrame:
        """获取A股实时行情"""
        try:
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes
            return get_realtime_quotes()
        except Exception as e:
            logger.error("Failed to get realtime data", error=str(e))
            return pd.DataFrame()

    def _get_northbound_holdings(self) -> pd.DataFrame:
        """获取北向资金持仓排名（沪深港通持股 - 今日）"""
        # 1. 优先尝试 akshare 本地
        try:
            anti_scrape_delay("nb_hold", *DELAY_NORMAL)
            for indicator in ["今日排行", "持股排行"]:
                try:
                    df = self.ak.stock_hsgt_hold_stock_em(
                        market="北向", indicator=indicator
                    )
                    if df is not None and not df.empty:
                        logger.info("Got northbound holdings (local)", indicator=indicator, count=len(df))
                        return df
                except Exception:
                    continue

            # 降级：尝试获取沪股通
            try:
                df = self.ak.stock_hsgt_hold_stock_em(
                    market="沪股通", indicator="今日排行"
                )
                if df is not None and not df.empty:
                    return df
            except Exception:
                pass
        except Exception as e:
            logger.warning("Local northbound holdings failed", error=str(e))

        # 2. 降级到 gateway
        if get_gateway_url():
            try:
                for indicator in ["今日排行", "持股排行"]:
                    try:
                        df = gateway_call(
                            "/api/stock/hsgt_hold_stock_em",
                            params={"market": "北向", "indicator": indicator},
                            timeout=30,
                        )
                        if df is not None and not df.empty:
                            logger.info("Got northbound holdings (gateway)", indicator=indicator, count=len(df))
                            return df
                    except Exception:
                        continue
            except Exception as e:
                logger.warning("Gateway northbound holdings failed", error=str(e))

        logger.error("All northbound holdings sources failed")
        return pd.DataFrame()

    def _get_northbound_holdings_period(self, indicator: str) -> pd.DataFrame:
        """获取北向资金多日排行（5日/10日），用于判断连续增持趋势"""
        # 1. 本地
        try:
            anti_scrape_delay(f"nb_hold_{indicator}", *DELAY_LIGHT)
            df = self.ak.stock_hsgt_hold_stock_em(
                market="北向", indicator=indicator
            )
            if df is not None and not df.empty:
                logger.info("Got northbound period holdings (local)", indicator=indicator, count=len(df))
                return df
        except Exception as e:
            logger.warning("Local period holdings failed", indicator=indicator, error=str(e))

        # 2. gateway 降级
        if get_gateway_url():
            try:
                df = gateway_call(
                    "/api/stock/hsgt_hold_stock_em",
                    params={"market": "北向", "indicator": indicator},
                    timeout=30,
                )
                if df is not None and not df.empty:
                    logger.info("Got northbound period holdings (gateway)", indicator=indicator, count=len(df))
                    return df
            except Exception as e:
                logger.warning("Gateway period holdings failed", indicator=indicator, error=str(e))

        return pd.DataFrame()

    def _get_fund_flow_rank(self) -> pd.DataFrame:
        """获取个股资金流向排名（大单/超大单净流入）"""
        # 1. 本地
        try:
            anti_scrape_delay("nb_flow", *DELAY_NORMAL)
            df = self.ak.stock_individual_fund_flow_rank(indicator="今日")
            if df is not None and not df.empty:
                logger.info("Got fund flow rank (local)", count=len(df))
                return df
        except Exception as e:
            logger.warning("Local fund flow rank failed", error=str(e))

        # 2. gateway 降级
        if get_gateway_url():
            try:
                df = gateway_call(
                    "/api/stock/individual_fund_flow_rank",
                    params={"indicator": "今日"},
                    timeout=30,
                )
                if df is not None and not df.empty:
                    logger.info("Got fund flow rank (gateway)", count=len(df))
                    return df
            except Exception as e:
                logger.warning("Gateway fund flow rank failed", error=str(e))

        logger.error("All fund flow rank sources failed")
        return pd.DataFrame()

    def _normalize_hold_df(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """标准化北向持仓DataFrame -> code字典"""
        result = {}
        if df.empty:
            return result

        # 防止重复列名导致 df[col] 返回 DataFrame 而非 Series
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        h_map = {}
        mapped = set()
        for col in df.columns:
            col_lower = str(col)
            if '代码' in col_lower and 'code' not in mapped:
                h_map[col] = 'code'; mapped.add('code')
            elif '名称' in col_lower and 'name' not in mapped:
                h_map[col] = 'name'; mapped.add('name')
            elif ('持股数量' in col_lower or '持股数' in col_lower) and 'hold_shares' not in mapped:
                h_map[col] = 'hold_shares'; mapped.add('hold_shares')
            elif '持股市值' in col_lower and 'hold_value' not in mapped:
                h_map[col] = 'hold_value'; mapped.add('hold_value')
            elif ('占流通' in col_lower or '占A股' in col_lower) and 'hold_ratio' not in mapped:
                h_map[col] = 'hold_ratio'; mapped.add('hold_ratio')
            elif ('日增持' in col_lower or '增持' in col_lower) and 'increase' not in mapped:
                h_map[col] = 'increase'; mapped.add('increase')
        df = df.rename(columns=h_map)
        # 重命名后再去一次重复列
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        for col in ['hold_shares', 'hold_value', 'hold_ratio', 'increase']:
            if col in df.columns:
                data = df[col]
                # 双保险：如果仍然是 DataFrame，取第一列
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        if 'code' in df.columns:
            for _, row in df.iterrows():
                code = str(row.get('code', ''))
                if code:
                    result[code] = {
                        'name': str(row.get('name', '')),
                        'hold_shares': float(row.get('hold_shares', 0) or 0),
                        'hold_value': float(row.get('hold_value', 0) or 0),
                        'hold_ratio': float(row.get('hold_ratio', 0) or 0),
                        'increase': float(row.get('increase', 0) or 0),
                    }
        return result

    def _merge_and_score(self, hold_df: pd.DataFrame,
                          hold_5d_df: pd.DataFrame, hold_10d_df: pd.DataFrame,
                          flow_df: pd.DataFrame,
                          realtime_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """合并今日/5日/10日持仓、资金流向、实时行情数据，七维度评分"""
        results = []

        # 标准化实时行情
        rt_map = {}
        if not realtime_df.empty:
            for col in realtime_df.columns:
                if '代码' in col:
                    rt_map[col] = 'code'
                elif '名称' in col:
                    rt_map[col] = 'name'
                elif '涨跌幅' in col:
                    rt_map[col] = 'change_pct'
                elif ('最新价' in col or '收盘' in col):
                    rt_map[col] = 'price'
                elif '成交额' in col:
                    rt_map[col] = 'amount'
                elif '总市值' in col:
                    rt_map[col] = 'total_market_cap'
                elif '流通市值' in col:
                    rt_map[col] = 'float_market_cap'
                elif '换手率' in col:
                    rt_map[col] = 'turnover_rate'
            realtime_df = realtime_df.rename(columns=rt_map)
            realtime_df = realtime_df.loc[:, ~realtime_df.columns.duplicated(keep='first')]
            for col in ['change_pct', 'price', 'amount', 'total_market_cap', 'float_market_cap', 'turnover_rate']:
                if col in realtime_df.columns:
                    data = realtime_df[col]
                    if isinstance(data, pd.DataFrame):
                        data = data.iloc[:, 0]
                    realtime_df[col] = pd.to_numeric(data, errors='coerce')

        # 构建实时行情字典
        rt_dict = {}
        if not realtime_df.empty and 'code' in realtime_df.columns:
            for _, row in realtime_df.iterrows():
                code = str(row.get('code', ''))
                if code:
                    rt_dict[code] = row

        # 标准化各期北向持仓数据
        hold_dict = self._normalize_hold_df(hold_df)
        hold_5d_dict = self._normalize_hold_df(hold_5d_df)
        hold_10d_dict = self._normalize_hold_df(hold_10d_df)

        # 处理资金流向数据
        flow_dict = {}
        if not flow_df.empty:
            flow_df = flow_df.loc[:, ~flow_df.columns.duplicated(keep='first')]
            f_map = {}
            mapped = set()
            for col in flow_df.columns:
                col_lower = str(col)
                if '代码' in col_lower and 'code' not in mapped:
                    f_map[col] = 'code'; mapped.add('code')
                elif '名称' in col_lower and 'name' not in mapped:
                    f_map[col] = 'name'; mapped.add('name')
                elif '主力净流入' in col_lower and '净额' in col_lower and 'main_net_inflow' not in mapped:
                    f_map[col] = 'main_net_inflow'; mapped.add('main_net_inflow')
                elif '超大单净流入' in col_lower and '净额' in col_lower and 'super_large_net' not in mapped:
                    f_map[col] = 'super_large_net'; mapped.add('super_large_net')
                elif '大单净流入' in col_lower and '净额' in col_lower and 'large_net' not in mapped:
                    f_map[col] = 'large_net'; mapped.add('large_net')
                elif '涨跌幅' in col_lower and 'change_pct_flow' not in mapped:
                    f_map[col] = 'change_pct_flow'; mapped.add('change_pct_flow')
            flow_df = flow_df.rename(columns=f_map)
            flow_df = flow_df.loc[:, ~flow_df.columns.duplicated(keep='first')]
            for col in ['main_net_inflow', 'super_large_net', 'large_net', 'change_pct_flow']:
                if col in flow_df.columns:
                    data = flow_df[col]
                    if isinstance(data, pd.DataFrame):
                        data = data.iloc[:, 0]
                    flow_df[col] = pd.to_numeric(data, errors='coerce')

            if 'code' in flow_df.columns:
                for _, row in flow_df.iterrows():
                    code = str(row.get('code', ''))
                    if code:
                        flow_dict[code] = row

        # 汇总所有候选代码（以北向持仓为主）
        all_codes = set(hold_dict.keys())
        if flow_dict:
            all_codes = all_codes | set(list(flow_dict.keys())[:200])

        for code in all_codes:
            try:
                hold_info = hold_dict.get(code, {})
                hold_5d_info = hold_5d_dict.get(code, {})
                hold_10d_info = hold_10d_dict.get(code, {})
                flow_row = flow_dict.get(code)
                rt_row = rt_dict.get(code)

                name = ""
                price = 0.0
                change_pct = 0.0
                amount = 0.0
                total_market_cap = 0.0
                float_market_cap = 0.0
                turnover_rate = 0.0

                if rt_row is not None:
                    name = str(rt_row.get('name', ''))
                    price = float(rt_row.get('price', 0) or 0)
                    change_pct = float(rt_row.get('change_pct', 0) or 0)
                    amount = float(rt_row.get('amount', 0) or 0)
                    total_market_cap = float(rt_row.get('total_market_cap', 0) or 0)
                    float_market_cap = float(rt_row.get('float_market_cap', 0) or 0)
                    turnover_rate = float(rt_row.get('turnover_rate', 0) or 0)
                elif hold_info:
                    name = hold_info.get('name', '')

                if not name or 'ST' in name or '退' in name:
                    continue
                if not code[:2] in ('00', '30', '60'):
                    continue
                if price <= 0:
                    continue

                # 北向持仓指标（今日）
                in_northbound = bool(hold_info)
                hold_value = hold_info.get('hold_value', 0.0)
                hold_ratio = hold_info.get('hold_ratio', 0.0)
                increase = hold_info.get('increase', 0.0)

                # 多周期持仓变化（连续增持分析）
                hold_ratio_5d = hold_5d_info.get('hold_ratio', 0.0)
                increase_5d = hold_5d_info.get('increase', 0.0)
                hold_ratio_10d = hold_10d_info.get('hold_ratio', 0.0)
                increase_10d = hold_10d_info.get('increase', 0.0)

                # 计算持仓加速度（占比变化速度）
                # ratio_change = 今日占比 - 5日前占比 -> 占比增量
                ratio_change_5d = hold_ratio - hold_ratio_5d if hold_ratio_5d > 0 else 0
                ratio_change_10d = hold_ratio - hold_ratio_10d if hold_ratio_10d > 0 else 0

                # 判断连续增持天数（近似）：
                # 今日增持>0 + 5日累计增持>0 + 10日累计增持>0 → 近似判断连续性
                consecutive_days = 0
                if increase > 0:
                    consecutive_days = 1
                    if increase_5d > 0:
                        consecutive_days = 3  # 5日排行有增持，近似连续3天
                        if increase_10d > 0 and increase_10d > increase_5d:
                            consecutive_days = 5  # 10日内一直在增持

                # 判断是否加速增持
                is_accelerating = False
                if ratio_change_5d > 0 and ratio_change_10d > 0:
                    # 近5日增速 > 前5日增速 = 加速
                    recent_rate = ratio_change_5d  # 近5日变化
                    earlier_rate = ratio_change_10d - ratio_change_5d  # 前5日变化
                    is_accelerating = recent_rate > earlier_rate and recent_rate > 0

                # 资金流向指标
                main_net_inflow = 0.0
                super_large_net = 0.0
                large_net = 0.0

                if flow_row is not None:
                    main_net_inflow = float(flow_row.get('main_net_inflow', 0) or 0)
                    super_large_net = float(flow_row.get('super_large_net', 0) or 0)
                    large_net = float(flow_row.get('large_net', 0) or 0)

                # ===== 七维度百分制评分 =====

                # 1. 北向持仓分（25分）：持股占比越高越好
                hold_score = 0
                if in_northbound:
                    if hold_ratio >= 10:
                        hold_score = 25
                    elif hold_ratio >= 5:
                        hold_score = 21
                    elif hold_ratio >= 2:
                        hold_score = 17
                    elif hold_ratio >= 1:
                        hold_score = 12
                    else:
                        hold_score = 8

                # 2. 今日增持分（15分）：日增持数量
                increase_score = 0
                if increase > 0:
                    if increase >= 5e6:
                        increase_score = 15
                    elif increase >= 1e6:
                        increase_score = 12
                    elif increase >= 5e5:
                        increase_score = 9
                    else:
                        increase_score = 6
                elif increase < 0:
                    # 减持扣分
                    increase_score = -5

                # 3. 连续增持分（15分）：多日连续加仓比单日更有意义
                consecutive_score = 0
                if consecutive_days >= 5:
                    consecutive_score = 15
                elif consecutive_days >= 3:
                    consecutive_score = 11
                elif consecutive_days >= 1:
                    consecutive_score = 6

                # 4. 持仓变化趋势分（10分）：加速增持 vs 减速
                trend_score = 0
                if is_accelerating:
                    trend_score = 10
                elif ratio_change_5d > 0:
                    trend_score = 7
                elif ratio_change_5d == 0 and hold_ratio > 0:
                    trend_score = 3  # 持平
                elif ratio_change_5d < 0:
                    trend_score = -3  # 占比在下降

                # 5. 主力资金分（15分）：主力净流入
                capital_score = 0
                if main_net_inflow > 0:
                    if main_net_inflow >= 5e8:
                        capital_score = 15
                    elif main_net_inflow >= 1e8:
                        capital_score = 12
                    elif main_net_inflow >= 5e7:
                        capital_score = 9
                    else:
                        capital_score = 6
                elif main_net_inflow < -1e8:
                    capital_score = -3

                # 6. 市场表现分（10分）：涨跌幅与活跃度
                market_score = 0
                if 0 <= change_pct <= 5:
                    market_score = 10  # 温和上涨最佳
                elif 5 < change_pct < 9.8:
                    market_score = 8
                elif -2 <= change_pct < 0:
                    market_score = 7  # 小幅下跌
                elif change_pct < -2:
                    market_score = 3
                else:
                    market_score = 2

                # 7. 逆势加仓分（10分）：大盘跌但北向加仓
                contrarian_score = 0
                if change_pct < 0 and increase > 0:
                    contrarian_score = 10
                    if consecutive_days >= 3:
                        contrarian_score = 10  # 连续逆势加仓，满分
                elif change_pct < -1 and main_net_inflow > 1e8:
                    contrarian_score = 7

                total_score = max(0, min(100,
                    hold_score + increase_score + consecutive_score +
                    trend_score + capital_score + market_score + contrarian_score
                ))

                # 过滤条件：至少在北向持仓 或 主力净流入>5000万
                if not in_northbound and main_net_inflow < 5e7:
                    continue

                results.append({
                    "rank": 0,
                    "code": code,
                    "name": name,
                    "price": round(price, 2),
                    "change_pct": round(change_pct, 2),
                    "amount": round(amount, 2),
                    "total_market_cap": round(total_market_cap, 2),
                    "float_market_cap": round(float_market_cap, 2),
                    "turnover_rate": round(turnover_rate, 2),
                    "in_northbound": in_northbound,
                    "hold_value": round(hold_value, 2),
                    "hold_ratio": round(hold_ratio, 2),
                    "increase": round(increase, 2),
                    "main_net_inflow": round(main_net_inflow, 2),
                    "super_large_net": round(super_large_net, 2),
                    "large_net": round(large_net, 2),
                    "is_contrarian": bool(change_pct < 0 and increase > 0),
                    # 新增指标
                    "consecutive_days": consecutive_days,
                    "is_accelerating": is_accelerating,
                    "ratio_change_5d": round(ratio_change_5d, 4),
                    "ratio_change_10d": round(ratio_change_10d, 4),
                    "hold_ratio_5d": round(hold_ratio_5d, 2),
                    "hold_ratio_10d": round(hold_ratio_10d, 2),
                    "northbound_score": total_score,
                    "score_detail": {
                        "hold": hold_score,
                        "increase": increase_score,
                        "consecutive": consecutive_score,
                        "trend": trend_score,
                        "capital": capital_score,
                        "market": market_score,
                        "contrarian": contrarian_score,
                    },
                    "reasons": [],
                    "recommendation_level": "关注",
                })

            except Exception as e:
                logger.warning("Failed to process northbound stock", code=code, error=str(e))
                continue

        # 按评分排序
        results.sort(key=lambda x: x["northbound_score"], reverse=True)
        for idx, stock in enumerate(results):
            stock["rank"] = idx + 1

        logger.info("Scored northbound stocks", count=len(results))
        return results

    def _build_recommendations(self, stocks: List[Dict], limit: int) -> Dict[str, Any]:
        """构建推荐结果"""
        now = datetime.now()

        for stock in stocks:
            reasons = []
            if stock.get("in_northbound"):
                reasons.append(f"北向持仓占比 {stock.get('hold_ratio', 0):.2f}%")
            if stock.get("increase", 0) > 0:
                inc = stock["increase"]
                if inc >= 1e6:
                    reasons.append(f"今日增持 {inc / 1e4:.0f}万股")
                else:
                    reasons.append(f"今日增持 {inc:.0f}股")
            elif stock.get("increase", 0) < 0:
                reasons.append("⚠️ 今日减持")

            # 连续增持信号
            cd = stock.get("consecutive_days", 0)
            if cd >= 5:
                reasons.append(f"🔥 连续增持约{cd}天")
            elif cd >= 3:
                reasons.append(f"📈 近期连续增持{cd}天")

            # 持仓加速信号
            if stock.get("is_accelerating"):
                reasons.append("⚡ 增持加速中")
            elif stock.get("ratio_change_5d", 0) > 0:
                reasons.append(f"近5日占比+{stock['ratio_change_5d']:.3f}%")
            elif stock.get("ratio_change_5d", 0) < 0:
                reasons.append(f"⚠️ 近5日占比{stock['ratio_change_5d']:.3f}%")

            if stock.get("main_net_inflow", 0) > 0:
                inflow = stock["main_net_inflow"]
                if inflow >= 1e8:
                    reasons.append(f"主力净流入 {inflow / 1e8:.2f}亿")
                else:
                    reasons.append(f"主力净流入 {inflow / 1e4:.0f}万")
            if stock.get("is_contrarian"):
                reasons.append("🔥 逆势加仓信号")

            stock["reasons"] = reasons
            score = stock.get("northbound_score", 0)
            if score >= 80:
                stock["recommendation_level"] = "强烈推荐"
            elif score >= 60:
                stock["recommendation_level"] = "推荐"
            elif score >= 40:
                stock["recommendation_level"] = "关注"
            else:
                stock["recommendation_level"] = "回避"

        # 统计
        nb_count = sum(1 for s in stocks if s.get("in_northbound"))
        contrarian = sum(1 for s in stocks if s.get("is_contrarian"))
        consecutive = sum(1 for s in stocks if s.get("consecutive_days", 0) >= 3)
        accelerating = sum(1 for s in stocks if s.get("is_accelerating"))

        return {
            "status": "success",
            "data": {
                "recommendations": stocks[:limit],
                "total": len(stocks),
                "signal_summary": {
                    "northbound_total": nb_count,
                    "contrarian_count": contrarian,
                    "consecutive_count": consecutive,
                    "accelerating_count": accelerating,
                },
                "strategy_report": self._generate_report(stocks, limit),
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
                "llm_enhanced": False,
            }
        }

    def _generate_report(self, stocks: List[Dict], limit: int) -> str:
        total = len(stocks)
        nb_count = sum(1 for s in stocks if s.get("in_northbound"))
        contrarian = sum(1 for s in stocks if s.get("is_contrarian"))
        consecutive = sum(1 for s in stocks if s.get("consecutive_days", 0) >= 3)
        accelerating = sum(1 for s in stocks if s.get("is_accelerating"))
        avg_score = sum(s.get("northbound_score", 0) for s in stocks) / max(total, 1)
        top5 = stocks[:5]
        top_names = "、".join(s.get("name", "?") for s in top5)

        return (
            f"## 北向资金跟踪报告\n\n"
            f"筛选到 **{total}** 只北向资金关注股，平均评分 **{avg_score:.0f}**分，其中：\n"
            f"- 北向持仓股: {nb_count} 只\n"
            f"- 🔥逆势加仓: {contrarian} 只\n"
            f"- 📈连续增持(≥3日): {consecutive} 只\n"
            f"- ⚡增持加速: {accelerating} 只\n\n"
            f"### Top 5 推荐\n{top_names}\n\n"
            f"### 核心逻辑\n"
            f"北向资金（沪深港通）被视为\"聪明钱\"。本战法采用七维度评分"
            f"（持仓25+今日增持15+连续增持15+持仓趋势10+主力资金15+市场10+逆势10），"
            f"重点关注**连续增持**和**增持加速**信号，"
            f"连续3日以上增持+主力资金流入双共振的个股胜率最高。\n\n"
            f"⚠️ **风险提示**: 北向资金可能存在\"假外资\"（内地资金借道香港回流），"
            f"需结合基本面判断。占比下降的股票应警惕外资撤退风险。"
        )


# 全局单例
northbound_strategy = NorthboundStrategy()
