"""
情绪战法 - 每日选股模块

根据情绪周期阶段，采用不同选股策略：
- 冰点阶段 → 超跌反弹策略：选择近期超跌但资金开始流入的票
- 修复阶段 → 情绪修复策略：选择率先放量突破的短线票
- 升温阶段 → 突破放量策略：选择量价齐升、突破前高的票
- 高潮阶段 → 连板龙头策略：选择连板最高、封板最强的龙头
- 退潮阶段 → 防御策略：减少推荐，只选强势抗跌股

数据源：AkShare (stock_zh_a_spot_em 全市场实时行情)
降级源：涨停池 (stock_zt_pool_em) — 在全市场行情不可用时自动降级
"""

import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import structlog

logger = structlog.get_logger()

# 选股数量
PICK_COUNT = 13
# 实时行情重试次数
REALTIME_MAX_RETRY = 3


class SentimentStockPicker:
    """情绪战法选股引擎"""

    def __init__(self):
        self._ak = None

    @property
    def ak(self):
        if self._ak is None:
            from app.akshare_client import ak_client as ak
            self._ak = ak
        return self._ak

    async def pick_stocks(
        self,
        emotion_phase: str,
        today_snapshot: Dict[str, Any],
        limit_up_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        根据情绪阶段选股

        Args:
            emotion_phase: 情绪阶段 (冰点/修复/升温/高潮/退潮)
            today_snapshot: 今日情绪快照
            limit_up_df: 今日涨停股 DataFrame

        Returns:
            {
                "pick_strategy": str,
                "stocks": [{ stock_code, stock_name, ... }],
                "pick_count": int,
            }
        """
        import asyncio
        loop = asyncio.get_event_loop()

        try:
            # 获取全市场实时行情（带重试）
            logger.info("Fetching realtime market data for stock picking")
            realtime_df = await loop.run_in_executor(None, self._fetch_realtime)

            use_fallback = False
            if realtime_df is None or realtime_df.empty:
                logger.warning("Realtime data unavailable, using limit_up fallback")
                use_fallback = True

            # 标准化列名
            if not use_fallback:
                realtime_df = self._normalize_columns(realtime_df)
                if realtime_df is None:
                    use_fallback = True

            # ========== 降级逻辑：用涨停池数据生成推荐 ==========
            if use_fallback:
                stocks = self._fallback_from_limit_up(limit_up_df, emotion_phase, today_snapshot)
                for i, s in enumerate(stocks):
                    s["rank"] = i + 1

                strategy_name = self._phase_to_strategy(emotion_phase)
                logger.info("Fallback stock picking completed",
                             strategy=strategy_name, count=len(stocks))
                return {
                    "pick_strategy": strategy_name,
                    "stocks": stocks,
                    "pick_count": len(stocks),
                }

            # ========== 正常逻辑：用全市场行情选股 ==========
            strategy_map = {
                "冰点": ("ice_reversal", self._pick_ice_reversal),
                "修复": ("recovery_momentum", self._pick_recovery),
                "升温": ("warm_breakout", self._pick_warm_breakout),
                "高潮": ("climax_leader", self._pick_climax_leader),
                "退潮": ("retreat_defense", self._pick_retreat_defense),
            }

            strategy_name, picker_fn = strategy_map.get(
                emotion_phase, ("normal_momentum", self._pick_normal_momentum)
            )

            logger.info("Stock picking strategy selected",
                         phase=emotion_phase, strategy=strategy_name)

            stocks = picker_fn(realtime_df, limit_up_df, today_snapshot)

            # 截取前 PICK_COUNT 只
            stocks = stocks[:PICK_COUNT]

            # 添加排名
            for i, s in enumerate(stocks):
                s["rank"] = i + 1

            logger.info("Stock picking completed",
                         strategy=strategy_name, count=len(stocks))

            return {
                "pick_strategy": strategy_name,
                "stocks": stocks,
                "pick_count": len(stocks),
            }

        except Exception as e:
            logger.error("Stock picking failed", error=str(e),
                         traceback=traceback.format_exc())
            # 最终兜底：直接从涨停池出推荐
            try:
                stocks = self._fallback_from_limit_up(limit_up_df, emotion_phase, today_snapshot)
                for i, s in enumerate(stocks):
                    s["rank"] = i + 1
                return {
                    "pick_strategy": self._phase_to_strategy(emotion_phase),
                    "stocks": stocks,
                    "pick_count": len(stocks),
                }
            except Exception:
                return {"pick_strategy": "error", "stocks": [], "pick_count": 0}

    # ========================= 数据获取 =========================

    def _fetch_realtime(self) -> Optional[pd.DataFrame]:
        """获取全市场实时行情（多源降级）"""
        try:
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes
            df = get_realtime_quotes()
            if df is not None and not df.empty:
                logger.info("Got realtime market data", count=len(df))
                return df
        except Exception as e:
            logger.error("All data sources failed", error=str(e))
        return None

    def _normalize_columns(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """标准化列名，返回统一格式"""
        col_map = {}
        for col in df.columns:
            lower = col.lower()
            if '代码' in col:
                col_map[col] = 'code'
            elif '名称' in col:
                col_map[col] = 'name'
            elif '最新价' in col:
                col_map[col] = 'price'
            elif '涨跌幅' in col and '涨跌额' not in col:
                col_map[col] = 'pct_chg'
            elif '成交额' in col:
                col_map[col] = 'amount'
            elif '换手率' in col:
                col_map[col] = 'turnover'
            elif '总市值' in col:
                col_map[col] = 'total_mv'
            elif '流通市值' in col:
                col_map[col] = 'float_mv'
            elif '量比' in col:
                col_map[col] = 'volume_ratio'
            elif '振幅' in col:
                col_map[col] = 'amplitude'
            elif '60日涨跌幅' in col:
                col_map[col] = 'pct_60d'
            elif '年初至今涨跌幅' in col:
                col_map[col] = 'pct_ytd'

        if 'code' not in col_map.values() or 'name' not in col_map.values():
            logger.error("Cannot find code/name columns in realtime data")
            return None

        df = df.rename(columns=col_map)

        # 过滤 ST、退市、北交所
        if 'name' in df.columns:
            df = df[~df['name'].str.contains('ST|退|N/A', na=False)]
        if 'code' in df.columns:
            df = df[df['code'].apply(lambda x: str(x).startswith(('00', '30', '60')))]

        # 转数值
        for col in ['price', 'pct_chg', 'amount', 'turnover', 'total_mv',
                     'float_mv', 'volume_ratio', 'amplitude']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 过滤停牌（成交额=0 或价格=0）
        if 'amount' in df.columns:
            df = df[df['amount'] > 0]
        if 'price' in df.columns:
            df = df[df['price'] > 0]

        return df

    # ========================= 选股策略 =========================

    def _pick_ice_reversal(self, df: pd.DataFrame, limit_up_df: pd.DataFrame,
                           snapshot: Dict) -> List[Dict]:
        """
        冰点阶段 → 超跌反弹策略（优化版）
        选股逻辑：
        1. 近期跌幅较大（60日跌幅 < -10%）
        2. 今日放量（量比 > 1.3）反弹
        3. 今日涨幅 2-7%（不追涨停，要有空间）
        4. 流通市值 30-500 亿（中小盘弹性大）
        5. 【新增】放量但跌幅大的股票优先（量价背离说明抛压出尽）
        6. 【新增】振幅 > 3% 表明资金有争夺
        """
        candidates = df.copy()

        if 'pct_chg' in candidates.columns:
            candidates = candidates[(candidates['pct_chg'] >= 2) & (candidates['pct_chg'] <= 7)]

        if 'volume_ratio' in candidates.columns:
            candidates = candidates[candidates['volume_ratio'] >= 1.3]

        if 'float_mv' in candidates.columns:
            candidates = candidates[(candidates['float_mv'] >= 30e8) & (candidates['float_mv'] <= 500e8)]

        if 'pct_60d' in candidates.columns:
            candidates = candidates[candidates['pct_60d'] < -10]

        # 【新增】振幅过滤：振幅太小说明无争夺
        if 'amplitude' in candidates.columns:
            candidates = candidates[candidates['amplitude'] >= 3]

        # 【优化】评分逻辑：量比 * 涨幅 + 60日跌幅的绝对值加权（跌得越深反弹空间越大）
        score_parts = []
        if 'volume_ratio' in candidates.columns and 'pct_chg' in candidates.columns:
            score_parts.append(candidates['volume_ratio'] * candidates['pct_chg'])
        if 'pct_60d' in candidates.columns:
            score_parts.append(candidates['pct_60d'].abs() * 0.3)  # 60日跌庅越大越好

        if score_parts:
            candidates['score'] = sum(score_parts)
            candidates = candidates.sort_values('score', ascending=False)

        return self._to_stock_list(candidates, "超跌反弹")

    def _pick_recovery(self, df: pd.DataFrame, limit_up_df: pd.DataFrame,
                       snapshot: Dict) -> List[Dict]:
        """
        修复阶段 → 情绪修复策略
        选股逻辑：
        1. 今日涨幅 3-9%（有力度但不追涨停）
        2. 量比 > 1.5（放量）
        3. 换手率 3-15%（活跃但不过热）
        4. 成交额 > 2 亿（有资金关注）
        """
        candidates = df.copy()

        if 'pct_chg' in candidates.columns:
            candidates = candidates[(candidates['pct_chg'] >= 3) & (candidates['pct_chg'] <= 9)]

        if 'volume_ratio' in candidates.columns:
            candidates = candidates[candidates['volume_ratio'] >= 1.5]

        if 'turnover' in candidates.columns:
            candidates = candidates[(candidates['turnover'] >= 3) & (candidates['turnover'] <= 15)]

        if 'amount' in candidates.columns:
            candidates = candidates[candidates['amount'] >= 2e8]

        # 按涨幅*量比排序
        if 'pct_chg' in candidates.columns and 'volume_ratio' in candidates.columns:
            candidates['score'] = candidates['pct_chg'] * candidates['volume_ratio']
            candidates = candidates.sort_values('score', ascending=False)

        return self._to_stock_list(candidates, "情绪修复")

    def _pick_warm_breakout(self, df: pd.DataFrame, limit_up_df: pd.DataFrame,
                            snapshot: Dict) -> List[Dict]:
        """
        升温阶段 → 突破放量策略（优化版）
        选股逻辑：
        1. 今日涨幅 5%+ 或涨停
        2. 量比 > 1.8（显著放量）
        3. 成交额 > 3 亿
        4. 【新增】优先选封板质量好的：涨停池中封单金额大者优先
        5. 【新增】换手率 3-20%（太低无流动性，太高说明分歧大）
        """
        candidates = df.copy()

        if 'pct_chg' in candidates.columns:
            candidates = candidates[candidates['pct_chg'] >= 5]

        if 'volume_ratio' in candidates.columns:
            candidates = candidates[candidates['volume_ratio'] >= 1.8]

        if 'amount' in candidates.columns:
            candidates = candidates[candidates['amount'] >= 3e8]

        # 【新增】换手率过滤
        if 'turnover' in candidates.columns:
            candidates = candidates[(candidates['turnover'] >= 3) & (candidates['turnover'] <= 20)]

        # 优先合并涨停池的封单数据
        if limit_up_df is not None and not limit_up_df.empty:
            seal_col = None
            zt_code_col = None
            for col in limit_up_df.columns:
                if '封单' in col and seal_col is None:
                    seal_col = col
                if '代码' in col and zt_code_col is None:
                    zt_code_col = col

            if seal_col and zt_code_col and 'code' in candidates.columns:
                seal_map = {}
                for _, row in limit_up_df.iterrows():
                    code = str(row.get(zt_code_col, ''))
                    seal_val = float(pd.to_numeric(row.get(seal_col, 0), errors='coerce') or 0)
                    if code:
                        seal_map[code] = seal_val
                candidates['seal_score'] = candidates['code'].map(seal_map).fillna(0)
            else:
                candidates['seal_score'] = 0
        else:
            candidates['seal_score'] = 0

        # 综合评分：涨幅*量比 + 封单加权
        if 'pct_chg' in candidates.columns and 'volume_ratio' in candidates.columns:
            candidates['score'] = (
                candidates['pct_chg'] * candidates['volume_ratio'] +
                candidates['seal_score'] / 1e8 * 0.5  # 封单金额换算成亿元加权
            )
            candidates = candidates.sort_values('score', ascending=False)

        return self._to_stock_list(candidates, "突破放量")

    def _pick_climax_leader(self, df: pd.DataFrame, limit_up_df: pd.DataFrame,
                            snapshot: Dict) -> List[Dict]:
        """
        高潮阶段 → 连板龙头策略
        选股逻辑：
        1. 优先选涨停股（从 limit_up_df 取）
        2. 连板天数越多越好
        3. 封单金额大优先
        4. 补充涨幅 8%+ 的非涨停强势股
        """
        stocks = []

        # 优先从涨停池取
        if limit_up_df is not None and not limit_up_df.empty:
            limit_up_df_sorted = limit_up_df.copy()

            # 找到连板天数列和封单列
            days_col = None
            seal_col = None
            code_col = None
            name_col = None
            price_col = None
            for col in limit_up_df_sorted.columns:
                if '连板' in col or '涨停天' in col:
                    days_col = col
                elif '封单' in col:
                    seal_col = col
                elif '代码' in col and code_col is None:
                    code_col = col
                elif '名称' in col and name_col is None:
                    name_col = col
                elif '最新价' in col and price_col is None:
                    price_col = col

            if days_col:
                limit_up_df_sorted[days_col] = pd.to_numeric(limit_up_df_sorted[days_col], errors='coerce').fillna(1)
                limit_up_df_sorted = limit_up_df_sorted.sort_values(days_col, ascending=False)

            for _, row in limit_up_df_sorted.head(15).iterrows():
                stock = {
                    "stock_code": str(row.get(code_col, "")) if code_col else "",
                    "stock_name": str(row.get(name_col, "")) if name_col else "",
                    "price": float(row.get(price_col, 0)) if price_col else 0,
                    "change_pct": 0,
                    "amount": 0,
                    "turnover_rate": 0,
                    "total_market_cap": 0,
                    "float_market_cap": 0,
                    "limit_up_days": int(row.get(days_col, 1)) if days_col else 1,
                    "seal_amount": float(row.get(seal_col, 0)) if seal_col else 0,
                    "pick_reason_tag": "连板龙头",
                    "industry": "",
                }
                # 补充实时数据
                if 'code' in df.columns:
                    match = df[df['code'] == stock["stock_code"]]
                    if not match.empty:
                        m = match.iloc[0]
                        stock["change_pct"] = float(m.get('pct_chg', 0) or 0)
                        stock["amount"] = float(m.get('amount', 0) or 0)
                        stock["turnover_rate"] = float(m.get('turnover', 0) or 0)
                        stock["total_market_cap"] = float(m.get('total_mv', 0) or 0)
                        stock["float_market_cap"] = float(m.get('float_mv', 0) or 0)

                if stock["stock_code"]:
                    stocks.append(stock)

        # 补充强势股
        if len(stocks) < PICK_COUNT and 'pct_chg' in df.columns:
            existing_codes = {s["stock_code"] for s in stocks}
            strong = df[df['pct_chg'] >= 8].sort_values('pct_chg', ascending=False)
            for _, row in strong.iterrows():
                code = str(row.get('code', ''))
                if code and code not in existing_codes:
                    stocks.append(self._row_to_stock(row, "强势跟涨"))
                    existing_codes.add(code)
                if len(stocks) >= PICK_COUNT:
                    break

        return stocks

    def _pick_retreat_defense(self, df: pd.DataFrame, limit_up_df: pd.DataFrame,
                              snapshot: Dict) -> List[Dict]:
        """
        退潮阶段 → 防御策略（优化版 - 缩减推荐数量）
        选股逻辑：
        1. 逆势上涨（涨幅 > 2%）
        2. 大盘蓝筹（流通市值 > 200 亿）【提高门槛，原100亿】
        3. 换手率适中（< 5%，非炒作）【收紧，原8%】
        4. 成交额 > 5 亿（有真实资金）
        5. 【新增】只推荐5只（退潮期严控数量）
        """
        RETREAT_PICK_COUNT = 5  # 退潮期只推荐5只
        candidates = df.copy()

        if 'pct_chg' in candidates.columns:
            candidates = candidates[candidates['pct_chg'] >= 2]

        if 'float_mv' in candidates.columns:
            candidates = candidates[candidates['float_mv'] >= 200e8]  # 提高到200亿

        if 'turnover' in candidates.columns:
            candidates = candidates[candidates['turnover'] <= 5]  # 收紧到5%

        if 'amount' in candidates.columns:
            candidates = candidates[candidates['amount'] >= 5e8]

        if 'pct_chg' in candidates.columns:
            candidates = candidates.sort_values('pct_chg', ascending=False)

        return self._to_stock_list(candidates.head(RETREAT_PICK_COUNT + 3), "抗跌防御")

    def _pick_normal_momentum(self, df: pd.DataFrame, limit_up_df: pd.DataFrame,
                              snapshot: Dict) -> List[Dict]:
        """
        默认/正常阶段 → 动量策略
        选股逻辑：
        1. 涨幅 3-9%
        2. 量比 > 1.2
        3. 成交额 > 2 亿
        """
        candidates = df.copy()

        if 'pct_chg' in candidates.columns:
            candidates = candidates[(candidates['pct_chg'] >= 3) & (candidates['pct_chg'] <= 9)]

        if 'volume_ratio' in candidates.columns:
            candidates = candidates[candidates['volume_ratio'] >= 1.2]

        if 'amount' in candidates.columns:
            candidates = candidates[candidates['amount'] >= 2e8]

        if 'pct_chg' in candidates.columns:
            candidates = candidates.sort_values('pct_chg', ascending=False)

        return self._to_stock_list(candidates, "动量选股")

    # ========================= 工具方法 =========================

    def _to_stock_list(self, df: pd.DataFrame, tag: str) -> List[Dict]:
        """DataFrame 转换为标准股票列表"""
        stocks = []
        for _, row in df.head(PICK_COUNT + 5).iterrows():
            stock = self._row_to_stock(row, tag)
            if stock["stock_code"]:
                stocks.append(stock)
            if len(stocks) >= PICK_COUNT:
                break
        return stocks

    def _row_to_stock(self, row, tag: str) -> Dict[str, Any]:
        """单行转股票字典"""
        return {
            "stock_code": str(row.get('code', '')),
            "stock_name": str(row.get('name', '')),
            "price": float(row.get('price', 0) or 0),
            "change_pct": float(row.get('pct_chg', 0) or 0),
            "amount": float(row.get('amount', 0) or 0),
            "turnover_rate": float(row.get('turnover', 0) or 0),
            "total_market_cap": float(row.get('total_mv', 0) or 0),
            "float_market_cap": float(row.get('float_mv', 0) or 0),
            "limit_up_days": 0,
            "first_limit_time": "",
            "seal_amount": 0,
            "pick_reason_tag": tag,
            "industry": "",
        }

    # ========================= 降级逻辑 =========================

    def _phase_to_strategy(self, phase: str) -> str:
        """情绪阶段转策略名"""
        m = {
            "冰点": "ice_reversal",
            "修复": "recovery_momentum",
            "升温": "warm_breakout",
            "高潮": "climax_leader",
            "退潮": "retreat_defense",
        }
        return m.get(phase, "normal_momentum")

    def _fallback_from_limit_up(self, limit_up_df: pd.DataFrame,
                                emotion_phase: str,
                                snapshot: Dict) -> List[Dict]:
        """
        降级选股：当全市场行情获取失败时，使用涨停池数据推荐

        涨停池(stock_zt_pool_em)字段通常包含：
        代码, 名称, 最新价, 涨跌幅, 成交额, 流通市值, 总市值,
        换手率, 连板数, 首次封板时间, 封单资金, 所属行业 等
        """
        if limit_up_df is None or limit_up_df.empty:
            logger.warning("Limit up data also empty, no fallback available")
            return []

        logger.info("Using limit_up fallback for stock picking",
                     available=len(limit_up_df))

        # 动态匹配列名（zt_pool 的列名可能因 akshare 版本不同而异）
        col_map = {}
        for col in limit_up_df.columns:
            if '代码' in col and 'code' not in col_map.values():
                col_map[col] = 'code'
            elif '名称' in col and 'name' not in col_map.values():
                col_map[col] = 'name'
            elif '最新价' in col and 'price' not in col_map.values():
                col_map[col] = 'price'
            elif '涨跌幅' in col and '涨跌额' not in col and 'pct_chg' not in col_map.values():
                col_map[col] = 'pct_chg'
            elif '成交额' in col and 'amount' not in col_map.values():
                col_map[col] = 'amount'
            elif '换手率' in col and 'turnover' not in col_map.values():
                col_map[col] = 'turnover'
            elif '流通市值' in col and 'float_mv' not in col_map.values():
                col_map[col] = 'float_mv'
            elif '总市值' in col and 'total_mv' not in col_map.values():
                col_map[col] = 'total_mv'
            elif ('连板' in col or '涨停天' in col) and 'limit_days' not in col_map.values():
                col_map[col] = 'limit_days'
            elif '首次封板' in col and 'first_time' not in col_map.values():
                col_map[col] = 'first_time'
            elif '封单' in col and 'seal_amount' not in col_map.values():
                col_map[col] = 'seal_amount'
            elif '行业' in col and 'industry' not in col_map.values():
                col_map[col] = 'industry'

        df = limit_up_df.rename(columns=col_map)

        # 过滤 ST
        if 'name' in df.columns:
            df = df[~df['name'].str.contains('ST|退', na=False)]

        # 数值转换
        for c in ['price', 'pct_chg', 'amount', 'turnover', 'float_mv',
                   'total_mv', 'limit_days', 'seal_amount']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

        # 根据情绪阶段不同排序方式
        if emotion_phase in ("高潮", "升温"):
            # 高潮/升温：连板数 > 封单金额
            sort_cols = []
            if 'limit_days' in df.columns:
                sort_cols.append('limit_days')
            if 'seal_amount' in df.columns:
                sort_cols.append('seal_amount')
            if sort_cols:
                df = df.sort_values(sort_cols, ascending=False)
            tag = "连板龙头" if emotion_phase == "高潮" else "强势涨停"
        elif emotion_phase == "冰点":
            # 冰点：换手高的涨停股更有参考价值
            if 'turnover' in df.columns:
                df = df.sort_values('turnover', ascending=False)
            tag = "涨停反弹"
        elif emotion_phase == "退潮":
            # 退潮：总市值大的更抗跌
            if 'total_mv' in df.columns:
                df = df.sort_values('total_mv', ascending=False)
            tag = "龙头抗跌"
        else:
            # 修复/默认：成交额排序
            if 'amount' in df.columns:
                df = df.sort_values('amount', ascending=False)
            tag = "涨停活跃"

        # 构建推荐列表
        stocks = []
        for _, row in df.head(PICK_COUNT + 5).iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            if not code or not name:
                continue

            stock = {
                "stock_code": code,
                "stock_name": name,
                "price": float(row.get('price', 0) or 0),
                "change_pct": float(row.get('pct_chg', 0) or 0),
                "amount": float(row.get('amount', 0) or 0),
                "turnover_rate": float(row.get('turnover', 0) or 0),
                "total_market_cap": float(row.get('total_mv', 0) or 0),
                "float_market_cap": float(row.get('float_mv', 0) or 0),
                "limit_up_days": int(row.get('limit_days', 0) or 0),
                "first_limit_time": str(row.get('first_time', '') or ''),
                "seal_amount": float(row.get('seal_amount', 0) or 0),
                "pick_reason_tag": tag,
                "industry": str(row.get('industry', '') or ''),
            }
            stocks.append(stock)
            if len(stocks) >= PICK_COUNT:
                break

        logger.info("Fallback generated stocks from limit_up pool",
                     count=len(stocks), tag=tag)
        # 【优化】退潮期只推荐5只
        if emotion_phase == "退潮" and len(stocks) > 5:
            stocks = stocks[:5]
        return stocks


# 全局单例
sentiment_stock_picker = SentimentStockPicker()
