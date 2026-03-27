"""
竞价/尾盘战法（Auction Strategy）核心逻辑模块

实现集合竞价 + 尾盘战法：
1. 集合竞价分析 - 高开 + 竞价量能
2. 尾盘拉升检测 - 14:30后涨幅加速
3. 尾盘突破信号 - 尾盘突破日内高点

数据源：AkShare
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import structlog

logger = structlog.get_logger()


class AuctionStrategy:
    """竞价/尾盘战法核心策略类"""

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
        """获取竞价/尾盘战法推荐列表"""
        cache_key = f"auction_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)

            # 根据当前时间判断使用哪个子策略
            now = datetime.now()
            hour = now.hour

            if hour < 10:
                # 早盘集合竞价期间
                signals = await loop.run_in_executor(
                    None, self._analyze_auction, realtime_df
                )
                strategy_mode = "集合竞价"
            elif hour >= 14:
                # 尾盘期间
                signals = await loop.run_in_executor(
                    None, self._analyze_late_session, realtime_df
                )
                strategy_mode = "尾盘战法"
            else:
                # 盘中同时分析两种
                auction_signals = await loop.run_in_executor(
                    None, self._analyze_auction, realtime_df
                )
                late_signals = await loop.run_in_executor(
                    None, self._analyze_late_session, realtime_df
                )
                signals = auction_signals + late_signals
                # 去重
                seen = set()
                unique_signals = []
                for s in signals:
                    if s["code"] not in seen:
                        seen.add(s["code"])
                        unique_signals.append(s)
                signals = unique_signals
                strategy_mode = "综合分析"

            # 排序
            signals.sort(key=lambda x: x.get("signal_score", 0), reverse=True)
            for idx, s in enumerate(signals):
                s["rank"] = idx + 1

            result = self._build_recommendations(signals, limit, strategy_mode)

            # GPT-5.2 enhancement
            if signals:
                try:
                    try:
                        from auction_llm import auction_llm
                    except ImportError:
                        from app.auction_llm import auction_llm

                    llm_result = await auction_llm.enhance_recommendations(
                        stocks=result["data"]["recommendations"][:5],
                        strategy_mode=strategy_mode,
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
                    logger.error("Auction LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False

            # Persist
            try:
                try:
                    from auction_repository import auction_repo
                except ImportError:
                    from app.auction_repository import auction_repo
                await auction_repo.save_strategy_result(result)
            except Exception as db_e:
                logger.error("Auction persistence failed", error=str(db_e))

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Auction strategy failed", error=str(e),
                         traceback=traceback.format_exc())
            raise

    def _get_realtime_data(self) -> pd.DataFrame:
        try:
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes
            return get_realtime_quotes()
        except Exception as e:
            logger.error("Failed to get realtime data", error=str(e))
            return pd.DataFrame()

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """统一列名标准化"""
        col_map = {}
        mapped = set()
        for col in df.columns:
            if '代码' in col and 'code' not in mapped:
                col_map[col] = 'code'; mapped.add('code')
            elif '名称' in col and 'name' not in mapped:
                col_map[col] = 'name'; mapped.add('name')
            elif '涨跌幅' in col and 'change_pct' not in mapped:
                col_map[col] = 'change_pct'; mapped.add('change_pct')
            elif ('最新价' in col or '收盘' in col) and 'price' not in mapped:
                col_map[col] = 'price'; mapped.add('price')
            elif '成交额' in col and 'amount' not in mapped:
                col_map[col] = 'amount'; mapped.add('amount')
            elif '成交量' in col and 'volume' not in mapped:
                col_map[col] = 'volume'; mapped.add('volume')
            elif '流通市值' in col and 'float_market_cap' not in mapped:
                col_map[col] = 'float_market_cap'; mapped.add('float_market_cap')
            elif '总市值' in col and 'total_market_cap' not in mapped:
                col_map[col] = 'total_market_cap'; mapped.add('total_market_cap')
            elif '换手率' in col and 'turnover_rate' not in mapped:
                col_map[col] = 'turnover_rate'; mapped.add('turnover_rate')
            elif '最高' in col and 'high' not in mapped:
                col_map[col] = 'high'; mapped.add('high')
            elif '最低' in col and 'low' not in mapped:
                col_map[col] = 'low'; mapped.add('low')
            elif '今开' in col and 'open' not in mapped:
                col_map[col] = 'open'; mapped.add('open')
            elif '昨收' in col and 'pre_close' not in mapped:
                col_map[col] = 'pre_close'; mapped.add('pre_close')

        df = df.rename(columns=col_map)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        for col in ['change_pct', 'price', 'amount', 'volume', 'float_market_cap',
                     'total_market_cap', 'turnover_rate', 'high', 'low', 'open', 'pre_close']:
            if col in df.columns:
                data = df[col]
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        return df

    def _analyze_auction(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        集合竞价分析：
        - 高开 2%~5%（非大幅高开）
        - 成交额有一定量能
        - 排除ST、北交所
        """
        if df.empty:
            return []

        df = self._standardize_columns(df)
        results = []

        mask = pd.Series(True, index=df.index)
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            valid_prefix = df['code'].astype(str).str.match(r'^(00|30|60)')
            mask &= valid_prefix
        if 'amount' in df.columns:
            mask &= df['amount'] >= 5e7  # 3000万→5000万

        filtered = df[mask].copy()

        # 计算开盘涨幅
        if 'open' in filtered.columns and 'pre_close' in filtered.columns:
            filtered['open_pct'] = ((filtered['open'] - filtered['pre_close']) /
                                     filtered['pre_close'] * 100)
            # 高开 2%~7%
            auction_stocks = filtered[
                (filtered['open_pct'] >= 2) & (filtered['open_pct'] <= 7)
            ].copy()
        else:
            return []

        # 涨幅配合（当前涨幅高于开盘涨幅=强势）
        if 'change_pct' in auction_stocks.columns:
            auction_stocks['strength'] = auction_stocks['change_pct'] - auction_stocks['open_pct']

        auction_stocks = auction_stocks.sort_values('open_pct', ascending=False).head(30)

        for _, row in auction_stocks.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            open_pct = round(float(row.get('open_pct', 0)), 2)
            strength = round(float(row.get('strength', 0)), 2)
            change_pct = round(float(row.get('change_pct', 0)), 2)

            signal_score = 50
            # 适度高开为最佳（太高为诱多风险）
            if 3 <= open_pct <= 5:
                signal_score += 20
            elif 2 <= open_pct < 3:
                signal_score += 12
            elif open_pct > 5:
                signal_score += 8  # 过高风险

            if strength > 1:
                signal_score += 18  # 竞价后显著走强
            elif strength > 0:
                signal_score += 12
            elif strength < -2:
                signal_score -= 15  # 高开低走严重
            elif strength < -1:
                signal_score -= 8

            if 5 <= float(row.get('turnover_rate', 0)) <= 12:
                signal_score += 5

            # 【新增】高开低走风险标记
            is_high_open_drop = bool(strength < -1)

            results.append({
                "rank": 0,
                "code": code,
                "name": name,
                "price": round(float(row.get('price', 0)), 2),
                "change_pct": change_pct,
                "amount": round(float(row.get('amount', 0)), 2),
                "volume": float(row.get('volume', 0)),
                "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                "turnover_rate": round(float(row.get('turnover_rate', 0)), 2),
                "signal_type": "集合竞价高开",
                "signal_score": max(0, min(100, signal_score)),
                "open_pct": open_pct,
                "strength": strength,
                "is_high_open_drop": is_high_open_drop,
                "reasons": [],
                "recommendation_level": "关注",
            })

        logger.info("Auction signals", count=len(results))
        return results

    def _analyze_late_session(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        尾盘战法分析：
        - 当日涨幅 3%~9.5%
        - 最高价接近最新价（尾盘拉升）
        - 成交额充足
        """
        if df.empty:
            return []

        df = self._standardize_columns(df)
        results = []

        mask = pd.Series(True, index=df.index)
        if 'change_pct' in df.columns:
            mask &= (df['change_pct'] >= 3) & (df['change_pct'] < 9.5)
        if 'amount' in df.columns:
            mask &= df['amount'] >= 8e7  # 5000万→8000万
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            valid_prefix = df['code'].astype(str).str.match(r'^(00|30|60)')
            mask &= valid_prefix

        filtered = df[mask].copy()

        # 尾盘拉升特征：最新价接近日内最高价
        if 'price' in filtered.columns and 'high' in filtered.columns and 'low' in filtered.columns:
            filtered['high_low_range'] = filtered['high'] - filtered['low']
            filtered['price_to_high'] = (filtered['high'] - filtered['price'])
            # 最新价距最高价的比例 < 20% 日内波幅
            filtered['near_high_pct'] = (
                filtered['price_to_high'] / filtered['high_low_range'] * 100
            ).fillna(100)

            late_stocks = filtered[filtered['near_high_pct'] <= 20].copy()
        else:
            late_stocks = filtered.head(30)

        late_stocks = late_stocks.sort_values('change_pct', ascending=False).head(30)

        for _, row in late_stocks.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            near_high = round(float(row.get('near_high_pct', 100)), 2)
            change_pct = round(float(row.get('change_pct', 0)), 2)

            signal_score = 50
            if change_pct >= 7:
                signal_score += 20
            elif change_pct >= 5:
                signal_score += 15
            else:
                signal_score += 10

            if near_high <= 5:
                signal_score += 20  # 收在最高价附近
            elif near_high <= 10:
                signal_score += 15
            elif near_high <= 20:
                signal_score += 10

            if 5 <= float(row.get('turnover_rate', 0)) <= 15:
                signal_score += 5

            results.append({
                "rank": 0,
                "code": code,
                "name": name,
                "price": round(float(row.get('price', 0)), 2),
                "change_pct": change_pct,
                "amount": round(float(row.get('amount', 0)), 2),
                "volume": float(row.get('volume', 0)),
                "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                "turnover_rate": round(float(row.get('turnover_rate', 0)), 2),
                "signal_type": "尾盘拉升",
                "signal_score": signal_score,
                "near_high_pct": near_high,
                "open_pct": 0,
                "strength": 0,
                "reasons": [],
                "recommendation_level": "关注",
            })

        logger.info("Late session signals", count=len(results))
        return results

    def _build_recommendations(self, stocks: List[Dict], limit: int, mode: str) -> Dict[str, Any]:
        now = datetime.now()

        for stock in stocks:
            reasons = []
            st = stock.get("signal_type", "")
            if "集合竞价" in st:
                reasons.append(f"竞价高开 {stock.get('open_pct', 0):.1f}%")
                if stock.get("strength", 0) > 0:
                    reasons.append("竞价后持续走强，资金认可")
                else:
                    reasons.append("注意高开低走风险")
            elif "尾盘" in st:
                reasons.append(f"尾盘强势拉升，收盘价距日高仅 {stock.get('near_high_pct', 0):.1f}%")
                reasons.append("次日大概率有溢价空间")

            if 5 <= stock.get("turnover_rate", 0) <= 12:
                reasons.append("换手率适中，筹码交换充分")

            stock["reasons"] = reasons
            score = stock.get("signal_score", 0)
            is_drop = stock.get("is_high_open_drop", False)

            # 高开低走的最高只能到"关注"
            if is_drop:
                stock["recommendation_level"] = "关注" if score >= 40 else "回避"
            elif score >= 80:
                stock["recommendation_level"] = "强烈推荐"
            elif score >= 60:
                stock["recommendation_level"] = "推荐"
            elif score >= 40:
                stock["recommendation_level"] = "关注"
            else:
                stock["recommendation_level"] = "回避"

        signal_counts = {}
        for s in stocks:
            st = s.get("signal_type", "其他")
            signal_counts[st] = signal_counts.get(st, 0) + 1

        return {
            "status": "success",
            "data": {
                "recommendations": stocks[:limit],
                "total": len(stocks),
                "strategy_mode": mode,
                "signal_summary": signal_counts,
                "strategy_report": self._generate_report(stocks, mode),
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
                "llm_enhanced": False,
            }
        }

    def _generate_report(self, stocks: List[Dict], mode: str) -> str:
        total = len(stocks)
        auction_count = sum(1 for s in stocks if "竞价" in s.get("signal_type", ""))
        late_count = sum(1 for s in stocks if "尾盘" in s.get("signal_type", ""))

        return (
            f"## 竞价/尾盘战法扫描报告 ({mode})\n\n"
            f"扫描到 **{total}** 只信号股：\n"
            f"- 🌅 竞价信号: {auction_count} 只\n"
            f"- 🌆 尾盘信号: {late_count} 只\n\n"
            f"### 核心逻辑\n"
            f"**集合竞价**: 通过9:25竞价结果判断资金意图，高开+量能匹配=资金抢筹信号。\n"
            f"**尾盘战法**: 14:30后选股买入，规避日内波动，博弈次日溢价。\n\n"
            f"⚠️ **风险提示**: 竞价高开可能是诱多；尾盘拉升可能是主力对倒。"
        )


# 全局单例
auction_strategy = AuctionStrategy()
