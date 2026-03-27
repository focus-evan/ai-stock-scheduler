"""
突破战法（Breakthrough Strategy）核心逻辑模块

实现突破战法的核心模块：
1. 获取实时行情 + 历史K�?
2. 识别关键阻力位（前高、平台上沿、N日高点）
3. 判断放量突破（当日成交量 > N日均�?* 倍数�?
4. 综合打分排序
5. GPT-5.2 深度分析增强

数据源：AkShare
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd
import structlog

# 统一反爬模块
try:
    from anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT
except ImportError:
    from app.anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT

logger = structlog.get_logger()


class BreakthroughStrategy:
    """突破战法核心策略�?""

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
        获取突破战法推荐列表

        流程:
        1. 获取实时行情
        2. 筛选今日涨�?3%-9.8% 的活跃股（排除已涨停、ST�?
        3. 拉取候选股�?0日K�?
        4. 计算关键阻力�?+ 判断是否放量突破
        5. 综合评分排序
        6. GPT-5.2 增强
        """
        cache_key = f"breakthrough_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            logger.info("Returning cached breakthrough recommendations")
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            # 步骤1: 获取实时行情
            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)

            # 步骤2: 筛选候选股
            candidates = await loop.run_in_executor(
                None, self._filter_candidates, realtime_df
            )

            # 步骤3+4: 对候选股检测突破信�?
            breakthrough_stocks = await loop.run_in_executor(
                None, self._detect_breakthroughs, candidates
            )

            # 步骤5: 综合评分排序
            result = self._build_recommendations(breakthrough_stocks, limit)

            # 步骤6: GPT-5.2 深度分析
            if breakthrough_stocks:
                try:
                    try:
                        from breakthrough_llm import breakthrough_llm
                    except ImportError:
                        from app.breakthrough_llm import breakthrough_llm

                    llm_result = await breakthrough_llm.enhance_recommendations(
                        stocks=result["data"]["recommendations"][:5],
                    )
                    if llm_result:
                        if llm_result.get("enhanced_stocks"):
                            result["data"]["recommendations"] = llm_result["enhanced_stocks"][:limit]
                        if llm_result.get("strategy_report"):
                            result["data"]["strategy_report"] = llm_result["strategy_report"]
                        if llm_result.get("market_assessment"):
                            result["data"]["market_assessment"] = llm_result["market_assessment"]
                        result["data"]["llm_enhanced"] = True
                    else:
                        result["data"]["llm_enhanced"] = False
                except Exception as llm_e:
                    logger.error("Breakthrough LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False
            else:
                result["data"]["llm_enhanced"] = False

            # 持久�?
            try:
                try:
                    from breakthrough_repository import breakthrough_repo
                except ImportError:
                    from app.breakthrough_repository import breakthrough_repo
                batch_id = await breakthrough_repo.save_strategy_result(result)
                if batch_id:
                    logger.info("Breakthrough data persisted", batch_id=batch_id)
            except Exception as db_e:
                logger.error("Breakthrough persistence failed", error=str(db_e))

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Breakthrough strategy failed", error=str(e),
                         traceback=traceback.format_exc())
            raise

    def _get_realtime_data(self) -> pd.DataFrame:
        """获取A股实时行情（多源降级�?""
        try:
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes
            return get_realtime_quotes()
        except Exception as e:
            logger.error("Failed to get realtime data", error=str(e))
            return pd.DataFrame()

    def _filter_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选候选股（优化版）：
        - 涨幅 3% ~ 9.8%（排除已涨停，下限提高至3%确保突破力度�?
        - 成交�?> 8000万（流动性要求提高）
        - 排除ST、北交所、退市股
        - 【新增】总市�?> 30亿（避免小盘股假突破�?
        """
        if df.empty:
            return df

        # 标准化列�?
        col_map = {}
        mapped = set()
        for col in df.columns:
            if '代码' in col and 'code' not in mapped:
                col_map[col] = 'code'; mapped.add('code')
            elif '名称' in col and 'name' not in mapped:
                col_map[col] = 'name'; mapped.add('name')
            elif '涨跌�? in col and 'change_pct' not in mapped:
                col_map[col] = 'change_pct'; mapped.add('change_pct')
            elif ('最新价' in col or '收盘' in col) and 'price' not in mapped:
                col_map[col] = 'price'; mapped.add('price')
            elif '成交�? in col and 'amount' not in mapped:
                col_map[col] = 'amount'; mapped.add('amount')
            elif '成交�? in col and 'volume' not in mapped:
                col_map[col] = 'volume'; mapped.add('volume')
            elif '流通市�? in col and 'float_market_cap' not in mapped:
                col_map[col] = 'float_market_cap'; mapped.add('float_market_cap')
            elif '总市�? in col and 'total_market_cap' not in mapped:
                col_map[col] = 'total_market_cap'; mapped.add('total_market_cap')
            elif '换手�? in col and 'turnover_rate' not in mapped:
                col_map[col] = 'turnover_rate'; mapped.add('turnover_rate')
            elif '最�? in col and 'high' not in mapped:
                col_map[col] = 'high'; mapped.add('high')
            elif '最�? in col and 'low' not in mapped:
                col_map[col] = 'low'; mapped.add('low')
            elif '今开' in col and 'open' not in mapped:
                col_map[col] = 'open'; mapped.add('open')

        df = df.rename(columns=col_map)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        for col in ['change_pct', 'price', 'amount', 'volume', 'float_market_cap',
                     'total_market_cap', 'turnover_rate', 'high', 'low', 'open']:
            if col in df.columns:
                data = df[col]
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        # 过滤条件（优化版�?
        mask = pd.Series(True, index=df.index)
        if 'change_pct' in df.columns:
            mask &= (df['change_pct'] >= 3) & (df['change_pct'] < 9.8)  # 下限2%�?%
        if 'amount' in df.columns:
            mask &= df['amount'] >= 8e7  # 5000万→8000�?
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            mask &= ~df['code'].astype(str).str.startswith('8')
        # 【新增】总市�?30�?
        if 'total_market_cap' in df.columns:
            mask &= (df['total_market_cap'] >= 3e9) | (df['total_market_cap'].isna())

        filtered = df[mask].copy()
        if 'change_pct' in filtered.columns:
            filtered = filtered.sort_values('change_pct', ascending=False).head(80)

        logger.info("Filtered breakthrough candidates", count=len(filtered))
        return filtered

    def _detect_breakthroughs(self, candidates: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        检测突破信号：
        1. 拉取�?0日K�?
        2. 计算20�?60日最高价作为阻力�?
        3. 判断今日是否突破 + 放量验证
        """
        results = []
        if candidates.empty or 'code' not in candidates.columns:
            return results

        # 限制最多分�?5只以控制API调用�?
        to_check = candidates.head(25)

        for _, row in to_check.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            if not code:
                continue

            try:
                # 【优化】使用统一反爬模块
                anti_scrape_delay(f"bt_kline_{code}", *DELAY_NORMAL)

                # 获取�?0日K线（带重试）
                hist = None
                for attempt in range(3):
                    try:
                        hist = self.ak.stock_zh_a_hist(
                            symbol=code, period="daily",
                            start_date=(datetime.now() - timedelta(days=120)).strftime("%Y%m%d"),
                            end_date=datetime.now().strftime("%Y%m%d"),
                            adjust="qfq"
                        )
                        break
                    except Exception:
                        if attempt < 2:
                            anti_scrape_delay(f"bt_retry_{code}_{attempt}", *DELAY_LIGHT)
                        else:
                            raise

                if hist is None or len(hist) < 20:
                    continue

                # 标准化历史K线列�?
                h_col_map = {}
                h_mapped = set()
                for c in hist.columns:
                    if '收盘' in c and 'close' not in h_mapped:
                        h_col_map[c] = 'close'; h_mapped.add('close')
                    elif '最�? in c and 'high' not in h_mapped:
                        h_col_map[c] = 'high'; h_mapped.add('high')
                    elif '最�? in c and 'low' not in h_mapped:
                        h_col_map[c] = 'low'; h_mapped.add('low')
                    elif '开�? in c and 'open' not in h_mapped:
                        h_col_map[c] = 'open'; h_mapped.add('open')
                    elif '成交�? in c and 'volume' not in h_mapped:
                        h_col_map[c] = 'volume'; h_mapped.add('volume')
                    elif '成交�? in c and 'amount' not in h_mapped:
                        h_col_map[c] = 'amount'; h_mapped.add('amount')

                hist = hist.rename(columns=h_col_map)
                for c in ['close', 'high', 'low', 'open', 'volume', 'amount']:
                    if c in hist.columns:
                        hist[c] = pd.to_numeric(hist[c], errors='coerce')

                if 'close' not in hist.columns or 'high' not in hist.columns:
                    continue

                # 当前价格
                current_price = float(row.get('price', 0))
                if current_price <= 0:
                    continue

                # 排除今日数据，用前N日数据计算阻力位
                hist_prev = hist.iloc[:-1] if len(hist) > 1 else hist

                # 计算阻力�?
                high_20d = hist_prev['high'].tail(20).max() if len(hist_prev) >= 20 else hist_prev['high'].max()
                high_60d = hist_prev['high'].tail(60).max() if len(hist_prev) >= 60 else hist_prev['high'].max()

                # 计算均量
                avg_vol_5d = hist_prev['volume'].tail(5).mean() if 'volume' in hist_prev.columns else 0
                avg_vol_20d = hist_prev['volume'].tail(20).mean() if 'volume' in hist_prev.columns else 0
                today_vol = float(row.get('volume', 0))

                # 判断突破类型
                breakthrough_type = ""
                breakthrough_price = 0

                if current_price > high_60d and high_60d > 0:
                    breakthrough_type = "60日新高突�?
                    breakthrough_price = high_60d
                elif current_price > high_20d and high_20d > 0:
                    breakthrough_type = "20日新高突�?
                    breakthrough_price = high_20d
                else:
                    continue  # 没有突破信号

                # 放量验证
                volume_ratio = float((today_vol / avg_vol_5d)) if avg_vol_5d > 0 else 0.0
                vol_ratio_20 = float((today_vol / avg_vol_20d)) if avg_vol_20d > 0 else 0.0
                is_volume_confirmed = bool(volume_ratio >= 1.5)

                # 突破幅度
                breakthrough_pct = ((current_price - breakthrough_price) / breakthrough_price * 100) if breakthrough_price > 0 else 0

                # 均线体系
                closes = hist['close'].values.astype(float)
                ma5 = float(np.mean(closes[-5:])) if len(closes) >= 5 else current_price
                ma10 = float(np.mean(closes[-10:])) if len(closes) >= 10 else current_price
                ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else current_price
                ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else current_price
                above_ma = bool(current_price > ma5 and current_price > ma20)
                # 【新增】多头排列判�?
                is_bull_aligned = bool(ma5 > ma10 > ma20)

                # 换手�?
                turnover = float(row.get('turnover_rate', 0))

                # 【新增】RSI 技术验�?
                rsi_14 = self._calc_rsi(closes, 14)

                # 【新增】假突破识别：上影线过长说明抛压�?
                today_high = float(row.get('high', current_price))
                today_low = float(row.get('low', current_price))
                body = today_high - today_low
                upper_shadow = (today_high - current_price) / body if body > 0 else 0
                is_fake_risk = bool(upper_shadow > 0.5)

                # ===== 六维度百分制评分 =====
                bt_level_score = 30 if "60�? in breakthrough_type else 20

                vol_score = 25 if volume_ratio >= 2.0 else (20 if volume_ratio >= 1.5 else (12 if volume_ratio >= 1.2 else 5))

                ma_score = 15 if is_bull_aligned else (10 if above_ma else 3)

                tech_score = 0
                if rsi_14 is not None:
                    tech_score += 5 if 50 <= rsi_14 <= 70 else (-3 if rsi_14 > 80 else 0)
                if not is_fake_risk:
                    tech_score += 5

                chip_score = 10 if 3 <= turnover <= 12 else (6 if turnover <= 18 else 2)

                pct_score = 10 if 1 <= breakthrough_pct <= 5 else (6 if breakthrough_pct <= 8 else (2 if breakthrough_pct > 8 else 4))

                breakthrough_score = max(0, min(100, bt_level_score + vol_score + ma_score + tech_score + chip_score + pct_score))

                results.append({
                    "rank": 0,
                    "code": code,
                    "name": name,
                    "price": current_price,
                    "change_pct": round(float(row.get('change_pct', 0)), 2),
                    "amount": round(float(row.get('amount', 0)), 2),
                    "volume": today_vol,
                    "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                    "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                    "turnover_rate": round(turnover, 2),
                    "breakthrough_type": breakthrough_type,
                    "breakthrough_price": round(breakthrough_price, 2),
                    "breakthrough_pct": round(breakthrough_pct, 2),
                    "volume_ratio": round(volume_ratio, 2),
                    "vol_ratio_20d": round(vol_ratio_20, 2),
                    "is_volume_confirmed": is_volume_confirmed,
                    "high_20d": round(float(high_20d), 2),
                    "high_60d": round(float(high_60d), 2),
                    "ma5": round(float(ma5), 2),
                    "ma10": round(float(ma10), 2),
                    "ma20": round(float(ma20), 2),
                    "ma60": round(float(ma60), 2),
                    "above_ma": above_ma,
                    "is_bull_aligned": is_bull_aligned,
                    "rsi_14": round(rsi_14, 1) if rsi_14 else None,
                    "is_fake_risk": is_fake_risk,
                    "upper_shadow_ratio": round(upper_shadow, 2),
                    "breakthrough_score": breakthrough_score,
                    "score_detail": {
                        "bt_level": bt_level_score,
                        "volume": vol_score,
                        "ma_form": ma_score,
                        "tech": tech_score,
                        "chip": chip_score,
                        "pct": pct_score,
                    },
                    "reasons": [],
                    "recommendation_level": "关注",
                })

            except Exception as e:
                logger.warning("Failed to analyze stock", code=code, error=str(e))
                continue

        # 按突破评分排�?
        results.sort(key=lambda x: x["breakthrough_score"], reverse=True)

        for idx, stock in enumerate(results):
            stock["rank"] = idx + 1

        logger.info("Detected breakthrough stocks", count=len(results))
        return results

    def _calc_rsi(self, closes: np.ndarray, period: int = 14) -> Optional[float]:
        """计算 RSI 指标"""
        if len(closes) < period + 1:
            return None
        deltas = np.diff(closes[-(period + 1):])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = float(np.mean(gains))
        avg_loss = float(np.mean(losses))
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)

    def _build_recommendations(self, stocks: List[Dict], limit: int) -> Dict[str, Any]:
        """构建推荐结果（优化版�?""
        now = datetime.now()

        for stock in stocks:
            reasons = []
            bt = stock.get("breakthrough_type", "")
            if bt:
                reasons.append(f"{bt}，突破价 {stock.get('breakthrough_price', 0)} �?)
            if stock.get("is_volume_confirmed"):
                reasons.append(f"量比 {stock.get('volume_ratio', 0):.1f}x 放量确认")
            else:
                reasons.append(f"量比 {stock.get('volume_ratio', 0):.1f}x ⚠️放量不足")
            if stock.get("is_bull_aligned"):
                reasons.append("MA5>MA10>MA20 多头排列")
            elif stock.get("above_ma"):
                reasons.append("站上5�?20日均�?)
            if stock.get("rsi_14"):
                rsi = stock["rsi_14"]
                if rsi > 80:
                    reasons.append(f"RSI={rsi} ⚠️超买�?)
                elif 50 <= rsi <= 70:
                    reasons.append(f"RSI={rsi} 动能健康")
            if stock.get("is_fake_risk"):
                reasons.append("⚠️ 上影线过长，假突破风�?)

            stock["reasons"] = reasons
            score = stock.get("breakthrough_score", 0)
            is_fake = stock.get("is_fake_risk", False)

            # 有假突破风险的最高只能到"关注"
            if is_fake:
                stock["recommendation_level"] = "关注" if score >= 50 else "回避"
            elif score >= 80:
                stock["recommendation_level"] = "强烈推荐"
            elif score >= 60:
                stock["recommendation_level"] = "推荐"
            elif score >= 40:
                stock["recommendation_level"] = "关注"
            else:
                stock["recommendation_level"] = "回避"

        bt_types = {}
        for s in stocks:
            bt = s.get("breakthrough_type", "其他")
            bt_types[bt] = bt_types.get(bt, 0) + 1

        return {
            "status": "success",
            "data": {
                "recommendations": stocks[:limit],
                "total": len(stocks),
                "breakthrough_summary": bt_types,
                "strategy_report": self._generate_report(stocks, limit),
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
                "llm_enhanced": False,
            }
        }

    def _generate_report(self, stocks: List[Dict], limit: int) -> str:
        """生成策略报告（优化版�?""
        total = len(stocks)
        vol_confirmed = sum(1 for s in stocks if s.get("is_volume_confirmed"))
        h60_count = sum(1 for s in stocks if "60�? in s.get("breakthrough_type", ""))
        h20_count = sum(1 for s in stocks if "20�? in s.get("breakthrough_type", ""))
        bull_aligned = sum(1 for s in stocks if s.get("is_bull_aligned"))
        fake_risk = sum(1 for s in stocks if s.get("is_fake_risk"))
        avg_score = sum(s.get("breakthrough_score", 0) for s in stocks) / max(total, 1)

        return (
            f"## 突破战法扫描报告\n\n"
            f"扫描�?**{total}** 只突破信号股，平均评�?**{avg_score:.0f}**分，其中：\n"
            f"- 60日新高突�? {h60_count} 只\n"
            f"- 20日新高突�? {h20_count} 只\n"
            f"- 放量确认: {vol_confirmed}/{total} 只\n"
            f"- 多头排列: {bull_aligned}/{total} 只\n"
            f"- ⚠️假突破风�? {fake_risk}/{total} 只\n\n"
            f"### 核心逻辑\n"
            f"突破战法关注价格突破关键阻力�?20�?60日新�?的个股，配合"
            f"六维度评分（突破级别30%+量能25%+均线15%+技�?0%+筹码10%+幅度10%�?
            f"验证突破有效性。\n\n"
            f"⚠️ **风险提示**: 假突破（上影线过�?未放量）是最大风险，"
            f"次日需观察是否站稳突破位，跌破止损�?
        )


# 全局单例
breakthrough_strategy = BreakthroughStrategy()

