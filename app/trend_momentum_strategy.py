"""
趋势动量战法（Trend Momentum Strategy）核心逻辑模块

实现趋势动量战法的核心模块：
1. 获取实时行情 + 历史K线
2. 计算多周期动量指标（5日/20日/60日回报率）
3. MACD 趋势确认 + 柱状图背离检测
4. ADX 趋势强度判断
5. 动量衰竭检测（5日 vs 20日动量斜率对比）
6. RSI 顶背离预警（价格新高但RSI回落）
7. 综合打分排序（含衰竭扣分）
8. GPT 深度分析增强

数据源：AkShare（凌晨预运行，基于历史收盘数据）

优化点：
- 5日短期动量判断近期速度
- 动量减速/衰竭预警（5d动量 < 20d动量/4 = 减速）
- RSI顶背离检测（价格创新高但RSI未创新高）
- MACD柱状图缩短预警（趋势可能转弱）
- 衰竭扣分机制（多个衰竭信号叠加扣分）
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd
import structlog

try:
    from anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT
except ImportError:
    from app.anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT

logger = structlog.get_logger()


class TrendMomentumStrategy:
    """趋势动量战法核心策略类"""

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
        获取趋势动量推荐列表

        流程:
        1. 获取实时行情，筛选活跃标的
        2. 拉取候选股近120日K线
        3. 计算动量指标（20日/60日回报率、MACD、ADX）
        4. 综合评分排序
        5. GPT 深度分析增强
        """
        cache_key = f"trend_momentum_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            logger.info("Returning cached trend momentum recommendations")
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            # 步骤1: 获取实时行情
            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)

            # 步骤2: 筛选候选股
            candidates = await loop.run_in_executor(
                None, self._filter_candidates, realtime_df
            )

            # 步骤3+4: 对候选股计算动量+趋势信号
            momentum_stocks = await loop.run_in_executor(
                None, self._detect_momentum, candidates
            )

            # 步骤5: 构建推荐结果
            result = self._build_recommendations(momentum_stocks, limit)

            # 步骤6: LLM 增强
            if momentum_stocks:
                try:
                    try:
                        from trend_momentum_llm import trend_momentum_llm
                    except ImportError:
                        from app.trend_momentum_llm import trend_momentum_llm

                    llm_result = await trend_momentum_llm.enhance_recommendations(
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
                    logger.error("Trend Momentum LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False
            else:
                result["data"]["llm_enhanced"] = False

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Trend momentum strategy failed", error=str(e),
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

    def _filter_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选候选股：
        - 涨幅 1% ~ 9.8%（处于上涨趋势中）
        - 成交额 > 1亿（高流动性）
        - 排除ST、北交所、退市股
        - 总市值 > 50亿（避免小盘股噪音）
        """
        if df.empty:
            return df

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

        df = df.rename(columns=col_map)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        for col in ['change_pct', 'price', 'amount', 'volume',
                     'float_market_cap', 'total_market_cap', 'turnover_rate']:
            if col in df.columns:
                data = df[col]
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        mask = pd.Series(True, index=df.index)
        if 'change_pct' in df.columns:
            mask &= (df['change_pct'] >= 1) & (df['change_pct'] < 9.8)
        if 'amount' in df.columns:
            mask &= df['amount'] >= 1e8  # 成交额>1亿
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            valid_prefix = df['code'].astype(str).str.match(r'^(00|30|60)')
            mask &= valid_prefix
        if 'total_market_cap' in df.columns:
            mask &= (df['total_market_cap'] >= 5e9) | (df['total_market_cap'].isna())

        filtered = df[mask].copy()
        if 'change_pct' in filtered.columns:
            filtered = filtered.sort_values('change_pct', ascending=False).head(100)

        logger.info("Filtered trend momentum candidates", count=len(filtered))
        return filtered

    def _detect_momentum(self, candidates: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        检测趋势动量信号：
        1. 拉取近120日K线
        2. 计算20日/60日动量（回报率）
        3. MACD 金叉/死叉判断
        4. ADX 趋势强度
        5. 创N日新高检测
        """
        results = []
        if candidates.empty or 'code' not in candidates.columns:
            return results

        to_check = candidates.head(30)

        for _, row in to_check.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            if not code:
                continue

            try:
                anti_scrape_delay(f"tm_kline_{code}", *DELAY_NORMAL)

                hist = None
                for attempt in range(3):
                    try:
                        hist = self.ak.stock_zh_a_hist(
                            symbol=code, period="daily",
                            start_date=(datetime.now() - timedelta(days=180)).strftime("%Y%m%d"),
                            end_date=datetime.now().strftime("%Y%m%d"),
                            adjust="qfq"
                        )
                        break
                    except Exception:
                        if attempt < 2:
                            anti_scrape_delay(f"tm_retry_{code}_{attempt}", *DELAY_LIGHT)
                        else:
                            raise

                if hist is None or len(hist) < 30:
                    continue

                # 标准化列名
                h_map = {}
                h_mapped = set()
                for c in hist.columns:
                    if '收盘' in c and 'close' not in h_mapped:
                        h_map[c] = 'close'; h_mapped.add('close')
                    elif '最高' in c and 'high' not in h_mapped:
                        h_map[c] = 'high'; h_mapped.add('high')
                    elif '最低' in c and 'low' not in h_mapped:
                        h_map[c] = 'low'; h_mapped.add('low')
                    elif '开盘' in c and 'open' not in h_mapped:
                        h_map[c] = 'open'; h_mapped.add('open')
                    elif '成交量' in c and 'volume' not in h_mapped:
                        h_map[c] = 'volume'; h_mapped.add('volume')

                hist = hist.rename(columns=h_map)
                for c in ['close', 'high', 'low', 'open', 'volume']:
                    if c in hist.columns:
                        hist[c] = pd.to_numeric(hist[c], errors='coerce')

                if 'close' not in hist.columns:
                    continue

                closes = hist['close'].values.astype(float)
                highs = hist['high'].values.astype(float) if 'high' in hist.columns else closes
                lows = hist['low'].values.astype(float) if 'low' in hist.columns else closes
                current_price = float(row.get('price', closes[-1]))

                # 多周期动量计算
                momentum_5d = ((closes[-1] / closes[-6]) - 1) * 100 if len(closes) >= 6 else 0
                momentum_20d = ((closes[-1] / closes[-21]) - 1) * 100 if len(closes) >= 21 else 0
                momentum_60d = ((closes[-1] / closes[-61]) - 1) * 100 if len(closes) >= 61 else 0

                # 动量衰竭检测：5日动量 vs 20日动量的比例
                # 如果20日动量很高但5日动量接近0或为负 -> 动量在减速
                momentum_deceleration = False
                momentum_exhaustion = False
                if momentum_20d > 5:
                    # 近5日动量占20日动量的比例 -> 越小说明越减速
                    speed_ratio = momentum_5d / momentum_20d if momentum_20d != 0 else 1.0
                    if speed_ratio < 0.1:  # 5日动量不到20日的10% -> 严重减速
                        momentum_exhaustion = True
                    elif speed_ratio < 0.25:  # 5日动量不到20日的25% -> 减速中
                        momentum_deceleration = True

                # MACD 计算
                ema12 = self._calc_ema(closes, 12)
                ema26 = self._calc_ema(closes, 26)
                dif = ema12 - ema26
                dea = self._calc_ema(dif, 9)
                macd_histogram = 2 * (dif - dea)
                macd_golden = bool(dif[-1] > dea[-1] and dif[-2] <= dea[-2]) if len(dif) >= 2 else False
                macd_bullish = bool(dif[-1] > dea[-1])

                # MACD柱状图背离：价格创新高但MACD柱缩短
                macd_bar_divergence = False
                if len(macd_histogram) >= 10:
                    recent_bar = float(macd_histogram[-1])
                    prev_bar_max = float(np.max(macd_histogram[-10:-1]))
                    if recent_bar > 0 and prev_bar_max > 0 and recent_bar < prev_bar_max * 0.7:
                        # 当前MACD柱不到前期高点的70% -> 可能背离
                        macd_bar_divergence = True

                # ADX 计算
                adx = self._calc_adx(highs, lows, closes, 14)

                # 创N日新高
                high_20d = float(np.max(highs[-20:])) if len(highs) >= 20 else current_price
                high_60d = float(np.max(highs[-60:])) if len(highs) >= 60 else current_price
                is_20d_high = bool(current_price >= high_20d * 0.98)
                is_60d_high = bool(current_price >= high_60d * 0.98)

                # RSI 顶背离检测：价格创20日新高但RSI未创新高
                rsi_14 = self._calc_rsi(closes, 14)
                rsi_divergence = False
                if rsi_14 is not None and is_20d_high and len(closes) >= 20:
                    # 计算10天前的RSI
                    rsi_prev = self._calc_rsi(closes[:-10], 14)
                    if rsi_prev is not None and rsi_14 < rsi_prev and rsi_14 < 65:
                        rsi_divergence = True  # 价格新高但RSI回落 = 顶背离

                # 均线体系
                ma5 = float(np.mean(closes[-5:])) if len(closes) >= 5 else current_price
                ma10 = float(np.mean(closes[-10:])) if len(closes) >= 10 else current_price
                ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else current_price
                ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else current_price
                is_bull_aligned = bool(ma5 > ma10 > ma20)

                # 换手率
                turnover = float(row.get('turnover_rate', 0))

                # ===== 六维度百分制评分 + 衰竭扣分 =====
                # 1. 动量分(30): 5日+20日+60日动量
                mom_score = 0
                if momentum_5d >= 5:
                    mom_score += 8  # 5日加速
                elif momentum_5d >= 2:
                    mom_score += 5
                else:
                    mom_score += 2

                if momentum_20d >= 15:
                    mom_score += 12
                elif momentum_20d >= 8:
                    mom_score += 10
                elif momentum_20d >= 3:
                    mom_score += 6
                else:
                    mom_score += 2

                if momentum_60d >= 30:
                    mom_score += 10
                elif momentum_60d >= 15:
                    mom_score += 8
                elif momentum_60d >= 5:
                    mom_score += 5
                else:
                    mom_score += 2

                # 2. MACD趋势分(20)
                macd_score = 0
                if macd_golden:
                    macd_score = 20  # 刚金叉最佳
                elif macd_bullish and macd_histogram[-1] > 0:
                    macd_score = 15
                elif macd_bullish:
                    macd_score = 10
                else:
                    macd_score = 3

                # 3. ADX趋势强度分(15)
                adx_score = 0
                if adx is not None:
                    if adx >= 30:
                        adx_score = 15  # 强趋势
                    elif adx >= 25:
                        adx_score = 12
                    elif adx >= 20:
                        adx_score = 8
                    else:
                        adx_score = 3

                # 4. 新高分(15): 创20日/60日新高
                high_score = 0
                if is_60d_high:
                    high_score = 15
                elif is_20d_high:
                    high_score = 10
                else:
                    high_score = 3

                # 5. 均线形态分(10)
                ma_score = 10 if is_bull_aligned else (7 if current_price > ma20 else 3)

                # 6. 技术健康分(10): RSI+换手率
                tech_score = 0
                if rsi_14 is not None:
                    if 50 <= rsi_14 <= 70:
                        tech_score += 5
                    elif rsi_14 > 80:
                        tech_score -= 2
                if 3 <= turnover <= 15:
                    tech_score += 5
                elif turnover <= 25:
                    tech_score += 3

                # 7. 衰竭扣分：检测动量减速/背离信号
                exhaustion_penalty = 0
                exhaustion_warnings = []
                if momentum_exhaustion:
                    exhaustion_penalty += 12
                    exhaustion_warnings.append("动量严重衰竭")
                elif momentum_deceleration:
                    exhaustion_penalty += 6
                    exhaustion_warnings.append("动量减速中")

                if rsi_divergence:
                    exhaustion_penalty += 8
                    exhaustion_warnings.append("RSI顶背离")

                if macd_bar_divergence:
                    exhaustion_penalty += 5
                    exhaustion_warnings.append("MACD柱缩短")

                if rsi_14 is not None and rsi_14 > 80 and momentum_5d < 1:
                    exhaustion_penalty += 5
                    exhaustion_warnings.append("超买+动量停滞")

                total_score = max(0, min(100,
                    mom_score + macd_score + adx_score + high_score + ma_score + tech_score - exhaustion_penalty
                ))

                # 信号类型判定（加入衰竭判断）
                signal_type = ""
                if exhaustion_penalty >= 15:
                    signal_type = "⚠️ 动量衰竭预警"
                elif momentum_20d >= 10 and macd_bullish and adx and adx >= 25:
                    signal_type = "强趋势动量"
                elif macd_golden:
                    signal_type = "MACD金叉启动"
                elif momentum_60d >= 20 and is_20d_high:
                    signal_type = "中期动量新高"
                elif momentum_deceleration:
                    signal_type = "动量减速关注"
                elif momentum_20d >= 5 and is_bull_aligned:
                    signal_type = "多头排列趋势"
                else:
                    signal_type = "动量关注"

                results.append({
                    "rank": 0,
                    "code": code,
                    "name": name,
                    "price": round(current_price, 2),
                    "change_pct": round(float(row.get('change_pct', 0)), 2),
                    "amount": round(float(row.get('amount', 0)), 2),
                    "volume": float(row.get('volume', 0)),
                    "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                    "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                    "turnover_rate": round(turnover, 2),
                    "signal_type": signal_type,
                    "momentum_5d": round(momentum_5d, 2),
                    "momentum_20d": round(momentum_20d, 2),
                    "momentum_60d": round(momentum_60d, 2),
                    "momentum_deceleration": momentum_deceleration,
                    "momentum_exhaustion": momentum_exhaustion,
                    "rsi_divergence": rsi_divergence,
                    "macd_bar_divergence": macd_bar_divergence,
                    "exhaustion_warnings": exhaustion_warnings,
                    "macd_golden": macd_golden,
                    "macd_bullish": macd_bullish,
                    "macd_histogram": round(float(macd_histogram[-1]), 4) if len(macd_histogram) > 0 else 0,
                    "adx": round(adx, 1) if adx else None,
                    "is_20d_high": is_20d_high,
                    "is_60d_high": is_60d_high,
                    "ma5": round(ma5, 2),
                    "ma10": round(ma10, 2),
                    "ma20": round(ma20, 2),
                    "ma60": round(ma60, 2),
                    "is_bull_aligned": is_bull_aligned,
                    "rsi_14": round(rsi_14, 1) if rsi_14 else None,
                    "momentum_score": total_score,
                    "score_detail": {
                        "momentum": mom_score,
                        "macd": macd_score,
                        "adx": adx_score,
                        "new_high": high_score,
                        "ma_form": ma_score,
                        "tech": tech_score,
                        "exhaustion_penalty": -exhaustion_penalty,
                    },
                    "reasons": [],
                    "recommendation_level": "关注",
                })

            except Exception as e:
                logger.warning("Failed to analyze stock for momentum", code=code, error=str(e))
                continue

        results.sort(key=lambda x: x["momentum_score"], reverse=True)
        for idx, stock in enumerate(results):
            stock["rank"] = idx + 1

        logger.info("Detected trend momentum stocks", count=len(results))
        return results

    def _calc_ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """计算指数移动平均线"""
        ema = np.zeros_like(data, dtype=float)
        ema[0] = data[0]
        multiplier = 2.0 / (period + 1)
        for i in range(1, len(data)):
            ema[i] = data[i] * multiplier + ema[i - 1] * (1 - multiplier)
        return ema

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

    def _calc_adx(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                   period: int = 14) -> Optional[float]:
        """计算 ADX（平均趋向指标）"""
        if len(closes) < period + 2:
            return None
        try:
            n = len(closes)
            tr = np.zeros(n)
            plus_dm = np.zeros(n)
            minus_dm = np.zeros(n)

            for i in range(1, n):
                h_diff = highs[i] - highs[i - 1]
                l_diff = lows[i - 1] - lows[i]
                tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                plus_dm[i] = h_diff if (h_diff > l_diff and h_diff > 0) else 0
                minus_dm[i] = l_diff if (l_diff > h_diff and l_diff > 0) else 0

            atr = self._calc_ema(tr[1:], period)
            plus_di = 100 * self._calc_ema(plus_dm[1:], period) / np.where(atr > 0, atr, 1)
            minus_di = 100 * self._calc_ema(minus_dm[1:], period) / np.where(atr > 0, atr, 1)

            dx = 100 * np.abs(plus_di - minus_di) / np.where((plus_di + minus_di) > 0, (plus_di + minus_di), 1)
            adx = self._calc_ema(dx, period)
            return float(adx[-1])
        except Exception:
            return None

    def _build_recommendations(self, stocks: List[Dict], limit: int) -> Dict[str, Any]:
        """构建推荐结果"""
        now = datetime.now()

        for stock in stocks:
            reasons = []
            st = stock.get("signal_type", "")
            if st:
                reasons.append(st)

            # 动量信息
            m5 = stock.get('momentum_5d', 0)
            m20 = stock.get('momentum_20d', 0)
            m60 = stock.get('momentum_60d', 0)
            if m5 > 0:
                reasons.append(f"5日+{m5:.1f}% / 20日+{m20:.1f}%")
            else:
                reasons.append(f"5日{m5:.1f}% / 20日+{m20:.1f}%")
            if m60 > 0:
                reasons.append(f"60日动量 +{m60:.1f}%")

            # MACD信号
            if stock.get("macd_golden"):
                reasons.append("🔥 MACD金叉")
            elif stock.get("macd_bullish"):
                reasons.append("MACD多头")
            if stock.get("macd_bar_divergence"):
                reasons.append("⚠️ MACD柱缩短")

            # ADX
            if stock.get("adx") and stock["adx"] >= 25:
                reasons.append(f"ADX={stock['adx']:.0f} 趋势明确")

            # 新高
            if stock.get("is_60d_high"):
                reasons.append("创60日新高")
            elif stock.get("is_20d_high"):
                reasons.append("创20日新高")

            if stock.get("is_bull_aligned"):
                reasons.append("MA5>MA10>MA20 多头排列")

            # 衰竭预警
            for w in stock.get("exhaustion_warnings", []):
                reasons.append(f"⚠️ {w}")

            if stock.get("rsi_14") and stock["rsi_14"] > 80:
                if stock.get("rsi_divergence"):
                    reasons.append(f"⚠️ RSI={stock['rsi_14']} 顶背离")
                else:
                    reasons.append(f"⚠️ RSI={stock['rsi_14']} 超买区")

            stock["reasons"] = reasons
            score = stock.get("momentum_score", 0)
            has_exhaustion = len(stock.get("exhaustion_warnings", [])) > 0
            if has_exhaustion:
                # 衰竭信号 -> 最高只给"关注"级别，不能是"强烈推荐"
                if score >= 60:
                    stock["recommendation_level"] = "关注"
                else:
                    stock["recommendation_level"] = "回避"
            elif score >= 80:
                stock["recommendation_level"] = "强烈推荐"
            elif score >= 60:
                stock["recommendation_level"] = "推荐"
            elif score >= 40:
                stock["recommendation_level"] = "关注"
            else:
                stock["recommendation_level"] = "回避"

        # 信号类型统计
        signal_types = {}
        for s in stocks:
            st = s.get("signal_type", "其他")
            signal_types[st] = signal_types.get(st, 0) + 1

        return {
            "status": "success",
            "data": {
                "recommendations": stocks[:limit],
                "total": len(stocks),
                "signal_summary": signal_types,
                "strategy_report": self._generate_report(stocks, limit),
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
                "llm_enhanced": False,
            }
        }

    def _generate_report(self, stocks: List[Dict], limit: int) -> str:
        total = len(stocks)
        strong_trend = sum(1 for s in stocks if "强趋势" in s.get("signal_type", ""))
        golden_cross = sum(1 for s in stocks if s.get("macd_golden"))
        new_high = sum(1 for s in stocks if s.get("is_60d_high"))
        exhaustion_count = sum(1 for s in stocks if len(s.get("exhaustion_warnings", [])) > 0)
        deceleration_count = sum(1 for s in stocks if s.get("momentum_deceleration"))
        avg_score = sum(s.get("momentum_score", 0) for s in stocks) / max(total, 1)

        return (
            f"## 趋势动量扫描报告\n\n"
            f"扫描到 **{total}** 只动量信号股，平均评分 **{avg_score:.0f}**分，其中：\n"
            f"- 强趋势动量: {strong_trend} 只\n"
            f"- MACD金叉: {golden_cross} 只\n"
            f"- 60日新高: {new_high} 只\n"
            f"- ⚠️ 动量减速: {deceleration_count} 只\n"
            f"- 🔴 衰竭预警: {exhaustion_count} 只\n\n"
            f"### 核心逻辑\n"
            f"趋势动量战法捕捉中周期趋势确立且动量排名靠前的个股，"
            f"基于六维度评分+衰竭扣分体系。通过5日/20日/60日三周期动量交叉验证，"
            f"结合RSI顶背离和MACD柱缩短检测，可在动量衰竭前发出预警。\n\n"
            f"⚠️ **风险提示**: 带有衰竭预警(⚠️)的标的需控制仓位或等待回调，"
            f"动量严重衰竭的标的已自动降低推荐级别。"
        )


# 全局单例
trend_momentum_strategy = TrendMomentumStrategy()
