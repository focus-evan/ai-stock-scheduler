"""
均线战法（Moving Average Strategy）核心逻辑模块

实现均线战法的核心分析：
1. 均线多头排列 - MA5 > MA10 > MA20 > MA60 向上发散
2. 金叉信号 - 短期均线上穿长期均线
3. 回踩支撑 - 强势股回�?0日均线获支撑后反�?
4. 均线粘合突破 - 多根均线收敛后向上发�?

数据源：AkShare
"""

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import numpy as np
import structlog

# 统一反爬模块
try:
    from anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT
except ImportError:
    from app.anti_scrape import anti_scrape_delay, DELAY_NORMAL, DELAY_LIGHT

logger = structlog.get_logger()


class MovingAverageStrategy:
    """均线战法核心策略�?""

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
        """获取均线战法推荐列表"""
        cache_key = f"moving_average_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)
            candidates = await loop.run_in_executor(
                None, self._filter_candidates, realtime_df
            )
            ma_signals = await loop.run_in_executor(
                None, self._analyze_moving_averages, candidates
            )

            result = self._build_recommendations(ma_signals, limit)

            # GPT-5.2 enhancement
            if ma_signals:
                try:
                    try:
                        from moving_average_llm import moving_average_llm
                    except ImportError:
                        from app.moving_average_llm import moving_average_llm

                    llm_result = await moving_average_llm.enhance_recommendations(
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
                    logger.error("Moving Average LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False

            # Persist
            try:
                try:
                    from moving_average_repository import moving_average_repo
                except ImportError:
                    from app.moving_average_repository import moving_average_repo
                await moving_average_repo.save_strategy_result(result)
            except Exception as db_e:
                logger.error("Moving Average persistence failed", error=str(db_e))

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Moving Average strategy failed", error=str(e),
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

    def _filter_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        """筛选候选股"""
        if df.empty:
            return df

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

        df = df.rename(columns=col_map)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        for col in ['change_pct', 'price', 'amount', 'volume', 'float_market_cap',
                     'total_market_cap', 'turnover_rate']:
            if col in df.columns:
                data = df[col]
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        mask = pd.Series(True, index=df.index)
        if 'change_pct' in df.columns:
            mask &= (df['change_pct'] >= 0) & (df['change_pct'] < 9.8)
        if 'amount' in df.columns:
            mask &= df['amount'] >= 6e7  # 3000万→6000�?
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            mask &= ~df['code'].astype(str).str.startswith('8')
        # 【新增】市�?20�?
        if 'total_market_cap' in df.columns:
            mask &= (df['total_market_cap'] >= 2e9) | (df['total_market_cap'].isna())

        filtered = df[mask].copy()
        if 'change_pct' in filtered.columns:
            filtered = filtered.sort_values('change_pct', ascending=False).head(60)

        logger.info("MA candidates", count=len(filtered))
        return filtered

    def _analyze_moving_averages(self, candidates: pd.DataFrame) -> List[Dict[str, Any]]:
        """分析均线信号（优化版�?""
        results = []
        if candidates.empty or 'code' not in candidates.columns:
            return results

        to_check = candidates.head(25)

        for _, row in to_check.iterrows():
            code = str(row.get('code', ''))
            name = str(row.get('name', ''))
            if not code:
                continue

            try:
                # 【优化】统一反爬模块
                anti_scrape_delay(f"ma_kline_{code}", *DELAY_NORMAL)

                hist = None
                for attempt in range(3):
                    try:
                        hist = self.ak.stock_zh_a_hist(
                            symbol=code, period="daily",
                            start_date=(datetime.now() - timedelta(days=150)).strftime("%Y%m%d"),
                            end_date=datetime.now().strftime("%Y%m%d"),
                            adjust="qfq"
                        )
                        break
                    except Exception:
                        if attempt < 2:
                            anti_scrape_delay(f"ma_retry_{code}_{attempt}", *DELAY_LIGHT)
                        else:
                            raise

                if hist is None or len(hist) < 60:
                    continue

                h_col_map = {}
                h_mapped = set()
                for c in hist.columns:
                    if '收盘' in c and 'close' not in h_mapped:
                        h_col_map[c] = 'close'; h_mapped.add('close')
                    elif '成交�? in c and 'volume' not in h_mapped:
                        h_col_map[c] = 'volume'; h_mapped.add('volume')
                    elif '最�? in c and 'high' not in h_mapped:
                        h_col_map[c] = 'high'; h_mapped.add('high')
                    elif '最�? in c and 'low' not in h_mapped:
                        h_col_map[c] = 'low'; h_mapped.add('low')

                hist = hist.rename(columns=h_col_map)
                for c in ['close', 'volume', 'high', 'low']:
                    if c in hist.columns:
                        hist[c] = pd.to_numeric(hist[c], errors='coerce')

                if 'close' not in hist.columns:
                    continue

                closes = hist['close'].values.astype(float)
                current_price = float(row.get('price', closes[-1]))

                # 计算均线
                ma5 = float(np.mean(closes[-5:]))
                ma10 = float(np.mean(closes[-10:]))
                ma20 = float(np.mean(closes[-20:]))
                ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else float(np.mean(closes))

                # 前一日均线（用于金叉判断�?
                prev_ma5 = float(np.mean(closes[-6:-1]))
                prev_ma10 = float(np.mean(closes[-11:-1]))
                prev_ma20 = float(np.mean(closes[-21:-1]))

                # 【新增】均线斜率（上升速度�?
                ma20_slope = (ma20 - float(np.mean(closes[-25:-5]))) / float(np.mean(closes[-25:-5])) * 100 if len(closes) >= 25 else 0
                ma60_slope = (ma60 - float(np.mean(closes[-65:-5]))) / float(np.mean(closes[-65:-5])) * 100 if len(closes) >= 65 else 0

                # 【新增】MACD计算
                macd_line, macd_signal, macd_hist = self._calc_macd(closes)
                is_macd_golden = bool(macd_hist is not None and macd_hist > 0)
                is_macd_just_golden = False
                if macd_line is not None and macd_signal is not None and len(closes) >= 28:
                    prev_macd_hist = self._calc_macd(closes[:-1])[2]
                    is_macd_just_golden = bool(prev_macd_hist is not None and prev_macd_hist <= 0 and macd_hist > 0)

                # 信号检�?
                signal_type = ""
                signal_base_score = 0

                is_bull_aligned = bool(ma5 > ma10 > ma20 > ma60)
                is_partial_bull = bool(ma5 > ma10 > ma20)  # 不含60�?

                # 1. 均线多头排列
                if is_bull_aligned and current_price > ma5:
                    signal_type = "均线多头排列"
                    signal_base_score = 35

                # 2. 金叉信号
                elif prev_ma5 <= prev_ma10 and ma5 > ma10:
                    signal_type = "5�?10日金�?
                    signal_base_score = 30
                    if current_price > ma20:
                        signal_base_score += 5

                elif prev_ma5 <= prev_ma20 and ma5 > ma20:
                    signal_type = "5�?20日金�?
                    signal_base_score = 28

                # 3. 回踩20日线支撑
                elif (abs(current_price - ma20) / ma20 * 100 < 2
                      and current_price > ma20
                      and ma5 > ma20 > ma60):
                    signal_type = "回踩20日线支撑"
                    signal_base_score = 35

                # 4. 均线粘合突破
                elif (abs(ma5 - ma10) / ma10 * 100 < 1
                      and abs(ma10 - ma20) / ma20 * 100 < 1.5
                      and current_price > max(ma5, ma10, ma20)):
                    signal_type = "均线粘合突破"
                    signal_base_score = 33

                else:
                    continue

                # ===== 五维度百分制评分 =====

                # 均线形�?(25%)
                form_score = 0
                if is_bull_aligned:
                    form_score = 25
                elif is_partial_bull:
                    form_score = 18
                elif current_price > ma20:
                    form_score = 10
                else:
                    form_score = 3

                # 【新增】均线斜率加�?
                if ma20_slope > 3:  # 20日线明显上升
                    form_score = min(25, form_score + 3)

                # 量能配合 (15%)
                vol_score = 0
                vol_ratio = 0.0
                if 'volume' in hist.columns:
                    volumes = hist['volume'].values.astype(float)
                    avg_vol = float(np.mean(volumes[-6:-1])) if len(volumes) >= 6 else float(np.mean(volumes))
                    today_vol = float(row.get('volume', 0))
                    vol_ratio = float(today_vol / avg_vol) if avg_vol > 0 else 0.0
                    if vol_ratio >= 2.0:
                        vol_score = 15
                    elif vol_ratio >= 1.5:
                        vol_score = 12
                    elif vol_ratio >= 1.0:
                        vol_score = 7
                    else:
                        vol_score = 3

                # 【新增】MACD共振 (15%)
                macd_score = 0
                if is_macd_just_golden:
                    macd_score = 15  # MACD刚金叉，满分
                elif is_macd_golden:
                    macd_score = 10  # MACD在零轴上
                else:
                    macd_score = 2

                # 筹码结构 (10%)
                turnover = float(row.get('turnover_rate', 0))
                chip_score = 10 if 3 <= turnover <= 12 else (6 if turnover <= 18 else 3)

                signal_score = signal_base_score + form_score + vol_score + macd_score + chip_score
                signal_score = max(0, min(100, signal_score))

                results.append({
                    "rank": 0,
                    "code": code,
                    "name": name,
                    "price": current_price,
                    "change_pct": round(float(row.get('change_pct', 0)), 2),
                    "amount": round(float(row.get('amount', 0)), 2),
                    "volume": float(row.get('volume', 0)),
                    "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                    "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                    "turnover_rate": round(turnover, 2),
                    "signal_type": signal_type,
                    "signal_score": signal_score,
                    "ma5": round(ma5, 2), "ma10": round(ma10, 2),
                    "ma20": round(ma20, 2), "ma60": round(ma60, 2),
                    "ma20_slope": round(ma20_slope, 2),
                    "is_bull_aligned": bool(is_bull_aligned),
                    "is_partial_bull": bool(is_partial_bull),
                    "price_vs_ma20": round(float((current_price - ma20) / ma20 * 100), 2) if ma20 > 0 else 0.0,
                    "vol_ratio": round(vol_ratio, 2),
                    "is_macd_golden": is_macd_golden,
                    "is_macd_just_golden": is_macd_just_golden,
                    "score_detail": {
                        "signal_base": signal_base_score,
                        "form": form_score,
                        "volume": vol_score,
                        "macd": macd_score,
                        "chip": chip_score,
                    },
                    "reasons": [],
                    "recommendation_level": "关注",
                })

            except Exception as e:
                logger.warning("MA analysis failed", code=code, error=str(e))
                continue

        results.sort(key=lambda x: x["signal_score"], reverse=True)
        for idx, stock in enumerate(results):
            stock["rank"] = idx + 1

        logger.info("MA signals detected", count=len(results))
        return results

    def _calc_macd(self, closes: np.ndarray, fast=12, slow=26, signal=9):
        """计算MACD指标"""
        if len(closes) < slow + signal:
            return None, None, None
        # EMA计算
        def ema(data, period):
            alpha = 2 / (period + 1)
            result = np.zeros_like(data, dtype=float)
            result[0] = data[0]
            for i in range(1, len(data)):
                result[i] = alpha * data[i] + (1 - alpha) * result[i-1]
            return result

        ema_fast = ema(closes, fast)
        ema_slow = ema(closes, slow)
        macd_line = ema_fast[-1] - ema_slow[-1]
        # Signal line = EMA of MACD line
        diff_series = ema_fast - ema_slow
        signal_ema = ema(diff_series, signal)
        macd_signal = signal_ema[-1]
        macd_hist = macd_line - macd_signal
        return round(macd_line, 4), round(macd_signal, 4), round(macd_hist, 4)

    def _build_recommendations(self, stocks: List[Dict], limit: int) -> Dict[str, Any]:
        """构建推荐结果（优化版�?""
        now = datetime.now()

        for stock in stocks:
            reasons = []
            st = stock.get("signal_type", "")

            if "多头排列" in st:
                reasons.append("MA5>MA10>MA20>MA60 多头排列，趋势向�?)
            elif "金叉" in st:
                reasons.append(f"{st}，短期均线上穿长期均线，买入信号")
            elif "回踩" in st:
                reasons.append("回踩20日均线获得支撑，短期调整结束")
            elif "粘合突破" in st:
                reasons.append("多根均线粘合后向上突破，方向选择确定")

            # 【新增】MACD共振
            if stock.get("is_macd_just_golden"):
                reasons.append("🟢 MACD刚刚金叉，均�?MACD双重确认")
            elif stock.get("is_macd_golden"):
                reasons.append("MACD在零轴上方，动能为正")

            # 【新增】均线斜�?
            slope = stock.get("ma20_slope", 0)
            if slope > 5:
                reasons.append(f"20日线上升斜率{slope:.1f}%，趋势加�?)
            elif slope > 2:
                reasons.append(f"20日线温和上升{slope:.1f}%")

            pvs = stock.get("price_vs_ma20", 0)
            if 0 < pvs < 3:
                reasons.append("紧贴20日均线，支撑有效")

            if stock.get("vol_ratio", 0) >= 1.5:
                reasons.append(f"放量配合，量�?{stock.get('vol_ratio', 0):.1f}x")

            stock["reasons"] = reasons
            score = stock.get("signal_score", 0)
            if score >= 80:
                stock["recommendation_level"] = "强烈推荐"
            elif score >= 60:
                stock["recommendation_level"] = "推荐"
            elif score >= 40:
                stock["recommendation_level"] = "关注"
            else:
                stock["recommendation_level"] = "回避"

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
                "strategy_report": self._generate_report(stocks),
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
                "llm_enhanced": False,
            }
        }

    def _generate_report(self, stocks: List[Dict]) -> str:
        """生成策略报告（优化版�?""
        total = len(stocks)
        bull = sum(1 for s in stocks if "多头" in s.get("signal_type", ""))
        golden = sum(1 for s in stocks if "金叉" in s.get("signal_type", ""))
        pullback = sum(1 for s in stocks if "回踩" in s.get("signal_type", ""))
        converge = sum(1 for s in stocks if "粘合" in s.get("signal_type", ""))
        macd_golden = sum(1 for s in stocks if s.get("is_macd_golden"))
        macd_just = sum(1 for s in stocks if s.get("is_macd_just_golden"))
        avg_score = sum(s.get("signal_score", 0) for s in stocks) / max(total, 1)

        return (
            f"## 均线战法扫描报告\n\n"
            f"扫描�?**{total}** 只均线信号股，平均评�?**{avg_score:.0f}**分：\n"
            f"- 📊 多头排列: {bull} 只\n"
            f"- �?金叉信号: {golden} 只\n"
            f"- 🔄 回踩支撑: {pullback} 只\n"
            f"- 🔀 粘合突破: {converge} 只\n"
            f"- 🟢 MACD共振: {macd_golden} 只（其中刚金�?{macd_just} 只）\n\n"
            f"### 核心逻辑\n"
            f"均线是趋势的表征。多头排�?上升趋势明确�?
            f"金叉=趋势转换起点，回踩均�?低风险介入点�?
            f"粘合突破=方向选择确定。\n"
            f"配合MACD共振验证（五维度评分：信�?5%+形�?5%+量能15%+MACD15%+筹码10%）\n\n"
            f"⚠️ **风险提示**: 均线系统有滞后性，需结合MACD共振和量能配合验证�?
        )


# 全局单例
moving_average_strategy = MovingAverageStrategy()

