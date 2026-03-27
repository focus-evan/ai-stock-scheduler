"""
量价关系战法（Volume-Price Strategy）核心逻辑模块

实现量价关系的核心分析：
1. 量价齐升 - 价格上涨伴随成交量放大
2. 底部放量 - 长期下跌后成交量异常放大（主力建仓）
3. 缩量回调 - 回调时成交量萎缩（洗盘）后放量突破
4. 量价背离 - 价升量缩（风险信号）

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


class VolumePriceStrategy:
    """量价关系战法核心策略类"""

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
        获取量价关系战法推荐列表

        流程:
        1. 获取全市场实时行情
        2. 筛选活跃候选股
        3. 拉取候选股近60日K线
        4. 识别量价信号（齐升/底部放量/缩量回调/量价背离）
        5. 综合评分排序
        6. GPT-5.2 增强
        """
        cache_key = f"volume_price_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)

            candidates = await loop.run_in_executor(
                None, self._filter_candidates, realtime_df
            )

            signal_stocks = await loop.run_in_executor(
                None, self._analyze_volume_price, candidates
            )

            result = self._build_recommendations(signal_stocks, limit)

            # GPT-5.2 enhancement
            if signal_stocks:
                try:
                    try:
                        from volume_price_llm import volume_price_llm
                    except ImportError:
                        from app.volume_price_llm import volume_price_llm

                    llm_result = await volume_price_llm.enhance_recommendations(
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
                    logger.error("Volume-Price LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False

            # Persist
            try:
                try:
                    from volume_price_repository import volume_price_repo
                except ImportError:
                    from app.volume_price_repository import volume_price_repo
                await volume_price_repo.save_strategy_result(result)
            except Exception as db_e:
                logger.error("Volume-Price persistence failed", error=str(db_e))

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Volume-Price strategy failed", error=str(e),
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
        """筛选候选股：有一定涨幅、成交额足够、排除异常股"""
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

        for col in ['change_pct', 'price', 'amount', 'volume', 'float_market_cap',
                     'total_market_cap', 'turnover_rate']:
            if col in df.columns:
                data = df[col]
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        mask = pd.Series(True, index=df.index)
        if 'change_pct' in df.columns:
            mask &= (df['change_pct'] >= 1) & (df['change_pct'] < 9.8)
        if 'amount' in df.columns:
            mask &= df['amount'] >= 6e7  # 3000万→6000万
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            mask &= ~df['code'].astype(str).str.startswith('8')
        # 【新增】市值>20亿
        if 'total_market_cap' in df.columns:
            mask &= (df['total_market_cap'] >= 2e9) | (df['total_market_cap'].isna())

        filtered = df[mask].copy()
        if 'amount' in filtered.columns:
            filtered = filtered.sort_values('amount', ascending=False).head(60)

        logger.info("Volume-Price candidates", count=len(filtered))
        return filtered

    def _analyze_volume_price(self, candidates: pd.DataFrame) -> List[Dict[str, Any]]:
        """量价关系信号分析（优化版）"""
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
                anti_scrape_delay(f"vp_kline_{code}", *DELAY_NORMAL)

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
                            anti_scrape_delay(f"vp_retry_{code}_{attempt}", *DELAY_LIGHT)
                        else:
                            raise

                if hist is None or len(hist) < 20:
                    continue

                h_col_map = {}
                h_mapped = set()
                for c in hist.columns:
                    if '收盘' in c and 'close' not in h_mapped:
                        h_col_map[c] = 'close'; h_mapped.add('close')
                    elif '成交量' in c and 'volume' not in h_mapped:
                        h_col_map[c] = 'volume'; h_mapped.add('volume')
                    elif '涨跌幅' in c and 'change_pct' not in h_mapped:
                        h_col_map[c] = 'change_pct'; h_mapped.add('change_pct')
                    elif '最高' in c and 'high' not in h_mapped:
                        h_col_map[c] = 'high'; h_mapped.add('high')

                hist = hist.rename(columns=h_col_map)
                for c in ['close', 'volume', 'change_pct', 'high']:
                    if c in hist.columns:
                        hist[c] = pd.to_numeric(hist[c], errors='coerce')

                if 'close' not in hist.columns or 'volume' not in hist.columns:
                    continue

                closes = hist['close'].values.astype(float)
                volumes = hist['volume'].values.astype(float)

                current_price = float(row.get('price', 0))
                today_vol = float(row.get('volume', 0))
                today_amount = float(row.get('amount', 0))
                if current_price <= 0:
                    continue

                # 量能指标
                avg_vol_5 = float(np.mean(volumes[-6:-1])) if len(volumes) >= 6 else float(np.mean(volumes))
                avg_vol_20 = float(np.mean(volumes[-21:-1])) if len(volumes) >= 21 else float(np.mean(volumes))
                vol_ratio_5 = today_vol / avg_vol_5 if avg_vol_5 > 0 else 0.0
                vol_ratio_20 = today_vol / avg_vol_20 if avg_vol_20 > 0 else 0.0

                # 价格趋势
                recent_closes = closes[-5:]
                price_trend_5d = float((recent_closes[-1] - recent_closes[0]) / recent_closes[0] * 100) if recent_closes[0] > 0 else 0.0
                recent_vols = volumes[-5:]
                vol_trend_5d = float((recent_vols[-1] - recent_vols[0]) / recent_vols[0] * 100) if recent_vols[0] > 0 else 0.0

                # 【新增】量能持续性：近3日量能都在均量之上
                vol_sustained = 0
                for i in range(-3, 0):
                    if len(volumes) > abs(i) and volumes[i] > avg_vol_20:
                        vol_sustained += 1

                # 【新增】天量天价风险：成交量是60日均量的3倍以上
                avg_vol_60 = float(np.mean(volumes[-61:-1])) if len(volumes) >= 61 else avg_vol_20
                is_extreme_volume = bool(today_vol > avg_vol_60 * 3)

                # 识别信号类型
                signal_type = ""
                signal_base_score = 0

                if price_trend_5d > 3 and vol_trend_5d > 20 and vol_ratio_5 >= 1.5:
                    signal_type = "量价齐升"
                    signal_base_score = 40
                elif vol_ratio_5 >= 2.0 and price_trend_5d > 0:
                    price_20d_chg = float((current_price - closes[-20]) / closes[-20] * 100) if len(closes) >= 20 and closes[-20] > 0 else 0.0
                    if price_20d_chg < -10:
                        signal_type = "底部放量"
                        signal_base_score = 40
                    else:
                        signal_type = "异常放量"
                        signal_base_score = 25
                elif vol_ratio_5 < 0.7 and price_trend_5d < -2:
                    today_chg = float(row.get('change_pct', 0))
                    if today_chg > 2 and vol_ratio_5 >= 1.0:
                        signal_type = "缩量回调后放量反转"
                        signal_base_score = 35
                    else:
                        continue
                elif price_trend_5d > 5 and vol_trend_5d < -20:
                    signal_type = "量价背离（风险）"
                    signal_base_score = 15
                else:
                    continue

                # ===== 多维度评分 =====
                # 量能持续性 (25%)
                sustain_score = vol_sustained * 8  # 0/8/16/24

                # 趋势确认 (15%)
                trend_score = 0
                ma5 = float(np.mean(closes[-5:])) if len(closes) >= 5 else current_price
                ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else current_price
                if current_price > ma5 > ma20:
                    trend_score = 15
                elif current_price > ma20:
                    trend_score = 8

                # 筹码结构 (10%)
                turnover = float(row.get('turnover_rate', 0))
                chip_score = 10 if 5 <= turnover <= 15 else (6 if turnover <= 20 else 3)

                # 成交额加成 (10%)
                amount_score = 10 if today_amount >= 2e8 else (6 if today_amount >= 1e8 else 3)

                signal_score = signal_base_score + sustain_score + trend_score + chip_score + amount_score
                signal_score = max(0, min(100, signal_score))

                # 天量风险扣分
                if is_extreme_volume and "背离" not in signal_type:
                    signal_score -= 10

                results.append({
                    "rank": 0,
                    "code": code,
                    "name": name,
                    "price": current_price,
                    "change_pct": round(float(row.get('change_pct', 0)), 2),
                    "amount": round(today_amount, 2),
                    "volume": today_vol,
                    "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                    "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                    "turnover_rate": round(turnover, 2),
                    "signal_type": signal_type,
                    "signal_score": signal_score,
                    "vol_ratio_5": round(float(vol_ratio_5), 2),
                    "vol_ratio_20": round(float(vol_ratio_20), 2),
                    "price_trend_5d": round(float(price_trend_5d), 2),
                    "vol_trend_5d": round(float(vol_trend_5d), 2),
                    "vol_sustained_days": vol_sustained,
                    "is_extreme_volume": is_extreme_volume,
                    "avg_vol_5": round(float(avg_vol_5), 0),
                    "avg_vol_20": round(float(avg_vol_20), 0),
                    "score_detail": {
                        "signal_base": signal_base_score,
                        "sustain": sustain_score,
                        "trend": trend_score,
                        "chip": chip_score,
                        "amount": amount_score,
                    },
                    "reasons": [],
                    "recommendation_level": "关注",
                })

            except Exception as e:
                logger.warning("Volume-Price analysis failed", code=code, error=str(e))
                continue

        results.sort(key=lambda x: x["signal_score"], reverse=True)
        for idx, stock in enumerate(results):
            stock["rank"] = idx + 1

        logger.info("Volume-Price signals detected", count=len(results))
        return results

    def _build_recommendations(self, stocks: List[Dict], limit: int) -> Dict[str, Any]:
        """构建推荐结果（优化版）"""
        now = datetime.now()

        for stock in stocks:
            reasons = []
            st = stock.get("signal_type", "")
            if "量价齐升" in st:
                reasons.append(f"量价齐升信号，5日量比 {stock.get('vol_ratio_5', 0):.1f}x")
            elif "底部放量" in st:
                reasons.append(f"底部异常放量，量比 {stock.get('vol_ratio_5', 0):.1f}x，或为主力建仓")
            elif "缩量回调" in st:
                reasons.append("前期缩量回调后今日放量反转，洗盘结束信号")
            elif "量价背离" in st:
                reasons.append("⚠️ 价升量缩，量价背离，动能衰竭")
            elif "异常放量" in st:
                reasons.append(f"异常放量 量比{stock.get('vol_ratio_5', 0):.1f}x，需关注后续走势")

            # 【新增】天量风险提示
            if stock.get("is_extreme_volume"):
                reasons.append("⚠️ 天量风险：成交量超60日均量3倍，警惕主力出货")

            # 【新增】量能持续性
            sustained = stock.get("vol_sustained_days", 0)
            if sustained >= 3:
                reasons.append(f"量能持续放大{sustained}天，资金持续流入")
            elif sustained == 0:
                reasons.append("量能不持续，可能是脉冲式放量")

            # 换手率
            turnover = stock.get("turnover_rate", 0)
            if turnover > 20:
                reasons.append("超高换手率，市场分歧极大")
            elif turnover > 15:
                reasons.append("高换手率，市场分歧较大")
            elif 5 <= turnover <= 15:
                reasons.append("换手率适中，筹码交换充分")

            stock["reasons"] = reasons
            score = stock.get("signal_score", 0)
            is_extreme = stock.get("is_extreme_volume", False)
            is_diverge = "背离" in st

            # 天量和量价背离的推荐等级封顶
            if is_diverge:
                stock["recommendation_level"] = "回避"
            elif is_extreme:
                stock["recommendation_level"] = "关注" if score >= 50 else "回避"
            elif score >= 80:
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
        """生成策略报告（优化版）"""
        total = len(stocks)
        qisheng = sum(1 for s in stocks if "量价齐升" in s.get("signal_type", ""))
        dibu = sum(1 for s in stocks if "底部放量" in s.get("signal_type", ""))
        suoliang = sum(1 for s in stocks if "缩量回调" in s.get("signal_type", ""))
        beili = sum(1 for s in stocks if "量价背离" in s.get("signal_type", ""))
        extreme = sum(1 for s in stocks if s.get("is_extreme_volume"))
        sustained = sum(1 for s in stocks if s.get("vol_sustained_days", 0) >= 3)
        avg_score = sum(s.get("signal_score", 0) for s in stocks) / max(total, 1)

        return (
            f"## 量价关系扫描报告\n\n"
            f"扫描到 **{total}** 只量价信号股，平均评分 **{avg_score:.0f}**分：\n"
            f"- 🔥 量价齐升: {qisheng} 只\n"
            f"- 📈 底部放量: {dibu} 只\n"
            f"- 🔄 缩量回调后放量: {suoliang} 只\n"
            f"- ⚠️ 量价背离: {beili} 只\n"
            f"- 📊 量能持续放大(≥3天): {sustained} 只\n"
            f"- 🚨 天量风险: {extreme} 只\n\n"
            f"### 核心逻辑\n"
            f"成交量是价格的先行指标。量价齐升=趋势确认，底部放量=主力建仓信号，"
            f"缩量回调后放量=洗盘结束主升浪启动。\n\n"
            f"⚠️ **风险提示**: 天量天价（量超60日均量3倍）可能是主力出货，"
            f"量价背离必须回避。需结合K线形态综合判断。"
        )


# 全局单例
volume_price_strategy = VolumePriceStrategy()

