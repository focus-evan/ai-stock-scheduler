"""
隔夜施工法（Overnight Construction Strategy）核心逻辑模块

策略机制：
1. 14:30后七步筛选强势股
2. 尾盘买入，次日集合竞价/开盘冲高卖出
3. 赚取隔夜价差

七步筛选：
1. 涨幅 3%~5%
2. 量比 > 1
3. 换手率 5%~10%
4. 流通市值 50亿~200亿
5. 成交量台阶式放大
6. 均线多头排列 (MA5>MA10>MA20>MA60)
7. 分时强于均价线（以高于VWAP近似）

数据源：AkShare
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


class OvernightStrategy:
    """隔夜施工法核心策略类"""

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
        隔夜施工法推荐列表

        流程:
        1. 获取全市场实时行情
        2. 七步筛选候选股
        3. 拉取候选股近60日K线进行均线+成交量分析
        4. 综合评分排序
        5. LLM 增强
        """
        cache_key = f"overnight_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            loop = asyncio.get_event_loop()

            realtime_df = await loop.run_in_executor(None, self._get_realtime_data)

            # 第1-4步：基础筛选
            candidates = await loop.run_in_executor(
                None, self._filter_candidates, realtime_df
            )

            # 第5-7步：K线深度分析
            signal_stocks = await loop.run_in_executor(
                None, self._analyze_overnight, candidates
            )

            result = self._build_recommendations(signal_stocks, limit)

            # LLM 增强
            if signal_stocks:
                try:
                    try:
                        from auction_llm import overnight_llm
                    except ImportError:
                        from app.auction_llm import overnight_llm

                    llm_result = await overnight_llm.enhance_recommendations(
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
                    logger.error("Overnight LLM enhancement failed", error=str(llm_e))
                    result["data"]["llm_enhanced"] = False

            # 持久化
            try:
                try:
                    from auction_repository import overnight_repo
                except ImportError:
                    from app.auction_repository import overnight_repo
                await overnight_repo.save_strategy_result(result)
            except Exception as db_e:
                logger.error("Overnight persistence failed", error=str(db_e))

            self._cache[cache_key] = result
            self._cache_time = datetime.now()
            return result

        except Exception as e:
            logger.error("Overnight strategy failed", error=str(e),
                         traceback=traceback.format_exc())
            raise

    def _get_realtime_data(self) -> pd.DataFrame:
        """获取全量A股实时行情（优先使用含量比/换手率/流通市值的接口）"""
        # 优先: gateway 全量接口（含量比字段）
        try:
            import json
            import urllib.request
            try:
                from anti_scrape import get_gateway_url
            except ImportError:
                from app.anti_scrape import get_gateway_url

            gw = get_gateway_url()
            if gw:
                url = f"{gw.rstrip('/')}/api/stock/zh_a_spot_full"
                headers = {"Accept": "application/json", "User-Agent": "ai-stock/1.0"}
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=60) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                data = body.get("data", [])
                if data and len(data) > 1000:
                    df = pd.DataFrame(data)
                    logger.info("Full A-spot via gateway", rows=len(df))
                    return df
                else:
                    logger.warning("Gateway full A-spot too few rows", count=len(data) if data else 0)
        except Exception as e:
            logger.warning("Gateway full A-spot failed, fallback to get_realtime_quotes", error=str(e))

        # 降级: 原有的多源实时行情
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
        """七步筛选 — 前4步（基于实时行情快速过滤）"""
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
            elif '量比' in col and 'volume_ratio' not in mapped:
                col_map[col] = 'volume_ratio'; mapped.add('volume_ratio')

        df = df.rename(columns=col_map)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        for col in ['change_pct', 'price', 'amount', 'volume', 'float_market_cap',
                     'total_market_cap', 'turnover_rate', 'volume_ratio']:
            if col in df.columns:
                data = df[col]
                if isinstance(data, pd.DataFrame):
                    data = data.iloc[:, 0]
                df[col] = pd.to_numeric(data, errors='coerce')

        total = len(df)
        logger.info("Overnight filter start", total_rows=total)

        # 诊断：打印关键列的范围
        for diag_col in ['change_pct', 'volume_ratio', 'turnover_rate', 'float_market_cap']:
            if diag_col in df.columns:
                col_data = pd.to_numeric(df[diag_col], errors='coerce').dropna()
                if len(col_data) > 0:
                    logger.info(f"  column [{diag_col}]",
                                min=round(float(col_data.min()), 4),
                                max=round(float(col_data.max()), 4),
                                median=round(float(col_data.median()), 4),
                                sample_count=len(col_data))

        mask = pd.Series(True, index=df.index)

        # === 第1步：涨幅 3%~5% ===
        if 'change_pct' in df.columns:
            mask &= (df['change_pct'] >= 3) & (df['change_pct'] <= 5)
        logger.info("Step 1 (change_pct 3-5%)", remaining=int(mask.sum()))

        # === 第2步：量比 > 1 ===
        if 'volume_ratio' in df.columns:
            mask &= df['volume_ratio'] > 1
        logger.info("Step 2 (volume_ratio > 1)", remaining=int(mask.sum()))

        # === 第3步：换手率 5%~10% ===
        if 'turnover_rate' in df.columns:
            mask &= (df['turnover_rate'] >= 5) & (df['turnover_rate'] <= 10)
        logger.info("Step 3 (turnover_rate 5-10%)", remaining=int(mask.sum()))

        # === 第4步：流通市值 50亿~200亿 ===
        if 'float_market_cap' in df.columns:
            mask &= (df['float_market_cap'] >= 5e9) & (df['float_market_cap'] <= 2e10)
        logger.info("Step 4 (float_market_cap 50-200亿)", remaining=int(mask.sum()))

        # 排除ST/退市/北交所
        if 'name' in df.columns:
            mask &= ~df['name'].str.contains('ST|N|退', case=False, na=False)
        if 'code' in df.columns:
            valid_prefix = df['code'].astype(str).str.match(r'^(00|30|60)')
            mask &= valid_prefix
        logger.info("Step exclude (ST/N/退/北交所)", remaining=int(mask.sum()))

        filtered = df[mask].copy()

        # 按量比排名（量比越大活跃度越高）
        if 'volume_ratio' in filtered.columns and len(filtered) > 0:
            filtered = filtered.sort_values('volume_ratio', ascending=False).head(40)

        logger.info("Overnight candidates (step 1-4)", count=len(filtered))
        return filtered

    def _analyze_overnight(self, candidates: pd.DataFrame) -> List[Dict[str, Any]]:
        """七步筛选 — 第5-7步（K线深度分析）"""
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
                anti_scrape_delay(f"overnight_kline_{code}", *DELAY_NORMAL)

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
                            anti_scrape_delay(f"overnight_retry_{code}_{attempt}", *DELAY_LIGHT)
                        else:
                            raise

                if hist is None or len(hist) < 20:
                    continue

                h_col_map = {}
                h_mapped = set()
                for c in hist.columns:
                    if '收盘' in c and 'close' not in h_mapped:
                        h_col_map[c] = 'close'; h_mapped.add('close')
                    elif '开盘' in c and 'open' not in h_mapped:
                        h_col_map[c] = 'open'; h_mapped.add('open')
                    elif '最高' in c and 'high' not in h_mapped:
                        h_col_map[c] = 'high'; h_mapped.add('high')
                    elif '最低' in c and 'low' not in h_mapped:
                        h_col_map[c] = 'low'; h_mapped.add('low')
                    elif '成交量' in c and 'volume' not in h_mapped:
                        h_col_map[c] = 'volume'; h_mapped.add('volume')
                    elif '涨跌幅' in c and 'change_pct' not in h_mapped:
                        h_col_map[c] = 'change_pct'; h_mapped.add('change_pct')

                hist = hist.rename(columns=h_col_map)
                for c in ['close', 'open', 'high', 'low', 'volume', 'change_pct']:
                    if c in hist.columns:
                        hist[c] = pd.to_numeric(hist[c], errors='coerce')

                if 'close' not in hist.columns or 'volume' not in hist.columns:
                    continue

                closes = hist['close'].values.astype(float)
                volumes = hist['volume'].values.astype(float)
                current_price = float(row.get('price', 0))
                if current_price <= 0:
                    continue

                # === 第5步：成交量台阶式放大 ===
                vol_staircase_score = self._check_staircase_volume(volumes)
                if vol_staircase_score <= 0:
                    continue  # 不满足台阶式放大

                # === 第6步：均线多头排列 ===
                ma5 = float(np.mean(closes[-5:])) if len(closes) >= 5 else current_price
                ma10 = float(np.mean(closes[-10:])) if len(closes) >= 10 else current_price
                ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else current_price
                ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else current_price

                ma_bullish = current_price > ma5 > ma10 > ma20
                ma_full_bullish = current_price > ma5 > ma10 > ma20 > ma60
                if not ma_bullish:
                    continue  # 不满足均线多头

                # === 第7步：分时强于均价线（近似：当日收盘>VWAP） ===
                # 用当日涨幅持续正向来近似
                today_change = float(row.get('change_pct', 0))
                above_vwap = today_change >= 3  # 涨幅3%+基本在均价线之上

                # === 涨停记忆（加分）===
                has_limit_up = False
                if 'change_pct' in hist.columns:
                    recent_changes = hist['change_pct'].values[-5:]
                    has_limit_up = any(float(c) >= 9.5 for c in recent_changes if not np.isnan(c))

                # === 综合评分 ===
                score = 0

                # 基础分（通过7步筛选）
                score += 40

                # 成交量台阶式放大 (0-20)
                score += vol_staircase_score

                # 均线排列 (10-20)
                score += 20 if ma_full_bullish else 10

                # 量比加成 (0-10)
                vol_ratio = float(row.get('volume_ratio', 1))
                if vol_ratio >= 2:
                    score += 10
                elif vol_ratio >= 1.5:
                    score += 6
                else:
                    score += 3

                # 涨停记忆 (0-15)
                if has_limit_up:
                    score += 15

                # 换手率适中加分 (0-5)
                turnover = float(row.get('turnover_rate', 0))
                if 6 <= turnover <= 8:
                    score += 5  # 最优区间
                elif 5 <= turnover <= 10:
                    score += 3

                score = max(0, min(100, score))

                # 信号类型
                signal_parts = []
                if has_limit_up:
                    signal_parts.append("涨停记忆")
                if ma_full_bullish:
                    signal_parts.append("均线全多头")
                elif ma_bullish:
                    signal_parts.append("均线多头")
                if vol_staircase_score >= 15:
                    signal_parts.append("量能台阶放大")
                elif vol_staircase_score >= 8:
                    signal_parts.append("量能温和放大")
                signal_type = " + ".join(signal_parts) if signal_parts else "隔夜施工信号"

                results.append({
                    "rank": 0,
                    "code": code,
                    "name": name,
                    "price": current_price,
                    "change_pct": round(today_change, 2),
                    "amount": round(float(row.get('amount', 0)), 2),
                    "volume": float(row.get('volume', 0)),
                    "float_market_cap": round(float(row.get('float_market_cap', 0)), 2),
                    "total_market_cap": round(float(row.get('total_market_cap', 0)), 2),
                    "turnover_rate": round(turnover, 2),
                    "volume_ratio": round(vol_ratio, 2),
                    "signal_type": signal_type,
                    "signal_score": score,
                    "ma5": round(ma5, 2),
                    "ma10": round(ma10, 2),
                    "ma20": round(ma20, 2),
                    "ma60": round(ma60, 2),
                    "ma_bullish": ma_full_bullish,
                    "has_limit_up": has_limit_up,
                    "vol_staircase_score": vol_staircase_score,
                    "reasons": [],
                    "recommendation_level": "关注",
                })

            except Exception as e:
                logger.warning("Overnight analysis failed", code=code, error=str(e))
                continue

        results.sort(key=lambda x: x["signal_score"], reverse=True)
        for idx, stock in enumerate(results):
            stock["rank"] = idx + 1

        logger.info("Overnight signals detected", count=len(results))
        return results

    def _check_staircase_volume(self, volumes: np.ndarray) -> int:
        """检查成交量是否台阶式放大，返回评分 0-20"""
        if len(volumes) < 6:
            return 0

        recent = volumes[-5:]
        avg_prev = float(np.mean(volumes[-20:-5])) if len(volumes) >= 20 else float(np.mean(volumes[:-5]))

        if avg_prev <= 0:
            return 0

        # 近5日量是否都高于前期均量
        above_avg_count = sum(1 for v in recent if v > avg_prev)

        # 近5日是否递增或保持高位
        increasing_count = sum(1 for i in range(1, len(recent)) if recent[i] >= recent[i-1] * 0.9)

        # 今日量相对前期放大倍数
        today_ratio = recent[-1] / avg_prev if avg_prev > 0 else 0

        score = 0
        if above_avg_count >= 4:
            score += 10
        elif above_avg_count >= 3:
            score += 6
        elif above_avg_count >= 2:
            score += 3

        if increasing_count >= 3:
            score += 6
        elif increasing_count >= 2:
            score += 3

        if today_ratio >= 2:
            score += 4
        elif today_ratio >= 1.5:
            score += 2

        return min(20, score)

    def _build_recommendations(self, stocks: List[Dict], limit: int) -> Dict[str, Any]:
        """构建推荐结果"""
        now = datetime.now()

        for stock in stocks:
            reasons = []

            # 基于信号类型生成理由
            signal = stock.get("signal_type", "")
            if "涨停记忆" in signal:
                reasons.append("近期有涨停记忆，短线资金关注度高")
            if "全多头" in signal:
                reasons.append("MA5>MA10>MA20>MA60 均线全多头排列，趋势强劲")
            elif "多头" in signal:
                reasons.append("MA5>MA10>MA20 均线多头排列，趋势向上")
            if "台阶放大" in signal:
                reasons.append("成交量台阶式持续放大，增量资金进场明显")
            elif "温和放大" in signal:
                reasons.append("成交量温和放大，有资金关注")

            # 量比
            vol_ratio = stock.get("volume_ratio", 1)
            if vol_ratio >= 2:
                reasons.append(f"量比{vol_ratio:.1f}，交易活跃度极高")
            elif vol_ratio >= 1.5:
                reasons.append(f"量比{vol_ratio:.1f}，交易较为活跃")

            # 换手率
            turnover = stock.get("turnover_rate", 0)
            if 6 <= turnover <= 8:
                reasons.append(f"换手率{turnover:.1f}%，筹码交换充分且不过度")
            elif 5 <= turnover <= 10:
                reasons.append(f"换手率{turnover:.1f}%，在合理区间")

            # 涨幅
            change_pct = stock.get("change_pct", 0)
            reasons.append(f"当日涨幅{change_pct:.1f}%，处于隔夜施工最佳区间(3-5%)")

            stock["reasons"] = reasons

            # 推荐等级
            score = stock.get("signal_score", 0)
            if score >= 80:
                stock["recommendation_level"] = "强烈推荐"
            elif score >= 65:
                stock["recommendation_level"] = "推荐"
            elif score >= 50:
                stock["recommendation_level"] = "关注"
            else:
                stock["recommendation_level"] = "观望"

        # 信号统计
        signal_types = {}
        for s in stocks:
            parts = s.get("signal_type", "").split(" + ")
            for p in parts:
                if p:
                    signal_types[p] = signal_types.get(p, 0) + 1

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
        """生成策略报告"""
        total = len(stocks)
        limit_up = sum(1 for s in stocks if s.get("has_limit_up"))
        full_bullish = sum(1 for s in stocks if s.get("ma_bullish"))
        strong_vol = sum(1 for s in stocks if s.get("vol_staircase_score", 0) >= 15)
        avg_score = sum(s.get("signal_score", 0) for s in stocks) / max(total, 1)

        return (
            f"## 隔夜施工法扫描报告\n\n"
            f"七步筛选后扫描到 **{total}** 只候选股，平均评分 **{avg_score:.0f}**分：\n"
            f"- 🔥 涨停记忆股: {limit_up} 只\n"
            f"- 📈 均线全多头: {full_bullish} 只\n"
            f"- 📊 量能台阶放大: {strong_vol} 只\n\n"
            f"### 策略逻辑\n"
            f"14:30后筛选涨幅3-5%、量比>1、换手率5-10%、流通市值50-200亿的强势股，"
            f"经均线多头确认+成交量台阶式放大验证后尾盘介入，次日集合竞价/开盘冲高卖出。\n\n"
            f"⚠️ **风险提示**: 隔夜持仓存在外盘波动、突发利空等不可控风险。"
            f"严格止损-2%，次日必须卖出，绝不隔第二夜。"
        )


# 全局单例
overnight_strategy = OvernightStrategy()
