"""
个股深度数据采集模块

通过多源方案（新浪财经优�?+ akshare 兜底）获取个股的 K线数据�?
实时指标、行业信息等，为 LLM 综合分析提供全面的数据支撑�?

解决 Docker/云服务器环境下东方财富反爬导�?akshare 请求失败的问题�?
"""

import json
import random
import time
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import structlog

# 统一反爬模块
try:
    from anti_scrape import get_ssl_context, UA_POOL
except ImportError:
    from app.anti_scrape import get_ssl_context, UA_POOL

logger = structlog.get_logger()

# 复用统一 SSL 上下文和 UA �?
_SSL_CTX = get_ssl_context()
_UA_LIST = UA_POOL


def detect_market(stock_code: str) -> str:
    """检测股票市场类�? 'a' or 'hk'"""
    code = stock_code.strip()
    # 港股: 5位数�?�?开�? �?09988, 00700
    if len(code) == 5 and code.isdigit():
        return "hk"
    # A�? 6位数�?
    return "a"


class StockDataFetcher:
    """个股深度数据采集器（多源降级�?""

    def __init__(self):
        try:
            from app.akshare_client import ak_client as ak
            self.ak = ak
        except ImportError:
            self.ak = None
            logger.warning("akshare not installed, StockDataFetcher limited")

    async def fetch_comprehensive_data(
        self, stock_code: str, stock_name: str, market: str = "a"
    ) -> Dict[str, Any]:
        """
        获取个股全面数据，供 LLM 分析使用

        Args:
            market: 'a' (A�? �?'hk' (港股)
        """
        result = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "market": market,
            "kline_analysis": None,
            "key_metrics": None,
            "financial_reports": None,
            "industry_info": None,
            "warnings": [],
        }

        # 1. K线技术分析（新浪 �?akshare 降级�?
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            result["kline_analysis"] = await loop.run_in_executor(None, self._get_kline_analysis, stock_code, market)
        except Exception as e:
            logger.warning("Failed to get K-line data", stock_code=stock_code, market=market, error=str(e))
            result["warnings"].append(f"K线数据获取失�? {str(e)}")

        # 2. 关键指标（PE/PB/价格等）
        try:
            result["key_metrics"] = await loop.run_in_executor(None, self._get_key_metrics, stock_code, market)
        except Exception as e:
            logger.warning("Failed to get key metrics", stock_code=stock_code, error=str(e))
            result["warnings"].append(f"关键指标获取失败: {str(e)}")

        # 3. 财务报表
        try:
            result["financial_reports"] = await loop.run_in_executor(None, self._get_financial_reports, stock_code, market)
        except Exception as e:
            logger.warning("Failed to get financial reports", stock_code=stock_code, error=str(e))
            result["warnings"].append(f"财务数据获取失败: {str(e)}")

        # 4. 行业信息
        try:
            result["industry_info"] = await loop.run_in_executor(None, self._get_industry_info, stock_code, result.get("key_metrics"), market)
        except Exception as e:
            logger.warning("Failed to get industry info", stock_code=stock_code, error=str(e))
            result["warnings"].append(f"行业信息获取失败: {str(e)}")

        return result

    # ======================================================================
    #  K线数�? 新浪 K �?API �?akshare 降级
    # ======================================================================

    def _fetch_kline_from_sina(self, stock_code: str, days: int = 180, market: str = "a") -> Optional[pd.DataFrame]:
        """
        从新浪财经获取个股日K线数据（支持 A 股和港股�?

        A �? symbol=sh600519 / sz000001
        港股: symbol=hk09988
        """
        # 构造新浪股票代码格�?
        if market == "hk":
            sina_code = f"hk{stock_code}"
        elif stock_code.startswith('6') or stock_code.startswith('9'):
            sina_code = f"sh{stock_code}"
        else:
            sina_code = f"sz{stock_code}"

        url = (
            f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            f"CN_MarketData.getKLineData?"
            f"symbol={sina_code}&scale=240&ma=no&datalen={days}"
        )

        headers = {
            "User-Agent": random.choice(_UA_LIST),
            "Referer": "https://finance.sina.com.cn/",
            "Accept": "application/json, text/plain, */*",
        }

        req = urllib.request.Request(url, headers=headers)

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=15, context=_SSL_CTX) as resp:
                    raw = resp.read().decode("utf-8")

                data = json.loads(raw)
                if not data:
                    return None

                df = pd.DataFrame(data)
                # 新浪返回: day, open, high, low, close, volume
                df = df.rename(columns={
                    "day": "日期", "open": "开�?, "high": "最�?,
                    "low": "最�?, "close": "收盘", "volume": "成交�?,
                })
                for col in ["开�?, "最�?, "最�?, "收盘"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df["成交�?] = pd.to_numeric(df["成交�?], errors="coerce")

                logger.info("Sina K-line data fetched", stock_code=stock_code, market=market, rows=len(df))
                return df
            except Exception as e:
                if attempt < 2:
                    time.sleep(1 + attempt)
                else:
                    logger.warning("Sina K-line failed after 3 attempts",
                                 stock_code=stock_code, market=market, error=str(e))
                    return None
        return None

    def _get_kline_analysis(self, stock_code: str, market: str = "a") -> Optional[Dict[str, Any]]:
        """获取 K线数据并计算技术指标（A�?港股通用�?""

        hist = None

        # 方案1: 新浪财经 K �?
        try:
            hist = self._fetch_kline_from_sina(stock_code, days=180, market=market)
        except Exception as e:
            logger.warning("Sina kline failed", market=market, error=str(e))

        # 方案2: akshare 兜底
        if (hist is None or hist.empty) and self.ak:
            for attempt in range(3):
                try:
                    if market == "hk":
                        hist = self.ak.stock_hk_hist(
                            symbol=stock_code, period="daily",
                            start_date=(datetime.now() - timedelta(days=180)).strftime("%Y%m%d"),
                            end_date=datetime.now().strftime("%Y%m%d"),
                            adjust="qfq"
                        )
                    else:
                        hist = self.ak.stock_zh_a_hist(
                            symbol=stock_code, period="daily",
                            start_date=(datetime.now() - timedelta(days=180)).strftime("%Y%m%d"),
                            end_date=datetime.now().strftime("%Y%m%d"),
                            adjust="qfq"
                        )
                    break
                except Exception:
                    if attempt < 2:
                        time.sleep(1 + attempt)
                    else:
                        raise

        if hist is None or len(hist) < 20:
            return None

        closes = hist['收盘'].values.astype(float)
        highs = hist['最�?].values.astype(float)
        lows = hist['最�?].values.astype(float)
        volumes = hist['成交�?].values.astype(float)

        current_price = float(closes[-1])
        prev_close = float(closes[-2]) if len(closes) >= 2 else current_price

        # 均线
        ma5 = float(np.mean(closes[-5:])) if len(closes) >= 5 else current_price
        ma10 = float(np.mean(closes[-10:])) if len(closes) >= 10 else current_price
        ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else current_price
        ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else current_price

        high_20d = float(np.max(highs[-20:])) if len(highs) >= 20 else current_price
        low_20d = float(np.min(lows[-20:])) if len(lows) >= 20 else current_price
        high_60d = float(np.max(highs[-60:])) if len(highs) >= 60 else current_price
        low_60d = float(np.min(lows[-60:])) if len(lows) >= 60 else current_price

        rsi_14 = self._calc_rsi(closes, 14)
        macd_line, signal_line, macd_hist = self._calc_macd(closes)

        avg_vol_5 = float(np.mean(volumes[-5:])) if len(volumes) >= 5 else 0
        avg_vol_20 = float(np.mean(volumes[-20:])) if len(volumes) >= 20 else 0
        vol_ratio = float(volumes[-1] / avg_vol_5) if avg_vol_5 > 0 else 0

        is_bull_aligned = bool(ma5 > ma10 > ma20)
        price_vs_ma20_pct = round(float((current_price - ma20) / ma20 * 100), 2) if ma20 > 0 else 0
        change_5d = round(float((closes[-1] - closes[-6]) / closes[-6] * 100), 2) if len(closes) >= 6 else 0
        change_20d = round(float((closes[-1] - closes[-21]) / closes[-21] * 100), 2) if len(closes) >= 21 else 0

        return {
            "current_price": round(current_price, 2),
            "prev_close": round(prev_close, 2),
            "change_pct": round(float((current_price - prev_close) / prev_close * 100), 2),
            "ma5": round(ma5, 2), "ma10": round(ma10, 2),
            "ma20": round(ma20, 2), "ma60": round(ma60, 2),
            "is_bull_aligned": is_bull_aligned,
            "price_vs_ma20_pct": price_vs_ma20_pct,
            "high_20d": round(high_20d, 2), "low_20d": round(low_20d, 2),
            "high_60d": round(high_60d, 2), "low_60d": round(low_60d, 2),
            "rsi_14": rsi_14,
            "macd": round(macd_line, 4) if macd_line else None,
            "macd_signal": round(signal_line, 4) if signal_line else None,
            "macd_histogram": round(macd_hist, 4) if macd_hist else None,
            "vol_ratio": round(vol_ratio, 2),
            "avg_vol_5": round(avg_vol_5, 0),
            "avg_vol_20": round(avg_vol_20, 0),
            "change_5d": change_5d,
            "change_20d": change_20d,
            "support_1": round(float(low_20d), 2),
            "support_2": round(float(low_60d), 2),
            "resistance_1": round(float(high_20d), 2),
            "resistance_2": round(float(high_60d), 2),
            "data_days": len(closes),
        }

    # ======================================================================
    #  关键指标: 复用 market_data_provider（新浪→gateway→akshare 降级�?
    # ======================================================================

    def _get_key_metrics(self, stock_code: str, market: str = "a") -> Optional[Dict[str, Any]]:
        """获取关键指标（PE、PB、总市值、流通市值等�?""
        metrics = {}

        if market == "hk":
            # 港股: 通过新浪港股实时行情 API
            metrics = self._get_hk_realtime_metrics(stock_code)
        else:
            # A�? 通过 market_data_provider 获取全市场行情中的该股数�?
            try:
                try:
                    from market_data_provider import get_realtime_quotes
                except ImportError:
                    from app.market_data_provider import get_realtime_quotes

                df = get_realtime_quotes()
                if df is not None and not df.empty:
                    row = df[df['代码'] == stock_code]
                    if not row.empty:
                        r = row.iloc[0]
                        metrics["pe_ttm"] = round(float(r.get("市盈�?动�?, 0) or 0), 2)
                        metrics["pb"] = round(float(r.get("市净�?, 0) or 0), 2)
                        metrics["current_price"] = round(float(r.get("最新价", 0) or 0), 2)
                        metrics["change_pct"] = round(float(r.get("涨跌�?, 0) or 0), 2)
                        metrics["turnover_rate"] = round(float(r.get("换手�?, 0) or 0), 2)
                        metrics["amount"] = float(r.get("成交�?, 0) or 0)
                        if "量比" in r:
                            metrics["volume_ratio"] = round(float(r.get("量比", 0) or 0), 2)
                        if "总市�? in r and r.get("总市�?):
                            cap = float(r["总市�?])
                            if cap > 1e12:
                                metrics["total_market_cap"] = f"{cap/1e12:.2f}万亿"
                            elif cap > 1e8:
                                metrics["total_market_cap"] = f"{cap/1e8:.2f}�?
                            else:
                                metrics["total_market_cap"] = str(cap)
                        if "流通市�? in r and r.get("流通市�?):
                            fcap = float(r["流通市�?])
                            if fcap > 1e12:
                                metrics["float_market_cap"] = f"{fcap/1e12:.2f}万亿"
                            elif fcap > 1e8:
                                metrics["float_market_cap"] = f"{fcap/1e8:.2f}�?
                            else:
                                metrics["float_market_cap"] = str(fcap)

                        logger.info("Key metrics from realtime quotes", stock_code=stock_code,
                                   pe=metrics.get("pe_ttm"), pb=metrics.get("pb"))
            except Exception as e:
                logger.warning("market_data_provider failed for key metrics", error=str(e))

        # A股补�? 尝试 akshare stock_individual_info_em
        if market == "a" and not metrics.get("industry") and self.ak:
            try:
                time.sleep(0.3)
                info = self.ak.stock_individual_info_em(symbol=stock_code)
                if info is not None and not info.empty:
                    for _, row in info.iterrows():
                        key = str(row.get("item", ""))
                        val = row.get("value", "")
                        if "行业" in key and not metrics.get("industry"):
                            metrics["industry"] = str(val)
                        elif "上市时间" in key and not metrics.get("listing_date"):
                            metrics["listing_date"] = str(val)
                        elif "总市�? in key and not metrics.get("total_market_cap"):
                            metrics["total_market_cap"] = str(val)
                        elif "流通市�? in key and not metrics.get("float_market_cap"):
                            metrics["float_market_cap"] = str(val)
            except Exception as e:
                logger.debug("stock_individual_info_em failed (expected in Docker)", error=str(e))

        return metrics if metrics else None

    def _get_hk_realtime_metrics(self, stock_code: str) -> Dict[str, Any]:
        """通过新浪港股实时行情 API 获取港股指标"""
        metrics = {}
        try:
            url = f"https://hq.sinajs.cn/list=rt_hk{stock_code}"
            headers = {
                "User-Agent": random.choice(_UA_LIST),
                "Referer": "https://finance.sina.com.cn/",
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10, context=_SSL_CTX) as resp:
                raw = resp.read().decode("gbk")

            # 解析: var hq_str_rt_hk09988="阿里巴巴-W,...";
            if '="' not in raw or raw.strip().endswith('="";'):
                logger.warning("Empty HK realtime data", stock_code=stock_code)
                return metrics

            content = raw.split('="')[1].rstrip('";\n')
            fields = content.split(',')

            # 新浪港股实时数据字段（典型）:
            # 0:中文�? 1:英文�? 2:开盘价, 3:昨收, 4:最�? 5:最�?
            # 6:最新价, 7:涨跌�? 8:涨跌�? 9:买入, 10:卖出,
            # 11:成交�? 12:成交�? ..., 17:总市�? 18:市盈�?
            if len(fields) >= 10:
                try:
                    current_price = float(fields[6]) if fields[6] else 0
                    change_pct = float(fields[8]) if fields[8] else 0
                    prev_close = float(fields[3]) if fields[3] else 0

                    metrics["current_price"] = round(current_price, 3)
                    metrics["change_pct"] = round(change_pct, 2)
                    metrics["prev_close"] = round(prev_close, 3)

                    if len(fields) > 11 and fields[11]:
                        amount = float(fields[11])
                        metrics["amount"] = amount
                    if len(fields) > 12 and fields[12]:
                        metrics["volume"] = float(fields[12])
                    if len(fields) > 17 and fields[17]:
                        cap = float(fields[17])
                        if cap > 0:
                            if cap > 1e12:
                                metrics["total_market_cap"] = f"{cap/1e12:.2f}万亿"
                            elif cap > 1e8:
                                metrics["total_market_cap"] = f"{cap/1e8:.2f}�?
                            else:
                                metrics["total_market_cap"] = str(cap)
                    if len(fields) > 18 and fields[18]:
                        pe = float(fields[18])
                        if pe > 0:
                            metrics["pe_ttm"] = round(pe, 2)

                    logger.info("HK realtime metrics fetched", stock_code=stock_code,
                               price=metrics.get("current_price"),
                               change=metrics.get("change_pct"))
                except (ValueError, IndexError) as e:
                    logger.warning("HK realtime parse error", error=str(e))

        except Exception as e:
            logger.warning("HK realtime API failed", stock_code=stock_code, error=str(e))

        # akshare 兜底
        if not metrics.get("current_price") and self.ak:
            try:
                time.sleep(0.3)
                df = self.ak.stock_hk_spot_em()
                if df is not None and not df.empty:
                    row = df[df['代码'] == stock_code]
                    if not row.empty:
                        r = row.iloc[0]
                        metrics["current_price"] = round(float(r.get("最新价", 0) or 0), 3)
                        metrics["change_pct"] = round(float(r.get("涨跌�?, 0) or 0), 2)
                        if r.get("市盈�?):
                            metrics["pe_ttm"] = round(float(r["市盈�?]), 2)
                        logger.info("HK metrics from akshare spot", stock_code=stock_code)
            except Exception as e:
                logger.warning("akshare HK spot failed", error=str(e))

        return metrics

    # ======================================================================
    #  财务数据
    # ======================================================================

    def _get_financial_reports(self, stock_code: str, market: str = "a") -> Optional[List[Dict[str, Any]]]:
        """获取最�?季度财务数据"""
        if not self.ak:
            return None

        # 港股财务数据: 尝试 akshare stock_financial_hk_report_em
        if market == "hk":
            try:
                time.sleep(0.5)
                df = self.ak.stock_financial_hk_report_em(stock=stock_code, symbol="利润")
                if df is not None and not df.empty:
                    reports = []
                    for _, row in df.head(3).iterrows():
                        report = {}
                        for col in df.columns:
                            val = row[col]
                            if val is not None and str(val) != 'nan':
                                if hasattr(val, 'item'):
                                    val = val.item()
                                report[col] = val
                        reports.append(report)
                    return reports if reports else None
            except Exception as e:
                logger.warning("HK financial report failed", stock_code=stock_code, error=str(e))
            return None

        # A股财务数�?
        time.sleep(0.5)
        try:
            df = self.ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
            if df is None or df.empty:
                return None

            reports = []
            for _, row in df.head(3).iterrows():
                report = {}
                for col in df.columns:
                    val = row[col]
                    if val is not None and str(val) != 'nan':
                        if hasattr(val, 'item'):
                            val = val.item()
                        report[col] = val
                reports.append(report)
            return reports if reports else None
        except Exception as e:
            logger.warning("stock_financial_abstract_ths failed", stock_code=stock_code, error=str(e))

            try:
                time.sleep(0.5)
                df = self.ak.stock_financial_analysis_indicator(symbol=stock_code)
                if df is not None and not df.empty:
                    reports = []
                    for _, row in df.head(3).iterrows():
                        report = {}
                        for col in df.columns:
                            val = row[col]
                            if val is not None and str(val) != 'nan':
                                if hasattr(val, 'item'):
                                    val = val.item()
                                report[col] = val
                        reports.append(report)
                    return reports if reports else None
            except Exception:
                pass
            return None

    # ======================================================================
    #  行业信息
    # ======================================================================

    def _get_industry_info(
        self, stock_code: str, key_metrics: Optional[Dict] = None, market: str = "a"
    ) -> Optional[Dict[str, Any]]:
        """获取行业信息（A股用 akshare，港股用 ai_stock_list 数据库）"""
        info = {}

        # 从已获取�?key_metrics 中提�?
        if key_metrics and key_metrics.get("industry"):
            info["industry"] = key_metrics["industry"]

        # 港股: �?ai_stock_list 表读取行业信�?
        if market == "hk" and not info.get("industry"):
            try:
                try:
                    from database.connection import get_db_connection
                except ImportError:
                    from app.database.connection import get_db_connection

                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT main_business, sector FROM ai_stock_list WHERE stock_code = %s LIMIT 1",
                            (stock_code,),
                        )
                        row = cursor.fetchone()
                        if row:
                            if row.get("main_business"):
                                info["industry"] = str(row["main_business"])
                            if row.get("sector"):
                                info["sector"] = str(row["sector"])
            except Exception as e:
                logger.debug("HK industry from ai_stock_list failed", error=str(e))

        # A�? akshare 补充（可能被反爬，不强求�?
        if market == "a" and not info.get("industry") and self.ak:
            try:
                time.sleep(0.3)
                data = self.ak.stock_individual_info_em(symbol=stock_code)
                if data is not None and not data.empty:
                    for _, row in data.iterrows():
                        key = str(row.get("item", ""))
                        val = str(row.get("value", ""))
                        if "行业" in key:
                            info["industry"] = val
                        elif "板块" in key or "概念" in key:
                            info["sector"] = val
            except Exception as e:
                logger.debug("Industry info from akshare failed (expected)", error=str(e))

        return info if info else None

    # ======================================================================
    #  技术指标计�?
    # ======================================================================

    def _calc_rsi(self, closes: np.ndarray, period: int = 14) -> Optional[float]:
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

    def _calc_macd(self, closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9):
        if len(closes) < slow + signal:
            return None, None, None

        def ema(data, period):
            result = np.zeros_like(data, dtype=float)
            result[0] = data[0]
            multiplier = 2 / (period + 1)
            for i in range(1, len(data)):
                result[i] = (data[i] - result[i-1]) * multiplier + result[i-1]
            return result

        ema_fast = ema(closes, fast)
        ema_slow = ema(closes, slow)
        macd_line = ema_fast - ema_slow
        signal_line = ema(macd_line, signal)
        histogram = macd_line - signal_line
        return float(macd_line[-1]), float(signal_line[-1]), float(histogram[-1])


# 全局单例
stock_data_fetcher = StockDataFetcher()
