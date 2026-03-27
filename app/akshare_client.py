"""
AKShare Gateway 客户端

ai-stock 通过此模块调用 akshare-gateway 微服务，
替代直接 import akshare 的方式。

用法:
    from akshare_client import ak_client

    # 与 akshare 完全相同的调用方式
    df = ak_client.stock_zh_a_spot_em()
    df = ak_client.stock_zh_a_hist(symbol="600519", period="daily",
                                    start_date="20240101", end_date="20241231",
                                    adjust="qfq")
"""

import os
import time
from typing import Any, Optional

import pandas as pd
import requests
import structlog

logger = structlog.get_logger()

# 网关地址 — 可以通过环境变量覆盖
# 生产环境/阿里云服务器公网 IP 为 120.26.22.21
AKSHARE_GATEWAY_URL = os.environ.get(
    "AKSHARE_GATEWAY_URL",
    "http://120.26.22.21:9898"
)

# 超时（秒）— 部分接口（如 stock_zh_a_spot_em）数据量大，需要较长超时
REQUEST_TIMEOUT = int(os.environ.get("AKSHARE_GATEWAY_TIMEOUT", "60"))


class AkShareClient:
    """AKShare Gateway HTTP 客户端，API 与原生 akshare 完全兼容"""

    def __init__(self, base_url: str = AKSHARE_GATEWAY_URL, timeout: int = REQUEST_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
        })
        logger.info("AkShareClient initialized", base_url=self.base_url)

    def _call(self, path: str, params: Optional[dict] = None,
              max_retries: int = 3) -> pd.DataFrame:
        """
        调用网关接口并返回 DataFrame

        Args:
            path: API 路径（如 /api/stock/zh_a_spot_em）
            params: 查询参数
            max_retries: 重试次数

        Returns:
            pd.DataFrame
        """
        url = f"{self.base_url}{path}"
        last_err = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                body = resp.json()

                data = body.get("data", [])
                if not data:
                    return pd.DataFrame()

                df = pd.DataFrame(data)
                return df

            except Exception as e:
                last_err = e
                logger.warning(
                    "akshare-gateway call failed",
                    url=url,
                    attempt=attempt,
                    error=str(e),
                )
                if attempt < max_retries:
                    time.sleep(1.5 * attempt)

        logger.error("akshare-gateway exhausted retries", url=url, error=str(last_err))
        raise RuntimeError(f"akshare-gateway 调用失败: {path} — {last_err}")

    # ==================== 与 akshare 兼容的接口方法 ====================

    def stock_zh_a_spot_em(self) -> pd.DataFrame:
        """沪深京 A 股实时行情"""
        return self._call("/api/stock/zh_a_spot_em")

    def stock_zh_a_hist(self, symbol: str, period: str = "daily",
                        start_date: str = "", end_date: str = "",
                        adjust: str = "qfq") -> pd.DataFrame:
        """个股历史K线"""
        params = {"symbol": symbol, "period": period, "adjust": adjust}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._call("/api/stock/zh_a_hist", params=params)

    def stock_zt_pool_em(self, date: str) -> pd.DataFrame:
        """涨停股池"""
        return self._call("/api/stock/zt_pool_em", params={"date": date})

    def stock_zt_pool_dtgc_em(self, date: str) -> pd.DataFrame:
        """跌停股池"""
        return self._call("/api/stock/zt_pool_dtgc_em", params={"date": date})

    def stock_board_concept_name_em(self) -> pd.DataFrame:
        """东方财富概念板块列表"""
        return self._call("/api/stock/board_concept_name_em")

    def stock_board_concept_cons_em(self, symbol: str) -> pd.DataFrame:
        """东方财富概念板块成份股"""
        return self._call("/api/stock/board_concept_cons_em", params={"symbol": symbol})

    def stock_board_concept_name_ths(self) -> pd.DataFrame:
        """同花顺概念板块列表"""
        return self._call("/api/stock/board_concept_name_ths")

    def stock_board_concept_cons_ths(self, symbol: str) -> pd.DataFrame:
        """同花顺概念板块成份股"""
        return self._call("/api/stock/board_concept_cons_ths", params={"symbol": symbol})

    def stock_info_global_em(self) -> pd.DataFrame:
        """全球财经快讯（东方财富）"""
        return self._call("/api/stock/info_global_em")

    def stock_info_global_sina(self) -> pd.DataFrame:
        """全球财经快讯（新浪）"""
        return self._call("/api/stock/info_global_sina")

    def stock_info_global_cls(self) -> pd.DataFrame:
        """全球财经快讯（财联社）"""
        return self._call("/api/stock/info_global_cls")

    def stock_info_a_code_name(self) -> pd.DataFrame:
        """A股全部股票代码和名称"""
        return self._call("/api/stock/info_a_code_name")

    def stock_gdfx_top_10_em(self, symbol: str, date: str) -> pd.DataFrame:
        """个股十大股东"""
        return self._call("/api/stock/gdfx_top_10_em",
                          params={"symbol": symbol, "date": date})

    def stock_ipo_declare_em(self) -> pd.DataFrame:
        """A股IPO申报信息"""
        return self._call("/api/stock/ipo_declare_em")

    def stock_hk_ipo_wait_board_em(self) -> pd.DataFrame:
        """港股IPO待上市板"""
        return self._call("/api/stock/hk_ipo_wait_board_em")

    def stock_hk_spot_em(self) -> pd.DataFrame:
        """港股实时行情"""
        return self._call("/api/stock/hk_spot_em")

    def __getattr__(self, name: str):
        """
        兜底：对于未单独封装的 akshare 函数，
        尝试通过通用代理接口调用，支持查询参数
        """
        def generic_call(**kwargs):
            return self._call(f"/api/akshare/{name}", params=kwargs)
        return generic_call


# 全局单例
ak_client = AkShareClient()
