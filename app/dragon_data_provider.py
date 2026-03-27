"""
龙头战法多源数据提供器

当东方财富 API 被反爬封锁时，自动降级到其他数据源：
  1. akshare-gateway（独立微服务，可部署在不同IP）
  2. 新浪财经 API（直接HTTP请求，不走东财）
  3. 腾讯财经 API（直接HTTP请求，不走东财）
  4. akshare 本地调用（最后兜底，可能也被封）

提供的数据接口：
  - get_limit_up_stocks()   → 涨停股池
  - get_concept_boards()    → 概念板块列表
  - get_financial_news()    → 财经快讯
  - get_lianban_stocks()    → 连板股数据
"""

import json
import os
import re
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

# 统一反爬模块
try:
    from anti_scrape import (
        http_get, gateway_call, anti_scrape_delay,
        get_random_ua, get_ssl_context, get_gateway_url,
        DELAY_LIGHT, DELAY_NORMAL, DELAY_HEAVY,
    )
except ImportError:
    from app.anti_scrape import (
        http_get, gateway_call, anti_scrape_delay,
        get_random_ua, get_ssl_context, get_gateway_url,
        DELAY_LIGHT, DELAY_NORMAL, DELAY_HEAVY,
    )

logger = structlog.get_logger()

# ==================== 配置 ====================

AKSHARE_GATEWAY_URL = get_gateway_url()


# ==================== 涨停股池 ====================

def _limit_up_from_gateway(date_str: str) -> pd.DataFrame:
    """从 akshare-gateway 获取涨停股池"""
    df = gateway_call("/api/stock/zt_pool_em", params={"date": date_str})
    if not df.empty:
        logger.info("涨停股池: akshare-gateway", count=len(df))
    return df


def _limit_up_from_sina() -> pd.DataFrame:
    """
    从新浪财经获取涨停股（通过筛选涨幅>=9.8%的股票模拟）

    新浪不提供直接的涨停池接口，但可以通过全市场行情筛选
    """
    try:
        from market_data_provider import fetch_from_sina
    except ImportError:
        from app.market_data_provider import fetch_from_sina
    # 在调用外部 API 前延迟
    anti_scrape_delay("sina_limit_up", *DELAY_LIGHT)

    df = fetch_from_sina(max_pages=120)
    if df is None or df.empty:
        return pd.DataFrame()

    # 筛选涨幅 >= 9.8% 的股票（近似涨停）
    change_col = None
    for col in df.columns:
        if '涨跌幅' in col:
            change_col = col
            break

    if change_col is None:
        return pd.DataFrame()

    df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
    limit_up = df[df[change_col] >= 9.8].copy()

    if limit_up.empty:
        return pd.DataFrame()

    # 标准化列名，使其与东财涨停池格式兼容
    rename = {}
    for col in limit_up.columns:
        if '代码' in col:
            rename[col] = '代码'
        elif '名称' in col:
            rename[col] = '名称'
        elif '最新价' in col:
            rename[col] = '最新价'
        elif '涨跌幅' in col:
            rename[col] = '涨跌幅'
        elif '成交额' in col:
            rename[col] = '成交额'
        elif '换手率' in col:
            rename[col] = '换手率'
        elif '流通市值' in col:
            rename[col] = '流通市值'
        elif '总市值' in col:
            rename[col] = '总市值'

    limit_up = limit_up.rename(columns=rename)

    # 排除ST股
    if '名称' in limit_up.columns:
        limit_up = limit_up[~limit_up['名称'].str.contains('ST|退', na=False)]

    # 排除代码以300/688开头的创业板/科创板（涨跌幅20%）
    if '代码' in limit_up.columns:
        # 创业板/科创板涨停是 20%，这里筛选的 >=9.8% 可能误包含
        # 保留 >=19.8% 的 300/688 开头股票
        code_col = limit_up['代码'].astype(str)
        is_special = code_col.str.startswith(('300', '688', '30', '68'))
        special_mask = is_special & (limit_up.get('涨跌幅', pd.Series(dtype=float)) < 19.8)
        limit_up = limit_up[~special_mask]

    logger.info("涨停股池: 新浪财经(模拟)", count=len(limit_up))
    return limit_up


def _limit_up_from_akshare(date_str: str) -> pd.DataFrame:
    """从 akshare 本地获取涨停股池（兜底）"""
    from app.akshare_client import ak_client as ak
    df = ak.stock_zt_pool_em(date=date_str)
    if df is not None and not df.empty:
        logger.info("涨停股池: akshare本地", count=len(df))
    return df


def get_limit_up_stocks(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    获取涨停股池（多源降级）

    优先级: gateway → 新浪模拟 → akshare本地
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    errors = []

    # 1. akshare-gateway
    if AKSHARE_GATEWAY_URL:
        try:
            df = _limit_up_from_gateway(date_str)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning("gateway涨停池失败", error=str(e))
            errors.append(f"gateway: {e}")

    # 2. 新浪财经模拟
    try:
        anti_scrape_delay("limit_up_sina", *DELAY_LIGHT)
        df = _limit_up_from_sina()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning("新浪涨停池失败", error=str(e))
        errors.append(f"sina: {e}")

    # 3. akshare 本地（可能也被封）
    try:
        anti_scrape_delay("limit_up_akshare", *DELAY_NORMAL)
        df = _limit_up_from_akshare(date_str)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning("akshare涨停池失败", error=str(e))
        errors.append(f"akshare: {e}")

    # 4. 回退历史日期
    for days_back in range(1, 5):
        prev_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        if AKSHARE_GATEWAY_URL:
            try:
                df = _limit_up_from_gateway(prev_date)
                if df is not None and not df.empty:
                    logger.info("使用历史涨停数据", date=prev_date)
                    return df
            except Exception:
                pass
        try:
            anti_scrape_delay(f"limit_up_history_{prev_date}", *DELAY_NORMAL)
            df = _limit_up_from_akshare(prev_date)
            if df is not None and not df.empty:
                logger.info("使用历史涨停数据(akshare)", date=prev_date)
                return df
        except Exception:
            continue

    logger.error("涨停股池所有数据源均失败", errors=errors)
    return pd.DataFrame()


# ==================== 概念板块 ====================

def _concept_boards_from_gateway() -> pd.DataFrame:
    """从 akshare-gateway 获取概念板块"""
    df = gateway_call("/api/stock/board_concept_name_em")
    if not df.empty:
        logger.info("概念板块: akshare-gateway", count=len(df))
    return df


def _concept_boards_from_sina() -> pd.DataFrame:
    """
    从新浪财经获取行业板块（作为概念板块的替代）

    新浪没有直接的概念板块接口，用行业板块替代
    """
    try:
        url = (
            "https://vip.stock.finance.sina.com.cn/quotes_service/api/"
            "json_v2.php/Market_Center.getHQNodeStockCountSimple"
            "&node=hs_a"
        )
        # 新浪的行业分类数据
        url_industry = (
            "https://vip.stock.finance.sina.com.cn/quotes_service/api/"
            "json_v2.php/Market_Center.getHQNodes"
        )
        headers = {
            "User-Agent": get_random_ua(),
            "Referer": "https://finance.sina.com.cn/",
        }
        raw = http_get(url_industry, headers=headers, timeout=20)
        if raw:
            data = json.loads(raw)
            # 解析新浪行业分类
            records = []
            if isinstance(data, list):
                for category in data:
                    if isinstance(category, dict):
                        name = category.get("node_name", "")
                        records.append({"板块名称": name})
            if records:
                df = pd.DataFrame(records)
                logger.info("概念板块: 新浪行业(替代)", count=len(df))
                return df
    except Exception as e:
        logger.warning("新浪行业板块获取失败", error=str(e))

    return pd.DataFrame()


def _concept_boards_from_akshare() -> pd.DataFrame:
    """从 akshare 本地获取概念板块（兜底）"""
    from app.akshare_client import ak_client as ak
    # 先尝试同花顺概念板块（不走东方财富）
    try:
        df = ak.stock_board_concept_name_ths()
        if df is not None and not df.empty:
            logger.info("概念板块: 同花顺(akshare)", count=len(df))
            return df
    except Exception:
        pass

    # 再尝试东财
    try:
        df = ak.stock_board_concept_name_em()
        if df is not None and not df.empty:
            logger.info("概念板块: 东财(akshare)", count=len(df))
            return df
    except Exception:
        pass

    return pd.DataFrame()


def get_concept_boards() -> pd.DataFrame:
    """
    获取概念板块列表（多源降级）

    优先级: gateway → akshare(同花顺) → 新浪(行业替代) → akshare(东财)
    """
    errors = []

    # 1. akshare-gateway
    if AKSHARE_GATEWAY_URL:
        try:
            df = _concept_boards_from_gateway()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            errors.append(f"gateway: {e}")

    # 2. akshare 同花顺（不走东财，优先）
    try:
        anti_scrape_delay("concept_ths", *DELAY_LIGHT)
        from app.akshare_client import ak_client as ak
        df = ak.stock_board_concept_name_ths()
        if df is not None and not df.empty:
            logger.info("概念板块: 同花顺(直接)", count=len(df))
            return df
    except Exception as e:
        errors.append(f"ths: {e}")

    # 3. gateway 同花顺概念
    if AKSHARE_GATEWAY_URL:
        try:
            df = gateway_call("/api/stock/board_concept_name_ths")
            if df is not None and not df.empty:
                logger.info("概念板块: gateway同花顺", count=len(df))
                return df
        except Exception as e:
            errors.append(f"gateway_ths: {e}")

    # 4. akshare 东财（可能被封）
    try:
        anti_scrape_delay("concept_akshare", *DELAY_NORMAL)
        df = _concept_boards_from_akshare()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        errors.append(f"akshare: {e}")

    logger.error("概念板块所有数据源均失败", errors=errors)
    return pd.DataFrame()


# ==================== 财经快讯（新闻） ====================

def _news_from_cls() -> pd.DataFrame:
    """从财联社获取快讯（不走东财）"""
    try:
        from app.akshare_client import ak_client as ak
        df = ak.stock_info_global_cls()
        if df is not None and not df.empty:
            logger.info("财经快讯: 财联社", count=len(df))
            return df
    except Exception:
        pass
    return pd.DataFrame()


def _news_from_sina() -> pd.DataFrame:
    """从新浪获取快讯（不走东财）"""
    try:
        from app.akshare_client import ak_client as ak
        df = ak.stock_info_global_sina()
        if df is not None and not df.empty:
            logger.info("财经快讯: 新浪", count=len(df))
            return df
    except Exception:
        pass
    return pd.DataFrame()


def _news_from_gateway_em() -> pd.DataFrame:
    """从 gateway 获取东财快讯"""
    df = gateway_call("/api/stock/info_global_em")
    if not df.empty:
        logger.info("财经快讯: gateway东财", count=len(df))
    return df


def _news_from_gateway_cls() -> pd.DataFrame:
    """从 gateway 获取财联社快讯"""
    df = gateway_call("/api/stock/info_global_cls")
    if not df.empty:
        logger.info("财经快讯: gateway财联社", count=len(df))
    return df


def get_financial_news() -> pd.DataFrame:
    """
    获取财经快讯（多源降级）

    优先级: 财联社 → 新浪 → gateway(财联社) → gateway(东财) → akshare(东财)
    """
    errors = []

    # 1. 财联社（不走东财，最优先）
    try:
        df = _news_from_cls()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        errors.append(f"cls: {e}")

    # 2. 新浪快讯
    try:
        anti_scrape_delay("news_sina", *DELAY_LIGHT)
        df = _news_from_sina()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        errors.append(f"sina: {e}")

    # 3. gateway 财联社
    if AKSHARE_GATEWAY_URL:
        try:
            df = _news_from_gateway_cls()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            errors.append(f"gateway_cls: {e}")

    # 4. gateway 东财
    if AKSHARE_GATEWAY_URL:
        try:
            df = _news_from_gateway_em()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            errors.append(f"gateway_em: {e}")

    # 5. akshare 本地东财（可能被封）
    try:
        anti_scrape_delay("news_akshare", *DELAY_NORMAL)
        from app.akshare_client import ak_client as ak
        df = ak.stock_info_global_em()
        if df is not None and not df.empty:
            logger.info("财经快讯: akshare东财(兜底)", count=len(df))
            return df
    except Exception as e:
        errors.append(f"akshare_em: {e}")

    logger.error("财经快讯所有数据源均失败", errors=errors)
    return pd.DataFrame()


# ==================== 连板股数据 ====================

def _lianban_from_gateway(date_str: str) -> pd.DataFrame:
    """从 gateway 获取连板数据"""
    df = gateway_call("/api/stock/zt_pool_dtgc_em", params={"date": date_str})
    if not df.empty:
        logger.info("连板数据: gateway", count=len(df))
    return df


def get_lianban_stocks(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    获取连板股数据（多源降级）

    优先级: gateway → akshare本地
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    errors = []

    # 1. gateway
    if AKSHARE_GATEWAY_URL:
        try:
            df = _lianban_from_gateway(date_str)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            errors.append(f"gateway: {e}")

    # 2. akshare 本地
    try:
        anti_scrape_delay("lianban_akshare", *DELAY_NORMAL)
        from app.akshare_client import ak_client as ak
        df = ak.stock_zt_pool_dtgc_em(date=date_str)
        if df is not None and not df.empty:
            logger.info("连板数据: akshare本地", count=len(df))
            return df
    except Exception as e:
        errors.append(f"akshare: {e}")

    # 3. 回退历史日期
    for days_back in range(1, 5):
        prev_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        if AKSHARE_GATEWAY_URL:
            try:
                df = _lianban_from_gateway(prev_date)
                if df is not None and not df.empty:
                    logger.info("使用历史连板数据", date=prev_date)
                    return df
            except Exception:
                pass
        try:
            anti_scrape_delay(f"lianban_history_{prev_date}", *DELAY_NORMAL)
            from app.akshare_client import ak_client as ak
            df = ak.stock_zt_pool_dtgc_em(date=prev_date)
            if df is not None and not df.empty:
                logger.info("使用历史连板数据(akshare)", date=prev_date)
                return df
        except Exception:
            continue

    logger.warning("连板数据所有数据源均失败", errors=errors)
    return pd.DataFrame()


# ==================== 个股概念查询 ====================

def get_stock_concepts(stock_code: str) -> pd.DataFrame:
    """
    获取个股所属概念板块（多源降级）

    优先级: gateway(东财) → gateway(同花顺) → akshare(东财) → akshare(同花顺)
    """
    errors = []

    # 1. gateway 东财
    if get_gateway_url():
        try:
            df = gateway_call("/api/stock/board_concept_cons_em",
                              params={"symbol": stock_code})
            if df is not None and not df.empty:
                return df
        except Exception as e:
            errors.append(f"gateway_em: {e}")

    # 2. gateway 同花顺
    if get_gateway_url():
        try:
            df = gateway_call("/api/stock/board_concept_cons_ths",
                              params={"symbol": stock_code})
            if df is not None and not df.empty:
                return df
        except Exception as e:
            errors.append(f"gateway_ths: {e}")

    # 3. akshare 东财
    try:
        anti_scrape_delay("concept_cons_em", *DELAY_NORMAL)
        from app.akshare_client import ak_client as ak
        df = ak.stock_board_concept_cons_em(symbol=stock_code)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        errors.append(f"akshare_em: {e}")

    # 4. akshare 同花顺
    try:
        anti_scrape_delay("concept_cons_ths", *DELAY_NORMAL)
        from app.akshare_client import ak_client as ak
        df = ak.stock_board_concept_cons_ths(symbol=stock_code)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        errors.append(f"akshare_ths: {e}")

    logger.debug("个股概念查询失败", stock=stock_code, errors=errors)
    return pd.DataFrame()
