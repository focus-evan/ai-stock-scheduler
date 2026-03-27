"""
多源 A 股实时行情获取模块

解决阿里云服务器被东方财富反爬封锁的问题。
按优先级尝试多个数据源获取全市场 A 股实时行情：

  1. 新浪财经 API  (vip.stock.finance.sina.com.cn) — 不走东财，最稳定
  2. akshare-gateway (独立部署的网关微服务)
  3. 腾讯财经 API  (qt.gtimg.cn) — 独立于东财和新浪的第三方源
  4. akshare 本地调用 (最后兜底)

返回的 DataFrame 列名与 akshare 的 stock_zh_a_spot_em() 完全兼容。

用法:
    from market_data_provider import get_realtime_quotes
    df = get_realtime_quotes()
"""

import json
import os
import random
import re
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional

import pandas as pd
import structlog

# 统一反爬模块
try:
    from anti_scrape import (
        get_ssl_context, get_random_ua, UA_POOL,
        get_gateway_url, anti_scrape_delay, DELAY_LIGHT,
    )
except ImportError:
    from app.anti_scrape import (
        get_ssl_context, get_random_ua, UA_POOL,
        get_gateway_url, anti_scrape_delay, DELAY_LIGHT,
    )

logger = structlog.get_logger()

# akshare-gateway 地址（独立服务器）
AKSHARE_GATEWAY_URL = get_gateway_url()

# 复用统一 SSL 上下文和 UA 池
_SSL_CTX = get_ssl_context()
_UA_LIST = UA_POOL


# ======================================================================
#  数据源 1: 新浪财经 Market Center API
# ======================================================================

def _fetch_sina_page(node: str, page: int, num: int = 80) -> List[Dict]:
    """
    获取新浪财经单页行情数据

    node: hs_a (沪深A股)
    """
    url = (
        f"https://vip.stock.finance.sina.com.cn/quotes_service/api/"
        f"json_v2.php/Market_Center.getHQNodeData"
        f"?page={page}&num={num}&sort=changepercent&asc=0"
        f"&node={node}&_s_r_a=auto"
    )
    headers = {
        "User-Agent": random.choice(_UA_LIST),
        "Referer": "https://finance.sina.com.cn/",
        "Accept": "*/*",
    }
    req = urllib.request.Request(url, headers=headers)

    with urllib.request.urlopen(req, context=_SSL_CTX, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")

    if not raw or raw.strip() == "null" or raw.strip() == "[]":
        return []

    # 新浪返回的 JSON key 没有引号，需要修正
    # {symbol:"sh600519",code:"600519",...} → {"symbol":"sh600519","code":"600519",...}
    fixed = re.sub(r'(\{|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', raw)
    try:
        data = json.loads(fixed)
    except json.JSONDecodeError:
        # 尝试 eval （新浪返回的是 JS 格式）
        try:
            data = eval(raw)  # noqa: S307
        except Exception:
            return []

    if isinstance(data, list):
        return data
    return []


def _sina_col_map(records: List[Dict]) -> pd.DataFrame:
    """将新浪字段映射为 akshare 兼容的中文列名"""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # 新浪字段 → akshare 列名
    rename_map = {
        "code": "代码",
        "name": "名称",
        "trade": "最新价",
        "changepercent": "涨跌幅",
        "pricechange": "涨跌额",
        "volume": "成交量",
        "amount": "成交额",
        "open": "今开",
        "high": "最高",
        "low": "最低",
        "settlement": "昨收",
        "per": "市盈率-动态",
        "pb": "市净率",
        "turnoverratio": "换手率",
        "mktcap": "总市值",
        "nmc": "流通市值",
    }

    # 只重命名存在的列
    existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_rename)

    # 数值转换
    numeric_cols = [
        "最新价", "涨跌幅", "涨跌额", "成交量", "成交额",
        "今开", "最高", "最低", "昨收", "市盈率-动态",
        "市净率", "换手率", "总市值", "流通市值",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 新浪的总市值/流通市值单位是万元，akshare 一般是元
    if "总市值" in df.columns:
        df["总市值"] = df["总市值"] * 10000
    if "流通市值" in df.columns:
        df["流通市值"] = df["流通市值"] * 10000

    # 计算振幅（akshare 有这个字段）
    if "最高" in df.columns and "最低" in df.columns and "昨收" in df.columns:
        df["振幅"] = ((df["最高"] - df["最低"]) / df["昨收"] * 100).round(2)

    return df


def fetch_from_sina(max_pages: int = 120) -> pd.DataFrame:
    """
    从新浪财经获取全部沪深 A 股实时行情

    新浪 API 不走东财，不受东财反爬影响。

    Returns:
        DataFrame，列名与 akshare 的 stock_zh_a_spot_em() 兼容
    """
    all_records = []
    page = 1

    while page <= max_pages:
        try:
            records = _fetch_sina_page("hs_a", page, num=80)
            if not records:
                break
            all_records.extend(records)
            page += 1
            # 温和的请求间隔
            time.sleep(0.1 + random.random() * 0.2)
        except Exception as e:
            logger.warning("Sina page fetch failed", page=page, error=str(e))
            if page == 1:
                # 第一页就失败，直接放弃
                raise
            # 已有部分数据，继续返回
            break

    if not all_records:
        return pd.DataFrame()

    df = _sina_col_map(all_records)
    logger.info("Sina Finance data fetched", total_rows=len(df))
    return df


# ======================================================================
#  数据源 2: akshare-gateway (独立部署的微服务)
# ======================================================================

def fetch_from_gateway() -> pd.DataFrame:
    """从 akshare-gateway 微服务获取 A 股实时行情"""
    gw = get_gateway_url()
    if not gw:
        raise RuntimeError("AKSHARE_GATEWAY_URL 未配置")

    url = f"{gw.rstrip('/')}/api/stock/zh_a_spot_em"
    headers = {"Accept": "application/json", "User-Agent": "ai-stock/1.0"}
    req = urllib.request.Request(url, headers=headers)

    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read().decode("utf-8"))

    data = body.get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    logger.info("Gateway data fetched", total_rows=len(df))
    return df


# ======================================================================
#  数据源 3: 腾讯财经 API (qt.gtimg.cn) — 独立于东财和新浪
# ======================================================================

# A 股代码缓存（每天更新一次，避免重复获取）
_tencent_codes_cache: List[str] = []
_tencent_codes_date: str = ""


def _get_a_share_codes() -> List[str]:
    """
    获取 A 股代码列表（带每日缓存）

    优先从 akshare 轻量级接口获取，失败则从已知编码范围生成。
    """
    global _tencent_codes_cache, _tencent_codes_date
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d")

    if _tencent_codes_cache and _tencent_codes_date == today:
        return _tencent_codes_cache

    # 方法1: akshare 轻量级接口（stock_info_a_code_name 走 SSE/SZSE，较少被封）
    try:
        from app.akshare_client import ak_client as ak
        df = ak.stock_info_a_code_name()
        if df is not None and not df.empty:
            code_col = None
            for c in df.columns:
                if 'code' in str(c).lower() or '代码' in str(c):
                    code_col = c
                    break
            if code_col:
                codes = df[code_col].astype(str).str.zfill(6).tolist()
                codes = [c for c in codes if len(c) == 6 and c[0] in ('0', '3', '6')]
                if codes:
                    _tencent_codes_cache = codes
                    _tencent_codes_date = today
                    logger.info("A-share code list via akshare", count=len(codes))
                    return codes
    except Exception as e:
        logger.debug("akshare code list failed, using fallback", error=str(e))

    # 方法2: 如果有旧缓存（昨天的代码列表仍然有效）
    if _tencent_codes_cache:
        return _tencent_codes_cache

    # 方法3: 从已知编码范围生成（兜底，覆盖 95%+ 的 A 股）
    codes = []
    # 上海主板
    codes.extend(str(i) for i in range(600000, 602000))
    codes.extend(str(i) for i in range(603000, 604000))
    codes.extend(str(i) for i in range(605000, 605600))
    # 深圳主板
    codes.extend(str(i).zfill(6) for i in range(1, 1100))
    # 深圳中小板
    codes.extend(str(i).zfill(6) for i in range(2001, 3000))
    # 深圳创业板
    codes.extend(str(i) for i in range(300001, 302000))

    _tencent_codes_cache = codes
    _tencent_codes_date = today
    logger.info("A-share code list from ranges (fallback)", count=len(codes))
    return codes


def _code_to_tencent_symbol(code: str) -> str:
    """股票代码转腾讯格式 (sh/sz 前缀)"""
    code = str(code).zfill(6)
    if code.startswith(('6', '9')):
        return f"sh{code}"
    return f"sz{code}"


def _parse_tencent_line(line: str) -> Optional[dict]:
    """
    解析腾讯行情单行数据

    格式: v_sh600519="1~贵州茅台~600519~1800.00~1790.00~...";
    字段以 ~ 分隔，关键索引:
      [1]=名称 [2]=代码 [3]=最新价 [4]=昨收 [5]=今开
      [6]=成交量(手) [31]=涨跌额 [32]=涨跌幅%
      [33]=最高 [34]=最低 [37]=成交额(万元) [38]=换手率%
      [39]=市盈率 [43]=振幅% [44]=流通市值(亿) [45]=总市值(亿) [46]=市净率
    """
    line = line.strip().rstrip(';')
    if '="' not in line:
        return None
    try:
        data_part = line.split('="', 1)[1].strip('"')
        if not data_part:
            return None
        fields = data_part.split('~')
        if len(fields) < 47:
            return None

        price = float(fields[3] or 0)
        if price <= 0:
            return None  # 停牌或无效

        code = fields[2]
        name = fields[1]
        # 过滤非 A 股（如 B 股代码 200xxx/900xxx）
        if not code or code[0] not in ('0', '3', '6'):
            return None

        return {
            '代码': code,
            '名称': name,
            '最新价': price,
            '昨收': float(fields[4] or 0),
            '今开': float(fields[5] or 0),
            '成交量': int(float(fields[6] or 0)) * 100,     # 手 → 股
            '涨跌额': float(fields[31] or 0),
            '涨跌幅': float(fields[32] or 0),
            '最高': float(fields[33] or 0),
            '最低': float(fields[34] or 0),
            '成交额': float(fields[37] or 0) * 10000,        # 万元 → 元
            '换手率': float(fields[38] or 0),
            '市盈率-动态': float(fields[39] or 0) if fields[39] else 0,
            '振幅': float(fields[43] or 0) if len(fields) > 43 and fields[43] else 0,
            '流通市值': float(fields[44] or 0) * 1e8 if len(fields) > 44 and fields[44] else 0,
            '总市值': float(fields[45] or 0) * 1e8 if len(fields) > 45 and fields[45] else 0,
            '市净率': float(fields[46] or 0) if len(fields) > 46 and fields[46] else 0,
        }
    except (IndexError, ValueError, TypeError):
        return None


def fetch_from_tencent(batch_size: int = 800) -> pd.DataFrame:
    """
    从腾讯财经批量获取 A 股实时行情

    腾讯 API (qt.gtimg.cn) 完全独立于东财和新浪，
    作为第三级降级方案，在新浪被反爬、gateway 不可用时提供数据。

    实现:
      1. 获取 A 股代码列表（缓存）
      2. 分批请求腾讯 API（每批 800 只）
      3. 解析 GBK 编码响应
      4. 映射为 akshare 兼容列名

    Returns:
        DataFrame，列名与 akshare 的 stock_zh_a_spot_em() 兼容
    """
    codes = _get_a_share_codes()
    if not codes:
        raise RuntimeError("无法获取 A 股代码列表")

    tencent_symbols = [_code_to_tencent_symbol(c) for c in codes]
    all_records: List[dict] = []
    failed_batches = 0

    for i in range(0, len(tencent_symbols), batch_size):
        batch = tencent_symbols[i:i + batch_size]
        codes_str = ",".join(batch)
        url = f"http://qt.gtimg.cn/q={codes_str}"

        try:
            headers = {
                "User-Agent": random.choice(_UA_LIST),
                "Referer": "https://finance.qq.com/",
                "Accept": "*/*",
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("gbk", errors="replace")

            for line in raw.strip().split('\n'):
                record = _parse_tencent_line(line)
                if record:
                    all_records.append(record)
        except Exception as e:
            failed_batches += 1
            logger.warning("Tencent batch fetch failed",
                           batch=f"{i}-{i + len(batch)}", error=str(e))
            if failed_batches >= 3:
                # 连续多批失败，说明腾讯也不可用
                logger.error("Tencent API appears down, aborting")
                break
            continue

        # 温和间隔，避免触发腾讯限流
        if i + batch_size < len(tencent_symbols):
            time.sleep(0.15 + random.random() * 0.2)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # 确保数值列为数字类型
    numeric_cols = [
        "最新价", "涨跌幅", "涨跌额", "成交量", "成交额",
        "今开", "最高", "最低", "昨收", "市盈率-动态",
        "市净率", "换手率", "总市值", "流通市值", "振幅",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("Tencent Finance data fetched", total_rows=len(df))
    return df


# ======================================================================
#  数据源 4: akshare 本地调用（最后兜底）
# ======================================================================

def fetch_from_akshare() -> pd.DataFrame:
    """直接调用本地 akshare (兜底方案)"""
    from app.akshare_client import ak_client as ak
    df = ak.stock_zh_a_spot_em()
    if df is not None and not df.empty:
        logger.info("akshare local data fetched", total_rows=len(df))
    return df


# ======================================================================
#  统一入口
# ======================================================================

def get_realtime_quotes(max_retries: int = 2) -> pd.DataFrame:
    """
    获取 A 股全市场实时行情（多源降级）

    优先级:
      1. akshare-gateway — 独立微服务（已稳定部署，自带缓存+多源回落）
      2. 腾讯财经 API   — 独立第三方源，不受东财反爬影响
      3. 新浪财经 API   — 备用源
      4. akshare 本地    — 最后兜底

    Returns:
        DataFrame，列名与 akshare 的 stock_zh_a_spot_em() 兼容
    """
    errors = []

    # 1. akshare-gateway（最高优先级：已部署稳定运行，内置 TTL 缓存和多源回落）
    gw_url = get_gateway_url()
    if gw_url:
        try:
            df = fetch_from_gateway()
            if df is not None and not df.empty:
                logger.info("✅ 数据源: akshare-gateway", rows=len(df))
                return df
        except Exception as e:
            logger.warning("akshare-gateway 获取失败", error=str(e))
            errors.append(f"gateway: {e}")

    # 2. 腾讯财经 API（独立于东财和新浪，稳定性高）
    try:
        df = fetch_from_tencent()
        if df is not None and not df.empty:
            logger.info("✅ 数据源: 腾讯财经", rows=len(df))
            return df
    except Exception as e:
        logger.warning("腾讯财经获取失败", error=str(e))
        errors.append(f"腾讯: {e}")

    # 3. 新浪财经（Docker 环境下容易被 456 封禁，降为备用）
    for attempt in range(1, max_retries + 1):
        try:
            df = fetch_from_sina()
            if df is not None and not df.empty:
                logger.info("✅ 数据源: 新浪财经", rows=len(df))
                return df
        except Exception as e:
            logger.warning("新浪财经获取失败", attempt=attempt, error=str(e))
            errors.append(f"新浪: {e}")
            if attempt < max_retries:
                time.sleep(2)

    # 4. akshare 本地
    for attempt in range(1, max_retries + 1):
        try:
            df = fetch_from_akshare()
            if df is not None and not df.empty:
                logger.info("✅ 数据源: akshare 本地", rows=len(df))
                return df
        except Exception as e:
            logger.warning("akshare 本地获取失败", attempt=attempt, error=str(e))
            errors.append(f"akshare: {e}")
            if attempt < max_retries:
                time.sleep(3)

    logger.error("所有数据源均失败", errors=errors)
    return pd.DataFrame()
