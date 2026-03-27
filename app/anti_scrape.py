"""
统一反爬防护模块

所有调用第三方 API（东财、新浪、腾讯、同花顺等）的请求都应通过本模块，
确保反爬策略统一管理，降低被封风险。

功能：
  1. 统一 SSL 上下文（跳过证书验证）
  2. User-Agent 池 + 随机轮换
  3. 随机延迟（同步/异步两种）
  4. 统一 HTTP GET 请求（自动带 UA + SSL）
  5. akshare-gateway 统一调用入口
  6. 请求频率控制（同一 URL 冷却期内不重复请求）

用法：
    from anti_scrape import (
        http_get,              # 带反爬的 HTTP GET
        gateway_call,          # akshare-gateway 调用
        anti_scrape_delay,     # 同步随机延迟
        async_anti_scrape_delay,  # 异步随机延迟
        get_ssl_context,       # 获取 SSL 上下文
        get_random_ua,         # 获取随机 User-Agent
        should_throttle,       # 频率检查
    )
"""

import asyncio
import json
import os
import random
import ssl
import time
import threading
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

logger = structlog.get_logger()


# ==================== 配置 ====================

# akshare-gateway 地址（独立服务器部署，用于绕过东财反爬）
# 生产环境阿里云 ECS 公网 IP: 120.26.22.21
AKSHARE_GATEWAY_URL = os.environ.get("AKSHARE_GATEWAY_URL", "http://120.26.22.21:9898")

# 延迟配置
DEFAULT_MIN_DELAY = 1.0   # 默认最小延迟秒数
DEFAULT_MAX_DELAY = 3.0   # 默认最大延迟秒数
HEAVY_MIN_DELAY = 2.0     # 重量级请求最小延迟
HEAVY_MAX_DELAY = 5.0     # 重量级请求最大延迟
SCHEDULER_MIN_DELAY = 3.0 # 调度器级别最小延迟
SCHEDULER_MAX_DELAY = 10.0  # 调度器级别最大延迟

# 冷却期配置（同一请求 key 在冷却期内不重复发起）
DEFAULT_COOLDOWN_SECONDS = 60  # 默认冷却期 60 秒


# ==================== SSL 上下文（全局单例） ====================

_ssl_ctx: Optional[ssl.SSLContext] = None
_ssl_lock = threading.Lock()


def get_ssl_context() -> ssl.SSLContext:
    """获取全局 SSL 上下文（跳过证书验证，线程安全）"""
    global _ssl_ctx
    if _ssl_ctx is None:
        with _ssl_lock:
            if _ssl_ctx is None:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                _ssl_ctx = ctx
    return _ssl_ctx


# ==================== User-Agent 池 ====================

UA_POOL = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0",
    # Chrome (Linux)
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Edge (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Safari (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    # Chrome (Windows 11)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# 东财 Referer 池 — 模拟站内页面跳转
_REFERER_POOL = [
    "https://quote.eastmoney.com/",
    "https://data.eastmoney.com/",
    "https://guba.eastmoney.com/",
    "https://so.eastmoney.com/",
    "https://www.eastmoney.com/",
    "https://fund.eastmoney.com/",
    "https://choice.eastmoney.com/",
]

# Accept-Language 池
_LANG_POOL = [
    "zh-CN,zh;q=0.9,en;q=0.8",
    "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "zh-CN,zh;q=0.9",
    "zh-CN,zh-TW;q=0.9,zh;q=0.8,en;q=0.7",
]


def get_random_ua() -> str:
    """获取随机 User-Agent"""
    return random.choice(UA_POOL)


def _build_browser_headers() -> Dict[str, str]:
    """
    构建高仿真浏览器请求头

    每次请求随机组合 UA + Referer + sec-ch-ua 等，
    模拟真实浏览器访问行为，绕过东财/新浪反爬检测。
    """
    ua = random.choice(UA_POOL)
    is_chrome = "Chrome" in ua and "Edg" not in ua

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": random.choice(_LANG_POOL),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": random.choice(_REFERER_POOL),
        "Cache-Control": random.choice(["no-cache", "max-age=0"]),
        "Upgrade-Insecure-Requests": "1",
    }

    # Chrome 特有的 sec- 系列头（东财检测的重点）
    if is_chrome:
        headers.update({
            "sec-ch-ua": random.choice([
                '"Chromium";v="125", "Google Chrome";v="125", "Not.A/Brand";v="24"',
                '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                '"Chromium";v="126", "Google Chrome";v="126", "Not/A)Brand";v="8"',
            ]),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": random.choice(['"Windows"', '"macOS"', '"Linux"']),
            "Sec-Fetch-Dest": random.choice(["document", "empty"]),
            "Sec-Fetch-Mode": random.choice(["navigate", "cors", "no-cors"]),
            "Sec-Fetch-Site": random.choice(["same-origin", "same-site", "none"]),
        })

    return headers


# ==================== Session 指纹注入 ====================
# Monkey-patch akshare 底层的 requests.Session，
# 让每次请求自动携带真实浏览器 headers，绕过反爬。
# akshare 默认发 "python-requests/x.x.x" UA，极易被识别为爬虫。

_session_patched = False


def _patch_akshare_session():
    """
    Patch requests.Session.request，对金融数据源自动注入浏览器指纹。

    只对东财/新浪/同花顺等域名生效，不影响其他 HTTP 请求。
    """
    global _session_patched
    if _session_patched:
        return

    try:
        import requests as _req
    except ImportError:
        logger.warning("requests not installed, session patch skipped")
        return

    _original_request = _req.Session.request

    def _patched_request(self, method, url, **kwargs):
        # 只对金融数据源域名注入 headers
        target_domains = (
            "eastmoney.com", "push2.eastmoney.com",
            "datacenter.eastmoney.com", "data.eastmoney.com",
            "quote.eastmoney.com",
            "sina.com", "sinajs.cn",
            "10jqka.com.cn",  # 同花顺
        )

        is_target = any(d in str(url) for d in target_domains)

        if is_target:
            browser_headers = _build_browser_headers()
            if "headers" not in kwargs or kwargs["headers"] is None:
                kwargs["headers"] = {}
            for key, value in browser_headers.items():
                if key not in kwargs["headers"]:
                    kwargs["headers"][key] = value

            # 确保 timeout 合理
            if "timeout" not in kwargs:
                kwargs["timeout"] = 30

        return _original_request(self, method, url, **kwargs)

    _req.Session.request = _patched_request
    _session_patched = True
    logger.info("✅ akshare Session patched — 浏览器指纹注入已启用")


# 模块加载时立即 patch
_patch_akshare_session()


# ==================== 随机延迟 ====================

def anti_scrape_delay(
    context: str = "",
    min_seconds: float = DEFAULT_MIN_DELAY,
    max_seconds: float = DEFAULT_MAX_DELAY,
) -> float:
    """
    同步反爬随机延迟

    Args:
        context: 日志上下文标识
        min_seconds: 最小延迟（秒）
        max_seconds: 最大延迟（秒）

    Returns:
        实际延迟秒数
    """
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug("Anti-scrape delay", seconds=round(delay, 1), context=context)
    time.sleep(delay)
    return delay


async def async_anti_scrape_delay(
    context: str = "",
    min_seconds: float = DEFAULT_MIN_DELAY,
    max_seconds: float = DEFAULT_MAX_DELAY,
) -> float:
    """
    异步反爬随机延迟（用于 async 函数中）

    Args:
        context: 日志上下文标识
        min_seconds: 最小延迟（秒）
        max_seconds: 最大延迟（秒）

    Returns:
        实际延迟秒数
    """
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug("Anti-scrape async delay", seconds=round(delay, 1), context=context)
    await asyncio.sleep(delay)
    return delay


# ==================== 频率控制 ====================

_request_timestamps: Dict[str, float] = {}
_throttle_lock = threading.Lock()


def should_throttle(key: str, cooldown: float = DEFAULT_COOLDOWN_SECONDS) -> bool:
    """
    检查某个请求 key 是否处于冷却期内

    Args:
        key: 请求标识（如 "limit_up_20260321"）
        cooldown: 冷却期秒数

    Returns:
        True = 应节流（冷却期内），False = 可以请求
    """
    now = time.time()
    with _throttle_lock:
        last = _request_timestamps.get(key, 0)
        if now - last < cooldown:
            logger.debug("Request throttled", key=key,
                         remaining=round(cooldown - (now - last), 1))
            return True
        return False


def mark_requested(key: str):
    """标记某个请求 key 已执行"""
    with _throttle_lock:
        _request_timestamps[key] = time.time()


# ==================== HTTP 请求 ====================

def http_get(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    referer: str = "",
) -> str:
    """
    统一 HTTP GET 请求（自动带随机 UA + SSL 跳过证书验证）

    Args:
        url: 请求 URL
        headers: 自定义 headers（会自动注入 UA）
        timeout: 超时秒数
        referer: Referer 头

    Returns:
        响应文本
    """
    if headers is None:
        headers = {}
    if "User-Agent" not in headers:
        headers["User-Agent"] = get_random_ua()
    if referer and "Referer" not in headers:
        headers["Referer"] = referer

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, context=get_ssl_context(), timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


# ==================== akshare-gateway 调用 ====================

def get_gateway_url() -> str:
    """获取 gateway URL（支持运行时更新环境变量）"""
    return os.environ.get("AKSHARE_GATEWAY_URL", AKSHARE_GATEWAY_URL)


def gateway_call(
    path: str,
    params: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    调用 akshare-gateway 微服务

    Args:
        path: API 路径（如 "/api/stock/zt_pool_em"）
        params: 查询参数
        timeout: 超时秒数

    Returns:
        DataFrame（空 DataFrame 如果失败）
    """
    gw_url = get_gateway_url()
    if not gw_url:
        raise RuntimeError("AKSHARE_GATEWAY_URL 未配置")

    base = gw_url.rstrip("/")
    url = f"{base}{path}"
    if params:
        import urllib.parse
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    raw = http_get(url, headers={"Accept": "application/json"}, timeout=timeout)
    body = json.loads(raw)
    data = body.get("data", [])
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


# ==================== 便捷延迟常量 ====================

# 用于各场景的预设延迟配置
DELAY_LIGHT = (0.5, 1.5)       # 轻量级请求（gateway, 不太敏感的接口）
DELAY_NORMAL = (1.0, 3.0)      # 普通请求（akshare 本地调用）
DELAY_HEAVY = (2.0, 5.0)       # 重量级请求（东财 API 直调）
DELAY_SCHEDULER = (3.0, 10.0)  # 调度器级别（策略推荐之间的间隔）
DELAY_PORTFOLIO = (5.0, 15.0)  # 组合级别（多组合之间的间隔）
DELAY_REVIEW = (5.0, 12.0)     # 复盘级别（GPT 重操作之间的间隔）
