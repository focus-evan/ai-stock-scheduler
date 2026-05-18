from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import structlog
from bs4 import BeautifulSoup

from app.industry_indicator_repository import industry_indicator_repository

logger = structlog.get_logger()


class IndicatorFetcher(ABC):
    @abstractmethod
    async def fetch(self, indicator: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError


class ManualFetcher(IndicatorFetcher):
    async def fetch(self, indicator: Dict[str, Any]) -> List[Dict[str, Any]]:
        values = indicator.get("source_config", {}).get("values", [])
        results: List[Dict[str, Any]] = []
        for item in values:
            results.append({
                "period": item["period"],
                "period_date": item["period_date"],
                "value": float(item["value"]),
                "value_yoy": item.get("value_yoy"),
                "value_qoq": item.get("value_qoq"),
                "source_url": item.get("source_url", ""),
                "source_title": item.get("source_title", "manual"),
                "source_excerpt": item.get("source_excerpt"),
                "confidence": float(item.get("confidence", 100)),
                "snapshot_at": datetime.now(),
            })
        return results


class AkshareFetcher(IndicatorFetcher):
    async def fetch(self, indicator: Dict[str, Any]) -> List[Dict[str, Any]]:
        config = indicator.get("source_config", {})
        function_name = config.get("function")
        if not function_name:
            raise ValueError("Akshare source_config.function is required")

        try:
            from app.akshare_client import ak_client
        except ImportError:
            from akshare_client import ak_client

        func = getattr(ak_client, function_name)
        params = config.get("params", {})
        df = func(**params)
        if df is None or df.empty:
            return []

        date_field = config.get("date_field")
        value_field = config.get("value_field")
        period_formatter = config.get("period_formatter", "date")
        source_url = config.get("source_url", "")
        source_title = config.get("source_title", function_name)
        limit = int(config.get("limit", 12))

        if not date_field or not value_field:
            raise ValueError("Akshare source_config.date_field and value_field are required")

        rows = df.tail(limit).to_dict(orient="records")
        results: List[Dict[str, Any]] = []
        for row in rows:
            period_date = self._to_date_str(row[date_field])
            results.append({
                "period": self._format_period(period_date, period_formatter),
                "period_date": period_date,
                "value": float(row[value_field]),
                "source_url": source_url,
                "source_title": source_title,
                "source_excerpt": None,
                "confidence": float(config.get("confidence", 95)),
                "snapshot_at": datetime.now(),
            })
        return results

    def _to_date_str(self, value: Any) -> str:
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10]

    def _format_period(self, period_date: str, mode: str) -> str:
        if mode == "month":
            return period_date[:7]
        if mode == "quarter":
            year, month = period_date[:4], int(period_date[5:7])
            quarter = (month - 1) // 3 + 1
            return f"{year}-Q{quarter}"
        return period_date


class WebFetcher(IndicatorFetcher):
    async def fetch(self, indicator: Dict[str, Any]) -> List[Dict[str, Any]]:
        config = indicator.get("source_config", {})
        url = config.get("url")
        selector = config.get("selector")
        if not url or not selector:
            raise ValueError("Web source_config.url and selector are required")

        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        element = soup.select_one(selector)
        if not element:
            raise ValueError(f"Selector not found: {selector}")

        text = element.get_text(" ", strip=True)
        value = self._extract_number(text, config.get("number_regex"))
        period_date = config.get("period_date") or datetime.now().strftime("%Y-%m-%d")
        period = config.get("period") or period_date[:7]
        return [{
            "period": period,
            "period_date": period_date,
            "value": value,
            "source_url": url,
            "source_title": config.get("source_title", url),
            "source_excerpt": text[:500],
            "confidence": float(config.get("confidence", 80)),
            "snapshot_at": datetime.now(),
        }]

    def _extract_number(self, text: str, pattern: Optional[str]) -> float:
        import re

        regex = re.compile(pattern or r"-?\d+(?:\.\d+)?")
        match = regex.search(text.replace(",", ""))
        if not match:
            raise ValueError("No numeric value found in scraped text")
        return float(match.group(0))


class LLMExtractFetcher(IndicatorFetcher):
    async def fetch(self, indicator: Dict[str, Any]) -> List[Dict[str, Any]]:
        config = indicator.get("source_config", {})
        document_url = config.get("url")
        if not document_url:
            raise ValueError("LLM extraction source_config.url is required")

        response = requests.get(document_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        content = response.text[:12000]

        extracted = self._call_llm_extract(
            indicator_name=indicator["name"],
            tech_name=indicator.get("tech_name", ""),
            content=content,
            source_url=document_url,
            default_period=config.get("period") or datetime.now().strftime("%Y-%m"),
            default_period_date=config.get("period_date") or datetime.now().strftime("%Y-%m-%d"),
        )
        extracted["confidence"] = min(float(extracted.get("confidence", 70)), float(config.get("max_confidence", 85)))
        extracted["source_url"] = document_url
        extracted["source_title"] = config.get("source_title", indicator["name"])
        extracted["snapshot_at"] = datetime.now()
        return [extracted]

    def _call_llm_extract(
        self,
        indicator_name: str,
        tech_name: str,
        content: str,
        source_url: str,
        default_period: str,
        default_period_date: str,
    ) -> Dict[str, Any]:
        api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("LLM_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"
        model = os.getenv("LLM_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
        if not api_key:
            raise ValueError("LLM_API_KEY or OPENAI_API_KEY is required for LLM extraction")

        prompt = (
            "你是产业数据抽取助手。请从给定文本中提取一个指标的最新数值，并严格返回 JSON。\n"
            f"产业: {tech_name}\n"
            f"指标: {indicator_name}\n"
            f"来源: {source_url}\n"
            "输出字段: period, period_date, value, source_excerpt, confidence。\n"
            f"若文本中无法明确找到，仍返回默认 period={default_period}, period_date={default_period_date}，"
            "并把 confidence 设为 30，value 设为 0，source_excerpt 说明未明确找到。"
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "你只返回 JSON，不要输出其他内容。"},
                {"role": "user", "content": prompt + "\n\n文本如下:\n" + content},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        response = requests.post(f"{base_url.rstrip('/')}/chat/completions", json=payload, headers=headers, timeout=120)
        response.raise_for_status()
        body = response.json()
        message = body["choices"][0]["message"]["content"]
        data = json.loads(message)
        data.setdefault("period", default_period)
        data.setdefault("period_date", default_period_date)
        data.setdefault("source_excerpt", "")
        data.setdefault("confidence", 70)
        data["value"] = float(data.get("value", 0) or 0)
        return data


class FetcherRegistry:
    def __init__(self):
        self._fetchers: Dict[str, IndicatorFetcher] = {
            "manual": ManualFetcher(),
            "akshare": AkshareFetcher(),
            "web": WebFetcher(),
            "llm_extract": LLMExtractFetcher(),
        }

    def get(self, source_type: str) -> IndicatorFetcher:
        if source_type not in self._fetchers:
            raise ValueError(f"Unsupported source_type: {source_type}")
        return self._fetchers[source_type]


fetcher_registry = FetcherRegistry()
