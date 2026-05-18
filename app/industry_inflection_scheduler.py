from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

from app.industry_indicator_fetcher import fetcher_registry
from app.industry_indicator_repository import industry_indicator_repository
from app.industry_signal_engine import industry_signal_engine

logger = structlog.get_logger()


class IndustryInflectionScheduler:
    def __init__(self):
        self._last_frequency_run: Dict[str, str] = {}

    async def run_indicator(self, indicator_code: str) -> Dict[str, Any]:
        indicator = await industry_indicator_repository.get_indicator_by_code(indicator_code)
        if not indicator:
            raise ValueError(f"Indicator not found: {indicator_code}")

        fetcher = fetcher_registry.get(indicator["source_type"])
        started_at = datetime.now()
        try:
            values = await fetcher.fetch(indicator)
            saved = 0
            for payload in values:
                computed = await self._enrich_value(indicator, payload)
                await industry_indicator_repository.save_indicator_value(indicator["id"], computed)
                saved += 1
            await industry_indicator_repository.record_fetch_run({
                "indicator_id": indicator["id"],
                "run_type": "fetch",
                "status": "success",
                "message": f"saved {saved} value rows",
                "source_url": values[-1].get("source_url", "") if values else "",
                "raw_payload": values,
                "confidence": values[-1].get("confidence") if values else None,
                "started_at": started_at,
                "finished_at": datetime.now(),
            })
            signal_count = await self._run_signals_for_indicator(indicator)
            point = await self.run_tech(indicator["tech_code"])
            return {
                "status": "success",
                "indicator": indicator_code,
                "saved_values": saved,
                "generated_signals": signal_count,
                "inflection_point": point,
            }
        except Exception as e:
            await industry_indicator_repository.record_fetch_run({
                "indicator_id": indicator["id"],
                "run_type": "fetch",
                "status": "failed",
                "message": str(e),
                "source_url": indicator.get("source_config", {}).get("url", ""),
                "raw_payload": {"indicator_code": indicator_code},
                "started_at": started_at,
                "finished_at": datetime.now(),
            })
            logger.error("Run indicator failed", indicator_code=indicator_code, error=str(e))
            raise

    async def run_frequency(self, frequency: str) -> Dict[str, Any]:
        indicators = await industry_indicator_repository.list_indicators(frequency=frequency)
        results = []
        for indicator in indicators:
            try:
                results.append(await self.run_indicator(indicator["code"]))
            except Exception as e:
                results.append({
                    "status": "failed",
                    "indicator": indicator["code"],
                    "error": str(e),
                })
        self._last_frequency_run[frequency] = datetime.now().isoformat()
        return {
            "status": "success",
            "frequency": frequency,
            "results": results,
        }

    async def run_tech(self, tech_code: str) -> Optional[Dict[str, Any]]:
        indicators = await industry_indicator_repository.list_indicators(tech_code=tech_code)
        if not indicators:
            return None

        signals = await industry_indicator_repository.get_recent_signals(tech_code, days=30)
        point = industry_signal_engine.summarize_inflection(
            tech_code=tech_code,
            tech_name=indicators[0].get("tech_name", tech_code),
            signals=signals,
        )
        if point:
            await industry_indicator_repository.replace_inflection_point(tech_code, point)
        return point

    async def get_status(self) -> Dict[str, Any]:
        summary = await industry_indicator_repository.get_status_summary()
        summary["last_frequency_run"] = self._last_frequency_run
        return summary

    async def _run_signals_for_indicator(self, indicator: Dict[str, Any]) -> int:
        series = await industry_indicator_repository.get_recent_values(indicator["id"], limit=24)
        signals = industry_signal_engine.evaluate_indicator(indicator, series)
        for signal in signals:
            await industry_indicator_repository.save_signal(signal)
        return len(signals)

    async def _enrich_value(self, indicator: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        recent_values = await industry_indicator_repository.get_recent_values(indicator["id"], limit=8)
        current_value = float(payload["value"])
        previous = recent_values[-1] if recent_values else None
        last_same_period = self._find_last_same_period(recent_values, payload["period_date"], indicator.get("frequency", "daily"))

        if payload.get("value_qoq") is None and previous and previous.get("value") not in (None, 0):
            payload["value_qoq"] = round((current_value - float(previous["value"])) / abs(float(previous["value"])) * 100, 4)
        if payload.get("value_yoy") is None and last_same_period and last_same_period.get("value") not in (None, 0):
            payload["value_yoy"] = round((current_value - float(last_same_period["value"])) / abs(float(last_same_period["value"])) * 100, 4)
        return payload

    def _find_last_same_period(self, rows: List[Dict[str, Any]], period_date: str, frequency: str) -> Optional[Dict[str, Any]]:
        if not rows:
            return None
        target = datetime.strptime(period_date, "%Y-%m-%d")
        for row in reversed(rows):
            row_date = datetime.strptime(str(row["period_date"]), "%Y-%m-%d")
            if frequency == "monthly" and row_date.month == target.month and row_date.year == target.year - 1:
                return row
            if frequency == "quarterly":
                row_quarter = (row_date.month - 1) // 3
                target_quarter = (target.month - 1) // 3
                if row_quarter == target_quarter and row_date.year == target.year - 1:
                    return row
            if frequency == "daily" and row_date.month == target.month and row_date.day == target.day and row_date.year == target.year - 1:
                return row
        return None


industry_inflection_scheduler = IndustryInflectionScheduler()
