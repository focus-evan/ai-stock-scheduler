from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

import structlog

logger = structlog.get_logger()


class IndustryIndicatorRepository:
    def __init__(self):
        self._pool = None
        self._tables_checked = False

    async def _get_pool(self):
        if self._pool is None:
            try:
                from database.async_connection import get_async_db_pool
            except ImportError:
                from app.database.async_connection import get_async_db_pool
            self._pool = get_async_db_pool()
            await self._pool.initialize()
        return self._pool

    async def ensure_tables(self):
        if self._tables_checked:
            return

        pool = await self._get_pool()
        create_sqls = [
            """
            CREATE TABLE IF NOT EXISTS ai_industry_indicator (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tech_code VARCHAR(64) NOT NULL,
                tech_name VARCHAR(128) NOT NULL DEFAULT '',
                chain_name VARCHAR(128) NOT NULL DEFAULT '',
                code VARCHAR(128) NOT NULL,
                name VARCHAR(128) NOT NULL,
                category VARCHAR(32) NOT NULL DEFAULT '',
                direction VARCHAR(16) NOT NULL DEFAULT 'positive',
                frequency VARCHAR(16) NOT NULL DEFAULT 'daily',
                unit VARCHAR(32) NOT NULL DEFAULT '',
                source_type VARCHAR(32) NOT NULL DEFAULT 'manual',
                source_config_json JSON DEFAULT NULL,
                detection_config_json JSON DEFAULT NULL,
                leading_lag_months INT NOT NULL DEFAULT 0,
                importance INT NOT NULL DEFAULT 5,
                enabled TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_indicator_code (code),
                KEY idx_tech_code (tech_code),
                KEY idx_enabled (enabled)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业指标定义'
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_industry_indicator_value (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                indicator_id BIGINT NOT NULL,
                period VARCHAR(32) NOT NULL,
                period_date DATE NOT NULL,
                value DECIMAL(20,6) NOT NULL,
                value_yoy DECIMAL(20,6) DEFAULT NULL,
                value_qoq DECIMAL(20,6) DEFAULT NULL,
                source_url VARCHAR(1024) NOT NULL DEFAULT '',
                source_title VARCHAR(255) NOT NULL DEFAULT '',
                source_excerpt TEXT DEFAULT NULL,
                confidence DECIMAL(5,2) NOT NULL DEFAULT 100,
                snapshot_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_indicator_period_snapshot (indicator_id, period, snapshot_at),
                KEY idx_indicator_period (indicator_id, period_date),
                KEY idx_snapshot_at (snapshot_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业指标时间序列'
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_industry_signal (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tech_code VARCHAR(64) NOT NULL,
                indicator_id BIGINT NOT NULL,
                signal_type VARCHAR(32) NOT NULL,
                direction VARCHAR(16) NOT NULL,
                strength INT NOT NULL DEFAULT 0,
                triggered_at DATETIME NOT NULL,
                evidence_json JSON DEFAULT NULL,
                llm_summary TEXT DEFAULT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'active',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                KEY idx_tech_triggered (tech_code, triggered_at),
                KEY idx_indicator_triggered (indicator_id, triggered_at),
                KEY idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业单指标信号'
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_industry_inflection_point (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                tech_code VARCHAR(64) NOT NULL,
                tech_name VARCHAR(128) NOT NULL DEFAULT '',
                direction VARCHAR(16) NOT NULL,
                score DECIMAL(10,2) NOT NULL DEFAULT 0,
                contributing_signals_json JSON DEFAULT NULL,
                related_stocks_json JSON DEFAULT NULL,
                narrative TEXT DEFAULT NULL,
                triggered_at DATETIME NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'active',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                KEY idx_tech_triggered (tech_code, triggered_at),
                KEY idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业级拐点事件'
            """,
            """
            CREATE TABLE IF NOT EXISTS ai_industry_fetch_run (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                indicator_id BIGINT NOT NULL,
                run_type VARCHAR(32) NOT NULL DEFAULT 'fetch',
                status VARCHAR(16) NOT NULL DEFAULT 'success',
                message TEXT DEFAULT NULL,
                source_url VARCHAR(1024) NOT NULL DEFAULT '',
                raw_payload_json JSON DEFAULT NULL,
                confidence DECIMAL(5,2) DEFAULT NULL,
                started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                KEY idx_indicator_started (indicator_id, started_at),
                KEY idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业指标抓取审计'
            """,
        ]

        for sql in create_sqls:
            await pool.execute_update(sql)

        self._tables_checked = True
        logger.info("Industry indicator tables ensured")

    async def list_indicators(
        self,
        frequency: Optional[str] = None,
        tech_code: Optional[str] = None,
        enabled_only: bool = True,
    ) -> List[Dict[str, Any]]:
        await self.ensure_tables()
        pool = await self._get_pool()

        conditions = []
        params: List[Any] = []
        if frequency:
            conditions.append("frequency = %s")
            params.append(frequency)
        if tech_code:
            conditions.append("tech_code = %s")
            params.append(tech_code)
        if enabled_only:
            conditions.append("enabled = 1")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = await pool.execute_query(
            f"""
            SELECT id, tech_code, tech_name, chain_name, code, name,
                   category, direction, frequency, unit, source_type,
                   source_config_json, detection_config_json,
                   leading_lag_months, importance, enabled
            FROM ai_industry_indicator
            {where_clause}
            ORDER BY tech_name, importance DESC, id ASC
            """,
            tuple(params),
        )
        return [self._normalize_indicator(row) for row in rows]

    async def get_indicator_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        await self.ensure_tables()
        pool = await self._get_pool()
        row = await pool.execute_one(
            """
            SELECT id, tech_code, tech_name, chain_name, code, name,
                   category, direction, frequency, unit, source_type,
                   source_config_json, detection_config_json,
                   leading_lag_months, importance, enabled
            FROM ai_industry_indicator
            WHERE code = %s
            LIMIT 1
            """,
            (code,),
        )
        return self._normalize_indicator(row) if row else None

    async def get_indicator_by_id(self, indicator_id: int) -> Optional[Dict[str, Any]]:
        await self.ensure_tables()
        pool = await self._get_pool()
        row = await pool.execute_one(
            """
            SELECT id, tech_code, tech_name, chain_name, code, name,
                   category, direction, frequency, unit, source_type,
                   source_config_json, detection_config_json,
                   leading_lag_months, importance, enabled
            FROM ai_industry_indicator
            WHERE id = %s
            LIMIT 1
            """,
            (indicator_id,),
        )
        return self._normalize_indicator(row) if row else None

    async def get_recent_values(self, indicator_id: int, limit: int = 24) -> List[Dict[str, Any]]:
        await self.ensure_tables()
        pool = await self._get_pool()
        rows = await pool.execute_query(
            """
            SELECT id, indicator_id, period, period_date, value, value_yoy, value_qoq,
                   source_url, source_title, source_excerpt, confidence, snapshot_at
            FROM ai_industry_indicator_value
            WHERE indicator_id = %s
            ORDER BY period_date DESC, snapshot_at DESC
            LIMIT %s
            """,
            (indicator_id, limit),
        )
        normalized = [self._normalize_value_row(row) for row in rows]
        return list(reversed(normalized))

    async def save_indicator_value(self, indicator_id: int, payload: Dict[str, Any]) -> None:
        await self.ensure_tables()
        pool = await self._get_pool()
        sql = """
        INSERT INTO ai_industry_indicator_value (
            indicator_id, period, period_date, value, value_yoy, value_qoq,
            source_url, source_title, source_excerpt, confidence, snapshot_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            indicator_id,
            payload["period"],
            payload["period_date"],
            payload["value"],
            payload.get("value_yoy"),
            payload.get("value_qoq"),
            payload.get("source_url", ""),
            payload.get("source_title", ""),
            payload.get("source_excerpt"),
            payload.get("confidence", 100),
            payload.get("snapshot_at") or datetime.now(),
        )
        await pool.execute_update(sql, params)

    async def save_signal(self, payload: Dict[str, Any]) -> None:
        await self.ensure_tables()
        pool = await self._get_pool()
        sql = """
        INSERT INTO ai_industry_signal (
            tech_code, indicator_id, signal_type, direction, strength,
            triggered_at, evidence_json, llm_summary, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            payload["tech_code"],
            payload["indicator_id"],
            payload["signal_type"],
            payload["direction"],
            payload.get("strength", 0),
            payload["triggered_at"],
            json.dumps(payload.get("evidence", {}), ensure_ascii=False),
            payload.get("llm_summary"),
            payload.get("status", "active"),
        )
        await pool.execute_update(sql, params)

    async def replace_inflection_point(self, tech_code: str, payload: Dict[str, Any]) -> None:
        await self.ensure_tables()
        pool = await self._get_pool()
        await pool.execute_update(
            "DELETE FROM ai_industry_inflection_point WHERE tech_code = %s AND DATE(triggered_at) = DATE(%s)",
            (tech_code, payload["triggered_at"]),
        )
        sql = """
        INSERT INTO ai_industry_inflection_point (
            tech_code, tech_name, direction, score,
            contributing_signals_json, related_stocks_json,
            narrative, triggered_at, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            payload["tech_code"],
            payload.get("tech_name", ""),
            payload["direction"],
            payload["score"],
            json.dumps(payload.get("contributing_signals", []), ensure_ascii=False),
            json.dumps(payload.get("related_stocks", []), ensure_ascii=False),
            payload.get("narrative"),
            payload["triggered_at"],
            payload.get("status", "active"),
        )
        await pool.execute_update(sql, params)

    async def record_fetch_run(self, payload: Dict[str, Any]) -> None:
        await self.ensure_tables()
        pool = await self._get_pool()
        sql = """
        INSERT INTO ai_industry_fetch_run (
            indicator_id, run_type, status, message, source_url,
            raw_payload_json, confidence, started_at, finished_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            payload["indicator_id"],
            payload.get("run_type", "fetch"),
            payload.get("status", "success"),
            payload.get("message"),
            payload.get("source_url", ""),
            json.dumps(payload.get("raw_payload"), ensure_ascii=False) if payload.get("raw_payload") is not None else None,
            payload.get("confidence"),
            payload.get("started_at") or datetime.now(),
            payload.get("finished_at") or datetime.now(),
        )
        await pool.execute_update(sql, params)

    async def get_recent_signals(self, tech_code: str, days: int = 30) -> List[Dict[str, Any]]:
        await self.ensure_tables()
        pool = await self._get_pool()
        rows = await pool.execute_query(
            """
            SELECT s.id, s.tech_code, s.indicator_id, i.name AS indicator_name,
                   s.signal_type, s.direction, s.strength, s.triggered_at,
                   s.evidence_json, s.llm_summary, s.status
            FROM ai_industry_signal s
            JOIN ai_industry_indicator i ON i.id = s.indicator_id
            WHERE s.tech_code = %s
              AND s.triggered_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            ORDER BY s.triggered_at DESC, s.id DESC
            """,
            (tech_code, days),
        )
        signals = []
        for row in rows:
            normalized = self._normalize_decimal_row(row)
            normalized["evidence"] = self._parse_json(normalized.pop("evidence_json", None), {})
            signals.append(normalized)
        return signals

    async def get_status_summary(self) -> Dict[str, Any]:
        await self.ensure_tables()
        pool = await self._get_pool()
        counts = await pool.execute_one(
            """
            SELECT
                (SELECT COUNT(*) FROM ai_industry_indicator WHERE enabled = 1) AS enabled_indicators,
                (SELECT COUNT(*) FROM ai_industry_indicator_value) AS total_values,
                (SELECT COUNT(*) FROM ai_industry_signal WHERE triggered_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)) AS recent_signals,
                (SELECT COUNT(*) FROM ai_industry_inflection_point WHERE triggered_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)) AS recent_points
            """
        )
        recent_runs = await pool.execute_query(
            """
            SELECT indicator_id, run_type, status, message, source_url, confidence, started_at, finished_at
            FROM ai_industry_fetch_run
            ORDER BY started_at DESC
            LIMIT 10
            """
        )
        return {
            **self._normalize_decimal_row(counts or {}),
            "recent_runs": [self._normalize_decimal_row(row) for row in recent_runs],
        }

    def _normalize_indicator(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self._normalize_decimal_row(row)
        normalized["source_config"] = self._parse_json(normalized.pop("source_config_json", None), {})
        normalized["detection_config"] = self._parse_json(normalized.pop("detection_config_json", None), {})
        normalized["enabled"] = bool(normalized.get("enabled", 0))
        return normalized

    def _normalize_value_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._normalize_decimal_row(row)

    def _normalize_decimal_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, Decimal):
                result[key] = float(value)
            else:
                result[key] = value
        return result

    def _parse_json(self, value: Any, default: Any):
        if value in (None, "", b""):
            return default
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return default


industry_indicator_repository = IndustryIndicatorRepository()
