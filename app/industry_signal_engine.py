from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class IndustrySignalEngine:
    def evaluate_indicator(
        self,
        indicator: Dict[str, Any],
        series: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if len(series) < 2:
            return []

        signals: List[Dict[str, Any]] = []
        config = indicator.get("detection_config", {})
        latest = series[-1]
        previous = series[-2]
        yoy_threshold = float(config.get("yoy_turn_threshold", 0))
        qoq_threshold = float(config.get("qoq_acceleration_threshold", 0))
        breakout_threshold = config.get("breakout_threshold")

        latest_yoy = latest.get("value_yoy")
        previous_yoy = previous.get("value_yoy")
        if latest_yoy is not None and previous_yoy is not None:
            if previous_yoy <= yoy_threshold < latest_yoy:
                signals.append(self._build_signal(
                    indicator=indicator,
                    signal_type="trend_reversal",
                    direction="bullish",
                    strength=self._strength_from_change(previous_yoy, latest_yoy, base=55),
                    triggered_at=latest.get("snapshot_at") or datetime.now(),
                    evidence={
                        "rule": "yoy_turn_positive",
                        "previous_yoy": previous_yoy,
                        "latest_yoy": latest_yoy,
                        "threshold": yoy_threshold,
                        "period": latest.get("period"),
                    },
                ))
            elif previous_yoy >= yoy_threshold > latest_yoy:
                signals.append(self._build_signal(
                    indicator=indicator,
                    signal_type="trend_reversal",
                    direction="bearish",
                    strength=self._strength_from_change(previous_yoy, latest_yoy, base=55),
                    triggered_at=latest.get("snapshot_at") or datetime.now(),
                    evidence={
                        "rule": "yoy_turn_negative",
                        "previous_yoy": previous_yoy,
                        "latest_yoy": latest_yoy,
                        "threshold": yoy_threshold,
                        "period": latest.get("period"),
                    },
                ))

        latest_qoq = latest.get("value_qoq")
        previous_qoq = previous.get("value_qoq")
        if latest_qoq is not None and previous_qoq is not None:
            if previous_qoq <= qoq_threshold < latest_qoq:
                signals.append(self._build_signal(
                    indicator=indicator,
                    signal_type="acceleration",
                    direction="bullish",
                    strength=self._strength_from_change(previous_qoq, latest_qoq, base=45),
                    triggered_at=latest.get("snapshot_at") or datetime.now(),
                    evidence={
                        "rule": "qoq_acceleration_up",
                        "previous_qoq": previous_qoq,
                        "latest_qoq": latest_qoq,
                        "threshold": qoq_threshold,
                        "period": latest.get("period"),
                    },
                ))
            elif previous_qoq >= qoq_threshold > latest_qoq:
                signals.append(self._build_signal(
                    indicator=indicator,
                    signal_type="acceleration",
                    direction="bearish",
                    strength=self._strength_from_change(previous_qoq, latest_qoq, base=45),
                    triggered_at=latest.get("snapshot_at") or datetime.now(),
                    evidence={
                        "rule": "qoq_acceleration_down",
                        "previous_qoq": previous_qoq,
                        "latest_qoq": latest_qoq,
                        "threshold": qoq_threshold,
                        "period": latest.get("period"),
                    },
                ))

        if breakout_threshold is not None:
            latest_value = latest.get("value")
            previous_value = previous.get("value")
            threshold = float(breakout_threshold)
            if latest_value is not None and previous_value is not None:
                if previous_value <= threshold < latest_value:
                    signals.append(self._build_signal(
                        indicator=indicator,
                        signal_type="breakout",
                        direction=self._positive_direction(indicator, bullish=True),
                        strength=self._strength_from_change(previous_value, latest_value, base=50),
                        triggered_at=latest.get("snapshot_at") or datetime.now(),
                        evidence={
                            "rule": "value_breakout_up",
                            "previous_value": previous_value,
                            "latest_value": latest_value,
                            "threshold": threshold,
                            "period": latest.get("period"),
                        },
                    ))
                elif previous_value >= threshold > latest_value:
                    signals.append(self._build_signal(
                        indicator=indicator,
                        signal_type="breakout",
                        direction=self._positive_direction(indicator, bullish=False),
                        strength=self._strength_from_change(previous_value, latest_value, base=50),
                        triggered_at=latest.get("snapshot_at") or datetime.now(),
                        evidence={
                            "rule": "value_breakout_down",
                            "previous_value": previous_value,
                            "latest_value": latest_value,
                            "threshold": threshold,
                            "period": latest.get("period"),
                        },
                    ))

        return signals

    def summarize_inflection(
        self,
        tech_code: str,
        tech_name: str,
        signals: List[Dict[str, Any]],
        min_score: float = 20,
    ) -> Optional[Dict[str, Any]]:
        if not signals:
            return None

        bullish_score = 0.0
        bearish_score = 0.0
        contributing_signals: List[Dict[str, Any]] = []
        for signal in signals:
            importance = float(signal.get("importance", 1))
            weighted_strength = float(signal.get("strength", 0)) * importance
            contributing_signals.append({
                "indicator_id": signal["indicator_id"],
                "indicator_name": signal.get("indicator_name", ""),
                "signal_type": signal["signal_type"],
                "direction": signal["direction"],
                "strength": signal["strength"],
                "importance": importance,
                "triggered_at": signal["triggered_at"],
            })
            if signal["direction"] == "bullish":
                bullish_score += weighted_strength
            else:
                bearish_score += weighted_strength

        if bullish_score < min_score and bearish_score < min_score:
            return None

        direction = "bullish" if bullish_score >= bearish_score else "bearish"
        score = round(max(bullish_score, bearish_score), 2)
        same_direction = [s for s in contributing_signals if s["direction"] == direction]
        top_names = [s["indicator_name"] for s in same_direction[:3] if s.get("indicator_name")]
        narrative = f"{tech_name or tech_code}近期出现{direction}共振，核心驱动指标包括：{'、'.join(top_names) if top_names else '多项指标'}。"

        return {
            "tech_code": tech_code,
            "tech_name": tech_name,
            "direction": direction,
            "score": score,
            "contributing_signals": same_direction,
            "related_stocks": [],
            "narrative": narrative,
            "triggered_at": datetime.now(),
            "status": "active",
        }

    def _build_signal(
        self,
        indicator: Dict[str, Any],
        signal_type: str,
        direction: str,
        strength: int,
        triggered_at: Any,
        evidence: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "tech_code": indicator["tech_code"],
            "indicator_id": indicator["id"],
            "indicator_name": indicator["name"],
            "signal_type": signal_type,
            "direction": direction,
            "strength": strength,
            "importance": indicator.get("importance", 1),
            "triggered_at": triggered_at,
            "evidence": evidence,
            "llm_summary": self._build_summary(indicator["name"], signal_type, direction, evidence),
            "status": "active",
        }

    def _build_summary(self, indicator_name: str, signal_type: str, direction: str, evidence: Dict[str, Any]) -> str:
        period = evidence.get("period", "最新一期")
        return f"{indicator_name}在{period}触发{signal_type}信号，方向为{direction}。"

    def _strength_from_change(self, previous: float, latest: float, base: int) -> int:
        delta = abs(float(latest) - float(previous))
        strength = base + min(int(delta), 45)
        return max(0, min(strength, 100))

    def _positive_direction(self, indicator: Dict[str, Any], bullish: bool) -> str:
        direction = indicator.get("direction", "positive")
        if direction == "negative":
            return "bearish" if bullish else "bullish"
        return "bullish" if bullish else "bearish"


industry_signal_engine = IndustrySignalEngine()
