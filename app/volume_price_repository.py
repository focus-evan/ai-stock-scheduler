"""
统一战法数据持久化模块（Breakthrough / Volume-Price / Auction / Moving Average）

通过参数化 table_prefix 共用相同的存储逻辑。
"""

import json
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class StrategyRepository:
    """通用战法数据存储"""

    def __init__(self, strategy_name: str, table_prefix: str):
        self.strategy_name = strategy_name
        self.table_prefix = table_prefix

    def _get_connection(self):
        try:
            from database.connection import get_db_connection
        except ImportError:
            from app.database.connection import get_db_connection
        return get_db_connection()

    async def save_strategy_result(self, result: Dict[str, Any]) -> Optional[int]:
        """保存策略批次结果"""
        try:
            data = result.get("data", {})
            recommendations = data.get("recommendations", [])
            trading_date = data.get("trading_date", datetime.now().strftime("%Y-%m-%d"))
            generated_at = data.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            strategy_report = data.get("strategy_report", "")
            llm_enhanced = data.get("llm_enhanced", False)

            with self._get_connection() as conn:
                cursor = conn.cursor()

                # 插入批次记录
                cursor.execute(f"""
                    INSERT INTO {self.table_prefix}_batches
                    (trading_date, generated_at, total_signals, strategy_report, llm_enhanced)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    trading_date, generated_at, len(recommendations),
                    strategy_report if isinstance(strategy_report, str) else json.dumps(strategy_report, ensure_ascii=False),
                    1 if llm_enhanced else 0,
                ))
                batch_id = cursor.lastrowid

                # 插入推荐股票
                for stock in recommendations:
                    cursor.execute(f"""
                        INSERT INTO {self.table_prefix}_stocks
                        (batch_id, `rank`, code, name, price, change_pct, amount, volume,
                         float_market_cap, turnover_rate, signal_type, signal_score,
                         recommendation_level, reasons, extra_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        batch_id,
                        stock.get("rank", 0),
                        stock.get("code", ""),
                        stock.get("name", ""),
                        stock.get("price", 0),
                        stock.get("change_pct", 0),
                        stock.get("amount", 0),
                        stock.get("volume", 0),
                        stock.get("float_market_cap", 0),
                        stock.get("turnover_rate", 0),
                        stock.get("signal_type", stock.get("breakthrough_type", "")),
                        stock.get("signal_score", stock.get("breakthrough_score", 0)),
                        stock.get("recommendation_level", "关注"),
                        json.dumps(stock.get("reasons", []), ensure_ascii=False),
                        json.dumps({k: v for k, v in stock.items()
                                   if k not in ("rank", "code", "name", "price", "change_pct",
                                                "amount", "volume", "float_market_cap",
                                                "turnover_rate", "signal_type", "signal_score",
                                                "recommendation_level", "reasons")},
                                  ensure_ascii=False, default=str),
                    ))

                cursor.close()
                # conn.commit() is handled by the context manager

            logger.info(f"{self.strategy_name} result saved", batch_id=batch_id,
                        stocks_count=len(recommendations))
            return batch_id

        except Exception as e:
            logger.error(f"{self.strategy_name} save failed", error=str(e),
                         traceback=traceback.format_exc())
            return None

    async def get_history_batches(self, start_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   limit: int = 30) -> List[Dict]:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                sql = f"SELECT * FROM {self.table_prefix}_batches WHERE 1=1"
                params = []
                if start_date:
                    sql += " AND trading_date >= %s"
                    params.append(start_date)
                if end_date:
                    sql += " AND trading_date <= %s"
                    params.append(end_date)
                sql += " ORDER BY id DESC LIMIT %s"
                params.append(limit)

                cursor.execute(sql, params)
                batches = cursor.fetchall()
                cursor.close()
                return batches

        except Exception as e:
            logger.error(f"{self.strategy_name} get history failed", error=str(e))
            return []


# 全局单例
breakthrough_repo = StrategyRepository("突破战法", "breakthrough")
volume_price_repo = StrategyRepository("量价关系", "volume_price")
auction_repo = StrategyRepository("竞价尾盘", "auction")
moving_average_repo = StrategyRepository("均线战法", "moving_average")
