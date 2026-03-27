"""
情绪战法数据持久化模块

将情绪指标数据入库 MySQL：
1. 每日情绪快照 (sentiment_daily_snapshot)
2. 涨停股明细   (sentiment_limit_up_detail) - 暂不实现，后续可扩展
"""

import json
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


def _safe_float(val, default=0.0, decimals=4):
    """安全转换 float"""
    try:
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return default


class SentimentRepository:
    """情绪战法数据仓库"""

    def __init__(self):
        self._pool = None
        self._tables_checked = False

    async def _get_pool(self):
        """获取数据库连接池"""
        if self._pool is None:
            import os
            import aiomysql
            self._pool = await aiomysql.create_pool(
                host=os.getenv("MYSQL_HOST", "127.0.0.1"),
                port=int(os.getenv("MYSQL_PORT", 3306)),
                user=os.getenv("MYSQL_USER", "root"),
                password=os.getenv("MYSQL_PASSWORD", ""),
                db=os.getenv("MYSQL_DB", "evan_test"),
                charset="utf8mb4",
                autocommit=True,
                minsize=2,
                maxsize=10,
            )
        return self._pool

    async def _ensure_tables(self):
        """确保表结构存在"""
        if self._tables_checked:
            return

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS `sentiment_daily_snapshot` (
                        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                        `trading_date` DATE NOT NULL,
                        `total_stocks` INT DEFAULT 0,
                        `limit_up_count` INT DEFAULT 0,
                        `limit_down_count` INT DEFAULT 0,
                        `pre_limit_up_count` INT DEFAULT 0,
                        `continuous_limit_up_count` INT DEFAULT 0,
                        `limit_up_premium` DECIMAL(8,4) DEFAULT NULL,
                        `up_down_ratio` DECIMAL(8,4) DEFAULT NULL,
                        `promotion_rate` DECIMAL(8,4) DEFAULT NULL,
                        `z_premium` DECIMAL(8,4) DEFAULT NULL,
                        `z_ratio` DECIMAL(8,4) DEFAULT NULL,
                        `z_promotion` DECIMAL(8,4) DEFAULT NULL,
                        `composite_sentiment` DECIMAL(8,4) DEFAULT NULL,
                        `sentiment_percentile` DECIMAL(8,4) DEFAULT NULL,
                        `is_ice_point` TINYINT(1) DEFAULT 0,
                        `is_climax_retreat` TINYINT(1) DEFAULT 0,
                        `signal_text` VARCHAR(64) DEFAULT NULL,
                        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `uk_trading_date` (`trading_date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                logger.info("Sentiment tables checked/created")
        self._tables_checked = True

    async def save_daily_snapshot(self, snapshot: Dict[str, Any]) -> Optional[int]:
        """
        保存或更新每日情绪快照

        使用 INSERT ... ON DUPLICATE KEY UPDATE 确保幂等
        """
        try:
            await self._ensure_tables()
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    sql = """
                        INSERT INTO sentiment_daily_snapshot (
                            trading_date, limit_up_count, limit_down_count,
                            pre_limit_up_count, continuous_limit_up_count,
                            limit_up_premium, up_down_ratio, promotion_rate,
                            z_premium, z_ratio, z_promotion,
                            composite_sentiment, sentiment_percentile,
                            is_ice_point, is_climax_retreat, signal_text
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON DUPLICATE KEY UPDATE
                            limit_up_count = VALUES(limit_up_count),
                            limit_down_count = VALUES(limit_down_count),
                            pre_limit_up_count = VALUES(pre_limit_up_count),
                            continuous_limit_up_count = VALUES(continuous_limit_up_count),
                            limit_up_premium = VALUES(limit_up_premium),
                            up_down_ratio = VALUES(up_down_ratio),
                            promotion_rate = VALUES(promotion_rate),
                            z_premium = VALUES(z_premium),
                            z_ratio = VALUES(z_ratio),
                            z_promotion = VALUES(z_promotion),
                            composite_sentiment = VALUES(composite_sentiment),
                            sentiment_percentile = VALUES(sentiment_percentile),
                            is_ice_point = VALUES(is_ice_point),
                            is_climax_retreat = VALUES(is_climax_retreat),
                            signal_text = VALUES(signal_text)
                    """
                    await cur.execute(sql, (
                        snapshot.get("trading_date"),
                        snapshot.get("limit_up_count", 0),
                        snapshot.get("limit_down_count", 0),
                        snapshot.get("pre_limit_up_count", 0),
                        snapshot.get("continuous_limit_up_count", 0),
                        _safe_float(snapshot.get("limit_up_premium")),
                        _safe_float(snapshot.get("up_down_ratio")),
                        _safe_float(snapshot.get("promotion_rate")),
                        _safe_float(snapshot.get("z_premium")),
                        _safe_float(snapshot.get("z_ratio")),
                        _safe_float(snapshot.get("z_promotion")),
                        _safe_float(snapshot.get("composite_sentiment")),
                        _safe_float(snapshot.get("sentiment_percentile")),
                        1 if snapshot.get("is_ice_point") else 0,
                        1 if snapshot.get("is_climax_retreat") else 0,
                        snapshot.get("signal_text", "正常"),
                    ))

                    row_id = cur.lastrowid
                    logger.info("Sentiment snapshot saved", trading_date=snapshot.get("trading_date"),
                                row_id=row_id)
                    return row_id

        except Exception as e:
            logger.error("Failed to save sentiment snapshot", error=str(e),
                         traceback=traceback.format_exc())
            return None

    async def get_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        获取历史情绪数据

        Args:
            days: 获取最近 N 天的数据

        Returns:
            按日期升序排列的字典列表
        """
        try:
            await self._ensure_tables()
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    sql = """
                        SELECT trading_date, limit_up_count, limit_down_count,
                               pre_limit_up_count, continuous_limit_up_count,
                               limit_up_premium, up_down_ratio, promotion_rate,
                               z_premium, z_ratio, z_promotion,
                               composite_sentiment, sentiment_percentile,
                               is_ice_point, is_climax_retreat, signal_text
                        FROM sentiment_daily_snapshot
                        ORDER BY trading_date DESC
                        LIMIT %s
                    """
                    await cur.execute(sql, (days,))
                    rows = await cur.fetchall()

                    columns = [
                        "trading_date", "limit_up_count", "limit_down_count",
                        "pre_limit_up_count", "continuous_limit_up_count",
                        "limit_up_premium", "up_down_ratio", "promotion_rate",
                        "z_premium", "z_ratio", "z_promotion",
                        "composite_sentiment", "sentiment_percentile",
                        "is_ice_point", "is_climax_retreat", "signal_text",
                    ]

                    result = []
                    for row in rows:
                        record = {}
                        for i, col in enumerate(columns):
                            val = row[i]
                            if col == "trading_date":
                                record[col] = val.strftime("%Y-%m-%d") if val else ""
                            elif col in ("is_ice_point", "is_climax_retreat"):
                                record[col] = bool(val)
                            elif isinstance(val, (int, float)):
                                record[col] = float(val)
                            else:
                                record[col] = val
                        result.append(record)

                    result.sort(key=lambda x: x.get("trading_date", ""))

                    logger.info("Got sentiment history", count=len(result))
                    return result

        except Exception as e:
            logger.error("Failed to get sentiment history", error=str(e))
            return []

    # ===================== 股票推荐持久化 =====================

    async def save_stock_picks(self, batch_data: Dict[str, Any],
                               stocks: List[Dict[str, Any]]) -> Optional[int]:
        """
        保存每日推荐股票

        Args:
            batch_data: 批次信息（含 trading_date, emotion_phase, etc.）
            stocks: 推荐股票列表

        Returns:
            batch_id 或 None
        """
        try:
            await self._ensure_tables()
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 1. 插入批次
                    batch_sql = """
                        INSERT INTO sentiment_stock_pick (
                            trading_date, batch_no, emotion_phase, signal_text,
                            composite_sentiment, sentiment_percentile,
                            limit_up_count, limit_down_count,
                            pick_strategy, pick_count,
                            llm_enhanced, llm_model,
                            market_overview, pick_logic,
                            generated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON DUPLICATE KEY UPDATE
                            emotion_phase = VALUES(emotion_phase),
                            signal_text = VALUES(signal_text),
                            composite_sentiment = VALUES(composite_sentiment),
                            sentiment_percentile = VALUES(sentiment_percentile),
                            pick_count = VALUES(pick_count),
                            llm_enhanced = VALUES(llm_enhanced),
                            market_overview = VALUES(market_overview),
                            pick_logic = VALUES(pick_logic)
                    """
                    batch_no = batch_data.get("batch_no",
                                              datetime.now().strftime("%Y%m%d_%H%M%S"))
                    await cur.execute(batch_sql, (
                        batch_data.get("trading_date"),
                        batch_no,
                        batch_data.get("emotion_phase"),
                        batch_data.get("signal_text"),
                        _safe_float(batch_data.get("composite_sentiment")),
                        _safe_float(batch_data.get("sentiment_percentile")),
                        batch_data.get("limit_up_count", 0),
                        batch_data.get("limit_down_count", 0),
                        batch_data.get("pick_strategy"),
                        len(stocks),
                        1 if batch_data.get("llm_enhanced") else 0,
                        batch_data.get("llm_model"),
                        batch_data.get("market_overview"),
                        batch_data.get("pick_logic"),
                        batch_data.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    ))

                    batch_id = cur.lastrowid

                    # 2. 删除旧的明细（同一批次号）
                    if batch_id:
                        await cur.execute(
                            "DELETE FROM sentiment_stock_pick_detail WHERE batch_id = %s",
                            (batch_id,)
                        )

                    # 3. 批量插入明细
                    if stocks and batch_id:
                        detail_sql = """
                            INSERT INTO sentiment_stock_pick_detail (
                                batch_id, trading_date, `rank`,
                                stock_code, stock_name, industry,
                                price, change_pct, amount,
                                turnover_rate, total_market_cap, float_market_cap,
                                limit_up_days, first_limit_time, seal_amount,
                                pick_reason_tag,
                                recommendation_level, llm_reason,
                                llm_risk_warning, llm_target_price, llm_operation
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        for s in stocks:
                            await cur.execute(detail_sql, (
                                batch_id,
                                batch_data.get("trading_date"),
                                s.get("rank", 0),
                                s.get("stock_code", ""),
                                s.get("stock_name", ""),
                                s.get("industry", ""),
                                _safe_float(s.get("price")),
                                _safe_float(s.get("change_pct")),
                                _safe_float(s.get("amount")),
                                _safe_float(s.get("turnover_rate")),
                                _safe_float(s.get("total_market_cap")),
                                _safe_float(s.get("float_market_cap")),
                                s.get("limit_up_days", 0),
                                s.get("first_limit_time", ""),
                                _safe_float(s.get("seal_amount")),
                                s.get("pick_reason_tag", ""),
                                s.get("recommendation_level", "关注"),
                                s.get("llm_reason"),
                                s.get("llm_risk_warning"),
                                s.get("llm_target_price"),
                                s.get("llm_operation"),
                            ))

                    logger.info("Stock picks saved", batch_id=batch_id,
                                count=len(stocks))
                    return batch_id

        except Exception as e:
            logger.error("Failed to save stock picks", error=str(e),
                         traceback=traceback.format_exc())
            return None

    async def get_stock_picks(self, trading_date: str = None) -> Dict[str, Any]:
        """
        获取指定日期的推荐股票

        Args:
            trading_date: 交易日期(YYYY-MM-DD)，默认取最新

        Returns:
            { batch: {...}, stocks: [...] }
        """
        try:
            await self._ensure_tables()
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 查询批次
                    if trading_date:
                        await cur.execute(
                            "SELECT * FROM sentiment_stock_pick WHERE trading_date = %s "
                            "ORDER BY id DESC LIMIT 1",
                            (trading_date,)
                        )
                    else:
                        await cur.execute(
                            "SELECT * FROM sentiment_stock_pick ORDER BY id DESC LIMIT 1"
                        )

                    batch_row = await cur.fetchone()
                    if not batch_row:
                        return {"batch": None, "stocks": []}

                    # 获取列名
                    batch_cols = [desc[0] for desc in cur.description]
                    batch = {}
                    for i, col in enumerate(batch_cols):
                        val = batch_row[i]
                        if hasattr(val, 'strftime'):
                            batch[col] = val.strftime("%Y-%m-%d %H:%M:%S") if isinstance(val, datetime) else val.strftime("%Y-%m-%d")
                        elif isinstance(val, (int, float)):
                            batch[col] = float(val) if isinstance(val, float) else int(val)
                        else:
                            batch[col] = val

                    batch_id = batch.get("id")

                    # 查询明细
                    await cur.execute(
                        "SELECT * FROM sentiment_stock_pick_detail "
                        "WHERE batch_id = %s ORDER BY `rank` ASC",
                        (batch_id,)
                    )
                    detail_rows = await cur.fetchall()
                    detail_cols = [desc[0] for desc in cur.description]

                    stocks = []
                    for row in detail_rows:
                        stock = {}
                        for i, col in enumerate(detail_cols):
                            val = row[i]
                            if hasattr(val, 'strftime'):
                                stock[col] = val.strftime("%Y-%m-%d")
                            elif isinstance(val, (int, float)):
                                stock[col] = float(val) if isinstance(val, float) else int(val)
                            else:
                                stock[col] = val
                        stocks.append(stock)

                    logger.info("Got stock picks", batch_id=batch_id,
                                count=len(stocks))
                    return {"batch": batch, "stocks": stocks}

        except Exception as e:
            logger.error("Failed to get stock picks", error=str(e))
            return {"batch": None, "stocks": []}


# 全局单例
sentiment_repo = SentimentRepository()
