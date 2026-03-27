"""
龙头战法数据持久化模块

将龙头战法全链路数据入库 MySQL：
1. 涨停股快照 (dragon_head_limit_up_snapshot)
2. 推荐批次 (dragon_head_batch)
3. 主线题材 (dragon_head_theme)
4. 推荐个股 (dragon_head_stock)
5. 新闻共振 (dragon_head_news_resonance)
"""

import json
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

logger = structlog.get_logger()


def _safe_int(val, default=0):
    """安全转换为int，支持 '6/3' 格式（取 '/' 前的数字）"""
    try:
        if pd.isna(val):
            return default
        s = str(val).strip()
        if '/' in s:
            return int(s.split('/')[0])
        return int(float(s))
    except (ValueError, TypeError):
        return default


def _safe_float(val, default=0.0, decimals=4):
    """安全转换为float，并round到指定小数位"""
    try:
        if pd.isna(val):
            return default
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return default


class DragonHeadRepository:
    """龙头战法数据仓库"""

    def __init__(self):
        self._pool = None
        self._tables_checked = False

    async def _get_pool(self):
        """获取数据库连接池"""
        if self._pool is None:
            try:
                from database.async_connection import get_async_db_pool
            except ImportError:
                from app.database.async_connection import get_async_db_pool
            self._pool = get_async_db_pool()
            await self._pool.initialize()
        return self._pool

    async def _ensure_tables(self):
        """确保表结构存在（应用启动时自动执行）"""
        if self._tables_checked:
            return

        pool = await self._get_pool()

        create_sqls = [
            # 批次表
            """
            CREATE TABLE IF NOT EXISTS `dragon_head_batch` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `trading_date` DATE NOT NULL,
                `batch_no` VARCHAR(32) NOT NULL,
                `limit_up_count` INT DEFAULT 0,
                `recommendation_count` INT DEFAULT 0,
                `llm_enhanced` TINYINT(1) DEFAULT 0,
                `llm_model` VARCHAR(32) DEFAULT NULL,
                `market_sentiment_phase` VARCHAR(10) DEFAULT NULL,
                `market_sentiment_risk` VARCHAR(10) DEFAULT NULL,
                `market_sentiment_desc` TEXT DEFAULT NULL,
                `strategy_explanation` LONGTEXT DEFAULT NULL,
                `generated_at` DATETIME NOT NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_batch_no` (`batch_no`),
                KEY `idx_trading_date` (`trading_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            # 主线题材表
            """
            CREATE TABLE IF NOT EXISTS `dragon_head_theme` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `batch_id` BIGINT UNSIGNED NOT NULL,
                `trading_date` DATE NOT NULL,
                `theme_name` VARCHAR(64) NOT NULL,
                `theme_rank` INT DEFAULT 0,
                `change_pct` DECIMAL(8,4) DEFAULT 0,
                `up_count` INT DEFAULT 0,
                `down_count` INT DEFAULT 0,
                `limit_up_count` INT DEFAULT 0,
                `logic_hardness` VARCHAR(10) DEFAULT NULL,
                `catalyst` VARCHAR(256) DEFAULT NULL,
                `sustainability` VARCHAR(256) DEFAULT NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                KEY `idx_batch_id` (`batch_id`),
                KEY `idx_trading_date` (`trading_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            # 推荐个股表
            """
            CREATE TABLE IF NOT EXISTS `dragon_head_stock` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `batch_id` BIGINT UNSIGNED NOT NULL,
                `trading_date` DATE NOT NULL,
                `rank` INT NOT NULL,
                `stock_code` VARCHAR(10) NOT NULL,
                `stock_name` VARCHAR(32) NOT NULL,
                `price` DECIMAL(10,4) DEFAULT 0,
                `change_pct` DECIMAL(8,4) DEFAULT 0,
                `amount` DECIMAL(20,4) DEFAULT 0,
                `float_market_cap` DECIMAL(20,4) DEFAULT 0,
                `total_market_cap` DECIMAL(20,4) DEFAULT 0,
                `turnover_rate` DECIMAL(8,4) DEFAULT 0,
                `limit_up_days` INT DEFAULT 1,
                `first_limit_time` VARCHAR(10) DEFAULT '',
                `seal_amount` DECIMAL(20,4) DEFAULT 0,
                `in_main_theme` TINYINT(1) DEFAULT 0,
                `related_themes` VARCHAR(256) DEFAULT '',
                `recommendation_level` VARCHAR(10) DEFAULT '关注',
                `reasons` TEXT DEFAULT NULL,
                `risk_warning` VARCHAR(512) DEFAULT NULL,
                `operation_suggestion` VARCHAR(256) DEFAULT NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                KEY `idx_batch_id` (`batch_id`),
                KEY `idx_trading_date` (`trading_date`),
                KEY `idx_stock_code` (`stock_code`),
                KEY `idx_stock_code_date` (`stock_code`, `trading_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            # 新闻情绪共振表
            """
            CREATE TABLE IF NOT EXISTS `dragon_head_news_resonance` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `batch_id` BIGINT UNSIGNED NOT NULL,
                `trading_date` DATE NOT NULL,
                `news_count` INT DEFAULT 0,
                `resonance_score` DECIMAL(5,2) DEFAULT 0,
                `matching_themes` VARCHAR(256) DEFAULT '',
                `news_keywords` TEXT DEFAULT NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                KEY `idx_batch_id` (`batch_id`),
                KEY `idx_trading_date` (`trading_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            # 涨停股快照表
            """
            CREATE TABLE IF NOT EXISTS `dragon_head_limit_up_snapshot` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `trading_date` DATE NOT NULL,
                `stock_code` VARCHAR(10) NOT NULL,
                `stock_name` VARCHAR(32) NOT NULL,
                `price` DECIMAL(10,4) DEFAULT 0,
                `change_pct` DECIMAL(8,4) DEFAULT 0,
                `amount` DECIMAL(20,4) DEFAULT 0,
                `float_market_cap` DECIMAL(20,4) DEFAULT 0,
                `total_market_cap` DECIMAL(20,4) DEFAULT 0,
                `turnover_rate` DECIMAL(8,4) DEFAULT 0,
                `limit_up_days` INT DEFAULT 1,
                `first_limit_time` VARCHAR(10) DEFAULT '',
                `seal_amount` DECIMAL(20,4) DEFAULT 0,
                `industry` VARCHAR(32) DEFAULT '',
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_date_code` (`trading_date`, `stock_code`),
                KEY `idx_trading_date` (`trading_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        ]

        try:
            for sql in create_sqls:
                await pool.execute_update(sql)
            self._tables_checked = True
            logger.info("Dragon head tables ensured")
        except Exception as e:
            logger.error("Failed to create dragon head tables", error=str(e))

    # ==================== 涨停股快照入库 ====================

    async def save_limit_up_snapshot(
        self,
        trading_date: str,
        limit_up_stocks: pd.DataFrame,
    ) -> int:
        """
        保存涨停股快照数据

        Args:
            trading_date: 交易日期 (YYYY-MM-DD)
            limit_up_stocks: 涨停股 DataFrame (已做列名标准化)

        Returns:
            插入的记录数
        """
        await self._ensure_tables()
        pool = await self._get_pool()

        if limit_up_stocks.empty:
            return 0

        inserted = 0
        try:
            # 标准化列名映射
            col_map = {}
            mapped = set()
            for col in limit_up_stocks.columns:
                if '代码' in col and 'code' not in mapped:
                    col_map[col] = 'code'
                    mapped.add('code')
                elif '名称' in col and 'name' not in mapped:
                    col_map[col] = 'name'
                    mapped.add('name')
                elif '涨跌幅' in col and 'change_pct' not in mapped:
                    col_map[col] = 'change_pct'
                    mapped.add('change_pct')
                elif ('最新价' in col or '收盘' in col) and 'price' not in mapped:
                    col_map[col] = 'price'
                    mapped.add('price')
                elif '成交额' in col and 'amount' not in mapped:
                    col_map[col] = 'amount'
                    mapped.add('amount')
                elif '流通市值' in col and 'float_market_cap' not in mapped:
                    col_map[col] = 'float_market_cap'
                    mapped.add('float_market_cap')
                elif '总市值' in col and 'total_market_cap' not in mapped:
                    col_map[col] = 'total_market_cap'
                    mapped.add('total_market_cap')
                elif '换手率' in col and 'turnover_rate' not in mapped:
                    col_map[col] = 'turnover_rate'
                    mapped.add('turnover_rate')
                elif '连板数' in col and 'limit_up_days' not in mapped:
                    col_map[col] = 'limit_up_days'
                    mapped.add('limit_up_days')
                elif '涨停统计' in col and 'zt_stat' not in mapped:
                    col_map[col] = 'zt_stat'
                    mapped.add('zt_stat')
                elif ('首次封板' in col or '封板时间' in col) and 'first_limit_time' not in mapped:
                    col_map[col] = 'first_limit_time'
                    mapped.add('first_limit_time')
                elif '封单' in col and 'seal_amount' not in mapped:
                    col_map[col] = 'seal_amount'
                    mapped.add('seal_amount')
                elif ('所属行业' in col or '行业' in col) and 'industry' not in mapped:
                    col_map[col] = 'industry'
                    mapped.add('industry')

            df = limit_up_stocks.rename(columns=col_map)
            df = df.loc[:, ~df.columns.duplicated(keep='first')]

            # 如果没有连板数字段，从涨停统计解析
            if 'limit_up_days' not in df.columns and 'zt_stat' in df.columns:
                df['limit_up_days'] = df['zt_stat'].apply(lambda v: _safe_int(v, 1))

            sql = """
                INSERT IGNORE INTO dragon_head_limit_up_snapshot
                (trading_date, stock_code, stock_name, price, change_pct, amount,
                 float_market_cap, total_market_cap, turnover_rate,
                 limit_up_days, first_limit_time, seal_amount, industry)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for _, row in df.iterrows():
                try:
                    params = (
                        trading_date,
                        str(row.get('code', '')),
                        str(row.get('name', '')),
                        _safe_float(row.get('price')),
                        _safe_float(row.get('change_pct')),
                        _safe_float(row.get('amount')),
                        _safe_float(row.get('float_market_cap')),
                        _safe_float(row.get('total_market_cap')),
                        _safe_float(row.get('turnover_rate')),
                        _safe_int(row.get('limit_up_days'), 1),
                        str(row.get('first_limit_time', '')) if pd.notna(row.get('first_limit_time')) else '',
                        _safe_float(row.get('seal_amount')),
                        str(row.get('industry', '')) if pd.notna(row.get('industry')) else '',
                    )
                    await pool.execute_update(sql, params)
                    inserted += 1
                except Exception as row_e:
                    logger.warning("Failed to insert limit-up snapshot row",
                                   error=str(row_e))
                    continue

            logger.info("Saved limit-up snapshot", trading_date=trading_date, count=inserted)

        except Exception as e:
            logger.error("Failed to save limit-up snapshot",
                         error=str(e), traceback=traceback.format_exc())

        return inserted

    # ==================== 策略结果入库 ====================

    async def save_strategy_result(
        self,
        result: Dict[str, Any],
        limit_up_count: int = 0,
    ) -> Optional[int]:
        """
        保存龙头战法策略完整结果

        Args:
            result: get_recommendations() 返回的完整结果字典
            limit_up_count: 涨停股总数

        Returns:
            batch_id 或 None
        """
        await self._ensure_tables()
        pool = await self._get_pool()

        data = result.get("data", {})
        if not data:
            return None

        try:
            now = datetime.now()
            trading_date = data.get("trading_date", now.strftime("%Y-%m-%d"))
            batch_no = f"{trading_date.replace('-', '')}_{now.strftime('%H%M%S')}"

            # 市场情绪
            sentiment = data.get("market_sentiment", {})

            # 1. 插入批次记录
            batch_sql = """
                INSERT INTO dragon_head_batch
                (trading_date, batch_no, limit_up_count, recommendation_count,
                 llm_enhanced, llm_model,
                 market_sentiment_phase, market_sentiment_risk, market_sentiment_desc,
                 strategy_explanation, generated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            batch_params = (
                trading_date,
                batch_no,
                limit_up_count,
                data.get("total", 0),
                1 if data.get("llm_enhanced") else 0,
                "gpt-5.2" if data.get("llm_enhanced") else None,
                sentiment.get("phase"),
                sentiment.get("risk_level"),
                sentiment.get("description"),
                data.get("strategy_explanation", ""),
                data.get("generated_at", now.strftime("%Y-%m-%d %H:%M:%S")),
            )
            await pool.execute_update(batch_sql, batch_params)

            # 获取 batch_id
            batch_row = await pool.execute_one(
                "SELECT id FROM dragon_head_batch WHERE batch_no = %s", (batch_no,)
            )
            if not batch_row:
                logger.error("Failed to get batch_id after insert")
                return None

            batch_id = batch_row["id"]

            # 2. 插入主线题材
            theme_analysis = {
                ta["name"]: ta
                for ta in data.get("theme_analysis", [])
            }
            for idx, theme in enumerate(data.get("main_themes", [])):
                theme_name = theme.get("name", "")
                details = theme.get("details", {})
                analysis = theme_analysis.get(theme_name, {})

                theme_sql = """
                    INSERT INTO dragon_head_theme
                    (batch_id, trading_date, theme_name, theme_rank,
                     change_pct, up_count, down_count, limit_up_count,
                     logic_hardness, catalyst, sustainability)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                theme_params = (
                    batch_id,
                    trading_date,
                    theme_name,
                    idx + 1,
                    float(details.get("change_pct", 0)),
                    int(details.get("up_count", 0)),
                    int(details.get("down_count", 0)),
                    int(details.get("limit_up_count", 0)),
                    analysis.get("logic_hardness"),
                    analysis.get("catalyst"),
                    analysis.get("sustainability"),
                )
                await pool.execute_update(theme_sql, theme_params)

            # 3. 插入推荐个股
            for stock in data.get("recommendations", []):
                stock_sql = """
                    INSERT INTO dragon_head_stock
                    (batch_id, trading_date, `rank`, stock_code, stock_name,
                     price, change_pct, amount, float_market_cap, total_market_cap,
                     turnover_rate, limit_up_days, first_limit_time, seal_amount,
                     in_main_theme, related_themes,
                     recommendation_level, reasons,
                     risk_warning, operation_suggestion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                stock_params = (
                    batch_id,
                    trading_date,
                    stock.get("rank", 0),
                    stock.get("code", ""),
                    stock.get("name", ""),
                    float(stock.get("price", 0)),
                    float(stock.get("change_pct", 0)),
                    float(stock.get("amount", 0)),
                    float(stock.get("float_market_cap", 0)),
                    float(stock.get("total_market_cap", 0)),
                    float(stock.get("turnover_rate", 0)),
                    int(stock.get("limit_up_days", 1)),
                    stock.get("first_limit_time", ""),
                    float(stock.get("seal_amount", 0)),
                    1 if stock.get("in_main_theme") else 0,
                    ",".join(stock.get("related_themes", [])),
                    stock.get("recommendation_level", "关注"),
                    json.dumps(stock.get("reasons", []), ensure_ascii=False),
                    stock.get("risk_warning"),
                    stock.get("operation_suggestion"),
                )
                await pool.execute_update(stock_sql, stock_params)

            # 4. 插入新闻共振
            resonance = data.get("news_resonance", {})
            if resonance:
                resonance_sql = """
                    INSERT INTO dragon_head_news_resonance
                    (batch_id, trading_date, news_count, resonance_score,
                     matching_themes, news_keywords)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                resonance_params = (
                    batch_id,
                    trading_date,
                    resonance.get("news_count", 0),
                    round(float(resonance.get("resonance_score", 0)), 2),
                    ",".join(resonance.get("matching_themes", [])),
                    json.dumps(resonance.get("news_keywords", []), ensure_ascii=False),
                )
                await pool.execute_update(resonance_sql, resonance_params)

            logger.info("Strategy result saved to database",
                        batch_id=batch_id,
                        batch_no=batch_no,
                        stocks=len(data.get("recommendations", [])),
                        themes=len(data.get("main_themes", [])))

            return batch_id

        except Exception as e:
            logger.error("Failed to save strategy result",
                         error=str(e), traceback=traceback.format_exc())
            return None

    # ==================== 查询接口 ====================

    async def get_latest_batch(self, trading_date: str = None) -> Optional[Dict]:
        """获取最近一次推荐批次"""
        await self._ensure_tables()
        pool = await self._get_pool()

        if trading_date:
            row = await pool.execute_one(
                "SELECT * FROM dragon_head_batch WHERE trading_date = %s ORDER BY id DESC LIMIT 1",
                (trading_date,)
            )
        else:
            row = await pool.execute_one(
                "SELECT * FROM dragon_head_batch ORDER BY id DESC LIMIT 1"
            )
        return row

    async def get_batch_stocks(self, batch_id: int) -> List[Dict]:
        """获取某批次的推荐个股"""
        await self._ensure_tables()
        pool = await self._get_pool()

        rows = await pool.execute_query(
            "SELECT * FROM dragon_head_stock WHERE batch_id = %s ORDER BY `rank`",
            (batch_id,)
        )
        # 解析 reasons JSON
        for row in rows:
            if row.get("reasons"):
                try:
                    row["reasons"] = json.loads(row["reasons"])
                except (json.JSONDecodeError, TypeError):
                    row["reasons"] = []
        return rows

    async def get_batch_themes(self, batch_id: int) -> List[Dict]:
        """获取某批次的主线题材"""
        await self._ensure_tables()
        pool = await self._get_pool()

        return await pool.execute_query(
            "SELECT * FROM dragon_head_theme WHERE batch_id = %s ORDER BY theme_rank",
            (batch_id,)
        )

    async def get_history_batches(
        self,
        start_date: str = None,
        end_date: str = None,
        limit: int = 30,
    ) -> List[Dict]:
        """获取历史推荐批次列表"""
        await self._ensure_tables()
        pool = await self._get_pool()

        conditions = []
        params = []
        if start_date:
            conditions.append("trading_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trading_date <= %s")
            params.append(end_date)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT id, trading_date, batch_no, limit_up_count,
                   recommendation_count, llm_enhanced, llm_model,
                   market_sentiment_phase, market_sentiment_risk,
                   generated_at, created_at
            FROM dragon_head_batch
            {where}
            ORDER BY trading_date DESC, id DESC
            LIMIT %s
        """
        params.append(limit)
        return await pool.execute_query(sql, tuple(params))

    async def get_stock_history(
        self,
        stock_code: str,
        limit: int = 30,
    ) -> List[Dict]:
        """获取某只股票的历史推荐记录"""
        await self._ensure_tables()
        pool = await self._get_pool()

        rows = await pool.execute_query(
            """SELECT s.*, b.batch_no, b.llm_enhanced
               FROM dragon_head_stock s
               JOIN dragon_head_batch b ON s.batch_id = b.id
               WHERE s.stock_code = %s
               ORDER BY s.trading_date DESC
               LIMIT %s""",
            (stock_code, limit)
        )
        for row in rows:
            if row.get("reasons"):
                try:
                    row["reasons"] = json.loads(row["reasons"])
                except (json.JSONDecodeError, TypeError):
                    row["reasons"] = []
        return rows


# 全局单例
dragon_head_repo = DragonHeadRepository()
