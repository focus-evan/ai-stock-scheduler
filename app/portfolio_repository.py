"""
模拟交易数据持久化模块 (Portfolio Repository)

将模拟交易系统全链路数据入库 MySQL：
1. 组合配置 (portfolio_config)
2. 持仓明细 (portfolio_position)
3. 交易记录 (portfolio_trade)
4. 每日汇总 (portfolio_daily_summary)
5. 策略推荐缓存 (strategy_recommendation_cache)
"""

import json
import traceback
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

# aiomysql 在文件顶部导入，确保 aiomysql.DictCursor 在所有方法中可用
try:
    import aiomysql
except ImportError:
    aiomysql = None  # type: ignore
    structlog.get_logger().warning(
        "aiomysql not installed, portfolio features will not work. "
        "Install with: pip install aiomysql"
    )

logger = structlog.get_logger()


def _safe_float(val, default=0.0, decimals=4):
    """安全转换 float"""
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return default


class PortfolioRepository:
    """模拟交易数据仓库"""

    def __init__(self):
        self._pool = None
        self._tables_checked = False

    async def _get_pool(self):
        """获取数据库连接池"""
        if self._pool is None:
            try:
                import aiomysql
                import os
                # 优先使用 DB_* 变量（与主应用 async_connection.py 一致），
                # 其次使用 MYSQL_* 变量
                self._pool = await aiomysql.create_pool(
                    host=os.getenv("DB_HOST") or os.getenv("MYSQL_HOST", "localhost"),
                    port=int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT", "3306")),
                    user=os.getenv("DB_USER") or os.getenv("MYSQL_USER", "root"),
                    password=os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD", ""),
                    db=os.getenv("DB_NAME") or os.getenv("MYSQL_DATABASE", "evan_test"),
                    charset="utf8mb4",
                    autocommit=True,
                    minsize=1,
                    maxsize=5,
                )
            except Exception as e:
                logger.error("Portfolio DB pool creation failed", error=str(e))
                raise
        return self._pool

    async def _ensure_tables(self):
        """确保表结构存在"""
        if self._tables_checked:
            return

        sql_path = Path(__file__).parent / "database" / "portfolio_tables.sql"
        if not sql_path.exists():
            # 如果 SQL 文件不在 database/ 下，尝试从同级目录找
            sql_path = Path(__file__).parent / "portfolio_tables.sql"
            if not sql_path.exists():
                # fallback: 内联建表
                logger.warning("portfolio_tables.sql not found, using inline DDL")
                await self._create_tables_inline()
                self._tables_checked = True
                return

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql_text = sql_path.read_text(encoding="utf-8")
                # Split by semicolon and execute each statement
                for stmt in sql_text.split(";"):
                    stmt = stmt.strip()
                    if stmt and not stmt.startswith("--"):
                        try:
                            await cur.execute(stmt)
                        except Exception as e:
                            # Ignore "table already exists" errors
                            if "already exists" not in str(e).lower():
                                logger.warning("DDL execution warning", stmt=stmt[:80], error=str(e))

        self._tables_checked = True
        logger.info("Portfolio tables ensured")

    async def _create_tables_inline(self):
        """内联建表（备用）"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # portfolio_config
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_config (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT DEFAULT NULL COMMENT '所属用户ID',
                        strategy_type VARCHAR(32) NOT NULL,
                        name VARCHAR(128) NOT NULL DEFAULT '',
                        initial_capital DECIMAL(18,2) NOT NULL DEFAULT 100000.00,
                        available_cash DECIMAL(18,2) NOT NULL DEFAULT 100000.00,
                        total_asset DECIMAL(18,2) NOT NULL DEFAULT 100000.00,
                        total_profit DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                        total_profit_pct DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        auto_trade TINYINT(1) NOT NULL DEFAULT 1,
                        status VARCHAR(16) NOT NULL DEFAULT 'active',
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_user (user_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                # 兼容已有表：添加 user_id 列
                try:
                    await cur.execute(
                        "ALTER TABLE portfolio_config ADD COLUMN user_id BIGINT DEFAULT NULL COMMENT '所属用户ID' AFTER id"
                    )
                    await cur.execute(
                        "ALTER TABLE portfolio_config ADD INDEX idx_user (user_id)"
                    )
                except Exception:
                    pass  # 列/索引已存在，忽略
                # portfolio_position
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_position (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        portfolio_id BIGINT NOT NULL,
                        stock_code VARCHAR(16) NOT NULL,
                        stock_name VARCHAR(32) NOT NULL DEFAULT '',
                        quantity INT NOT NULL DEFAULT 0,
                        avg_cost DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        current_price DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        market_value DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                        profit DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                        profit_pct DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        weight DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
                        status VARCHAR(16) NOT NULL DEFAULT 'holding',
                        buy_date DATE DEFAULT NULL,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                # portfolio_trade
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_trade (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        portfolio_id BIGINT NOT NULL,
                        stock_code VARCHAR(16) NOT NULL,
                        stock_name VARCHAR(32) NOT NULL DEFAULT '',
                        direction VARCHAR(8) NOT NULL,
                        price DECIMAL(10,4) NOT NULL,
                        quantity INT NOT NULL,
                        amount DECIMAL(18,2) NOT NULL,
                        profit DECIMAL(18,2) DEFAULT NULL,
                        profit_pct DECIMAL(10,4) DEFAULT NULL,
                        reason TEXT DEFAULT NULL,
                        trade_date DATE NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                # portfolio_daily_summary
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_daily_summary (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        portfolio_id BIGINT NOT NULL,
                        trading_date DATE NOT NULL,
                        total_asset DECIMAL(18,2) NOT NULL,
                        available_cash DECIMAL(18,2) NOT NULL,
                        market_value DECIMAL(18,2) NOT NULL,
                        daily_profit DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                        daily_profit_pct DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        total_profit DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                        total_profit_pct DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        position_count INT NOT NULL DEFAULT 0,
                        trade_count INT NOT NULL DEFAULT 0,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_portfolio_date (portfolio_id, trading_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                # strategy_recommendation_cache
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS strategy_recommendation_cache (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        strategy_type VARCHAR(32) NOT NULL COMMENT 'dragon_head / sentiment',
                        trading_date DATE NOT NULL COMMENT '交易日期',
                        session_type VARCHAR(16) NOT NULL COMMENT 'morning / afternoon / manual',
                        recommendations JSON NOT NULL COMMENT '推荐股票列表 JSON',
                        stock_count INT NOT NULL DEFAULT 0 COMMENT '推荐股票数',
                        generated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_strategy_date_session (strategy_type, trading_date, session_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略推荐历史'
                """)
                # portfolio_review
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_review (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        portfolio_id BIGINT NOT NULL,
                        strategy_type VARCHAR(32) NOT NULL,
                        trading_date DATE NOT NULL,
                        trade_count INT NOT NULL DEFAULT 0,
                        buy_count INT NOT NULL DEFAULT 0,
                        sell_count INT NOT NULL DEFAULT 0,
                        daily_profit DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                        daily_profit_pct DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
                        overall_score INT NOT NULL DEFAULT 0,
                        overall_comment TEXT NOT NULL,
                        highlights JSON NOT NULL,
                        shortcomings JSON NOT NULL,
                        lessons JSON NOT NULL,
                        suggestions JSON NOT NULL,
                        position_analysis JSON DEFAULT NULL,
                        risk_assessment TEXT DEFAULT NULL,
                        generated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_portfolio_date (portfolio_id, trading_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='模拟交易每日复盘'
                """)

                # 添加增强复盘字段（兼容旧表）
                for col, col_def in [
                    ("buy_analysis", "JSON DEFAULT NULL COMMENT '逐笔买入分析'"),
                    ("sell_analysis", "JSON DEFAULT NULL COMMENT '逐笔卖出分析'"),
                    ("improvement_steps", "JSON DEFAULT NULL COMMENT '改进措施'"),
                    ("next_day_actions", "JSON DEFAULT NULL COMMENT '次日行动计划'"),
                    ("strategy_effectiveness", "JSON DEFAULT NULL COMMENT '策略执行评价'"),
                ]:
                    try:
                        await cur.execute(
                            f"ALTER TABLE portfolio_review ADD COLUMN {col} {col_def}"
                        )
                    except Exception:
                        pass  # 列已存在，忽略

    # ==================== 组合配置 CRUD ====================

    async def create_portfolio(self, strategy_type: str, name: str,
                               initial_capital: float,
                               user_id: int = None) -> Optional[int]:
        """创建新组合，返回 portfolio_id"""
        await self._ensure_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """INSERT INTO portfolio_config
                           (user_id, strategy_type, name, initial_capital, available_cash, total_asset)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (user_id, strategy_type, name, initial_capital, initial_capital, initial_capital)
                    )
                    portfolio_id = cur.lastrowid
                    logger.info("Portfolio created", portfolio_id=portfolio_id,
                                strategy=strategy_type, capital=initial_capital,
                                user_id=user_id)
                    return portfolio_id
        except Exception as e:
            logger.error("Failed to create portfolio", error=str(e))
            return None

    async def get_portfolio(self, portfolio_id: int) -> Optional[Dict]:
        """获取组合配置"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT * FROM portfolio_config WHERE id = %s", (portfolio_id,)
                )
                row = await cur.fetchone()
                if row:
                    return self._serialize_row(row)
                return None

    async def list_portfolios(self, strategy_type: str = None,
                              status: str = "active",
                              user_id: int = None) -> List[Dict]:
        """列出组合（可按用户过滤）"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                sql = "SELECT * FROM portfolio_config WHERE 1=1"
                params = []
                if user_id is not None:
                    sql += " AND user_id = %s"
                    params.append(user_id)
                if strategy_type:
                    sql += " AND strategy_type = %s"
                    params.append(strategy_type)
                if status:
                    sql += " AND status = %s"
                    params.append(status)
                sql += " ORDER BY created_at DESC"
                await cur.execute(sql, params)
                rows = await cur.fetchall()
                return [self._serialize_row(r) for r in rows]

    async def update_portfolio_assets(self, portfolio_id: int,
                                      available_cash: float,
                                      total_asset: float,
                                      total_profit: float,
                                      total_profit_pct: float):
        """更新组合资产"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE portfolio_config
                       SET available_cash=%s, total_asset=%s,
                           total_profit=%s, total_profit_pct=%s
                       WHERE id=%s""",
                    (available_cash, total_asset, total_profit, total_profit_pct, portfolio_id)
                )

    async def toggle_auto_trade(self, portfolio_id: int, auto_trade: bool):
        """开启/关闭自动交易"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                status = "active" if auto_trade else "paused"
                await cur.execute(
                    "UPDATE portfolio_config SET auto_trade=%s, status=%s WHERE id=%s",
                    (1 if auto_trade else 0, status, portfolio_id)
                )

    # ==================== 持仓 CRUD ====================

    async def upsert_position(self, portfolio_id: int, stock_code: str,
                              stock_name: str, quantity: int, avg_cost: float,
                              current_price: float, weight: float,
                              buy_date: str = None):
        """新增或更新持仓"""
        pool = await self._get_pool()
        market_value = quantity * current_price
        profit = (current_price - avg_cost) * quantity
        profit_pct = ((current_price - avg_cost) / avg_cost * 100) if avg_cost > 0 else 0
        status = "holding" if quantity > 0 else "sold"

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Try update first
                await cur.execute(
                    """UPDATE portfolio_position
                       SET quantity=%s, avg_cost=%s, current_price=%s,
                           market_value=%s, profit=%s, profit_pct=%s,
                           weight=%s, status=%s
                       WHERE portfolio_id=%s AND stock_code=%s AND status='holding'""",
                    (quantity, avg_cost, current_price, market_value,
                     profit, profit_pct, weight, status, portfolio_id, stock_code)
                )
                if cur.rowcount == 0 and quantity > 0:
                    # Insert new
                    await cur.execute(
                        """INSERT INTO portfolio_position
                           (portfolio_id, stock_code, stock_name, quantity, avg_cost,
                            current_price, market_value, profit, profit_pct, weight, status, buy_date)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (portfolio_id, stock_code, stock_name, quantity, avg_cost,
                         current_price, market_value, profit, profit_pct, weight, status,
                         buy_date or datetime.now().strftime("%Y-%m-%d"))
                    )

    async def get_positions(self, portfolio_id: int,
                            status: str = "holding") -> List[Dict]:
        """获取持仓列表"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                sql = "SELECT * FROM portfolio_position WHERE portfolio_id=%s"
                params = [portfolio_id]
                if status:
                    sql += " AND status=%s"
                    params.append(status)
                sql += " ORDER BY weight DESC"
                await cur.execute(sql, params)
                rows = await cur.fetchall()
                return [self._serialize_row(r) for r in rows]

    async def update_position_price(self, portfolio_id: int,
                                    stock_code: str, current_price: float):
        """更新持仓最新价"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE portfolio_position
                       SET current_price=%s,
                           market_value=quantity*%s,
                           profit=((%s - avg_cost) * quantity),
                           profit_pct=IF(avg_cost>0, ((%s-avg_cost)/avg_cost*100), 0)
                       WHERE portfolio_id=%s AND stock_code=%s AND status='holding'""",
                    (current_price, current_price, current_price, current_price,
                     portfolio_id, stock_code)
                )

    async def delete_holding_positions(self, portfolio_id: int):
        """删除组合的所有 holding 状态持仓行（合并前清理用）"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM portfolio_position WHERE portfolio_id=%s AND status='holding'",
                    (portfolio_id,)
                )
                logger.info("Deleted all holding positions",
                             portfolio_id=portfolio_id, deleted=cur.rowcount)

    # ==================== 交易记录 ====================

    async def save_trade(self, portfolio_id: int, stock_code: str,
                         stock_name: str, direction: str, price: float,
                         quantity: int, reason: str = None,
                         profit: float = None, profit_pct: float = None,
                         trade_date: str = None) -> Optional[int]:
        """保存交易记录"""
        pool = await self._get_pool()
        amount = price * quantity
        if not trade_date:
            trade_date = datetime.now().strftime("%Y-%m-%d")

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """INSERT INTO portfolio_trade
                           (portfolio_id, stock_code, stock_name, direction,
                            price, quantity, amount, profit, profit_pct, reason, trade_date)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (portfolio_id, stock_code, stock_name, direction,
                         price, quantity, amount, profit, profit_pct, reason, trade_date)
                    )
                    return cur.lastrowid
        except Exception as e:
            logger.error("Failed to save trade", error=str(e))
            return None

    async def get_trades(self, portfolio_id: int,
                         limit: int = 50) -> List[Dict]:
        """获取交易记录"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """SELECT * FROM portfolio_trade
                       WHERE portfolio_id=%s
                       ORDER BY created_at DESC
                       LIMIT %s""",
                    (portfolio_id, limit)
                )
                rows = await cur.fetchall()
                return [self._serialize_row(r) for r in rows]

    async def get_trades_by_portfolio(self, portfolio_id: int) -> List[Dict]:
        """获取组合的全部交易记录（按时间正序，用于重新计算 avg_cost）"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """SELECT stock_code, direction, price, quantity
                       FROM portfolio_trade
                       WHERE portfolio_id=%s
                       ORDER BY trade_date ASC, created_at ASC""",
                    (portfolio_id,)
                )
                rows = await cur.fetchall()
                return [self._serialize_row(r) for r in rows]

    async def get_trades_paginated(self, portfolio_id: int,
                                    limit: int = 20,
                                    offset: int = 0) -> List[Dict]:
        """获取交易记录（分页）"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """SELECT * FROM portfolio_trade
                       WHERE portfolio_id=%s
                       ORDER BY created_at DESC
                       LIMIT %s OFFSET %s""",
                    (portfolio_id, limit, offset)
                )
                rows = await cur.fetchall()
                return [self._serialize_row(r) for r in rows]

    async def get_trades_count(self, portfolio_id: int) -> int:
        """获取交易记录总数"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT COUNT(*) FROM portfolio_trade WHERE portfolio_id=%s",
                    (portfolio_id,)
                )
                row = await cur.fetchone()
                return row[0] if row else 0

    async def get_stock_pnl_summary(self, portfolio_id: int) -> List[Dict]:
        """
        按股票维度汇总历史盈亏

        返回每只股票的：
        - 总买入金额/股数、总卖出金额/股数
        - 已实现盈亏（卖出时产生的）
        - 当前持仓量/浮动盈亏
        - 首次/最后交易日
        """
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # 从交易记录聚合
                await cur.execute("""
                    SELECT
                        t.stock_code,
                        t.stock_name,
                        SUM(CASE WHEN t.direction='buy'  THEN t.amount   ELSE 0 END) AS total_buy_amount,
                        SUM(CASE WHEN t.direction='sell'  THEN t.amount   ELSE 0 END) AS total_sell_amount,
                        SUM(CASE WHEN t.direction='buy'  THEN t.quantity  ELSE 0 END) AS total_buy_qty,
                        SUM(CASE WHEN t.direction='sell'  THEN t.quantity  ELSE 0 END) AS total_sell_qty,
                        SUM(CASE WHEN t.direction='buy'  THEN 1 ELSE 0 END) AS buy_count,
                        SUM(CASE WHEN t.direction='sell'  THEN 1 ELSE 0 END) AS sell_count,
                        SUM(CASE WHEN t.direction='sell' AND t.profit IS NOT NULL
                            THEN t.profit ELSE 0 END) AS realized_profit,
                        MIN(t.trade_date) AS first_trade_date,
                        MAX(t.trade_date) AS last_trade_date,
                        p.quantity    AS holding_qty,
                        p.avg_cost    AS holding_avg_cost,
                        p.current_price AS holding_current_price,
                        p.market_value  AS holding_market_value
                    FROM portfolio_trade t
                    LEFT JOIN portfolio_position p
                        ON p.portfolio_id = t.portfolio_id
                        AND p.stock_code = t.stock_code
                        AND p.quantity > 0
                    WHERE t.portfolio_id = %s
                    GROUP BY t.stock_code, t.stock_name,
                             p.quantity, p.avg_cost, p.current_price, p.market_value
                    ORDER BY last_trade_date DESC
                """, (portfolio_id,))
                rows = await cur.fetchall()

                result = []
                for row in rows:
                    item = self._serialize_row(row)
                    # 计算浮动盈亏
                    holding_qty = item.get("holding_qty") or 0
                    avg_cost = float(item.get("holding_avg_cost") or 0)
                    current_price = float(item.get("holding_current_price") or 0)
                    if holding_qty > 0 and avg_cost > 0:
                        item["unrealized_profit"] = round(
                            (current_price - avg_cost) * holding_qty, 2
                        )
                        item["unrealized_profit_pct"] = round(
                            (current_price - avg_cost) / avg_cost * 100, 2
                        )
                    else:
                        item["unrealized_profit"] = 0
                        item["unrealized_profit_pct"] = 0

                    # 总盈亏 = 已实现 + 浮动
                    realized = float(item.get("realized_profit") or 0)
                    unrealized = float(item.get("unrealized_profit") or 0)
                    item["total_pnl"] = round(realized + unrealized, 2)

                    # 状态
                    item["status"] = "holding" if holding_qty > 0 else "closed"

                    result.append(item)

                return result

    async def update_cash(self, portfolio_id: int, available_cash: float):
        """更新组合可用现金"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE portfolio_config SET available_cash=%s WHERE id=%s",
                    (available_cash, portfolio_id)
                )

    # ==================== 每日汇总 ====================

    async def save_daily_summary(self, portfolio_id: int, trading_date: str,
                                 total_asset: float, available_cash: float,
                                 market_value: float, daily_profit: float,
                                 daily_profit_pct: float, total_profit: float,
                                 total_profit_pct: float, position_count: int,
                                 trade_count: int):
        """保存或更新每日汇总"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO portfolio_daily_summary
                       (portfolio_id, trading_date, total_asset, available_cash,
                        market_value, daily_profit, daily_profit_pct,
                        total_profit, total_profit_pct, position_count, trade_count)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON DUPLICATE KEY UPDATE
                        total_asset=VALUES(total_asset),
                        available_cash=VALUES(available_cash),
                        market_value=VALUES(market_value),
                        daily_profit=VALUES(daily_profit),
                        daily_profit_pct=VALUES(daily_profit_pct),
                        total_profit=VALUES(total_profit),
                        total_profit_pct=VALUES(total_profit_pct),
                        position_count=VALUES(position_count),
                        trade_count=VALUES(trade_count)""",
                    (portfolio_id, trading_date, total_asset, available_cash,
                     market_value, daily_profit, daily_profit_pct,
                     total_profit, total_profit_pct, position_count, trade_count)
                )

    async def get_performance(self, portfolio_id: int,
                              days: int = 30) -> List[Dict]:
        """获取收益曲线"""
        await self._ensure_tables()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """SELECT * FROM portfolio_daily_summary
                       WHERE portfolio_id=%s
                       ORDER BY trading_date DESC
                       LIMIT %s""",
                    (portfolio_id, days)
                )
                rows = await cur.fetchall()
                return [self._serialize_row(r) for r in reversed(rows)]

    # ==================== 推荐缓存 ====================

    async def save_recommendation_cache(
        self,
        strategy_type: str,
        session_type: str,
        recommendations: List[Dict],
        trading_date: str = None,
    ):
        """
        保存策略推荐缓存

        Args:
            strategy_type: dragon_head / sentiment
            session_type: morning / afternoon
            recommendations: 推荐股票列表（最多10只）
            trading_date: 交易日期，默认今天
        """
        await self._ensure_tables()
        pool = await self._get_pool()
        if not trading_date:
            trading_date = datetime.now().strftime("%Y-%m-%d")

        recs_json = json.dumps(recommendations, ensure_ascii=False, default=str)
        stock_count = len(recommendations)

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """INSERT INTO strategy_recommendation_cache
                           (strategy_type, trading_date, session_type,
                            recommendations, stock_count, generated_at)
                           VALUES (%s, %s, %s, %s, %s, NOW())
                           ON DUPLICATE KEY UPDATE
                            recommendations=VALUES(recommendations),
                            stock_count=VALUES(stock_count),
                            generated_at=NOW()""",
                        (strategy_type, trading_date, session_type,
                         recs_json, stock_count)
                    )
            logger.info("Recommendation cache saved",
                        strategy=strategy_type, session=session_type,
                        count=stock_count, date=trading_date)
        except Exception as e:
            logger.error("Failed to save recommendation cache",
                         strategy=strategy_type, error=str(e))

    async def get_recommendation_cache(
        self,
        strategy_type: str,
        trading_date: str = None,
        include_meta: bool = False,
    ):
        """
        获取最新的策略推荐缓存

        优先取当日下午的推荐，其次取当日上午的推荐。
        如果当日没有推荐（如服务刚启动），取最近一个交易日的推荐。

        Args:
            strategy_type: dragon_head / sentiment
            trading_date: 交易日期，默认今天
            include_meta: True 则返回 dict{stocks, trading_date, session_type,
                          generated_at, stock_count}；False 则返回 List[Dict] 兼容旧调用

        Returns:
            include_meta=False: 推荐股票列表 List[Dict]
            include_meta=True:  {stocks, trading_date, session_type, generated_at, stock_count}
        """
        await self._ensure_tables()
        pool = await self._get_pool()
        if not trading_date:
            trading_date = datetime.now().strftime("%Y-%m-%d")

        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # 优先取当日最新一条（按 generated_at 倒序，下午 > 上午）
                    await cur.execute(
                        """SELECT recommendations, stock_count, session_type,
                                  trading_date, generated_at
                           FROM strategy_recommendation_cache
                           WHERE strategy_type=%s AND trading_date=%s
                           ORDER BY generated_at DESC
                           LIMIT 1""",
                        (strategy_type, trading_date)
                    )
                    row = await cur.fetchone()

                    if not row:
                        # 当日没有缓存，取最近一次
                        await cur.execute(
                            """SELECT recommendations, stock_count, session_type,
                                      trading_date, generated_at
                               FROM strategy_recommendation_cache
                               WHERE strategy_type=%s
                               ORDER BY trading_date DESC, generated_at DESC
                               LIMIT 1""",
                            (strategy_type,)
                        )
                        row = await cur.fetchone()

                    if row:
                        recs = row["recommendations"]
                        if isinstance(recs, str):
                            recs = json.loads(recs)
                        logger.info("Recommendation cache hit",
                                    strategy=strategy_type,
                                    session=row["session_type"],
                                    date=str(row["trading_date"]),
                                    count=row["stock_count"])

                        if include_meta:
                            generated_at = row.get("generated_at")
                            if generated_at and hasattr(generated_at, "strftime"):
                                generated_at = generated_at.strftime("%Y-%m-%d %H:%M:%S")
                            td = row.get("trading_date")
                            if td and hasattr(td, "strftime"):
                                td = td.strftime("%Y-%m-%d")
                            return {
                                "stocks": recs,
                                "trading_date": str(td) if td else trading_date,
                                "session_type": row.get("session_type", ""),
                                "generated_at": str(generated_at) if generated_at else None,
                                "stock_count": row.get("stock_count", len(recs)),
                            }
                        return recs

                    logger.warning("No recommendation cache found",
                                   strategy=strategy_type)
                    if include_meta:
                        return {
                            "stocks": [],
                            "trading_date": trading_date,
                            "session_type": "",
                            "generated_at": None,
                            "stock_count": 0,
                        }
                    return []

        except Exception as e:
            logger.error("Failed to get recommendation cache",
                         strategy=strategy_type, error=str(e))
            if include_meta:
                return {
                    "stocks": [],
                    "trading_date": trading_date,
                    "session_type": "",
                    "generated_at": None,
                    "stock_count": 0,
                }
            return []

    # ==================== 推荐历史 ====================

    async def get_recommendation_history(
        self,
        strategy_type: str = None,
        limit: int = 30,
    ) -> List[Dict]:
        """获取历史推荐记录列表"""
        await self._ensure_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    sql = """SELECT id, strategy_type, trading_date, session_type,
                                    stock_count, generated_at
                             FROM strategy_recommendation_cache
                             WHERE 1=1"""
                    params = []
                    if strategy_type:
                        sql += " AND strategy_type=%s"
                        params.append(strategy_type)
                    sql += " ORDER BY trading_date DESC, generated_at DESC LIMIT %s"
                    params.append(limit)
                    await cur.execute(sql, params)
                    rows = await cur.fetchall()
                    return [self._serialize_row(r) for r in rows]
        except Exception as e:
            logger.error("Failed to get recommendation history", error=str(e))
            return []

    # ==================== 复盘 CRUD ====================

    async def save_review(
        self,
        portfolio_id: int,
        strategy_type: str,
        trading_date: str,
        trade_count: int,
        buy_count: int,
        sell_count: int,
        daily_profit: float,
        daily_profit_pct: float,
        overall_score: int,
        overall_comment: str,
        highlights: List,
        shortcomings: List,
        lessons: List,
        suggestions: List,
        position_analysis: Any = None,
        risk_assessment: str = None,
        buy_analysis: List = None,
        sell_analysis: List = None,
        improvement_steps: List = None,
        next_day_actions: List = None,
        strategy_effectiveness: Dict = None,
    ):
        """保存或更新每日复盘（增强版）"""
        await self._ensure_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """INSERT INTO portfolio_review
                           (portfolio_id, strategy_type, trading_date,
                            trade_count, buy_count, sell_count,
                            daily_profit, daily_profit_pct,
                            overall_score, overall_comment,
                            highlights, shortcomings, lessons, suggestions,
                            position_analysis, risk_assessment,
                            buy_analysis, sell_analysis,
                            improvement_steps, next_day_actions,
                            strategy_effectiveness, generated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                           ON DUPLICATE KEY UPDATE
                            trade_count=VALUES(trade_count),
                            buy_count=VALUES(buy_count),
                            sell_count=VALUES(sell_count),
                            daily_profit=VALUES(daily_profit),
                            daily_profit_pct=VALUES(daily_profit_pct),
                            overall_score=VALUES(overall_score),
                            overall_comment=VALUES(overall_comment),
                            highlights=VALUES(highlights),
                            shortcomings=VALUES(shortcomings),
                            lessons=VALUES(lessons),
                            suggestions=VALUES(suggestions),
                            position_analysis=VALUES(position_analysis),
                            risk_assessment=VALUES(risk_assessment),
                            buy_analysis=VALUES(buy_analysis),
                            sell_analysis=VALUES(sell_analysis),
                            improvement_steps=VALUES(improvement_steps),
                            next_day_actions=VALUES(next_day_actions),
                            strategy_effectiveness=VALUES(strategy_effectiveness),
                            generated_at=NOW()""",
                        (portfolio_id, strategy_type, trading_date,
                         trade_count, buy_count, sell_count,
                         daily_profit, daily_profit_pct,
                         overall_score, overall_comment,
                         json.dumps(highlights, ensure_ascii=False, default=str),
                         json.dumps(shortcomings, ensure_ascii=False, default=str),
                         json.dumps(lessons, ensure_ascii=False, default=str),
                         json.dumps(suggestions, ensure_ascii=False, default=str),
                         json.dumps(position_analysis, ensure_ascii=False, default=str) if position_analysis else None,
                         risk_assessment,
                         json.dumps(buy_analysis, ensure_ascii=False, default=str) if buy_analysis else None,
                         json.dumps(sell_analysis, ensure_ascii=False, default=str) if sell_analysis else None,
                         json.dumps(improvement_steps, ensure_ascii=False, default=str) if improvement_steps else None,
                         json.dumps(next_day_actions, ensure_ascii=False, default=str) if next_day_actions else None,
                         json.dumps(strategy_effectiveness, ensure_ascii=False, default=str) if strategy_effectiveness else None)
                    )
            logger.info("Review saved", portfolio_id=portfolio_id, date=trading_date)
        except Exception as e:
            logger.error("Failed to save review", error=str(e))

    async def get_reviews(
        self,
        portfolio_id: int = None,
        strategy_type: str = None,
        limit: int = 30,
        user_id: int = None,
    ) -> List[Dict]:
        """获取复盘列表（支持按用户过滤）"""
        await self._ensure_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if user_id is not None:
                        sql = """SELECT r.* FROM portfolio_review r
                                 JOIN portfolio_config c ON r.portfolio_id = c.id
                                 WHERE c.user_id = %s"""
                        params = [user_id]
                    else:
                        sql = "SELECT * FROM portfolio_review WHERE 1=1"
                        params = []
                    if portfolio_id:
                        sql += " AND r.portfolio_id=%s" if user_id is not None else " AND portfolio_id=%s"
                        params.append(portfolio_id)
                    if strategy_type:
                        sql += " AND r.strategy_type=%s" if user_id is not None else " AND strategy_type=%s"
                        params.append(strategy_type)
                    sql += " ORDER BY r.trading_date DESC LIMIT %s" if user_id is not None else " ORDER BY trading_date DESC LIMIT %s"
                    params.append(limit)
                    await cur.execute(sql, params)
                    rows = await cur.fetchall()
                    result = []
                    for r in rows:
                        row = self._serialize_row(r)
                        for field in ['highlights', 'shortcomings', 'lessons',
                                      'suggestions', 'position_analysis',
                                      'buy_analysis', 'sell_analysis',
                                      'improvement_steps', 'next_day_actions',
                                      'strategy_effectiveness']:
                            if field in row and isinstance(row[field], str):
                                try:
                                    row[field] = json.loads(row[field])
                                except (json.JSONDecodeError, TypeError):
                                    pass
                        result.append(row)
                    return result
        except Exception as e:
            logger.error("Failed to get reviews", error=str(e))
            return []

    async def get_review(
        self,
        portfolio_id: int,
        trading_date: str,
    ) -> Optional[Dict]:
        """获取某日的复盘详情"""
        await self._ensure_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        "SELECT * FROM portfolio_review WHERE portfolio_id=%s AND trading_date=%s",
                        (portfolio_id, trading_date)
                    )
                    row = await cur.fetchone()
                    if row:
                        result = self._serialize_row(row)
                        for field in ['highlights', 'shortcomings', 'lessons',
                                      'suggestions', 'position_analysis',
                                      'buy_analysis', 'sell_analysis',
                                      'improvement_steps', 'next_day_actions',
                                      'strategy_effectiveness']:
                            if field in result and isinstance(result[field], str):
                                try:
                                    result[field] = json.loads(result[field])
                                except (json.JSONDecodeError, TypeError):
                                    pass
                        return result
                    return None
        except Exception as e:
            logger.error("Failed to get review", error=str(e))
            return None

    async def get_latest_review(self, portfolio_id: int) -> Optional[Dict]:
        """获取组合最近一次复盘（用于传递给下次交易决策）"""
        await self._ensure_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        """SELECT * FROM portfolio_review
                           WHERE portfolio_id=%s
                           ORDER BY trading_date DESC
                           LIMIT 1""",
                        (portfolio_id,)
                    )
                    row = await cur.fetchone()
                    if row:
                        result = self._serialize_row(row)
                        for field in ['highlights', 'shortcomings', 'lessons',
                                      'suggestions', 'position_analysis',
                                      'buy_analysis', 'sell_analysis',
                                      'improvement_steps', 'next_day_actions',
                                      'strategy_effectiveness']:
                            if field in result and isinstance(result[field], str):
                                try:
                                    result[field] = json.loads(result[field])
                                except (json.JSONDecodeError, TypeError):
                                    pass
                        return result
                    return None
        except Exception as e:
            logger.error("Failed to get latest review", error=str(e))
            return None
    # ==================== 跟投建议 ====================

    async def ensure_follow_table(self):
        """确保跟投建议表存在"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_follow_recommendation (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        portfolio_id BIGINT NOT NULL,
                        strategy_type VARCHAR(32) NOT NULL,
                        trading_date DATE NOT NULL,
                        session_type VARCHAR(16) NOT NULL DEFAULT 'morning',
                        recommendations JSON NOT NULL,
                        stock_count INT NOT NULL DEFAULT 0,
                        market_overview TEXT DEFAULT NULL,
                        strategy_summary TEXT DEFAULT NULL,
                        risk_warning TEXT DEFAULT NULL,
                        confidence_score INT NOT NULL DEFAULT 0,
                        generated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_portfolio_date_session (portfolio_id, trading_date, session_type),
                        INDEX idx_portfolio (portfolio_id),
                        INDEX idx_strategy (strategy_type),
                        INDEX idx_date (trading_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

    async def save_follow_recommendation(
        self, portfolio_id: int, strategy_type: str,
        trading_date: str, session_type: str,
        recommendations: list, stock_count: int,
        market_overview: str = "", strategy_summary: str = "",
        risk_warning: str = "", confidence_score: int = 0,
    ):
        """保存跟投建议"""
        await self.ensure_follow_table()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO portfolio_follow_recommendation
                       (portfolio_id, strategy_type, trading_date, session_type,
                        recommendations, stock_count, market_overview,
                        strategy_summary, risk_warning, confidence_score)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                        recommendations=VALUES(recommendations),
                        stock_count=VALUES(stock_count),
                        market_overview=VALUES(market_overview),
                        strategy_summary=VALUES(strategy_summary),
                        risk_warning=VALUES(risk_warning),
                        confidence_score=VALUES(confidence_score),
                        generated_at=NOW()""",
                    (portfolio_id, strategy_type, trading_date, session_type,
                     json.dumps(recommendations, ensure_ascii=False),
                     stock_count, market_overview or "",
                     strategy_summary or "", risk_warning or "",
                     confidence_score)
                )

    async def get_follow_recommendations(
        self, portfolio_id: int = None,
        strategy_type: str = None,
        limit: int = 20,
        user_id: int = None,
    ) -> List[Dict]:
        """获取跟投建议列表（支持按用户过滤）"""
        await self.ensure_follow_table()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if user_id is not None:
                    sql = """SELECT f.* FROM portfolio_follow_recommendation f
                             JOIN portfolio_config c ON f.portfolio_id = c.id
                             WHERE c.user_id = %s"""
                    params = [user_id]
                else:
                    sql = "SELECT * FROM portfolio_follow_recommendation WHERE 1=1"
                    params = []
                if portfolio_id:
                    sql += " AND f.portfolio_id=%s" if user_id is not None else " AND portfolio_id=%s"
                    params.append(portfolio_id)
                if strategy_type:
                    sql += " AND f.strategy_type=%s" if user_id is not None else " AND strategy_type=%s"
                    params.append(strategy_type)
                sql += " ORDER BY f.trading_date DESC, f.generated_at DESC LIMIT %s" if user_id is not None else " ORDER BY trading_date DESC, generated_at DESC LIMIT %s"
                params.append(limit)
                await cur.execute(sql, params)
                rows = await cur.fetchall()
                result = []
                for row in rows:
                    item = self._serialize_row(row)
                    if isinstance(item.get("recommendations"), str):
                        try:
                            item["recommendations"] = json.loads(item["recommendations"])
                        except (json.JSONDecodeError, TypeError):
                            pass
                    result.append(item)
                return result

    async def get_latest_follow(self, portfolio_id: int) -> Optional[Dict]:
        """获取某组合最新的跟投建议"""
        results = await self.get_follow_recommendations(
            portfolio_id=portfolio_id, limit=1
        )
        return results[0] if results else None

    # ==================== 辅助方法 ====================

    def _serialize_row(self, row: Dict) -> Dict:
        """序列化数据库行（处理 Decimal / datetime / date）"""
        result = {}
        for k, v in row.items():
            if isinstance(v, Decimal):
                result[k] = float(v)
            elif isinstance(v, datetime):
                result[k] = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, date):
                result[k] = v.strftime("%Y-%m-%d")
            else:
                result[k] = v
        return result

    # ==================== 跟投分析记录 ====================

    async def _ensure_follow_up_table(self):
        """确保跟投分析记录表存在"""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS follow_up_analysis_record (
                        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
                        stock_code      VARCHAR(16)    NOT NULL COMMENT '股票代码',
                        stock_name      VARCHAR(32)    NOT NULL DEFAULT '' COMMENT '股票名称',
                        shares          INT            NOT NULL COMMENT '买入股数（股）',
                        buy_price       DECIMAL(10,4)  NOT NULL COMMENT '买入时个股股价（元/股）',
                        buy_amount      DECIMAL(18,2)  NOT NULL COMMENT '买入总金额',
                        original_buy_price    DECIMAL(10,4) DEFAULT NULL,
                        original_target_price DECIMAL(10,4) DEFAULT NULL,
                        original_stop_loss    DECIMAL(10,4) DEFAULT NULL,
                        original_advice       TEXT          DEFAULT NULL,
                        current_price   DECIMAL(10,4) DEFAULT NULL COMMENT '分析时实时价格',
                        change_pct      DECIMAL(8,4)  DEFAULT NULL,
                        current_value   DECIMAL(18,2) DEFAULT NULL,
                        pnl_amount      DECIMAL(18,2) DEFAULT NULL,
                        pnl_pct         DECIMAL(8,4)  DEFAULT NULL,
                        analysis_result JSON          DEFAULT NULL,
                        core_decision   VARCHAR(64)   DEFAULT NULL,
                        position_assessment VARCHAR(128) DEFAULT NULL,
                        analyzed_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_stock_code (stock_code),
                        INDEX idx_analyzed_at (analyzed_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跟投分析记录表'
                """)

    async def save_follow_up_analysis(
        self,
        stock_code: str,
        stock_name: str,
        shares: int,
        buy_price: float,
        pnl_info: Dict,
        analysis: Dict,
        original_buy_price: Optional[float] = None,
        original_target_price: Optional[float] = None,
        original_stop_loss: Optional[float] = None,
        original_advice: Optional[str] = None,
    ) -> Optional[int]:
        """保存跟投分析记录，返回记录ID"""
        await self._ensure_follow_up_table()
        pool = await self._get_pool()
        buy_amount = round(shares * buy_price, 2)
        analysis_json = json.dumps(analysis, ensure_ascii=False, default=str)
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        INSERT INTO follow_up_analysis_record
                        (stock_code, stock_name, shares, buy_price, buy_amount,
                         original_buy_price, original_target_price, original_stop_loss,
                         original_advice, current_price, change_pct,
                         current_value, pnl_amount, pnl_pct,
                         analysis_result, core_decision, position_assessment)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        stock_code, stock_name, shares, buy_price, buy_amount,
                        original_buy_price, original_target_price, original_stop_loss,
                        original_advice,
                        pnl_info.get("current_price"), pnl_info.get("change_pct"),
                        pnl_info.get("current_value"), pnl_info.get("pnl_amount"),
                        pnl_info.get("pnl_pct"),
                        analysis_json,
                        analysis.get("core_decision", ""),
                        analysis.get("position_assessment", ""),
                    ))
                    record_id = cur.lastrowid
                    logger.info("Follow-up analysis saved", id=record_id, code=stock_code)
                    return record_id
        except Exception as e:
            logger.error("Failed to save follow-up analysis", code=stock_code, error=str(e))
            return None

    async def get_follow_up_analysis_history(
        self,
        stock_code: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict]:
        """查询跟投分析历史记录"""
        await self._ensure_follow_up_table()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if stock_code:
                        await cur.execute("""
                            SELECT * FROM follow_up_analysis_record
                            WHERE stock_code = %s
                            ORDER BY analyzed_at DESC
                            LIMIT %s
                        """, (stock_code, limit))
                    else:
                        await cur.execute("""
                            SELECT * FROM follow_up_analysis_record
                            ORDER BY analyzed_at DESC
                            LIMIT %s
                        """, (limit,))
                    rows = await cur.fetchall()
                    result = []
                    for row in rows:
                        item = self._serialize_row(row)
                        # 反序列化 JSON 字段
                        if isinstance(item.get("analysis_result"), str):
                            try:
                                item["analysis_result"] = json.loads(item["analysis_result"])
                            except Exception:
                                pass
                        result.append(item)
                    return result
        except Exception as e:
            logger.error("Failed to get follow-up history", error=str(e))
            return []

    # ==================== 综合战法自选盯盘 ====================

    _watchlist_tables_checked = False

    async def _ensure_watchlist_tables(self):
        """确保自选盯盘相关表存在"""
        if self._watchlist_tables_checked:
            return
        pool = await self._get_pool()
        sql_path = Path(__file__).parent / "database" / "combined_watchlist_tables.sql"
        if sql_path.exists():
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    sql_text = sql_path.read_text(encoding="utf-8")
                    for stmt in sql_text.split(";"):
                        stmt = stmt.strip()
                        if stmt and not stmt.startswith("--"):
                            try:
                                await cur.execute(stmt)
                            except Exception as e:
                                if "already exists" not in str(e).lower():
                                    logger.warning("Watchlist DDL warning", error=str(e))
        self._watchlist_tables_checked = True

    async def add_watchlist_item(
        self,
        stock_code: str,
        stock_name: str,
        buy_price: float,
        buy_shares: int,
        strategies: list,
        strategy_names: list,
        overlap_count: int = 1,
        source_date: str = "",
        source_session: str = "",
        note: str = "",
        user_id: str = "admin",
    ) -> Optional[int]:
        """加入自选"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        buy_amount = round(buy_price * buy_shares, 2)
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 检查是否已存在持仓中的同一只股票
                    await cur.execute(
                        "SELECT id FROM combined_watchlist WHERE user_id=%s AND stock_code=%s AND status=1",
                        (user_id, stock_code),
                    )
                    existing = await cur.fetchone()
                    if existing:
                        # 更新
                        await cur.execute("""
                            UPDATE combined_watchlist SET
                                buy_price=%s, buy_shares=%s, buy_amount=%s,
                                strategies=%s, strategy_names=%s, overlap_count=%s,
                                source_date=%s, source_session=%s, note=%s,
                                updated_at=NOW()
                            WHERE id=%s
                        """, (
                            buy_price, buy_shares, buy_amount,
                            json.dumps(strategies, ensure_ascii=False),
                            json.dumps(strategy_names, ensure_ascii=False),
                            overlap_count, source_date, source_session, note,
                            existing[0],
                        ))
                        logger.info("Watchlist item updated", id=existing[0], code=stock_code)
                        return existing[0]
                    else:
                        await cur.execute("""
                            INSERT INTO combined_watchlist
                            (user_id, stock_code, stock_name, buy_price, buy_shares, buy_amount,
                             strategies, strategy_names, overlap_count, source_date, source_session, note)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            user_id, stock_code, stock_name, buy_price, buy_shares, buy_amount,
                            json.dumps(strategies, ensure_ascii=False),
                            json.dumps(strategy_names, ensure_ascii=False),
                            overlap_count, source_date, source_session, note,
                        ))
                        record_id = cur.lastrowid
                        logger.info("Watchlist item added", id=record_id, code=stock_code)
                        return record_id
        except Exception as e:
            logger.error("Failed to add watchlist item", code=stock_code, error=str(e))
            return None

    async def get_watchlist(self, user_id: str = "admin", status: int = 1) -> List[Dict]:
        """获取自选列表"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("""
                        SELECT * FROM combined_watchlist
                        WHERE user_id=%s AND status=%s
                        ORDER BY updated_at DESC
                    """, (user_id, status))
                    rows = await cur.fetchall()
                    result = []
                    for row in rows:
                        item = self._serialize_row(row)
                        for jf in ("strategies", "strategy_names"):
                            if isinstance(item.get(jf), str):
                                try:
                                    item[jf] = json.loads(item[jf])
                                except Exception:
                                    pass
                        result.append(item)
                    return result
        except Exception as e:
            logger.error("Failed to get watchlist", error=str(e))
            return []

    async def remove_watchlist_item(self, item_id: int, user_id: str = "admin") -> bool:
        """移除自选（软删除）"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE combined_watchlist SET status=0 WHERE id=%s AND user_id=%s",
                        (item_id, user_id),
                    )
                    return cur.rowcount > 0
        except Exception as e:
            logger.error("Failed to remove watchlist item", error=str(e))
            return False

    async def close_watchlist_item(self, item_id: int, user_id: str = "admin") -> bool:
        """标记清仓"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE combined_watchlist SET status=2 WHERE id=%s AND user_id=%s",
                        (item_id, user_id),
                    )
                    return cur.rowcount > 0
        except Exception as e:
            logger.error("Failed to close watchlist item", error=str(e))
            return False

    async def get_watchlist_item(self, item_id: int) -> Optional[Dict]:
        """获取单条自选记录"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT * FROM combined_watchlist WHERE id=%s", (item_id,))
                    row = await cur.fetchone()
                    if not row:
                        return None
                    item = self._serialize_row(row)
                    for jf in ("strategies", "strategy_names"):
                        if isinstance(item.get(jf), str):
                            try:
                                item[jf] = json.loads(item[jf])
                            except Exception:
                                pass
                    return item
        except Exception as e:
            logger.error("Failed to get watchlist item", error=str(e))
            return None

    async def save_watchlist_guidance(
        self,
        watchlist_id: int,
        user_id: str,
        stock_code: str,
        stock_name: str,
        current_price: float,
        change_pct: float,
        pnl_amount: float,
        pnl_pct: float,
        strategy_analyses: list,
        overall_decision: str,
        overall_summary: str,
        trading_session: str = "",
    ) -> Optional[int]:
        """保存操作指导记录"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        INSERT INTO combined_watchlist_guidance
                        (watchlist_id, user_id, stock_code, stock_name,
                         current_price, change_pct, pnl_amount, pnl_pct,
                         strategy_analyses, overall_decision, overall_summary,
                         trading_session)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        watchlist_id, user_id, stock_code, stock_name,
                        current_price, change_pct, pnl_amount, pnl_pct,
                        json.dumps(strategy_analyses, ensure_ascii=False, default=str),
                        overall_decision, overall_summary, trading_session,
                    ))
                    record_id = cur.lastrowid
                    logger.info("Watchlist guidance saved", id=record_id, code=stock_code)
                    return record_id
        except Exception as e:
            logger.error("Failed to save watchlist guidance", error=str(e))
            return None

    async def get_watchlist_guidance(
        self,
        watchlist_id: Optional[int] = None,
        stock_code: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict]:
        """获取操作指导历史"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if watchlist_id:
                        await cur.execute("""
                            SELECT * FROM combined_watchlist_guidance
                            WHERE watchlist_id=%s ORDER BY guidance_time DESC LIMIT %s
                        """, (watchlist_id, limit))
                    elif stock_code:
                        await cur.execute("""
                            SELECT * FROM combined_watchlist_guidance
                            WHERE stock_code=%s ORDER BY guidance_time DESC LIMIT %s
                        """, (stock_code, limit))
                    else:
                        await cur.execute("""
                            SELECT * FROM combined_watchlist_guidance
                            ORDER BY guidance_time DESC LIMIT %s
                        """, (limit,))
                    rows = await cur.fetchall()
                    result = []
                    for row in rows:
                        item = self._serialize_row(row)
                        if isinstance(item.get("strategy_analyses"), str):
                            try:
                                item["strategy_analyses"] = json.loads(item["strategy_analyses"])
                            except Exception:
                                pass
                        result.append(item)
                    return result
        except Exception as e:
            logger.error("Failed to get watchlist guidance", error=str(e))
            return []

    async def get_latest_guidance_for_all(self, user_id: str = "admin") -> List[Dict]:
        """获取所有自选的最新一轮指导"""
        await self._ensure_watchlist_tables()
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("""
                        SELECT g.* FROM combined_watchlist_guidance g
                        INNER JOIN (
                            SELECT watchlist_id, MAX(guidance_time) as max_time
                            FROM combined_watchlist_guidance
                            WHERE user_id=%s
                            GROUP BY watchlist_id
                        ) latest ON g.watchlist_id = latest.watchlist_id
                            AND g.guidance_time = latest.max_time
                        ORDER BY g.guidance_time DESC
                    """, (user_id,))
                    rows = await cur.fetchall()
                    result = []
                    for row in rows:
                        item = self._serialize_row(row)
                        if isinstance(item.get("strategy_analyses"), str):
                            try:
                                item["strategy_analyses"] = json.loads(item["strategy_analyses"])
                            except Exception:
                                pass
                        result.append(item)
                    return result
        except Exception as e:
            logger.error("Failed to get latest guidance", error=str(e))
            return []


# 全局单例
portfolio_repo = PortfolioRepository()

