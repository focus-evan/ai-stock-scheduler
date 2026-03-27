"""Database connection pool management using pymysql."""
from __future__ import annotations

import os
from typing import Optional
import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager
import structlog
from dotenv import load_dotenv

load_dotenv()

logger = structlog.get_logger()

# 数据库连接池配置
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 's_ai_agent'),
    'charset': 'utf8mb4',
    'cursorclass': DictCursor,
    'autocommit': False,
}

# Druid连接池配置映射到pymysql
POOL_CONFIG = {
    'max_connections': int(os.getenv('MYSQL_MAX_ACTIVE', '100')),
    'connect_timeout': int(os.getenv('MYSQL_MAX_WAIT', '30000')) // 1000,  # 转换为秒
}

# 全局连接池
_connection_pool: Optional[pymysql.connections.Connection] = None


def init_db_pool():
    """初始化数据库连接池"""
    global _connection_pool
    try:
        logger.info("Initializing database connection pool", config=DB_CONFIG)
        # PyMySQL不直接支持连接池，这里使用简单的连接管理
        # 生产环境建议使用 DBUtils.PooledDB 或 SQLAlchemy
        _connection_pool = pymysql.connect(**DB_CONFIG)
        logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize database connection pool", error=str(e))
        raise


def close_db_pool():
    """关闭数据库连接池"""
    global _connection_pool
    if _connection_pool:
        try:
            _connection_pool.close()
            logger.info("Database connection pool closed")
        except Exception as e:
            logger.error("Failed to close database connection pool", error=str(e))
        finally:
            _connection_pool = None


@contextmanager
def get_db_connection():
    """获取数据库连接的上下文管理器"""
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        yield connection
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error("Database operation failed", error=str(e))
        raise
    finally:
        if connection:
            connection.close()


def execute_query(sql: str, params: Optional[tuple] = None, fetch_one: bool = False):
    """执行查询SQL"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or ())
            if fetch_one:
                return cursor.fetchone()
            return cursor.fetchall()


def execute_update(sql: str, params: Optional[tuple] = None) -> int:
    """执行更新SQL，返回影响的行数"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            affected_rows = cursor.execute(sql, params or ())
            conn.commit()
            return affected_rows
