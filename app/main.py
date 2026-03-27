"""
AI Stock Scheduler — 独立定时任务调度服务

功能：
1. 自动定时任务：推荐生成、交易决策、跟投建议、结算、复盘
2. API 接口：供 ai-stock 转发手动触发请求
3. 监控端点：心跳检测、任务状态查询

部署方式：
  - 阿里云 ECS 独立部署
  - docker-compose up -d
  - 或直接 uvicorn app.main:app --host 0.0.0.0 --port 8001
"""

import sys
import os
from pathlib import Path

# 将 app 目录加入 Python path，兼容原有的 import 方式
app_dir = Path(__file__).parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

project_root = app_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, Query, Path as PathParam
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import structlog

load_dotenv()

logger = structlog.get_logger()

# ==================== FastAPI App ====================

app = FastAPI(
    title="AI Stock Scheduler",
    description="定时任务调度服务 — 独立部署，供 ai-stock 转发调用",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== Request Models ====================

class TriggerRecommendationRequest(BaseModel):
    session_type: str = Field("manual", description="morning / afternoon / manual")
    strategy_type: Optional[str] = Field(None, description="指定策略类型，为空则触发所有")
    limit: int = Field(10, description="推荐数量")


class TriggerTradeRequest(BaseModel):
    strategy_type: str = Field(..., description="策略类型")
    session_type: str = Field("manual", description="morning / afternoon / manual")


class TriggerSettlementRequest(BaseModel):
    portfolio_id: Optional[int] = Field(None, description="组合ID，为空则结算所有")
    trading_date: Optional[str] = Field(None, description="结算日期 YYYY-MM-DD")


# ==================== Health & Status ====================

@app.get("/health", summary="健康检查")
async def health_check():
    return {"status": "ok", "service": "ai-stock-scheduler"}


@app.get("/api/scheduler/status", summary="调度器状态")
async def get_scheduler_status():
    """获取调度器运行状态和今日任务完成情况"""
    try:
        from portfolio_scheduler import portfolio_scheduler, _beijing_now
        now = _beijing_now()
        today = now.strftime("%Y-%m-%d")

        done_count = sum(
            1 for attr in dir(portfolio_scheduler)
            if attr.startswith('_last_') and getattr(portfolio_scheduler, attr, None) == today
        )
        return {
            "status": "success",
            "data": {
                "running": portfolio_scheduler._running,
                "current_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "tasks_done_today": done_count,
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ==================== 推荐生成 ====================

@app.post("/api/scheduler/recommendations", summary="触发推荐生成")
async def trigger_recommendations(req: TriggerRecommendationRequest):
    """触发推荐生成（供 ai-stock 转发调用）"""
    try:
        from portfolio_scheduler import portfolio_scheduler
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler

    try:
        if req.strategy_type:
            # 单个策略
            await portfolio_scheduler._run_recommendation(req.strategy_type, req.session_type)
            return {
                "status": "success",
                "message": f"{req.strategy_type} 推荐生成完成",
                "strategy": req.strategy_type,
            }
        else:
            # 所有策略
            result = await portfolio_scheduler.trigger_recommendations(req.session_type)
            return result
    except Exception as e:
        logger.error("Trigger recommendations failed", error=str(e))
        raise HTTPException(500, str(e))


# ==================== 交易决策 ====================

@app.post("/api/scheduler/trade", summary="触发交易决策")
async def trigger_trade(req: TriggerTradeRequest):
    """触发指定策略的交易决策"""
    try:
        from portfolio_scheduler import portfolio_scheduler
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler

    try:
        result = await portfolio_scheduler.trigger_strategy_trade(req.strategy_type)
        return result
    except Exception as e:
        logger.error("Trigger trade failed", error=str(e))
        raise HTTPException(500, str(e))


@app.post("/api/scheduler/rebalance/{portfolio_id}", summary="触发调仓")
async def trigger_rebalance(portfolio_id: int = PathParam(...)):
    """手动触发某个组合的调仓"""
    try:
        from portfolio_scheduler import portfolio_scheduler
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler

    try:
        result = await portfolio_scheduler.trigger_rebalance(portfolio_id)
        return result
    except Exception as e:
        logger.error("Trigger rebalance failed", error=str(e))
        raise HTTPException(500, str(e))


# ==================== 跟投建议 ====================

@app.post("/api/scheduler/follow/{portfolio_id}", summary="触发跟投建议")
async def trigger_follow(
    portfolio_id: int = PathParam(...),
    session_type: str = Query("manual"),
):
    """手动触发生成跟投建议"""
    try:
        from portfolio_manager import portfolio_manager
    except ImportError:
        from app.portfolio_manager import portfolio_manager

    try:
        result = await portfolio_manager.generate_follow_recommendations(
            portfolio_id, session_type=session_type
        )
        return result
    except Exception as e:
        logger.error("Trigger follow failed", error=str(e))
        raise HTTPException(500, str(e))


# ==================== 结算 ====================

@app.post("/api/scheduler/settle", summary="触发结算")
async def trigger_settlement(req: TriggerSettlementRequest):
    """手动触发结算"""
    try:
        from portfolio_scheduler import portfolio_scheduler
        from portfolio_manager import portfolio_manager
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler
        from app.portfolio_manager import portfolio_manager

    try:
        if req.portfolio_id:
            result = await portfolio_manager.settle_daily(
                req.portfolio_id, trading_date=req.trading_date
            )
            return result
        else:
            await portfolio_scheduler._run_all_settlement()
            return {"status": "success", "message": "全部组合结算完成"}
    except Exception as e:
        logger.error("Trigger settlement failed", error=str(e))
        raise HTTPException(500, str(e))


# ==================== 复盘 ====================

@app.post("/api/scheduler/review/{portfolio_id}", summary="触发复盘")
async def trigger_review(portfolio_id: int = PathParam(...)):
    """手动触发每日复盘"""
    try:
        from portfolio_scheduler import portfolio_scheduler
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler

    try:
        result = await portfolio_scheduler.trigger_review(portfolio_id)
        return result
    except Exception as e:
        logger.error("Trigger review failed", error=str(e))
        raise HTTPException(500, str(e))


@app.post("/api/scheduler/review-all", summary="触发所有复盘")
async def trigger_review_all():
    """手动触发所有组合的复盘"""
    try:
        from portfolio_scheduler import portfolio_scheduler
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler

    try:
        await portfolio_scheduler._run_daily_review()
        return {"status": "success", "message": "全部组合复盘完成"}
    except Exception as e:
        logger.error("Trigger review all failed", error=str(e))
        raise HTTPException(500, str(e))


# ==================== 盯盘指导 ====================

@app.post("/api/scheduler/watchlist-guidance", summary="触发盯盘指导")
async def trigger_watchlist_guidance(
    trading_session: str = Query("manual", description="交易时段标签"),
):
    """手动触发自选盯盘操作指导"""
    try:
        from combined_watchlist_guidance import watchlist_guidance
    except ImportError:
        from app.combined_watchlist_guidance import watchlist_guidance

    try:
        count = await watchlist_guidance.generate_guidance_for_all(
            trading_session=trading_session
        )
        return {"status": "success", "count": count}
    except Exception as e:
        logger.error("Watchlist guidance failed", error=str(e))
        raise HTTPException(500, str(e))


# ==================== Startup & Shutdown ====================

@app.on_event("startup")
async def startup():
    """应用启动：初始化调度器"""
    logger.info("=" * 60)
    logger.info("AI Stock Scheduler starting...")
    logger.info("=" * 60)

    auto_start = os.getenv("SCHEDULER_AUTO_START", "true").lower() == "true"

    if auto_start:
        try:
            from portfolio_scheduler import portfolio_scheduler
        except ImportError:
            from app.portfolio_scheduler import portfolio_scheduler

        try:
            await portfolio_scheduler.start()
            logger.info("Portfolio scheduler started successfully")
        except Exception as e:
            logger.error("Portfolio scheduler failed to start", error=str(e))
    else:
        logger.info("Scheduler auto-start disabled, API-only mode")

    logger.info("AI Stock Scheduler ready")


@app.on_event("shutdown")
async def shutdown():
    """应用关闭：停止调度器"""
    try:
        from portfolio_scheduler import portfolio_scheduler
    except ImportError:
        from app.portfolio_scheduler import portfolio_scheduler

    try:
        await portfolio_scheduler.stop()
        logger.info("Portfolio scheduler stopped")
    except Exception as e:
        logger.error("Error stopping scheduler", error=str(e))
