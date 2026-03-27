"""
模拟交易核心管理模块 (Portfolio Manager)

整合策略推荐 �?GPT资金分配 �?模拟买卖 �?收益计算的完整流程：
1. 创建组合 �?获取推荐 �?GPT分配 �?模拟建仓
2. 每日调仓 �?获取最新推�?�?GPT决策 �?执行买卖 �?汇总收�?
3. 更新持仓价格 �?计算浮动盈亏
"""

import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()

# ==================== 东方财富反爬补丁 ====================
# akshare 使用 requests 访问东财API，云服务器IP无浏览器UA会被拒绝
# �?个层级同时注入UA，确保无论akshare怎么调用都能生效
import requests
from requests.adapters import HTTPAdapter

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
_BROWSER_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Referer": "https://www.eastmoney.com/",
}

# 层级1: 补丁 HTTPAdapter.send �?网络请求前的最后一道关�?
_orig_adapter_send = HTTPAdapter.send


def _patched_adapter_send(self, request, *args, **kwargs):
    request.headers.update(_BROWSER_HEADERS)
    return _orig_adapter_send(self, request, *args, **kwargs)


HTTPAdapter.send = _patched_adapter_send

# 层级2: 补丁 Session.request
_orig_session_request = requests.Session.request


def _patched_session_request(self, method, url, **kwargs):
    self.headers.update(_BROWSER_HEADERS)
    return _orig_session_request(self, method, url, **kwargs)


requests.Session.request = _patched_session_request

# 层级3: 补丁 Session.__init__
_orig_session_init = requests.Session.__init__


def _patched_session_init(self, *args, **kwargs):
    _orig_session_init(self, *args, **kwargs)
    self.headers.update(_BROWSER_HEADERS)


requests.Session.__init__ = _patched_session_init

logger.info("Patched requests at 3 levels (adapter/request/init) with browser UA")

# 北京时区
try:
    from zoneinfo import ZoneInfo
    _BEIJING_TZ = ZoneInfo("Asia/Shanghai")
except ImportError:
    _BEIJING_TZ = timezone(timedelta(hours=8))

def _beijing_now() -> datetime:
    return datetime.now(_BEIJING_TZ)

# 每个组合最多持有的股票数量
MAX_POSITIONS = 5


class PortfolioManager:
    """模拟交易组合管理�?""

    def __init__(self):
        from portfolio_repository import portfolio_repo
        from portfolio_llm import portfolio_llm
        self.repo = portfolio_repo
        self.llm = portfolio_llm

    # ==================== 1. 创建组合并建�?====================

    async def create_portfolio(
        self,
        strategy_type: str,
        name: str,
        initial_capital: float,
        user_id: int = None,
    ) -> Dict[str, Any]:
        """
        创建新组合，获取推荐股票，GPT分配资金，模拟建�?

        Args:
            strategy_type: dragon_head / sentiment
            name: 组合名称
            initial_capital: 初始资金
            user_id: 所属用户ID

        Returns:
            组合信息 + 建仓结果
        """
        # 1. 创建组合记录
        portfolio_id = await self.repo.create_portfolio(
            strategy_type, name, initial_capital, user_id=user_id
        )
        if not portfolio_id:
            return {"status": "error", "message": "创建组合失败"}

        # 2. 获取当前推荐股票
        stocks = await self._get_recommendations(strategy_type)
        if not stocks:
            return {
                "status": "warning",
                "message": "创建成功但暂无推荐股票，等待下次定时任务分配",
                "portfolio_id": portfolio_id,
            }

        # 3. GPT-5.2 分配资金
        allocation = await self.llm.allocate_initial_capital(
            stocks, initial_capital, strategy_type
        )

        # 3.5 获取实时行情（确保用真实市场价格建仓�?
        try:
            import asyncio
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes
            realtime_df = await asyncio.to_thread(get_realtime_quotes)
        except Exception as e:
            logger.warning("Failed to fetch realtime quotes for init",
                            error=str(e))
            realtime_df = None

        # 4. 模拟建仓（最多持�?MAX_POSITIONS 只）
        trades_made = 0
        total_invested = 0
        allocations = allocation.get("allocations", [])[:MAX_POSITIONS]
        for alloc in allocations:
            code = alloc["stock_code"]
            name_s = alloc["stock_name"]
            qty = alloc["quantity"]

            # 价格优先�? 实时行情 > 推荐缓存（避免用过时价格建仓�?
            price = self._find_realtime_price(realtime_df, code)
            if price <= 0:
                price = self._find_price(stocks, code)
                logger.warning("Using recommendation cache price for init (realtime unavailable)",
                                stock=code, price=price)

            if qty <= 0 or price <= 0:
                continue

            # 已达持仓上限
            if trades_made >= MAX_POSITIONS:
                logger.info("Max positions reached during init",
                            max=MAX_POSITIONS, trades_made=trades_made)
                break

            cost = price * qty
            if cost > (initial_capital - total_invested):
                # 资金不足，调整股�?
                qty = int((initial_capital - total_invested) / price / 100) * 100
                if qty <= 0:
                    continue
                cost = price * qty

            # 记录买入交易
            await self.repo.save_trade(
                portfolio_id=portfolio_id,
                stock_code=code,
                stock_name=name_s,
                direction="buy",
                price=price,
                quantity=qty,
                reason=alloc.get("reason", "初始建仓"),
            )

            # 更新持仓
            weight = alloc.get("weight", 0)
            await self.repo.upsert_position(
                portfolio_id=portfolio_id,
                stock_code=code,
                stock_name=name_s,
                quantity=qty,
                avg_cost=price,
                current_price=price,
                weight=weight,
            )

            total_invested += cost
            trades_made += 1

        # 5. 更新组合资产
        available_cash = initial_capital - total_invested
        await self.repo.update_portfolio_assets(
            portfolio_id=portfolio_id,
            available_cash=available_cash,
            total_asset=initial_capital,  # 建仓后总资产不�?
            total_profit=0,
            total_profit_pct=0,
        )

        # 6. 保存当日汇�?
        today = _beijing_now().strftime("%Y-%m-%d")
        await self.repo.save_daily_summary(
            portfolio_id=portfolio_id,
            trading_date=today,
            total_asset=initial_capital,
            available_cash=available_cash,
            market_value=total_invested,
            daily_profit=0,
            daily_profit_pct=0,
            total_profit=0,
            total_profit_pct=0,
            position_count=trades_made,
            trade_count=trades_made,
        )

        portfolio = await self.repo.get_portfolio(portfolio_id)
        positions = await self.repo.get_positions(portfolio_id)

        return {
            "status": "success",
            "message": f"组合创建成功，买入{trades_made}只股�?,
            "portfolio": portfolio,
            "positions": positions,
            "allocation_summary": allocation.get("summary", ""),
        }

    # ==================== 2. 定时调仓（核心） ====================

    async def run_daily_rebalance(self, portfolio_id: int) -> Dict[str, Any]:
        """
        每日调仓流程（由定时任务调用）：
        1. 更新持仓最新价�?
        2. 获取最新推�?
        3. GPT决策买入/卖出
        4. 执行交易
        5. 汇总收�?
        """
        portfolio = await self.repo.get_portfolio(portfolio_id)
        if not portfolio:
            return {"status": "error", "message": f"组合{portfolio_id}不存�?}

        if portfolio.get("auto_trade") != 1:
            return {"status": "skipped", "message": "自动交易已暂�?}

        strategy_type = portfolio["strategy_type"]
        today = _beijing_now().strftime("%Y-%m-%d")

        try:
            # 1. 获取全市场实时行情（供后续定价使用）
            import asyncio
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes

            realtime_df = await asyncio.to_thread(get_realtime_quotes)

            # 1.1 更新持仓最新价
            positions = await self.repo.get_positions(portfolio_id)
            positions = await self._update_positions_price_with_df(
                portfolio_id, positions, realtime_df
            )

            # 1.5 合并相同股票持仓 + 自动卖出超出限制的多余持�?
            positions, excess_sell_count = await self._merge_and_trim_positions(
                portfolio_id, positions, today
            )

            # 1.6 硬性止损：亏损超过5%的持仓强制卖出（不依赖GPT判断�?
            available_cash_pre = float(portfolio.get("available_cash", 0))
            positions, stop_loss_count, available_cash_pre = await self._hard_stop_loss(
                portfolio_id, positions, today, available_cash_pre
            )
            if stop_loss_count > 0:
                # 更新现金到数据库
                await self.repo.update_cash(portfolio_id, available_cash_pre)
                # 重新获取组合信息（现金已变更�?
                portfolio = await self.repo.get_portfolio(portfolio_id)

            # 2. 获取最新推�?
            new_recs = await self._get_recommendations(strategy_type)

            # 2.5 获取前一日复盘（传给GPT，让交易决策参考之前的教训�?
            last_review = await self.repo.get_latest_review(portfolio_id)

            # 2.6 T+1约束：获取当天已买入的股票代码，这些股票今天不能卖出
            all_trades_today = await self.repo.get_trades_paginated(
                portfolio_id, limit=100, offset=0
            )
            today_bought_codes = set()
            for t in all_trades_today:
                trade_dt = str(t.get("trade_date", ""))
                if trade_dt.startswith(today) and t.get("direction") == "buy":
                    today_bought_codes.add(t.get("stock_code", ""))

            if today_bought_codes:
                logger.info("T+1 constraint: today bought stocks",
                            codes=list(today_bought_codes))

            # 3. GPT决策（传入持仓上限信�?+ 前日复盘 + 当天已买入股票列表）
            decisions = await self.llm.make_trading_decisions(
                positions=positions,
                new_recommendations=new_recs,
                portfolio_info=portfolio,
                strategy_type=strategy_type,
                max_positions=MAX_POSITIONS,
                last_review=last_review,
                today_bought_codes=today_bought_codes,
            )

            # 4. 执行交易（先卖后买，确保持仓不超限）
            trade_count = 0
            available_cash = float(portfolio.get("available_cash", 0))

            # 分离卖出和买入决策，先执行卖出以释放仓位
            sell_decisions = [d for d in decisions.get("decisions", []) if d.get("action") == "sell"]
            buy_decisions = [d for d in decisions.get("decisions", []) if d.get("action") == "buy"]
            ordered_decisions = sell_decisions + buy_decisions

            # 跟踪当前实际持仓股票数量
            current_holding_codes = set(
                p["stock_code"] for p in positions if p.get("quantity", 0) > 0
            )

            for decision in ordered_decisions:
                action = decision.get("action", "hold")
                code = decision.get("stock_code", "")
                name_s = decision.get("stock_name", "")
                qty = decision.get("quantity", 0)
                reason = decision.get("reason", "")

                if action == "hold" or qty <= 0:
                    continue

                # 价格优先�? 实时行情 > 持仓当前�?
                # 注意：不再fallback到推荐缓存价，推荐价可能严重过时
                price = self._find_realtime_price(realtime_df, code)
                if price <= 0:
                    price = self._find_current_price(positions, code)
                if price <= 0 and action == "sell":
                    # 卖出时可用推荐价兜底（已持有的股票需要退出）
                    price = self._find_price(new_recs, code)
                if price <= 0:
                    logger.warning("Cannot find valid realtime price, skip trade",
                                   stock=code, action=action)
                    continue


                if action == "sell":
                    # 查找持仓
                    held = self._find_position(positions, code)
                    if not held:
                        continue

                    # === T+1 硬性约束：不允许卖出当天买入的股票 ===
                    if code in today_bought_codes:
                        logger.info("T+1 blocked: cannot sell today's purchase",
                                    stock=code, name=name_s)
                        continue

                    sell_qty = min(qty, held.get("quantity", 0))
                    if sell_qty <= 0:
                        continue

                    avg_cost = float(held.get("avg_cost", 0))
                    profit = (price - avg_cost) * sell_qty
                    profit_pct = ((price - avg_cost) / avg_cost * 100) if avg_cost > 0 else 0

                    await self.repo.save_trade(
                        portfolio_id=portfolio_id,
                        stock_code=code, stock_name=name_s,
                        direction="sell", price=price, quantity=sell_qty,
                        reason=reason, profit=profit, profit_pct=profit_pct,
                        trade_date=today,
                    )

                    remaining = held["quantity"] - sell_qty
                    await self.repo.upsert_position(
                        portfolio_id=portfolio_id,
                        stock_code=code, stock_name=name_s,
                        quantity=remaining, avg_cost=avg_cost,
                        current_price=price, weight=0,
                    )

                    available_cash += price * sell_qty
                    trade_count += 1
                    # 更新持仓跟踪：卖完则移除
                    if remaining <= 0:
                        current_holding_codes.discard(code)

                elif action == "buy":
                    # 检查是否已有持仓（加仓不算新增持仓�?
                    held = self._find_position(positions, code)
                    is_new_position = not (held and held["quantity"] > 0)

                    # 新建仓位时检查持仓上�?
                    if is_new_position and len(current_holding_codes) >= MAX_POSITIONS:
                        logger.info("Max positions reached, skip new buy",
                                    stock=code, current=len(current_holding_codes),
                                    max=MAX_POSITIONS)
                        continue

                    cost = price * qty
                    if cost > available_cash:
                        qty = int(available_cash / price / 100) * 100
                        cost = price * qty
                    if qty <= 0:
                        continue

                    if not is_new_position:
                        # 加仓 �?计算新均�?
                        old_qty = held["quantity"]
                        old_cost = float(held["avg_cost"])
                        new_qty = old_qty + qty
                        new_avg = (old_cost * old_qty + price * qty) / new_qty
                        await self.repo.upsert_position(
                            portfolio_id=portfolio_id,
                            stock_code=code, stock_name=name_s,
                            quantity=new_qty, avg_cost=new_avg,
                            current_price=price, weight=0,
                        )
                    else:
                        await self.repo.upsert_position(
                            portfolio_id=portfolio_id,
                            stock_code=code, stock_name=name_s,
                            quantity=qty, avg_cost=price,
                            current_price=price, weight=0,
                        )
                        # 新增持仓，加入跟�?
                        current_holding_codes.add(code)

                    await self.repo.save_trade(
                        portfolio_id=portfolio_id,
                        stock_code=code, stock_name=name_s,
                        direction="buy", price=price, quantity=qty,
                        reason=reason, trade_date=today,
                    )

                    available_cash -= cost
                    trade_count += 1

            # 5. 重新计算资产
            positions = await self.repo.get_positions(portfolio_id)
            total_market = sum(float(p.get("market_value", 0)) for p in positions)
            total_asset = available_cash + total_market
            initial_cap = float(portfolio.get("initial_capital", 100000))
            total_profit = total_asset - initial_cap
            total_profit_pct = (total_profit / initial_cap * 100) if initial_cap > 0 else 0

            # 计算当日收益 (与前一日汇总比�?
            prev_summary = await self.repo.get_performance(portfolio_id, days=2)
            prev_asset = initial_cap
            if prev_summary and len(prev_summary) > 0:
                prev_asset = float(prev_summary[-1].get("total_asset", initial_cap))
            daily_profit = total_asset - prev_asset
            daily_profit_pct = (daily_profit / prev_asset * 100) if prev_asset > 0 else 0

            # 更新组合
            await self.repo.update_portfolio_assets(
                portfolio_id=portfolio_id,
                available_cash=available_cash,
                total_asset=total_asset,
                total_profit=total_profit,
                total_profit_pct=total_profit_pct,
            )

            # 更新仓位权重
            for p in positions:
                if total_asset > 0:
                    weight = float(p.get("market_value", 0)) / total_asset * 100
                    await self.repo.upsert_position(
                        portfolio_id=portfolio_id,
                        stock_code=p["stock_code"],
                        stock_name=p.get("stock_name", ""),
                        quantity=p["quantity"],
                        avg_cost=float(p["avg_cost"]),
                        current_price=float(p["current_price"]),
                        weight=weight,
                    )

            # 保存当日汇�?
            await self.repo.save_daily_summary(
                portfolio_id=portfolio_id,
                trading_date=today,
                total_asset=total_asset,
                available_cash=available_cash,
                market_value=total_market,
                daily_profit=daily_profit,
                daily_profit_pct=daily_profit_pct,
                total_profit=total_profit,
                total_profit_pct=total_profit_pct,
                position_count=len(positions),
                trade_count=trade_count,
            )

            logger.info("Daily rebalance completed",
                         portfolio_id=portfolio_id,
                         trades=trade_count,
                         total_asset=total_asset,
                         profit_pct=total_profit_pct)

            return {
                "status": "success",
                "portfolio_id": portfolio_id,
                "trade_count": trade_count,
                "total_asset": total_asset,
                "total_profit_pct": round(total_profit_pct, 2),
                "daily_profit_pct": round(daily_profit_pct, 2),
                "decision_summary": decisions.get("summary", ""),
            }

        except Exception as e:
            logger.error("Daily rebalance failed",
                         portfolio_id=portfolio_id,
                         error=str(e),
                         traceback=traceback.format_exc())
            return {"status": "error", "message": str(e)}

    # ==================== 3. 手动结算（仅更新价格+计算收益�?====================

    async def settle_daily(
        self,
        portfolio_id: int,
        trading_date: str = None,
        realtime_df=None,
    ) -> Dict[str, Any]:
        """
        手动结算：仅更新持仓价格、计算收益、生成每日汇总�?
        不触�?GPT 交易决策，不执行买卖�?

        适用场景�?
        - 定时任务未执行导致某天收益数据缺�?
        - 手动补算历史某天的收�?

        Args:
            portfolio_id: 组合ID
            trading_date: 结算日期 YYYY-MM-DD，默认今�?
            realtime_df: 预获取的行情DataFrame（多组合结算时共享，避免重复调API�?
        """
        portfolio = await self.repo.get_portfolio(portfolio_id)
        if not portfolio:
            return {"status": "error", "message": f"组合{portfolio_id}不存�?}

        if not trading_date:
            trading_date = _beijing_now().strftime("%Y-%m-%d")

        try:
            # 1. 获取持仓
            positions = await self.repo.get_positions(portfolio_id)
            if not positions:
                return {
                    "status": "warning",
                    "message": "暂无持仓，无需结算",
                    "portfolio_id": portfolio_id,
                }

            # 2. 获取持仓股票的最�?收盘价（复用已有数据或重新获取）
            price_updated = 0
            price_update_error = None
            try:
                df = realtime_df
                if df is None or (hasattr(df, 'empty') and df.empty):
                    import asyncio
                    try:
                        from market_data_provider import get_realtime_quotes
                    except ImportError:
                        from app.market_data_provider import get_realtime_quotes
                    df = await asyncio.to_thread(get_realtime_quotes)
                if df is None or df.empty:
                    raise RuntimeError("所有数据源均失�?)
                for p in positions:
                    code = p.get("stock_code", "")
                    matched = df[df["代码"] == code]
                    if not matched.empty:
                        price = float(matched.iloc[0].get("最新价", 0))
                        if price > 0:
                            p["current_price"] = price
                            p["market_value"] = price * p.get("quantity", 0)
                            avg_cost = float(p.get("avg_cost", 0))
                            if avg_cost > 0:
                                p["profit"] = (price - avg_cost) * p.get("quantity", 0)
                                p["profit_pct"] = (price - avg_cost) / avg_cost * 100
                            await self.repo.update_position_price(
                                portfolio_id, code, price
                            )
                            price_updated += 1
                            logger.info("Price updated",
                                        stock=code, price=price,
                                        name=p.get("stock_name", ""))
                    else:
                        logger.warning("Stock not found in market data",
                                       stock_code=code)
            except ImportError:
                price_update_error = "market_data_provider 未安装，无法获取最新价�?
                logger.error(price_update_error)
            except Exception as e:
                price_update_error = f"获取行情数据失败: {str(e)}"
                logger.error("Failed to fetch prices from akshare",
                             error=str(e))

            if price_updated == 0 and price_update_error:
                return {
                    "status": "error",
                    "message": f"结算失败 �?{price_update_error}",
                    "portfolio_id": portfolio_id,
                }

            logger.info("Price update summary",
                        portfolio_id=portfolio_id,
                        total_positions=len(positions),
                        updated=price_updated)

            # 3. 合并重复持仓
            positions, _ = await self._merge_and_trim_positions(
                portfolio_id, positions, trading_date
            )

            # 4. 重新计算资产
            available_cash = float(portfolio.get("available_cash", 0))
            total_market = sum(
                float(p.get("current_price", 0)) * p.get("quantity", 0)
                for p in positions
                if p.get("quantity", 0) > 0
            )
            total_asset = available_cash + total_market
            initial_cap = float(portfolio.get("initial_capital", 100000))
            total_profit = total_asset - initial_cap
            total_profit_pct = (total_profit / initial_cap * 100) if initial_cap > 0 else 0

            # 5. 计算当日收益（与前一日汇总比较）
            prev_summary = await self.repo.get_performance(portfolio_id, days=2)
            prev_asset = initial_cap
            if prev_summary and len(prev_summary) > 0:
                # 找到不是当天的最近一条汇�?
                for ps in reversed(prev_summary):
                    if ps.get("trading_date") != trading_date:
                        prev_asset = float(ps.get("total_asset", initial_cap))
                        break
            daily_profit = total_asset - prev_asset
            daily_profit_pct = (daily_profit / prev_asset * 100) if prev_asset > 0 else 0

            # 6. 更新组合资产
            await self.repo.update_portfolio_assets(
                portfolio_id=portfolio_id,
                available_cash=available_cash,
                total_asset=total_asset,
                total_profit=total_profit,
                total_profit_pct=total_profit_pct,
            )

            # 7. 更新仓位权重
            for p in positions:
                qty = p.get("quantity", 0)
                if qty > 0 and total_asset > 0:
                    mv = float(p.get("current_price", 0)) * qty
                    weight = mv / total_asset * 100
                    await self.repo.upsert_position(
                        portfolio_id=portfolio_id,
                        stock_code=p["stock_code"],
                        stock_name=p.get("stock_name", ""),
                        quantity=qty,
                        avg_cost=float(p.get("avg_cost", 0)),
                        current_price=float(p.get("current_price", 0)),
                        weight=weight,
                    )

            # 8. 获取当日交易笔数
            all_trades = await self.repo.get_trades(portfolio_id, limit=200)
            trade_count = sum(
                1 for t in all_trades if t.get("trade_date") == trading_date
            )

            # 9. 保存每日汇�?
            position_count = len([p for p in positions if p.get("quantity", 0) > 0])
            await self.repo.save_daily_summary(
                portfolio_id=portfolio_id,
                trading_date=trading_date,
                total_asset=total_asset,
                available_cash=available_cash,
                market_value=total_market,
                daily_profit=daily_profit,
                daily_profit_pct=daily_profit_pct,
                total_profit=total_profit,
                total_profit_pct=total_profit_pct,
                position_count=position_count,
                trade_count=trade_count,
            )

            logger.info("Daily settlement completed",
                        portfolio_id=portfolio_id,
                        trading_date=trading_date,
                        total_asset=round(total_asset, 2),
                        total_profit=round(total_profit, 2),
                        total_profit_pct=round(total_profit_pct, 2),
                        daily_profit=round(daily_profit, 2))

            return {
                "status": "success",
                "message": f"结算完成 [{trading_date}]，更新{price_updated}只股票价�?,
                "portfolio_id": portfolio_id,
                "trading_date": trading_date,
                "total_asset": round(total_asset, 2),
                "total_profit": round(total_profit, 2),
                "total_profit_pct": round(total_profit_pct, 2),
                "daily_profit": round(daily_profit, 2),
                "daily_profit_pct": round(daily_profit_pct, 2),
                "position_count": position_count,
                "price_updated_count": price_updated,
            }

        except Exception as e:
            logger.error("Daily settlement failed",
                         portfolio_id=portfolio_id,
                         trading_date=trading_date,
                         error=str(e),
                         traceback=traceback.format_exc())
            return {"status": "error", "message": str(e)}

    # ==================== 辅助方法 ====================

    # ==================== 硬性止损（代码层面强制执行�?====================

    async def _hard_stop_loss(
        self, portfolio_id: int, positions: List[Dict],
        today: str, available_cash: float,
        stop_loss_pct: float = -5.0,
    ) -> tuple:
        """
        硬性止损：亏损超过 stop_loss_pct 的持仓强制卖出（不依赖GPT判断）�?
        �?GPT 交易决策之前执行，确保风控不会被 GPT 忽略�?

        Args:
            stop_loss_pct: 止损阈值，默认 -5%（即亏损5%强制止损�?

        Returns:
            (updated_positions, stop_loss_count, updated_available_cash)
        """
        # T+1约束：获取当天已买入的股�?
        today_bought_codes = set()
        try:
            all_today_trades = await self.repo.get_trades_paginated(
                portfolio_id, limit=100, offset=0
            )
            for t in all_today_trades:
                trade_dt = str(t.get("trade_date", ""))
                if trade_dt.startswith(today) and t.get("direction") == "buy":
                    today_bought_codes.add(t.get("stock_code", ""))
        except Exception:
            pass

        stop_loss_count = 0
        remaining_positions = []

        for pos in positions:
            code = pos.get("stock_code", "")
            qty = pos.get("quantity", 0)
            avg_cost = float(pos.get("avg_cost", 0))
            current_price = float(pos.get("current_price", 0))

            if qty <= 0 or avg_cost <= 0 or current_price <= 0:
                remaining_positions.append(pos)
                continue

            profit_pct = (current_price - avg_cost) / avg_cost * 100

            if profit_pct <= stop_loss_pct:
                # T+1检�?
                if code in today_bought_codes:
                    logger.info("Hard stop-loss blocked by T+1",
                                stock=code, name=pos.get("stock_name", ""),
                                profit_pct=round(profit_pct, 2))
                    remaining_positions.append(pos)
                    continue

                # 强制卖出
                profit = (current_price - avg_cost) * qty
                name = pos.get("stock_name", "")

                await self.repo.save_trade(
                    portfolio_id=portfolio_id,
                    stock_code=code, stock_name=name,
                    direction="sell", price=current_price, quantity=qty,
                    reason=f"硬性止�? 亏损{profit_pct:.1f}%已超过{stop_loss_pct}%阈�?,
                    profit=round(profit, 2),
                    profit_pct=round(profit_pct, 2),
                    trade_date=today,
                )
                await self.repo.upsert_position(
                    portfolio_id=portfolio_id,
                    stock_code=code, stock_name=name,
                    quantity=0, avg_cost=avg_cost,
                    current_price=current_price, weight=0,
                )

                available_cash += current_price * qty
                stop_loss_count += 1

                logger.warning("Hard stop-loss executed",
                               stock=code, name=name,
                               profit_pct=round(profit_pct, 2),
                               loss_amount=round(profit, 2))
            else:
                remaining_positions.append(pos)

        if stop_loss_count > 0:
            logger.info("Hard stop-loss summary",
                         portfolio_id=portfolio_id,
                         stocks_sold=stop_loss_count)

        return remaining_positions, stop_loss_count, available_cash

    async def _merge_and_trim_positions(
        self, portfolio_id: int, positions: List[Dict], today: str
    ) -> tuple:
        """
        合并相同股票持仓 + 卖出超出 MAX_POSITIONS 的多余持�?

        关键改进：从 portfolio_trade 重新计算 avg_cost�?
        确保买入成本始终基于真实成交价，而非可能被污染的持仓数据�?

        Returns:
            (updated_positions, excess_sell_count)
        """
        # ── �?步：合并相同股票 ──
        merged = {}
        for pos in positions:
            code = pos.get("stock_code", "")
            qty = pos.get("quantity", 0)
            if qty <= 0:
                continue
            if code in merged:
                old = merged[code]
                old_qty = old["quantity"]
                old_cost = float(old.get("avg_cost", 0))
                new_qty = old_qty + qty
                new_cost = (old_cost * old_qty + float(pos.get("avg_cost", 0)) * qty) / new_qty if new_qty > 0 else 0
                old["quantity"] = new_qty
                old["avg_cost"] = new_cost
                old["current_price"] = pos.get("current_price", old.get("current_price", 0))
            else:
                merged[code] = dict(pos)

        active_count = len([p for p in positions if p.get("quantity", 0) > 0])

        # ── �?.5步：从交易记录重新计算每只股票的真实 avg_cost ──
        # portfolio_trade 是唯一的事实来�?
        try:
            all_trades = await self.repo.get_trades_by_portfolio(portfolio_id)
            if all_trades:
                # 按股票分组，模拟 FIFO 计算真实均价
                from collections import defaultdict
                trade_by_stock = defaultdict(list)
                for t in all_trades:
                    trade_by_stock[t.get("stock_code", "")].append(t)

                for code, pos in merged.items():
                    stock_trades = trade_by_stock.get(code, [])
                    if not stock_trades:
                        continue

                    calc_qty = 0
                    calc_total_cost = 0.0

                    for t in stock_trades:
                        direction = t.get("direction", "")
                        t_price = float(t.get("price", 0))
                        t_qty = int(t.get("quantity", 0))

                        if direction == "buy":
                            calc_total_cost += t_price * t_qty
                            calc_qty += t_qty
                        elif direction == "sell":
                            if calc_qty > 0:
                                sell_qty = min(t_qty, calc_qty)
                                avg = calc_total_cost / calc_qty
                                calc_total_cost -= avg * sell_qty
                                calc_qty -= sell_qty

                    if calc_qty > 0:
                        correct_avg_cost = calc_total_cost / calc_qty
                        old_avg_cost = float(pos.get("avg_cost", 0))
                        # 如果有偏差，用交易记录计算出的正确值覆�?
                        if old_avg_cost > 0 and abs(correct_avg_cost - old_avg_cost) / old_avg_cost > 0.005:
                            logger.info("avg_cost corrected from trades",
                                         stock=code,
                                         old_cost=round(old_avg_cost, 4),
                                         new_cost=round(correct_avg_cost, 4),
                                         portfolio_id=portfolio_id)
                        pos["avg_cost"] = round(correct_avg_cost, 4)

        except Exception as e:
            logger.warning("Failed to recalculate avg_cost from trades, using position data",
                            error=str(e))

        # 始终执行：先删除所�?holding 行，再重新插入合并后的持�?
        # 这样可以彻底清除重复�?
        if active_count > 0:
            logger.info("Rebuilding positions (delete+reinsert)",
                         portfolio_id=portfolio_id,
                         raw_rows=active_count,
                         merged_stocks=len(merged))
            # 先删掉所�?holding 记录
            await self.repo.delete_holding_positions(portfolio_id)
            # 再逐条重新插入合并后的持仓
            for code, pos in merged.items():
                await self.repo.upsert_position(
                    portfolio_id=portfolio_id,
                    stock_code=code,
                    stock_name=pos.get("stock_name", ""),
                    quantity=pos["quantity"],
                    avg_cost=pos["avg_cost"],
                    current_price=float(pos.get("current_price", 0)),
                    weight=0,
                )

        merged_positions = list(merged.values())

        # ── �?步：卖出超出 MAX_POSITIONS 的多余持�?──
        excess_count = 0
        if len(merged_positions) > MAX_POSITIONS:
            # 按盈亏比排序，卖出亏损最大的
            for pos in merged_positions:
                cost = float(pos.get("avg_cost", 0))
                price = float(pos.get("current_price", 0))
                pos["_profit_pct"] = ((price - cost) / cost * 100) if cost > 0 else 0

            sorted_positions = sorted(merged_positions, key=lambda p: p["_profit_pct"])
            to_sell = sorted_positions[:len(merged_positions) - MAX_POSITIONS]

            portfolio = await self.repo.get_portfolio(portfolio_id)
            available_cash = float(portfolio.get("available_cash", 0)) if portfolio else 0

            # T+1 约束：获取当天已买入的股票，自动减仓也不能卖出当天买入的
            today_bought_in_trim = set()
            try:
                all_today_trades = await self.repo.get_trades_paginated(
                    portfolio_id, limit=100, offset=0
                )
                for t in all_today_trades:
                    trade_dt = str(t.get("trade_date", ""))
                    if trade_dt.startswith(today) and t.get("direction") == "buy":
                        today_bought_in_trim.add(t.get("stock_code", ""))
            except Exception:
                pass

            for pos in to_sell:
                code = pos["stock_code"]
                name = pos.get("stock_name", "")
                qty = pos["quantity"]
                price = float(pos.get("current_price", 0))
                avg_cost = float(pos.get("avg_cost", 0))
                if price <= 0 or qty <= 0:
                    continue

                # T+1: 不卖出当天买入的股票
                if code in today_bought_in_trim:
                    logger.info("T+1 skip auto-trim for today's purchase",
                                stock=code, name=name)
                    continue

                profit = (price - avg_cost) * qty
                profit_pct = pos["_profit_pct"]

                await self.repo.save_trade(
                    portfolio_id=portfolio_id,
                    stock_code=code, stock_name=name,
                    direction="sell", quantity=qty,
                    price=price,
                    profit=round(profit, 2),
                    profit_pct=round(profit_pct, 2),
                    trade_date=today,
                    reason=f"持仓超{MAX_POSITIONS}只上限，自动减仓",
                )
                await self.repo.upsert_position(
                    portfolio_id=portfolio_id,
                    stock_code=code, stock_name=name,
                    quantity=0, avg_cost=avg_cost,
                    current_price=price, weight=0,
                )
                available_cash += price * qty
                excess_count += 1
                logger.info("Auto-sold excess position",
                             stock=code, name=name, qty=qty,
                             profit_pct=round(profit_pct, 2))

            # 更新现金
            if excess_count > 0:
                await self.repo.update_cash(portfolio_id, available_cash)

            # 刷新持仓
            merged_positions = [p for p in merged_positions if p not in to_sell]

        return merged_positions, excess_count

    async def _get_recommendations(self, strategy_type: str) -> List[Dict]:
        """
        获取策略推荐（从预生成的缓存中读取）

        推荐在每�?10:30 �?13:30 由定时任务预生成�?
        调仓时直接读取缓存，不再实时调用策略�?
        """
        try:
            cached = await self.repo.get_recommendation_cache(strategy_type)
            if cached:
                logger.info("Using cached recommendations",
                            strategy=strategy_type, count=len(cached))
                return cached

            # 缓存为空（首次运行或缓存失效），降级为实时生�?
            logger.warning("No cached recommendations, fallback to realtime",
                           strategy=strategy_type)
            return await self._realtime_recommendations(strategy_type)

        except Exception as e:
            logger.error("Failed to get recommendations",
                         strategy=strategy_type, error=str(e))
            return []

    async def _realtime_recommendations(self, strategy_type: str) -> List[Dict]:
        """实时获取推荐（仅在缓存未就绪时作为降级方案）"""
        try:
            if strategy_type == "dragon_head":
                try:
                    from dragon_head_strategy import dragon_head_strategy
                except ImportError:
                    from app.dragon_head_strategy import dragon_head_strategy
                result = await dragon_head_strategy.get_recommendations(limit=10)
                return result.get("data", {}).get("recommendations", [])

            elif strategy_type == "sentiment":
                try:
                    from sentiment_strategy import sentiment_strategy
                except ImportError:
                    from app.sentiment_strategy import sentiment_strategy

                sentiment = await sentiment_strategy.get_sentiment(days=7)
                data = sentiment.get("data", {})
                stock_picks = data.get("stock_picks", {})
                return stock_picks.get("stocks", [])

            elif strategy_type in ("breakthrough", "volume_price", "overnight", "moving_average", "northbound", "trend_momentum"):
                strategy_map = {
                    "breakthrough": ("breakthrough_strategy", "breakthrough_strategy"),
                    "volume_price": ("volume_price_strategy", "volume_price_strategy"),
                    "overnight": ("overnight_strategy", "overnight_strategy"),
                    "moving_average": ("moving_average_strategy", "moving_average_strategy"),
                    "northbound": ("northbound_strategy", "northbound_strategy"),
                    "trend_momentum": ("trend_momentum_strategy", "trend_momentum_strategy"),
                }
                mod_name, obj_name = strategy_map[strategy_type]
                try:
                    mod = __import__(mod_name)
                except ImportError:
                    mod = __import__(f"api.{mod_name}", fromlist=[obj_name])
                strategy_obj = getattr(mod, obj_name)
                result = await strategy_obj.get_recommendations(limit=10)
                return result.get("data", {}).get("recommendations", [])

            return []
        except Exception as e:
            logger.error("Realtime recommendations failed",
                         strategy=strategy_type, error=str(e))
            return []

    async def generate_recommendations(
        self,
        strategy_type: str,
        session_type: str = "morning",
        limit: int = MAX_POSITIONS,
    ) -> List[Dict]:
        """
        生成策略推荐并写入缓存（由定时任务调用）

        Args:
            strategy_type: dragon_head / sentiment / event_driven
            session_type: morning / afternoon
            limit: 推荐股票数量，默�?MAX_POSITIONS (13) �?

        Returns:
            推荐股票列表
        """
        logger.info("Generating recommendations",
                     strategy=strategy_type, session=session_type, limit=limit)

        stocks = []
        try:
            if strategy_type == "dragon_head":
                try:
                    from dragon_head_strategy import dragon_head_strategy
                except ImportError:
                    from app.dragon_head_strategy import dragon_head_strategy

                result = await dragon_head_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "sentiment":
                try:
                    from sentiment_strategy import sentiment_strategy
                except ImportError:
                    from app.sentiment_strategy import sentiment_strategy

                sentiment = await sentiment_strategy.get_sentiment(days=7)
                data = sentiment.get("data", {})
                stock_picks = data.get("stock_picks", {})
                stocks = stock_picks.get("stocks", [])

            elif strategy_type == "event_driven":
                try:
                    from event_driven_strategy import event_driven_strategy
                except ImportError:
                    from app.event_driven_strategy import event_driven_strategy

                result = await event_driven_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "breakthrough":
                try:
                    from breakthrough_strategy import breakthrough_strategy
                except ImportError:
                    from app.breakthrough_strategy import breakthrough_strategy

                result = await breakthrough_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "volume_price":
                try:
                    from volume_price_strategy import volume_price_strategy
                except ImportError:
                    from app.volume_price_strategy import volume_price_strategy

                result = await volume_price_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "overnight":
                try:
                    from overnight_strategy import overnight_strategy
                except ImportError:
                    from app.overnight_strategy import overnight_strategy

                result = await overnight_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "moving_average":
                try:
                    from moving_average_strategy import moving_average_strategy
                except ImportError:
                    from app.moving_average_strategy import moving_average_strategy

                result = await moving_average_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "combined":
                # 综合战法：从七种战法推荐缓存中取交集
                # 含实时行情获�?+ LLM操作指导分析
                try:
                    from combined_strategy import combined_strategy
                except ImportError:
                    from app.combined_strategy import combined_strategy

                result = await combined_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "northbound":
                try:
                    from northbound_strategy import northbound_strategy
                except ImportError:
                    from app.northbound_strategy import northbound_strategy

                result = await northbound_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

            elif strategy_type == "trend_momentum":
                try:
                    from trend_momentum_strategy import trend_momentum_strategy
                except ImportError:
                    from app.trend_momentum_strategy import trend_momentum_strategy

                result = await trend_momentum_strategy.get_recommendations(limit=limit)
                stocks = result.get("data", {}).get("recommendations", [])

        except Exception as e:
            logger.error("Strategy recommendation generation failed",
                         strategy=strategy_type, error=str(e),
                         traceback=traceback.format_exc())
            return []

        # 只保留最具投资效益的�?limit �?
        stocks = stocks[:limit]

        if stocks:
            await self.repo.save_recommendation_cache(
                strategy_type=strategy_type,
                session_type=session_type,
                recommendations=stocks,
            )
            logger.info("Recommendations generated and cached",
                        strategy=strategy_type, session=session_type,
                        count=len(stocks))
        else:
            logger.warning("No stocks recommended by strategy",
                           strategy=strategy_type, session=session_type)

        return stocks

    async def _update_positions_price(self, portfolio_id: int,
                                      positions: List[Dict]) -> List[Dict]:
        """通过多源数据更新持仓最新价"""
        try:
            import asyncio
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes

            df = await asyncio.to_thread(get_realtime_quotes)
            return await self._update_positions_price_with_df(
                portfolio_id, positions, df
            )

        except ImportError:
            logger.warning("market_data_provider not available, using last known prices")
        except Exception as e:
            logger.error("Failed to update prices", error=str(e))

        return positions

    async def _update_positions_price_with_df(
        self, portfolio_id: int, positions: List[Dict],
        df: "pd.DataFrame"
    ) -> List[Dict]:
        """使用已获取的实时行情 DataFrame 更新持仓最新价"""
        if df is None or df.empty:
            logger.warning("Realtime data is empty, using last known prices")
            return positions

        codes = [p["stock_code"] for p in positions if p.get("stock_code")]
        if not codes:
            return positions

        for p in positions:
            code = p.get("stock_code", "")
            matched = df[df["代码"] == code]
            if not matched.empty:
                price = float(matched.iloc[0].get("最新价", 0))
                if price > 0:
                    p["current_price"] = price
                    p["market_value"] = price * p.get("quantity", 0)
                    avg_cost = float(p.get("avg_cost", 0))
                    if avg_cost > 0:
                        p["profit"] = (price - avg_cost) * p.get("quantity", 0)
                        p["profit_pct"] = (price - avg_cost) / avg_cost * 100

                    try:
                        await self.repo.update_position_price(
                            portfolio_id, code, price
                        )
                    except Exception as e:
                        logger.error("Failed to persist price update",
                                     stock=code, error=str(e))

        return positions

    def _find_realtime_price(self, df: "pd.DataFrame", code: str) -> float:
        """从实时行�?DataFrame 查找股票当前�?""
        try:
            if df is None or df.empty:
                return 0
            matched = df[df["代码"] == code]
            if not matched.empty:
                price = float(matched.iloc[0].get("最新价", 0))
                return price if price > 0 else 0
        except Exception:
            pass
        return 0

    def _find_price(self, stocks: List[Dict], code: str) -> float:
        """从推荐列表找股票价格"""
        for s in stocks:
            s_code = s.get("code", s.get("stock_code", ""))
            if s_code == code:
                return float(s.get("price", s.get("current_price", 0)))
        return 0

    def _find_current_price(self, positions: List[Dict], code: str) -> float:
        """从持仓列表找当前�?""
        for p in positions:
            if p.get("stock_code") == code:
                return float(p.get("current_price", 0))
        return 0

    def _find_position(self, positions: List[Dict], code: str) -> Optional[Dict]:
        """查找持仓"""
        for p in positions:
            if p.get("stock_code") == code and p.get("quantity", 0) > 0:
                return p
        return None

    # ==================== 每日复盘 ====================

    async def generate_daily_review(self, portfolio_id: int) -> Dict[str, Any]:
        """
        生成每日复盘报告（由 16:00 定时任务或手动触发调用）

        流程�?
        1. 获取当日交易记录
        2. 获取当前持仓状况
        3. 获取组合信息
        4. GPT-5.2 分析操作优劣
        5. 存入 portfolio_review �?
        """
        portfolio = await self.repo.get_portfolio(portfolio_id)
        if not portfolio:
            return {"status": "error", "message": f"组合{portfolio_id}不存�?}

        strategy_type = portfolio["strategy_type"]
        today = _beijing_now().strftime("%Y-%m-%d")

        try:
            # 获取当日交易记录
            all_trades = await self.repo.get_trades(portfolio_id, limit=200)
            today_trades = [t for t in all_trades if t.get("trade_date") == today]

            # 获取当前持仓
            positions = await self.repo.get_positions(portfolio_id)

            # 统计
            buy_count = sum(1 for t in today_trades if t.get("direction") == "buy")
            sell_count = sum(1 for t in today_trades if t.get("direction") == "sell")
            trade_count = len(today_trades)

            # 当日收益 �?始终�?daily_summary 获取，避免用累计收益当日收益�?bug
            initial = float(portfolio.get("initial_capital", 100000))
            daily_profit = 0.0
            daily_profit_pct = 0.0

            summaries = await self.repo.get_performance(portfolio_id, days=2)
            if summaries:
                s = summaries[-1]  # 最近一天的汇�?
                daily_profit = float(s.get("daily_profit", daily_profit))
                daily_profit_pct = float(s.get("daily_profit_pct", daily_profit_pct))

            # GPT-5.2 复盘分析
            review = await self._gpt_review(
                portfolio=portfolio,
                positions=positions,
                today_trades=today_trades,
                strategy_type=strategy_type,
                daily_profit=daily_profit,
                daily_profit_pct=daily_profit_pct,
            )

            # 保存复盘
            await self.repo.save_review(
                portfolio_id=portfolio_id,
                strategy_type=strategy_type,
                trading_date=today,
                trade_count=trade_count,
                buy_count=buy_count,
                sell_count=sell_count,
                daily_profit=daily_profit,
                daily_profit_pct=daily_profit_pct,
                overall_score=review.get("overall_score", 50),
                overall_comment=review.get("overall_comment", ""),
                highlights=review.get("highlights", []),
                shortcomings=review.get("shortcomings", []),
                lessons=review.get("lessons", []),
                suggestions=review.get("suggestions", []),
                position_analysis=review.get("position_analysis"),
                risk_assessment=review.get("risk_assessment"),
                buy_analysis=review.get("buy_analysis"),
                sell_analysis=review.get("sell_analysis"),
                improvement_steps=review.get("improvement_steps"),
                next_day_actions=review.get("next_day_actions"),
                strategy_effectiveness=review.get("strategy_effectiveness"),
            )

            return {
                "status": "success",
                "portfolio_id": portfolio_id,
                "trading_date": today,
                "overall_score": review.get("overall_score", 50),
                "summary": review.get("overall_comment", ""),
            }

        except Exception as e:
            logger.error("Generate daily review failed",
                         portfolio_id=portfolio_id, error=str(e),
                         traceback=traceback.format_exc())
            return {"status": "error", "message": str(e)}

    async def _gpt_review(
        self,
        portfolio: Dict,
        positions: List[Dict],
        today_trades: List[Dict],
        strategy_type: str,
        daily_profit: float,
        daily_profit_pct: float,
    ) -> Dict:
        """调用 GPT-5.2 进行每日复盘分析（增强版�?""
        strategy_names = {
            "dragon_head": "龙头战法",
            "sentiment": "情绪战法",
            "event_driven": "事件驱动",
            "breakthrough": "突破战法",
            "volume_price": "量价关系",
            "overnight": "隔夜施工�?,
            "moving_average": "均线战法",
            "combined": "综合战法",
        }
        strategy_name = strategy_names.get(strategy_type, strategy_type)

        # 构建持仓摘要
        pos_summary = []
        for p in positions:
            pos_summary.append(
                f"  {p.get('stock_code')} {p.get('stock_name')} "
                f"持仓{p.get('quantity')}�?成本{float(p.get('avg_cost', 0)):.2f} "
                f"现价{float(p.get('current_price', 0)):.2f} "
                f"盈亏{float(p.get('profit', 0)):.2f}({float(p.get('profit_pct', 0)):.2f}%)"
            )

        # 构建交易摘要（分买入/卖出�?
        buy_trades = [t for t in today_trades if t.get("direction") == "buy"]
        sell_trades = [t for t in today_trades if t.get("direction") == "sell"]

        buy_summary = []
        for t in buy_trades:
            profit_info = ""
            if t.get("profit"):
                profit_info = f" 收益:{float(t.get('profit', 0)):.2f}�?
            buy_summary.append(
                f"  买入 {t.get('stock_code')} {t.get('stock_name')} "
                f"{t.get('quantity')}�?@ {float(t.get('price', 0)):.2f}�?"
                f"金额:{float(t.get('amount', 0)):.2f}�?"
                f"理由: {t.get('reason', '未知')}{profit_info}"
            )

        sell_summary = []
        for t in sell_trades:
            profit_info = ""
            if t.get("profit"):
                profit_info = f" 收益:{float(t.get('profit', 0)):.2f}�?
            sell_summary.append(
                f"  卖出 {t.get('stock_code')} {t.get('stock_name')} "
                f"{t.get('quantity')}�?@ {float(t.get('price', 0)):.2f}�?"
                f"金额:{float(t.get('amount', 0)):.2f}�?"
                f"理由: {t.get('reason', '未知')}{profit_info}"
            )

        # 计算持仓集中�?
        total_mv = sum(float(p.get('market_value', 0)) for p in positions)
        top3_mv = sum(float(p.get('market_value', 0)) for p in sorted(
            positions, key=lambda x: float(x.get('market_value', 0)), reverse=True
        )[:3])
        concentration = (top3_mv / total_mv * 100) if total_mv > 0 else 0

        # 盈利/亏损持仓统计
        profitable = [p for p in positions if float(p.get('profit_pct', 0)) > 0]
        losing = [p for p in positions if float(p.get('profit_pct', 0)) < 0]

        prompt = (
            f"{strategy_name}模拟交易复盘\n"
            f"初始:{portfolio.get('initial_capital')}�?"
            f"总资�?{portfolio.get('total_asset')}�?"
            f"现金:{portfolio.get('available_cash')}�?"
            f"累计收益:{portfolio.get('total_profit_pct')}%\n"
            f"当日收益:{daily_profit:.2f}�?{daily_profit_pct:.2f}%)\n"
            f"买入({len(buy_trades)}�?:\n"
            f"{chr(10).join(buy_summary) if buy_summary else '  �?}\n"
            f"卖出({len(sell_trades)}�?:\n"
            f"{chr(10).join(sell_summary) if sell_summary else '  �?}\n"
            f"持仓({len(positions)}�?盈{len(profitable)}亏{len(losing)},集中度{concentration:.0f}%):\n"
            f"{chr(10).join(pos_summary) if pos_summary else '  空仓'}\n\n"
            f"返回JSON(不要```标记):\n"
            f'{{"overall_score":1-100,"overall_comment":"100字总评",'
            f'"buy_analysis":[{{"stock_code":"","stock_name":"","buy_price":0,"quantity":0,'
            f'"is_good_decision":true,"analysis":"分析","improvement":"改进"}}],'
            f'"sell_analysis":[{{"stock_code":"","stock_name":"","sell_price":0,"quantity":0,'
            f'"profit":0,"is_good_decision":true,"analysis":"","improvement":""}}],'
            f'"highlights":["亮点"],"shortcomings":["不足"],"lessons":["教训"],'
            f'"improvement_steps":[{{"area":"领域","action":"措施"}}],'
            f'"next_day_actions":["明日操作"],"suggestions":["建议"],'
            f'"position_analysis":[{{"stock_code":"","stock_name":"","analysis":"","action_suggestion":""}}],'
            f'"risk_assessment":"风险评估",'
            f'"strategy_effectiveness":{{"score":0,"comment":"策略评价"}}}}'
        )

        try:
            messages = [
                {"role": "system", "content": (
                    f"A股{strategy_name}交易复盘分析师，逐笔分析买卖优劣，给出可执行改进。只返回JSON�?
                )},
                {"role": "user", "content": prompt}
            ]
            response = await self.llm._chat(messages, temperature=0.4, max_tokens=6000)

            # 解析 JSON
            review = self.llm._parse_json_response(response)
            if review:
                return review
        except Exception as e:
            logger.error("GPT review failed", error=str(e))

        # 降级：生成基本复�?
        return {
            "overall_score": 50,
            "overall_comment": f"今日{strategy_name}共执行{len(today_trades)}笔交易，"
                               f"买入{len(buy_trades)}笔，卖出{len(sell_trades)}笔，"
                               f"当日收益{daily_profit:.2f}�?,
            "buy_analysis": [],
            "sell_analysis": [],
            "highlights": ["系统正常运行"] if today_trades else ["维持现有持仓"],
            "shortcomings": ["需要更多数据积累以评估策略表现"],
            "lessons": ["保持纪律性交�?],
            "improvement_steps": [],
            "next_day_actions": ["继续观察市场走势"],
            "suggestions": ["继续观察市场走势"],
            "position_analysis": None,
            "risk_assessment": "需要更多交易数据以进行全面风险评估",
            "strategy_effectiveness": {"score": 50, "comment": "需积累更多数据"},
        }

    # ==================== 跟投建议 ====================

    async def generate_follow_recommendations(
        self, portfolio_id: int, session_type: str = "morning"
    ) -> Dict[str, Any]:
        """
        生成跟投建议：在交易完成�?分钟调用

        流程:
        1. 获取组合信息和当前持�?
        2. 获取当日交易记录
        3. 获取最新推�?
        4. GPT 生成5只跟投建�?
        5. 存入数据�?
        """
        portfolio = await self.repo.get_portfolio(portfolio_id)
        if not portfolio:
            return {"status": "error", "message": f"组合{portfolio_id}不存�?}

        strategy_type = portfolio["strategy_type"]
        today = _beijing_now().strftime("%Y-%m-%d")

        try:
            # 1. 获取当前持仓
            positions = await self.repo.get_positions(portfolio_id)

            # 2. 获取当日交易记录
            all_trades = await self.repo.get_trades_paginated(portfolio_id, limit=50, offset=0)
            recent_trades = [
                t for t in all_trades
                if t.get("trade_date") == today or str(t.get("trade_date", "")).startswith(today)
            ]

            # 3. 获取最新推�?
            new_recs = await self._get_recommendations(strategy_type)

            # 4. GPT 生成跟投建议
            result = await self.llm.generate_follow_recommendations(
                positions=positions,
                recent_trades=recent_trades,
                new_recommendations=new_recs,
                portfolio_info=portfolio,
                strategy_type=strategy_type,
                session_type=session_type,
            )

            stocks = result.get("stocks", [])

            # 5. 存入数据�?
            await self.repo.save_follow_recommendation(
                portfolio_id=portfolio_id,
                strategy_type=strategy_type,
                trading_date=today,
                session_type=session_type,
                recommendations=stocks,
                stock_count=len(stocks),
                market_overview=result.get("market_overview", ""),
                strategy_summary=result.get("strategy_summary", ""),
                risk_warning=result.get("risk_warning", ""),
                confidence_score=result.get("confidence_score", 0),
            )

            logger.info("Follow recommendations saved",
                        portfolio_id=portfolio_id,
                        strategy=strategy_type,
                        session=session_type,
                        stock_count=len(stocks))

            return {
                "status": "success",
                "portfolio_id": portfolio_id,
                "strategy_type": strategy_type,
                "session_type": session_type,
                "stock_count": len(stocks),
                "stocks": stocks,
                "market_overview": result.get("market_overview", ""),
                "confidence_score": result.get("confidence_score", 0),
            }

        except Exception as e:
            logger.error("Generate follow recommendations failed",
                         portfolio_id=portfolio_id,
                         error=str(e), traceback=traceback.format_exc())
            return {"status": "error", "message": str(e)}


# 全局单例
portfolio_manager = PortfolioManager()
