"""
龙头战法（Dragon Head Strategy）核心逻辑模块

实现龙头战法的四大核心模块：
1. 识别主线题材 (Theme Identification) - 通过涨停效应倒推主线
2. 确定真龙头 (Dragon Head Selection) - 连板高度/封板时间/封单量排序
3. 新闻情绪共振 (Sentiment Resonance) - 新闻关键词与主线题材比对
4. 生成推荐逻辑说明 (Recommendation Logic)

数据源：AkShare (免费开源金融数据接口)
"""

import asyncio
import random
import traceback
import time as _time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import structlog

logger = structlog.get_logger()


class DragonHeadStrategy:
    """龙头战法核心策略类"""

    def __init__(self):
        self._ak = None  # 延迟导入 akshare
        self._cache: Dict[str, Any] = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=10)  # 缓存10分钟
        self._last_api_call: float = 0  # 上次API调用时间戳
        self._min_api_interval: float = 1.5  # 最小API调用间隔（秒）

    @property
    def ak(self):
        """延迟导入 akshare"""
        if self._ak is None:
            from app.akshare_client import ak_client as ak
            self._ak = ak
        return self._ak

    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if not self._cache or not self._cache_time:
            return False
        return datetime.now() - self._cache_time < self._cache_ttl

    def _api_delay(self, reason: str = ""):
        """反爬延迟：确保两次API调用之间有足够间隔，加随机抖动"""
        now = _time.time()
        elapsed = now - self._last_api_call
        if elapsed < self._min_api_interval:
            wait = self._min_api_interval - elapsed
            _time.sleep(wait)
        # 额外加 0.5~2.0 秒随机延迟，模拟人工操作
        jitter = random.uniform(0.5, 2.0)
        _time.sleep(jitter)
        self._last_api_call = _time.time()
        if reason:
            logger.debug("API delay applied", reason=reason, jitter=f"{jitter:.1f}s")

    async def get_recommendations(self, limit: int = 13) -> Dict[str, Any]:
        """
        获取龙头战法推荐列表

        流程: 算法筛选(模块1-3) → 规则打分(模块4) → GPT-5.2 深度分析(模块5)

        Args:
            limit: 返回推荐数量，默认20条

        Returns:
            包含推荐列表和策略说明的字典
        """
        # 检查缓存
        cache_key = f"dragon_head_{limit}"
        if self._is_cache_valid() and cache_key in self._cache:
            logger.info("Returning cached dragon head recommendations")
            return self._cache[cache_key]

        try:
            # 在线程池中执行同步的 akshare 调用
            loop = asyncio.get_event_loop()

            # 模块一：获取今日涨停股
            limit_up_stocks = await loop.run_in_executor(
                None, self._get_limit_up_stocks
            )

            # 模块一（续）：识别主线题材
            main_themes, theme_details = await loop.run_in_executor(
                None, self._identify_main_themes, limit_up_stocks
            )

            # 模块二：确定真龙头
            dragon_heads = await loop.run_in_executor(
                None, self._select_dragon_heads, limit_up_stocks, main_themes, limit
            )

            # 模块三：新闻情绪共振
            news_resonance = await loop.run_in_executor(
                None, self._check_news_resonance, main_themes
            )

            # 模块四：生成规则推荐结果（作为 LLM 的 fallback）
            result = self._build_recommendations(
                dragon_heads, main_themes, theme_details, news_resonance, limit
            )

            # ==================== 模块五：GPT-5.2 深度分析增强 ====================
            if dragon_heads:
                try:
                    from dragon_head_llm import dragon_head_llm
                except ImportError:
                    from app.dragon_head_llm import dragon_head_llm

                try:
                    logger.info("Starting GPT-5.2 enhancement for dragon head strategy")

                    limit_up_count = len(limit_up_stocks) if not limit_up_stocks.empty else 0

                    llm_result = await dragon_head_llm.enhance_recommendations(
                        dragon_heads=dragon_heads,
                        main_themes=main_themes,
                        theme_details=theme_details,
                        news_resonance=news_resonance,
                        limit_up_count=limit_up_count,
                    )

                    if llm_result:
                        # 用 LLM 增强的个股数据覆盖原始数据
                        if llm_result.get("enhanced_stocks"):
                            result["data"]["recommendations"] = llm_result["enhanced_stocks"][:limit]

                        # 用 LLM 生成的策略报告替换模板报告
                        if llm_result.get("strategy_report"):
                            result["data"]["strategy_explanation"] = llm_result["strategy_report"]

                        # 添加市场情绪判断
                        if llm_result.get("market_sentiment"):
                            result["data"]["market_sentiment"] = llm_result["market_sentiment"]

                        # 添加题材深度分析
                        if llm_result.get("theme_analysis"):
                            result["data"]["theme_analysis"] = llm_result["theme_analysis"]

                        # 标记为 LLM 增强
                        result["data"]["llm_enhanced"] = True
                        logger.info("GPT-5.2 enhancement applied successfully")
                    else:
                        result["data"]["llm_enhanced"] = False
                        logger.warning("GPT-5.2 returned empty result, using rule-based fallback")

                except Exception as llm_e:
                    logger.error("GPT-5.2 enhancement failed, using rule-based fallback",
                                 error=str(llm_e))
                    result["data"]["llm_enhanced"] = False
            else:
                result["data"]["llm_enhanced"] = False

            # ==================== 模块六：数据持久化入库 ====================
            try:
                try:
                    from dragon_head_repository import dragon_head_repo
                except ImportError:
                    from app.dragon_head_repository import dragon_head_repo

                limit_up_count = len(limit_up_stocks) if not limit_up_stocks.empty else 0

                # 保存涨停股快照（原始数据）
                if not limit_up_stocks.empty:
                    trading_date_str = result.get("data", {}).get(
                        "trading_date", datetime.now().strftime("%Y-%m-%d")
                    )
                    await dragon_head_repo.save_limit_up_snapshot(
                        trading_date=trading_date_str,
                        limit_up_stocks=limit_up_stocks,
                    )

                # 保存策略结果（批次 + 题材 + 个股 + 新闻共振）
                batch_id = await dragon_head_repo.save_strategy_result(
                    result=result,
                    limit_up_count=limit_up_count,
                )
                if batch_id:
                    logger.info("Dragon head data persisted to MySQL", batch_id=batch_id)

            except Exception as db_e:
                logger.error("Data persistence failed (non-blocking)",
                             error=str(db_e), traceback=traceback.format_exc())

            # 更新缓存
            self._cache[cache_key] = result
            self._cache_time = datetime.now()

            return result

        except Exception as e:
            logger.error("Dragon head strategy failed", error=str(e), traceback=traceback.format_exc())
            raise

    # ==================== 模块一：识别主线题材 ====================

    def _get_limit_up_stocks(self) -> pd.DataFrame:
        """
        获取今日涨停股数据

        使用多源降级方案：gateway → 新浪 → akshare，自动应对东财封禁
        """
        try:
            today_str = datetime.now().strftime("%Y%m%d")
            logger.info("Fetching limit up stocks (multi-source)", date=today_str)

            # 使用多源数据提供器（自动降级）
            try:
                from dragon_data_provider import get_limit_up_stocks
            except ImportError:
                from app.dragon_data_provider import get_limit_up_stocks

            df = get_limit_up_stocks(date_str=today_str)
            if df is not None and not df.empty:
                logger.info("Got limit up stocks (multi-source)", count=len(df))
                return df

            # 最终备选：使用实时行情数据模拟涨停筛选
            logger.warning("All limit-up sources failed, using realtime detection")
            return self._detect_limit_up_from_realtime()

        except Exception as e:
            logger.error("Failed to get limit up stocks", error=str(e))
            return pd.DataFrame()

    def _detect_limit_up_from_realtime(self) -> pd.DataFrame:
        """
        从实时行情中检测涨停股（备选方案）

        通过比较涨跌幅>=9.8%来近似判断涨停
        """
        try:
            # 获取A股实时行情（多源降级）
            try:
                from market_data_provider import get_realtime_quotes
            except ImportError:
                from app.market_data_provider import get_realtime_quotes
            df = get_realtime_quotes()
            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名映射
            col_map = {}
            for col in df.columns:
                col_lower = col.lower()
                if '代码' in col or 'code' in col_lower:
                    col_map[col] = '代码'
                elif '名称' in col or 'name' in col_lower:
                    col_map[col] = '名称'
                elif '涨跌幅' in col or 'change' in col_lower:
                    col_map[col] = '涨跌幅'
                elif '最新价' in col or 'price' in col_lower or '收盘' in col:
                    col_map[col] = '最新价'
                elif '成交额' in col or 'amount' in col_lower:
                    col_map[col] = '成交额'
                elif '流通市值' in col:
                    col_map[col] = '流通市值'
                elif '总市值' in col:
                    col_map[col] = '总市值'
                elif '换手率' in col:
                    col_map[col] = '换手率'

            df = df.rename(columns=col_map)

            # 筛选涨幅>=9.8%（近似涨停，考虑ST股5%和北交所30%的差异）
            if '涨跌幅' in df.columns:
                df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')
                limit_up = df[df['涨跌幅'] >= 9.8].copy()

                # 排除ST股（涨停限制为5%）
                if '名称' in limit_up.columns:
                    limit_up = limit_up[~limit_up['名称'].str.contains('ST', case=False, na=False)]

                logger.info("Detected limit up stocks from realtime", count=len(limit_up))
                return limit_up

            return pd.DataFrame()

        except Exception as e:
            logger.error("Failed to detect limit up from realtime", error=str(e))
            return pd.DataFrame()

    def _identify_main_themes(self, limit_up_stocks: pd.DataFrame) -> tuple:
        """
        识别今日主线题材

        通过获取涨停股对应的概念板块，统计涨停家数最多的概念
        """
        main_themes = []
        theme_details = {}

        if limit_up_stocks.empty:
            logger.warning("No limit up stocks, cannot identify themes")
            return main_themes, theme_details

        try:
            # 获取概念板块资金流向，找出当日领涨的概念
            # 添加重试逻辑，最多重试2次
            concept_df = None
            try:
                from dragon_data_provider import get_concept_boards
            except ImportError:
                from app.dragon_data_provider import get_concept_boards

            try:
                logger.info("Fetching concept board data (multi-source)")
                concept_df = get_concept_boards()
            except Exception as e:
                logger.warning("Multi-source concept board fetch failed", error=str(e))

            if concept_df is not None and not concept_df.empty:
                # 标准化列名（避免重复映射）
                col_map = {}
                mapped_targets = set()
                for col in concept_df.columns:
                    if ('板块名称' in col or '概念名称' in col) and '板块名称' not in mapped_targets:
                        col_map[col] = '板块名称'
                        mapped_targets.add('板块名称')
                    elif '涨跌幅' in col and '板块涨跌幅' not in mapped_targets:
                        col_map[col] = '板块涨跌幅'
                        mapped_targets.add('板块涨跌幅')
                    elif '上涨家数' in col and '上涨家数' not in mapped_targets:
                        col_map[col] = '上涨家数'
                        mapped_targets.add('上涨家数')
                    elif '下跌家数' in col and '下跌家数' not in mapped_targets:
                        col_map[col] = '下跌家数'
                        mapped_targets.add('下跌家数')

                concept_df = concept_df.rename(columns=col_map)

                if '板块涨跌幅' in concept_df.columns:
                    concept_df['板块涨跌幅'] = pd.to_numeric(concept_df['板块涨跌幅'], errors='coerce')
                    # 按板块涨跌幅排序，取前10个概念
                    concept_df = concept_df.sort_values('板块涨跌幅', ascending=False)
                    top_concepts = concept_df.head(10)

                    for _, row in top_concepts.iterrows():
                        theme_name = row.get('板块名称', '')
                        if theme_name:
                            main_themes.append(theme_name)
                            theme_details[theme_name] = {
                                "name": theme_name,
                                "change_pct": float(row.get('板块涨跌幅', 0)),
                                "up_count": int(row.get('上涨家数', 0)) if pd.notna(row.get('上涨家数')) else 0,
                                "down_count": int(row.get('下跌家数', 0)) if pd.notna(row.get('下跌家数')) else 0,
                            }

            # 取前3个主线题材
            main_themes = main_themes[:3]
            logger.info("Identified main themes", themes=main_themes)

        except Exception as e:
            logger.error("Failed to identify main themes", error=str(e))

        # 如果主接口失败，尝试备选方案
        if not main_themes:
            try:
                logger.info("Trying backup theme identification from limit-up stocks")
                main_themes, theme_details = self._identify_themes_from_stocks(limit_up_stocks)
            except Exception as e2:
                logger.error("Backup theme identification also failed", error=str(e2))

        return main_themes, theme_details

    def _identify_themes_from_stocks(self, limit_up_stocks: pd.DataFrame) -> tuple:
        """
        通过涨停股数据中的所属板块字段来识别主线（备选方案）

        该方法不再逐只查询API，而是直接从涨停股池数据中提取板块信息
        """
        theme_count = {}
        theme_details = {}

        # 涨停股池数据中通常包含 '所属行业' 列
        industry_col = None
        for col in limit_up_stocks.columns:
            if '所属行业' in col or '行业' in col:
                industry_col = col
                break

        if industry_col:
            # 按行业统计涨停家数
            industry_counts = limit_up_stocks[industry_col].value_counts()
            for industry_name, count in industry_counts.head(10).items():
                if industry_name and str(industry_name).strip():
                    theme_count[str(industry_name)] = int(count)
        else:
            # 如果没有行业列，尝试用涨停股代码逐个查询（限制数量）
            code_col = None
            for col in limit_up_stocks.columns:
                if '代码' in col or 'code' in str(col).lower():
                    code_col = col
                    break

            if code_col is None:
                return [], {}

            stocks_to_check = limit_up_stocks.head(5)  # 减少请求量，只查5只
            for _, row in stocks_to_check.iterrows():
                code = str(row[code_col])
                try:
                    # 使用多源概念查询（自动降级）
                    try:
                        from dragon_data_provider import get_stock_concepts
                    except ImportError:
                        from app.dragon_data_provider import get_stock_concepts

                    self._api_delay(f"concept_cons_{code}")
                    concept = get_stock_concepts(code)
                    if concept is not None and not concept.empty:
                        for _, c_row in concept.iterrows():
                            name_col = None
                            for c in c_row.index:
                                if '板块名称' in c or '概念' in c:
                                    name_col = c
                                    break
                            if name_col:
                                c_name = str(c_row[name_col])
                                theme_count[c_name] = theme_count.get(c_name, 0) + 1
                except Exception:
                    continue

        # 按出现次数排序
        sorted_themes = sorted(theme_count.items(), key=lambda x: x[1], reverse=True)
        main_themes = [t[0] for t in sorted_themes[:3]]

        for theme_name, count in sorted_themes[:10]:
            theme_details[theme_name] = {
                "name": theme_name,
                "limit_up_count": count,
                "change_pct": 0,
            }

        logger.info("Backup theme identification completed", themes=main_themes)
        return main_themes, theme_details

    # ==================== 模块二：确定真龙头 ====================

    def _select_dragon_heads(
        self,
        limit_up_stocks: pd.DataFrame,
        main_themes: List[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        从涨停股中筛选龙头

        排序逻辑（增强版）：
        1. 连续涨停天数（连板高度）- 降序
        2. 首次封板时间 - 升序（越早越好）
        3. 封单量/流通市值 - 降序
        4. 龙头质量评分 - 综合辨识度
        """
        dragon_heads = []

        if limit_up_stocks.empty:
            logger.warning("No limit up stocks for dragon head selection")
            return dragon_heads

        try:
            # 先记录原始列名用于调试
            logger.info("Limit up stocks columns", columns=list(limit_up_stocks.columns))

            # 标准化列名 - 防止多个源列映射到同一目标列名（导致 DataFrame 而非 Series）
            col_map = {}
            mapped_targets = set()  # 跟踪已映射的目标名，避免重复
            for col in limit_up_stocks.columns:
                if '代码' in col and 'code' not in mapped_targets:
                    col_map[col] = 'code'
                    mapped_targets.add('code')
                elif '名称' in col and 'name' not in mapped_targets:
                    col_map[col] = 'name'
                    mapped_targets.add('name')
                elif '涨跌幅' in col and 'change_pct' not in mapped_targets:
                    col_map[col] = 'change_pct'
                    mapped_targets.add('change_pct')
                elif ('最新价' in col or '收盘' in col) and 'price' not in mapped_targets:
                    col_map[col] = 'price'
                    mapped_targets.add('price')
                elif '成交额' in col and 'amount' not in mapped_targets:
                    col_map[col] = 'amount'
                    mapped_targets.add('amount')
                elif '流通市值' in col and 'float_market_cap' not in mapped_targets:
                    col_map[col] = 'float_market_cap'
                    mapped_targets.add('float_market_cap')
                elif '总市值' in col and 'total_market_cap' not in mapped_targets:
                    col_map[col] = 'total_market_cap'
                    mapped_targets.add('total_market_cap')
                elif '换手率' in col and 'turnover_rate' not in mapped_targets:
                    col_map[col] = 'turnover_rate'
                    mapped_targets.add('turnover_rate')
                elif '连板数' in col and 'limit_up_days' not in mapped_targets:
                    col_map[col] = 'limit_up_days'
                    mapped_targets.add('limit_up_days')
                elif '涨停统计' in col and 'zt_stat' not in mapped_targets:
                    col_map[col] = 'zt_stat'
                    mapped_targets.add('zt_stat')
                elif ('首次封板' in col or '封板时间' in col) and 'first_limit_time' not in mapped_targets:
                    col_map[col] = 'first_limit_time'
                    mapped_targets.add('first_limit_time')
                elif '封单' in col and 'seal_amount' not in mapped_targets:
                    col_map[col] = 'seal_amount'
                    mapped_targets.add('seal_amount')

            logger.info("Column mapping", col_map=col_map)
            df = limit_up_stocks.rename(columns=col_map)

            # 删除可能存在的重复列名（安全保障）
            df = df.loc[:, ~df.columns.duplicated(keep='first')]

            # 如果没有连板数字段，尝试从涨停统计字段解析
            if 'limit_up_days' not in df.columns and 'zt_stat' in df.columns:
                # 涨停统计格式为 '6/3' (涨停次数/天数)，取 '/' 前的数字
                def parse_zt_stat(val):
                    try:
                        s = str(val)
                        if '/' in s:
                            return int(s.split('/')[0])
                        return int(float(s))
                    except (ValueError, TypeError):
                        return 1
                df['limit_up_days'] = df['zt_stat'].apply(parse_zt_stat)
                logger.info("Parsed limit_up_days from zt_stat column")

            if 'limit_up_days' not in df.columns:
                df['limit_up_days'] = 1  # 默认1板

                # 尝试获取连板天数数据
                try:
                    # 尝试获取连板股数据
                    for days_back in range(0, 5):
                        try:
                            check_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
                            # 使用多源连板数据
                            try:
                                from dragon_data_provider import get_lianban_stocks
                            except ImportError:
                                from app.dragon_data_provider import get_lianban_stocks

                            lianban_df = get_lianban_stocks(date_str=check_date)
                            if lianban_df is not None and not lianban_df.empty:
                                # 映射连板数据
                                lb_code_col = None
                                lb_days_col = None
                                for c in lianban_df.columns:
                                    if '代码' in c and lb_code_col is None:
                                        lb_code_col = c
                                    if ('连板数' in c or '涨停统计' in c) and lb_days_col is None:
                                        lb_days_col = c

                                if lb_code_col and lb_days_col:
                                    lb_series = lianban_df[lb_days_col]
                                    # 确保是 Series 而非 DataFrame
                                    if isinstance(lb_series, pd.DataFrame):
                                        lb_series = lb_series.iloc[:, 0]
                                    lb_map = dict(zip(
                                        lianban_df[lb_code_col].astype(str),
                                        pd.to_numeric(lb_series, errors='coerce').fillna(1).astype(int)
                                    ))
                                    if 'code' in df.columns:
                                        df['limit_up_days'] = df['code'].astype(str).map(lb_map).fillna(1).astype(int)
                                break
                        except Exception:
                            continue
                except Exception as e:
                    logger.warning("Could not get lianban data", error=str(e))

            # 确保数值列为数值类型（逐列安全转换）
            for col in ['change_pct', 'price', 'amount', 'float_market_cap',
                         'total_market_cap', 'turnover_rate', 'limit_up_days']:
                if col in df.columns:
                    try:
                        col_data = df[col]
                        # 如果意外得到 DataFrame（重复列），取第一列
                        if isinstance(col_data, pd.DataFrame):
                            logger.warning("Duplicate column detected, taking first", column=col)
                            col_data = col_data.iloc[:, 0]
                        df[col] = pd.to_numeric(col_data, errors='coerce')
                    except Exception as conv_e:
                        logger.warning("Failed to convert column to numeric",
                                       column=col, error=str(conv_e))
                        df[col] = 0

            # ==================== 情绪周期估算 ====================
            limit_up_count = len(df)
            max_lianban = int(df['limit_up_days'].max()) if 'limit_up_days' in df.columns else 1

            if limit_up_count >= 50 and max_lianban >= 7:
                emotion_phase = "高潮期"
            elif limit_up_count >= 30 and max_lianban >= 4:
                emotion_phase = "上升期"
            elif limit_up_count >= 10 and max_lianban >= 2:
                emotion_phase = "启动期"
            else:
                emotion_phase = "退潮期"

            logger.info("Emotion cycle estimated",
                        phase=emotion_phase,
                        limit_up_count=limit_up_count,
                        max_lianban=max_lianban)

            # 排序：连板高度降序 -> 首次封板时间升序 -> 成交额降序
            sort_cols = []
            sort_ascending = []

            if 'limit_up_days' in df.columns:
                sort_cols.append('limit_up_days')
                sort_ascending.append(False)

            if 'first_limit_time' in df.columns:
                sort_cols.append('first_limit_time')
                sort_ascending.append(True)  # 封板越早越好

            if 'amount' in df.columns:
                sort_cols.append('amount')
                sort_ascending.append(False)
            elif 'change_pct' in df.columns:
                sort_cols.append('change_pct')
                sort_ascending.append(False)

            if sort_cols:
                df = df.sort_values(by=sort_cols, ascending=sort_ascending)

            # 取前 limit 条记录
            top_stocks = df.head(limit)

            for idx, (_, row) in enumerate(top_stocks.iterrows()):
                limit_days = int(row.get('limit_up_days', 1)) if pd.notna(row.get('limit_up_days')) else 1
                first_time = str(row.get('first_limit_time', '')) if pd.notna(row.get('first_limit_time')) else ''
                float_cap = float(row.get('float_market_cap', 0)) if pd.notna(row.get('float_market_cap')) else 0

                # ==================== 龙头质量评分 ====================
                dragon_quality = 0
                # 连板高度加分（核心因子）
                dragon_quality += min(limit_days * 15, 60)
                # 封板时间加分
                if first_time and first_time < '09:35':
                    dragon_quality += 25  # 秒板
                elif first_time and first_time < '10:00':
                    dragon_quality += 18
                elif first_time and first_time < '11:00':
                    dragon_quality += 10
                # 流通市值辨识度（30-200亿为最佳，过大缺弹性，过小缺辨识度）
                cap_yi = float_cap / 1e8 if float_cap > 0 else 0  # 转换为亿
                if 30 <= cap_yi <= 200:
                    dragon_quality += 15  # 最佳辨识度区间
                elif 10 <= cap_yi < 30 or 200 < cap_yi <= 500:
                    dragon_quality += 8
                # 判断是否为真龙头（高分 + 连板≥2）
                is_true_dragon = dragon_quality >= 50 and limit_days >= 2

                stock = {
                    "rank": idx + 1,
                    "code": str(row.get('code', '')),
                    "name": str(row.get('name', '')),
                    "price": round(float(row.get('price', 0)), 2) if pd.notna(row.get('price')) else 0,
                    "change_pct": round(float(row.get('change_pct', 0)), 2) if pd.notna(row.get('change_pct')) else 0,
                    "amount": round(float(row.get('amount', 0)), 2) if pd.notna(row.get('amount')) else 0,
                    "float_market_cap": round(float_cap, 2),
                    "total_market_cap": round(float(row.get('total_market_cap', 0)), 2) if pd.notna(row.get('total_market_cap')) else 0,
                    "turnover_rate": round(float(row.get('turnover_rate', 0)), 2) if pd.notna(row.get('turnover_rate')) else 0,
                    "limit_up_days": limit_days,
                    "first_limit_time": first_time,
                    "seal_amount": round(float(row.get('seal_amount', 0)), 2) if pd.notna(row.get('seal_amount')) else 0,
                    # 新增字段
                    "dragon_quality_score": dragon_quality,
                    "is_true_dragon": is_true_dragon,
                    "emotion_cycle_phase": emotion_phase,
                }

                # 判断是否属于主线题材（加分信息）
                stock["in_main_theme"] = False
                stock["related_themes"] = []

                dragon_heads.append(stock)

            logger.info("Selected dragon heads", count=len(dragon_heads),
                        emotion_phase=emotion_phase)

        except Exception as e:
            logger.error("Failed to select dragon heads", error=str(e), traceback=traceback.format_exc())

        return dragon_heads

    # ==================== 模块三：新闻情绪共振 ====================

    def _check_news_resonance(self, main_themes: List[str]) -> Dict[str, Any]:
        """
        检查新闻情绪与主线题材的共振

        拉取财经快讯，统计高频关键词，与主线题材交叉验证
        """
        resonance = {
            "news_keywords": [],
            "matching_themes": [],
            "resonance_score": 0,
            "news_count": 0,
        }

        if not main_themes:
            return resonance

        try:
            # 使用多源新闻数据
            try:
                from dragon_data_provider import get_financial_news
            except ImportError:
                from app.dragon_data_provider import get_financial_news

            news_text = ""
            news_count = 0

            try:
                news_df = get_financial_news()
                if news_df is not None and not news_df.empty:
                    # 获取标题、内容列
                    text_cols = []
                    for col in news_df.columns:
                        if '标题' in col or '内容' in col or '摘要' in col or 'title' in col.lower():
                            text_cols.append(col)

                    for col in text_cols:
                        news_text += " ".join(news_df[col].dropna().astype(str).tolist()) + " "

                    news_count = len(news_df)
                    logger.info("Got financial news (multi-source)", count=news_count)
            except Exception as e:
                logger.warning("Financial news fetch failed", error=str(e))

            resonance["news_count"] = news_count

            # 检查主线题材关键词在新闻中的出现频率
            matching = []
            for theme in main_themes:
                # 简单的关键词匹配
                keywords = theme.replace("概念", "").replace("板块", "").strip()
                if keywords and keywords in news_text:
                    matching.append(theme)

            resonance["matching_themes"] = matching
            resonance["resonance_score"] = len(matching) / max(len(main_themes), 1) * 100

            # 提取新闻中的高频关键词（简单版本）
            if news_text:
                # 统计主线相关关键词
                for theme in main_themes:
                    count = news_text.count(theme.replace("概念", "").replace("板块", "").strip())
                    if count > 0:
                        resonance["news_keywords"].append({
                            "keyword": theme,
                            "count": count
                        })

            logger.info("News resonance check completed",
                        matching_count=len(matching),
                        score=resonance["resonance_score"])

        except Exception as e:
            logger.error("Failed to check news resonance", error=str(e))

        return resonance

    # ==================== 模块四：生成推荐结果 ====================

    def _build_recommendations(
        self,
        dragon_heads: List[Dict],
        main_themes: List[str],
        theme_details: Dict,
        news_resonance: Dict,
        limit: int
    ) -> Dict[str, Any]:
        """
        构建最终的推荐结果

        包含推荐列表 + 推荐逻辑说明
        """
        now = datetime.now()

        # 构建推荐逻辑说明
        strategy_explanation = self._generate_explanation(
            dragon_heads, main_themes, theme_details, news_resonance
        )

        # 为每只个股生成推荐理由
        for stock in dragon_heads:
            reasons = []

            if stock.get('limit_up_days', 0) >= 3:
                reasons.append(f"连板{stock['limit_up_days']}天，高度领先")
            elif stock.get('limit_up_days', 0) >= 2:
                reasons.append(f"连板{stock['limit_up_days']}天，具备连续性")
            else:
                reasons.append("当日涨停，强势封板")

            if stock.get('first_limit_time') and stock['first_limit_time'] < '10:00':
                reasons.append("早盘封板，资金抢筹明显")
            elif stock.get('first_limit_time') and stock['first_limit_time'] < '11:00':
                reasons.append("上午封板，资金认可度高")

            if stock.get('turnover_rate', 0) < 5:
                reasons.append("换手率低，筹码锁定良好")
            elif stock.get('turnover_rate', 0) > 15:
                reasons.append("高换手率，市场分歧较大（注意风险）")

            if stock.get('in_main_theme'):
                reasons.append(f"属于当日主线题材")

            stock["reasons"] = reasons
            stock["recommendation_level"] = self._calc_recommendation_level(stock)

        result = {
            "status": "success",
            "data": {
                "recommendations": dragon_heads[:limit],
                "total": len(dragon_heads),
                "main_themes": [
                    {
                        "name": theme,
                        "details": theme_details.get(theme, {})
                    }
                    for theme in main_themes
                ],
                "news_resonance": news_resonance,
                "strategy_explanation": strategy_explanation,
                "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "trading_date": now.strftime("%Y-%m-%d"),
            }
        }

        return result

    def _calc_recommendation_level(self, stock: Dict) -> str:
        """计算推荐等级：强烈推荐 / 推荐 / 关注"""
        score = 0

        # 连板日数加分
        limit_days = stock.get('limit_up_days', 0)
        if limit_days >= 4:
            score += 40
        elif limit_days >= 3:
            score += 30
        elif limit_days >= 2:
            score += 20
        else:
            score += 10

        # 封板时间加分
        first_time = stock.get('first_limit_time', '')
        if first_time and first_time < '09:35':
            score += 30  # 秒板
        elif first_time and first_time < '10:00':
            score += 20
        elif first_time and first_time < '11:00':
            score += 10

        # 主线题材加分
        if stock.get('in_main_theme'):
            score += 20

        if score >= 70:
            return "强烈推荐"
        elif score >= 40:
            return "推荐"
        else:
            return "关注"

    def _generate_explanation(
        self,
        dragon_heads: List[Dict],
        main_themes: List[str],
        theme_details: Dict,
        news_resonance: Dict
    ) -> str:
        """生成策略推荐逻辑说明"""
        now = datetime.now()
        lines = []

        lines.append(f"## 龙头战法 - {now.strftime('%Y年%m月%d日')} 推荐报告\n")

        # 主线题材
        lines.append("### 一、今日主线题材识别\n")
        if main_themes:
            lines.append("通过统计涨停股所属概念板块，筛选出以下今日主线题材：\n")
            for i, theme in enumerate(main_themes, 1):
                detail = theme_details.get(theme, {})
                change = detail.get('change_pct', 0)
                up_count = detail.get('up_count', 0)
                limit_count = detail.get('limit_up_count', 0)
                info = f"{i}. **{theme}**"
                if change:
                    info += f" (板块涨幅 {change:.2f}%"
                    if up_count:
                        info += f", 上涨{up_count}家"
                    if limit_count:
                        info += f", 涨停{limit_count}家"
                    info += ")"
                lines.append(info)
        else:
            lines.append("今日未识别出明确的主线题材，市场以个股行情为主。")

        lines.append("")

        # 龙头筛选逻辑
        lines.append("### 二、龙头筛选逻辑\n")
        lines.append("龙头筛选基于以下三个核心因子排序：")
        lines.append("1. **连板高度（权重最高）**：连续涨停天数越多，说明市场认可度越高")
        lines.append("2. **首次封板时间**：封板时间越早，说明主力资金态度越坚决")
        lines.append("3. **成交额/封单量**：成交额越大或封单量越高，流动性和筹码锁定越好")
        lines.append("")

        # 新闻共振
        lines.append("### 三、新闻情绪共振\n")
        if news_resonance.get('matching_themes'):
            lines.append(f"经过{news_resonance.get('news_count', 0)}条财经快讯分析，"
                         f"以下主线题材与新闻热点形成共振：")
            for theme in news_resonance['matching_themes']:
                lines.append(f"- ✅ **{theme}**（新闻热度验证通过）")
            lines.append(f"\n情绪共振评分：{news_resonance.get('resonance_score', 0):.0f}/100")
        else:
            lines.append("当前新闻面与主线题材暂未形成明显共振，建议以技术面为主参考。")

        lines.append("")

        # 风险提示
        lines.append("### 四、风险提示\n")
        lines.append("⚠️ 龙头战法属于短线交易策略，具有以下风险：")
        lines.append("1. 追涨停存在次日低开风险，尤其是尾盘封板的品种")
        lines.append("2. 连板股在高位容易出现核按钮（大面），需严格执行止损")
        lines.append("3. 龙头战法适合情绪高潮期操作，在退潮期需降低仓位")
        lines.append("4. 本推荐仅为量化模型输出，不构成投资建议")

        return "\n".join(lines)


# 全局单例
dragon_head_strategy = DragonHeadStrategy()
