"""
统一数据管理器
提供统一的接口来管理和调用不同平台的数据API
支持平台API混用，可以具体指定使用哪个平台的哪个功能
"""

from typing import Annotated, Dict, Any, List, Optional, Union
from datetime import datetime
import pandas as pd
from .config import get_config, DATA_DIR

# 导入各平台接口
from . import akshare_interface
from . import tushare_interface  
from . import myquant_interface
from . import interface as legacy_interface


class UnifiedDataManager:
    """统一数据管理器"""
    
    def __init__(self):
        """初始化数据管理器"""
        self.supported_platforms = {
            'akshare': 'AkShare数据源',
            'tushare': 'Tushare数据源', 
            'myquant': 'MyQuant数据源',
            'finnhub': 'Finnhub数据源',
            'yfinance': 'Yahoo Finance数据源',
            'google': 'Google News数据源',
            'reddit': 'Reddit数据源',
            'simfin': 'SimFin数据源',
            'stockstats': 'StockStats技术指标'
        }
        
        self.data_types = {
            'stock_data': '股票行情数据',
            'financial_data': '财务报表数据', 
            'financial_indicators': '财务指标数据',
            'news': '新闻数据',
            'technical_indicators': '技术指标数据',
            'market_data': '市场数据',
            'macro_data': '宏观经济数据',
            'sentiment': '情感分析数据',
            'backtest': '回测相关功能'
        }
        
        # 平台功能映射表
        self.platform_capabilities = {
            'akshare': {
                'stock_data': akshare_interface.get_akshare_stock_data,
                'financial_data': akshare_interface.get_akshare_financial_data,
                'financial_indicators': akshare_interface.get_akshare_financial_indicators,
                'news': akshare_interface.get_akshare_stock_news,
                'market_data': akshare_interface.get_akshare_concept_stocks,
                'macro_data': akshare_interface.get_akshare_macro_data,
                'hot_stocks': akshare_interface.get_akshare_hot_stocks,
                'stock_info': akshare_interface.get_akshare_stock_info,
                'stock_comment': akshare_interface.get_akshare_stock_comment,
                'industry_stocks': akshare_interface.get_akshare_industry_stocks
            },
            'tushare': {
                'stock_data': tushare_interface.get_tushare_daily_data,
                'financial_data': tushare_interface.get_tushare_financial_data,
                'financial_indicators': tushare_interface.get_tushare_financial_indicators,
                'news': tushare_interface.get_tushare_news,
                'index_data': tushare_interface.get_tushare_index_data
            },
            'myquant': {
                'stock_data': myquant_interface.get_myquant_stock_data,
                'financial_data': myquant_interface.get_myquant_financial_data,
                'technical_indicators': myquant_interface.get_myquant_technical_indicators,
                'market_data': myquant_interface.get_myquant_market_data,
                'sentiment': myquant_interface.get_myquant_news_sentiment,
                'factor_data': myquant_interface.get_myquant_factor_data,
                'backtest': myquant_interface.create_myquant_backtest,
                'portfolio_analysis': myquant_interface.get_myquant_portfolio_analysis
            },
            'finnhub': {
                'news': legacy_interface.get_finnhub_news,
                'insider_sentiment': legacy_interface.get_finnhub_company_insider_sentiment,
                'insider_transactions': legacy_interface.get_finnhub_company_insider_transactions
            },
            'yfinance': {
                'stock_data': legacy_interface.get_YFin_data_window,
                'stock_data_online': legacy_interface.get_YFin_data_online
            },
            'google': {
                'news': legacy_interface.get_google_news
            },
            'reddit': {
                'global_news': legacy_interface.get_reddit_global_news,
                'company_news': legacy_interface.get_reddit_company_news
            },
            'simfin': {
                'balance_sheet': legacy_interface.get_simfin_balance_sheet,
                'cashflow': legacy_interface.get_simfin_cashflow,
                'income_statement': legacy_interface.get_simfin_income_statements
            },
            'stockstats': {
                'technical_indicators': legacy_interface.get_stock_stats_indicators_window,
                'single_indicator': legacy_interface.get_stockstats_indicator
            }
        }
    
    def get_supported_platforms(self) -> Dict[str, str]:
        """获取支持的平台列表"""
        return self.supported_platforms.copy()
    
    def get_platform_capabilities(self, platform: str) -> Dict[str, str]:
        """获取指定平台的功能列表"""
        if platform not in self.platform_capabilities:
            raise ValueError(f"不支持的平台: {platform}")
        
        capabilities = self.platform_capabilities[platform]
        return {func_name: func.__doc__ or "无描述" for func_name, func in capabilities.items()}
    
    def get_data(
        self,
        platform: Annotated[str, "数据平台名称"],
        data_type: Annotated[str, "数据类型"],
        **kwargs
    ) -> str:
        """
        统一数据获取接口
        
        Args:
            platform: 数据平台名称 (akshare, tushare, myquant, finnhub, yfinance, google, reddit, simfin, stockstats)
            data_type: 数据类型 (stock_data, financial_data, news, etc.)
            **kwargs: 各平台特定的参数
            
        Returns:
            str: 格式化的数据结果
        """
        if platform not in self.platform_capabilities:
            available_platforms = ', '.join(self.platform_capabilities.keys())
            return f"错误：不支持的平台 '{platform}'。支持的平台: {available_platforms}"
        
        platform_funcs = self.platform_capabilities[platform]
        
        if data_type not in platform_funcs:
            available_types = ', '.join(platform_funcs.keys())
            return f"错误：平台 '{platform}' 不支持数据类型 '{data_type}'。支持的类型: {available_types}"
        
        try:
            func = platform_funcs[data_type]
            result = func(**kwargs)
            return result
        except Exception as e:
            return f"错误：调用 {platform}.{data_type} 失败: {str(e)}"
    
    def get_stock_data(
        self,
        symbol: Annotated[str, "股票代码"],
        curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
        look_back_days: Annotated[int, "回看天数"],
        platforms: Annotated[List[str], "使用的平台列表"] = None,
        **kwargs
    ) -> Dict[str, str]:
        """
        获取多平台股票数据对比
        
        Args:
            symbol: 股票代码
            curr_date: 当前日期
            look_back_days: 回看天数
            platforms: 平台列表，默认使用所有支持股票数据的平台
            **kwargs: 其他参数
            
        Returns:
            Dict[str, str]: 各平台的数据结果
        """
        if platforms is None:
            platforms = ['akshare', 'tushare', 'myquant', 'yfinance']
        
        results = {}
        
        for platform in platforms:
            try:
                if platform == 'akshare':
                    result = self.get_data(
                        platform='akshare',
                        data_type='stock_data',
                        symbol=symbol,
                        curr_date=curr_date,
                        look_back_days=look_back_days,
                        **kwargs
                    )
                elif platform == 'tushare':
                    # Tushare需要特定格式的股票代码
                    ts_code = symbol if '.' in symbol else f"{symbol}.SZ"
                    result = self.get_data(
                        platform='tushare',
                        data_type='stock_data',
                        ts_code=ts_code,
                        curr_date=curr_date,
                        look_back_days=look_back_days
                    )
                elif platform == 'myquant':
                    result = self.get_data(
                        platform='myquant',
                        data_type='stock_data',
                        symbol=symbol,
                        curr_date=curr_date,
                        look_back_days=look_back_days,
                        **kwargs
                    )
                elif platform == 'yfinance':
                    result = self.get_data(
                        platform='yfinance',
                        data_type='stock_data',
                        symbol=symbol,
                        curr_date=curr_date,
                        look_back_days=look_back_days
                    )
                else:
                    result = f"平台 {platform} 暂不支持股票数据获取"
                
                results[platform] = result
                
            except Exception as e:
                results[platform] = f"获取 {platform} 数据失败: {str(e)}"
        
        return results
    
    def get_news_data(
        self,
        query: Annotated[str, "查询关键词或股票代码"],
        curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
        look_back_days: Annotated[int, "回看天数"],
        platforms: Annotated[List[str], "使用的平台列表"] = None,
        **kwargs
    ) -> Dict[str, str]:
        """
        获取多平台新闻数据
        
        Args:
            query: 查询关键词或股票代码
            curr_date: 当前日期
            look_back_days: 回看天数
            platforms: 平台列表，默认使用所有支持新闻的平台
            **kwargs: 其他参数
            
        Returns:
            Dict[str, str]: 各平台的新闻数据
        """
        if platforms is None:
            platforms = ['akshare', 'tushare', 'finnhub', 'google', 'reddit']
        
        results = {}
        
        for platform in platforms:
            try:
                if platform == 'akshare':
                    result = self.get_data(
                        platform='akshare',
                        data_type='news',
                        symbol=query
                    )
                elif platform == 'tushare':
                    result = self.get_data(
                        platform='tushare',
                        data_type='news',
                        curr_date=curr_date,
                        look_back_days=look_back_days,
                        **kwargs
                    )
                elif platform == 'finnhub':
                    result = self.get_data(
                        platform='finnhub',
                        data_type='news',
                        ticker=query,
                        curr_date=curr_date,
                        look_back_days=look_back_days
                    )
                elif platform == 'google':
                    result = self.get_data(
                        platform='google',
                        data_type='news',
                        query=query,
                        curr_date=curr_date,
                        look_back_days=look_back_days
                    )
                elif platform == 'reddit':
                    if query.lower() in ['global', 'market', 'economy']:
                        result = self.get_data(
                            platform='reddit',
                            data_type='global_news',
                            start_date=curr_date,
                            look_back_days=look_back_days,
                            max_limit_per_day=10
                        )
                    else:
                        result = self.get_data(
                            platform='reddit',
                            data_type='company_news',
                            ticker=query,
                            start_date=curr_date,
                            look_back_days=look_back_days,
                            max_limit_per_day=10
                        )
                else:
                    result = f"平台 {platform} 暂不支持新闻数据获取"
                
                results[platform] = result
                
            except Exception as e:
                results[platform] = f"获取 {platform} 新闻失败: {str(e)}"
        
        return results
    
    def get_financial_data(
        self,
        symbol: Annotated[str, "股票代码"],
        report_type: Annotated[str, "报表类型"],
        platforms: Annotated[List[str], "使用的平台列表"] = None,
        **kwargs
    ) -> Dict[str, str]:
        """
        获取多平台财务数据
        
        Args:
            symbol: 股票代码
            report_type: 报表类型
            platforms: 平台列表
            **kwargs: 其他参数
            
        Returns:
            Dict[str, str]: 各平台的财务数据
        """
        if platforms is None:
            platforms = ['akshare', 'tushare', 'myquant', 'simfin']
        
        results = {}
        
        for platform in platforms:
            try:
                if platform == 'akshare':
                    result = self.get_data(
                        platform='akshare',
                        data_type='financial_data',
                        symbol=symbol,
                        indicator=report_type,
                        **kwargs
                    )
                elif platform == 'tushare':
                    ts_code = symbol if '.' in symbol else f"{symbol}.SZ"
                    result = self.get_data(
                        platform='tushare',
                        data_type='financial_data',
                        ts_code=ts_code,
                        report_type=report_type,
                        **kwargs
                    )
                elif platform == 'myquant':
                    result = self.get_data(
                        platform='myquant',
                        data_type='financial_data',
                        symbol=symbol,
                        report_type=report_type,
                        **kwargs
                    )
                elif platform == 'simfin':
                    if report_type in ['balance', 'balance_sheet']:
                        result = self.get_data(
                            platform='simfin',
                            data_type='balance_sheet',
                            ticker=symbol,
                            **kwargs
                        )
                    elif report_type in ['income', 'income_statement']:
                        result = self.get_data(
                            platform='simfin',
                            data_type='income_statement',
                            ticker=symbol,
                            **kwargs
                        )
                    elif report_type in ['cashflow', 'cash_flow']:
                        result = self.get_data(
                            platform='simfin',
                            data_type='cashflow',
                            ticker=symbol,
                            **kwargs
                        )
                    else:
                        result = f"SimFin 不支持报表类型: {report_type}"
                else:
                    result = f"平台 {platform} 暂不支持财务数据获取"
                
                results[platform] = result
                
            except Exception as e:
                results[platform] = f"获取 {platform} 财务数据失败: {str(e)}"
        
        return results
    
    def get_technical_indicators(
        self,
        symbol: Annotated[str, "股票代码"],
        indicators: Annotated[str, "技术指标"],
        curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
        look_back_days: Annotated[int, "回看天数"],
        platforms: Annotated[List[str], "使用的平台列表"] = None,
        **kwargs
    ) -> Dict[str, str]:
        """
        获取多平台技术指标数据
        
        Args:
            symbol: 股票代码
            indicators: 技术指标
            curr_date: 当前日期
            look_back_days: 回看天数
            platforms: 平台列表
            **kwargs: 其他参数
            
        Returns:
            Dict[str, str]: 各平台的技术指标数据
        """
        if platforms is None:
            platforms = ['myquant', 'stockstats']
        
        results = {}
        
        for platform in platforms:
            try:
                if platform == 'myquant':
                    result = self.get_data(
                        platform='myquant',
                        data_type='technical_indicators',
                        symbol=symbol,
                        indicators=indicators,
                        curr_date=curr_date,
                        look_back_days=look_back_days,
                        **kwargs
                    )
                elif platform == 'stockstats':
                    # StockStats 一次只能计算一个指标
                    indicator_list = [ind.strip() for ind in indicators.split(',')]
                    if len(indicator_list) == 1:
                        result = self.get_data(
                            platform='stockstats',
                            data_type='technical_indicators',
                            symbol=symbol,
                            indicator=indicator_list[0],
                            curr_date=curr_date,
                            look_back_days=look_back_days,
                            **kwargs
                        )
                    else:
                        # 多个指标需要分别计算
                        sub_results = []
                        for indicator in indicator_list:
                            sub_result = self.get_data(
                                platform='stockstats',
                                data_type='single_indicator',
                                symbol=symbol,
                                indicator=indicator,
                                curr_date=curr_date,
                                **kwargs
                            )
                            sub_results.append(f"## {indicator}:\n{sub_result}")
                        result = "\n\n".join(sub_results)
                else:
                    result = f"平台 {platform} 暂不支持技术指标计算"
                
                results[platform] = result
                
            except Exception as e:
                results[platform] = f"获取 {platform} 技术指标失败: {str(e)}"
        
        return results
    
    def create_data_pipeline(
        self,
        pipeline_config: Annotated[Dict[str, Any], "数据管道配置"]
    ) -> Dict[str, Any]:
        """
        创建数据获取管道
        
        Args:
            pipeline_config: 管道配置，包含多个数据获取任务
            
        Returns:
            Dict[str, Any]: 管道执行结果
        """
        results = {}
        
        for task_name, task_config in pipeline_config.items():
            try:
                platform = task_config.get('platform')
                data_type = task_config.get('data_type')
                params = task_config.get('params', {})
                
                if not platform or not data_type:
                    results[task_name] = "错误：缺少 platform 或 data_type 配置"
                    continue
                
                result = self.get_data(platform, data_type, **params)
                results[task_name] = result
                
            except Exception as e:
                results[task_name] = f"任务 {task_name} 执行失败: {str(e)}"
        
        return results


# 创建全局实例
unified_manager = UnifiedDataManager()


# 便捷函数
def get_data(platform: str, data_type: str, **kwargs) -> str:
    """便捷的数据获取函数"""
    return unified_manager.get_data(platform, data_type, **kwargs)


def get_multi_platform_stock_data(symbol: str, curr_date: str, look_back_days: int, platforms: List[str] = None) -> Dict[str, str]:
    """便捷的多平台股票数据获取函数"""
    return unified_manager.get_stock_data(symbol, curr_date, look_back_days, platforms)


def get_multi_platform_news(query: str, curr_date: str, look_back_days: int, platforms: List[str] = None) -> Dict[str, str]:
    """便捷的多平台新闻获取函数"""
    return unified_manager.get_news_data(query, curr_date, look_back_days, platforms)


def get_supported_platforms() -> Dict[str, str]:
    """获取支持的平台列表"""
    return unified_manager.get_supported_platforms()


def get_platform_capabilities(platform: str) -> Dict[str, str]:
    """获取平台功能列表"""
    return unified_manager.get_platform_capabilities(platform)
