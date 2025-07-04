"""
简易数据接口
提供最简单易用的数据获取方式
一行代码即可获取任何平台的任何数据
"""

from typing import Dict, List, Any, Optional, Union
from .smart_data_router import smart_router
from .data_config_manager import config_manager
from .unified_data_manager import unified_manager


class EasyData:
    """简易数据接口类"""
    
    def __init__(self):
        """初始化"""
        self.router = smart_router
        self.config = config_manager
        self.manager = unified_manager
    
    # ==================== 股票数据 ====================
    
    def stock(
        self,
        symbol: str,
        days: int = 30,
        date: str = "2024-12-31",
        platform: str = None
    ) -> str:
        """
        获取股票数据
        
        Args:
            symbol: 股票代码
            days: 回看天数
            date: 当前日期
            platform: 指定平台 (akshare, tushare, myquant, yfinance)
            
        Returns:
            str: 股票数据
        """
        if platform:
            return self.manager.get_data(
                platform=platform,
                data_type='stock_data',
                symbol=symbol,
                curr_date=date,
                look_back_days=days
            )
        else:
            result = self.router.route_data_request(
                data_type='stock_data',
                params={
                    'symbol': symbol,
                    'curr_date': date,
                    'look_back_days': days
                }
            )
            return result.get('data', result.get('error', '获取数据失败'))
    
    def stock_compare(
        self,
        symbol: str,
        days: int = 30,
        date: str = "2024-12-31",
        platforms: List[str] = None
    ) -> Dict[str, str]:
        """
        对比多个平台的股票数据
        
        Args:
            symbol: 股票代码
            days: 回看天数
            date: 当前日期
            platforms: 平台列表
            
        Returns:
            Dict[str, str]: 各平台的股票数据
        """
        results = self.router.get_multi_platform_data(
            data_type='stock_data',
            params={
                'symbol': symbol,
                'curr_date': date,
                'look_back_days': days
            },
            platforms=platforms
        )
        
        return {platform: result.get('data', result.get('error', '获取失败')) 
                for platform, result in results.items()}
    
    # ==================== 新闻数据 ====================
    
    def news(
        self,
        query: str,
        days: int = 7,
        date: str = "2024-12-31",
        platform: str = None
    ) -> str:
        """
        获取新闻数据
        
        Args:
            query: 查询关键词或股票代码
            days: 回看天数
            date: 当前日期
            platform: 指定平台 (akshare, google, finnhub, reddit)
            
        Returns:
            str: 新闻数据
        """
        if platform:
            if platform == 'akshare':
                return self.manager.get_data(
                    platform='akshare',
                    data_type='news',
                    symbol=query
                )
            elif platform == 'google':
                return self.manager.get_data(
                    platform='google',
                    data_type='news',
                    query=query,
                    curr_date=date,
                    look_back_days=days
                )
            elif platform == 'finnhub':
                return self.manager.get_data(
                    platform='finnhub',
                    data_type='news',
                    ticker=query,
                    curr_date=date,
                    look_back_days=days
                )
            elif platform == 'reddit':
                if query.lower() in ['global', 'market', 'economy']:
                    return self.manager.get_data(
                        platform='reddit',
                        data_type='global_news',
                        start_date=date,
                        look_back_days=days,
                        max_limit_per_day=10
                    )
                else:
                    return self.manager.get_data(
                        platform='reddit',
                        data_type='company_news',
                        ticker=query,
                        start_date=date,
                        look_back_days=days,
                        max_limit_per_day=10
                    )
        else:
            result = self.router.route_data_request(
                data_type='news',
                params={
                    'query': query,
                    'curr_date': date,
                    'look_back_days': days
                }
            )
            return result.get('data', result.get('error', '获取数据失败'))
    
    def news_compare(
        self,
        query: str,
        days: int = 7,
        date: str = "2024-12-31",
        platforms: List[str] = None
    ) -> Dict[str, str]:
        """
        对比多个平台的新闻数据
        
        Args:
            query: 查询关键词或股票代码
            days: 回看天数
            date: 当前日期
            platforms: 平台列表
            
        Returns:
            Dict[str, str]: 各平台的新闻数据
        """
        results = self.router.get_multi_platform_data(
            data_type='news',
            params={
                'query': query,
                'curr_date': date,
                'look_back_days': days
            },
            platforms=platforms
        )
        
        return {platform: result.get('data', result.get('error', '获取失败')) 
                for platform, result in results.items()}
    
    # ==================== 财务数据 ====================
    
    def financial(
        self,
        symbol: str,
        report_type: str = "利润表",
        period: str = "年报",
        platform: str = None
    ) -> str:
        """
        获取财务数据
        
        Args:
            symbol: 股票代码
            report_type: 报表类型 (利润表, 资产负债表, 现金流量表)
            period: 报告期 (年报, 中报, 一季报, 三季报)
            platform: 指定平台 (akshare, tushare, simfin)
            
        Returns:
            str: 财务数据
        """
        if platform:
            if platform == 'akshare':
                return self.manager.get_data(
                    platform='akshare',
                    data_type='financial_data',
                    symbol=symbol,
                    indicator=report_type,
                    period=period
                )
            elif platform == 'tushare':
                ts_code = symbol if '.' in symbol else f"{symbol}.SZ"
                return self.manager.get_data(
                    platform='tushare',
                    data_type='financial_data',
                    ts_code=ts_code,
                    report_type=report_type,
                    period="2024-12-31"
                )
            elif platform == 'simfin':
                freq = "annual" if period == "年报" else "quarterly"
                if report_type in ['利润表', 'income']:
                    return self.manager.get_data(
                        platform='simfin',
                        data_type='income_statement',
                        ticker=symbol,
                        freq=freq,
                        curr_date="2024-12-31"
                    )
                elif report_type in ['资产负债表', 'balance']:
                    return self.manager.get_data(
                        platform='simfin',
                        data_type='balance_sheet',
                        ticker=symbol,
                        freq=freq,
                        curr_date="2024-12-31"
                    )
                elif report_type in ['现金流量表', 'cashflow']:
                    return self.manager.get_data(
                        platform='simfin',
                        data_type='cashflow',
                        ticker=symbol,
                        freq=freq,
                        curr_date="2024-12-31"
                    )
        else:
            result = self.router.route_data_request(
                data_type='financial_data',
                params={
                    'symbol': symbol,
                    'report_type': report_type,
                    'period': period
                }
            )
            return result.get('data', result.get('error', '获取数据失败'))
    
    # ==================== 技术指标 ====================
    
    def technical(
        self,
        symbol: str,
        indicators: str = "MA,MACD,RSI",
        days: int = 30,
        date: str = "2024-12-31",
        platform: str = None
    ) -> str:
        """
        获取技术指标
        
        Args:
            symbol: 股票代码
            indicators: 技术指标，逗号分隔
            days: 回看天数
            date: 当前日期
            platform: 指定平台 (stockstats, myquant)
            
        Returns:
            str: 技术指标数据
        """
        if platform:
            if platform == 'stockstats':
                # StockStats 一次只能计算一个指标
                indicator_list = [ind.strip() for ind in indicators.split(',')]
                results = []
                for indicator in indicator_list:
                    result = self.manager.get_data(
                        platform='stockstats',
                        data_type='technical_indicators',
                        symbol=symbol,
                        indicator=indicator,
                        curr_date=date,
                        look_back_days=days,
                        online=False
                    )
                    results.append(f"## {indicator}:\n{result}")
                return "\n\n".join(results)
            elif platform == 'myquant':
                return self.manager.get_data(
                    platform='myquant',
                    data_type='technical_indicators',
                    symbol=symbol,
                    indicators=indicators,
                    curr_date=date,
                    look_back_days=days
                )
        else:
            result = self.router.route_data_request(
                data_type='technical_indicators',
                params={
                    'symbol': symbol,
                    'indicators': indicators,
                    'curr_date': date,
                    'look_back_days': days
                }
            )
            return result.get('data', result.get('error', '获取数据失败'))
    
    # ==================== 宏观数据 ====================
    
    def macro(
        self,
        indicator: str = "GDP",
        platform: str = "akshare"
    ) -> str:
        """
        获取宏观经济数据
        
        Args:
            indicator: 宏观指标 (GDP, CPI, PPI, PMI)
            platform: 指定平台 (akshare)
            
        Returns:
            str: 宏观数据
        """
        return self.manager.get_data(
            platform=platform,
            data_type='macro_data',
            indicator=indicator
        )
    
    # ==================== 配置管理 ====================
    
    def set_preference(self, data_type: str, platforms: List[str]):
        """
        设置数据类型的平台偏好
        
        Args:
            data_type: 数据类型
            platforms: 平台列表，按优先级排序
        """
        self.config.set_platform_preferences(data_type, platforms)
        print(f"已设置 {data_type} 的平台偏好: {' -> '.join(platforms)}")
    
    def enable_platform(self, platform: str):
        """启用平台"""
        self.config.enable_platform(platform)
        print(f"已启用平台: {platform}")
    
    def disable_platform(self, platform: str):
        """禁用平台"""
        self.config.disable_platform(platform)
        print(f"已禁用平台: {platform}")
    
    def show_config(self):
        """显示当前配置"""
        self.config.print_config_summary()
    
    def show_platforms(self) -> Dict[str, str]:
        """显示支持的平台"""
        return self.manager.get_supported_platforms()
    
    def show_capabilities(self, platform: str) -> Dict[str, str]:
        """显示平台功能"""
        return self.manager.get_platform_capabilities(platform)
    
    def performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        return self.router.get_platform_performance_report()
    
    def optimize(self):
        """优化平台偏好设置"""
        self.router.optimize_platform_preferences()
        print("已根据性能统计优化平台偏好设置")
    
    # ==================== 批量操作 ====================
    
    def batch_get(self, requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量获取数据
        
        Args:
            requests: 请求列表，每个请求包含 data_type 和 params
            
        Returns:
            Dict[str, Any]: 批量结果
        """
        return self.router.create_smart_pipeline(requests)
    
    def quick_analysis(
        self,
        symbol: str,
        date: str = "2024-12-31",
        days: int = 30
    ) -> Dict[str, str]:
        """
        快速分析：获取股票的多维度数据
        
        Args:
            symbol: 股票代码
            date: 当前日期
            days: 回看天数
            
        Returns:
            Dict[str, str]: 包含股票数据、新闻、财务、技术指标的综合分析
        """
        results = {}
        
        # 股票数据
        try:
            results['股票数据'] = self.stock(symbol, days, date)
        except Exception as e:
            results['股票数据'] = f"获取失败: {e}"
        
        # 新闻数据
        try:
            results['新闻数据'] = self.news(symbol, 7, date)
        except Exception as e:
            results['新闻数据'] = f"获取失败: {e}"
        
        # 财务数据
        try:
            results['财务数据'] = self.financial(symbol, "利润表", "年报")
        except Exception as e:
            results['财务数据'] = f"获取失败: {e}"
        
        # 技术指标
        try:
            results['技术指标'] = self.technical(symbol, "RSI,MACD,MA", days, date)
        except Exception as e:
            results['技术指标'] = f"获取失败: {e}"
        
        return results


# 创建全局实例
data = EasyData()


# ==================== 便捷函数 ====================

def stock(symbol: str, days: int = 30, date: str = "2024-12-31", platform: str = None) -> str:
    """获取股票数据"""
    return data.stock(symbol, days, date, platform)


def news(query: str, days: int = 7, date: str = "2024-12-31", platform: str = None) -> str:
    """获取新闻数据"""
    return data.news(query, days, date, platform)


def financial(symbol: str, report_type: str = "利润表", period: str = "年报", platform: str = None) -> str:
    """获取财务数据"""
    return data.financial(symbol, report_type, period, platform)


def technical(symbol: str, indicators: str = "MA,MACD,RSI", days: int = 30, date: str = "2024-12-31", platform: str = None) -> str:
    """获取技术指标"""
    return data.technical(symbol, indicators, days, date, platform)


def macro(indicator: str = "GDP", platform: str = "akshare") -> str:
    """获取宏观数据"""
    return data.macro(indicator, platform)


def compare_stock(symbol: str, days: int = 30, date: str = "2024-12-31", platforms: List[str] = None) -> Dict[str, str]:
    """对比多平台股票数据"""
    return data.stock_compare(symbol, days, date, platforms)


def compare_news(query: str, days: int = 7, date: str = "2024-12-31", platforms: List[str] = None) -> Dict[str, str]:
    """对比多平台新闻数据"""
    return data.news_compare(query, days, date, platforms)


def quick_analysis(symbol: str, date: str = "2024-12-31", days: int = 30) -> Dict[str, str]:
    """快速综合分析"""
    return data.quick_analysis(symbol, date, days)


def set_preference(data_type: str, platforms: List[str]):
    """设置平台偏好"""
    data.set_preference(data_type, platforms)


def show_config():
    """显示配置"""
    data.show_config()


def show_platforms() -> Dict[str, str]:
    """显示支持的平台"""
    return data.show_platforms()


def optimize():
    """优化设置"""
    data.optimize()


# ==================== 使用示例 ====================

def usage_examples():
    """使用示例"""
    print("""
# ==================== 使用示例 ====================

# 1. 基础用法 - 获取股票数据（自动选择最佳平台）
result = stock("000001", days=30, date="2024-12-31")

# 2. 指定平台 - 使用特定平台获取数据
result = stock("000001", days=30, date="2024-12-31", platform="akshare")

# 3. 多平台对比 - 对比不同平台的数据
results = compare_stock("000001", days=30, date="2024-12-31")

# 4. 获取新闻数据
news_data = news("000001", days=7, date="2024-12-31")

# 5. 获取财务数据
financial_data = financial("000001", "利润表", "年报")

# 6. 获取技术指标
tech_data = technical("000001", "RSI,MACD,MA", days=30)

# 7. 快速综合分析
analysis = quick_analysis("000001", date="2024-12-31")

# 8. 配置管理
set_preference("stock_data", ["akshare", "tushare"])  # 设置股票数据优先使用akshare
show_config()  # 显示当前配置
optimize()     # 根据性能优化设置

# 9. 批量操作
requests = [
    {"data_type": "stock_data", "params": {"symbol": "000001", "curr_date": "2024-12-31", "look_back_days": 30}},
    {"data_type": "news", "params": {"query": "000001", "curr_date": "2024-12-31", "look_back_days": 7}}
]
batch_results = data.batch_get(requests)

# 10. 宏观数据
gdp_data = macro("GDP")
""")


if __name__ == "__main__":
    usage_examples()
