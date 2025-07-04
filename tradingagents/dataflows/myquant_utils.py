"""
MyQuant (掘金量化) 数据接口工具类
使用 gm 库提供的官方 API
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Annotated
import json

from gm.api import *

from .config import get_config
from .utils import save_output, SavePathType


class MyQuantUtils:
    """MyQuant (掘金量化) 官方 API 工具类"""
    
    def __init__(self, token: Optional[str] = None, mode: str = 'cloud'):
        """初始化 MyQuant GM 接口
        
        Args:
            token: 掘金量化 token，如果不提供则从配置中获取
            mode: 运行模式，'cloud' 或 'local'
        """
        config = get_config()
        self.token = token or config.get('myquant_token', '')
        self.mode = mode
        
        # 设置 token
        if self.token:
            set_token(self.token)
        
        # 设置运行模式
        if mode == 'cloud':
            set_serv_addr('cloud.myquant.cn:9000')
        
    def get_stock_data(
        self,
        symbol: Annotated[str, "股票代码，如 SHSE.000001"],
        start_time: Annotated[str, "开始时间 YYYY-MM-DD HH:MM:SS"],
        end_time: Annotated[str, "结束时间 YYYY-MM-DD HH:MM:SS"],
        frequency: Annotated[str, "数据频率：1d, 60s, 300s, 900s, 1800s, 3600s"] = "1d",
        fields: Annotated[str, "数据字段"] = "open,high,low,close,volume,amount",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取股票历史数据"""
        try:
            # 转换时间格式
            start_time = pd.to_datetime(start_time).strftime('%Y-%m-%d %H:%M:%S')
            end_time = pd.to_datetime(end_time).strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取历史数据
            data = history(
                symbol=symbol,
                frequency=frequency,
                start_time=start_time,
                end_time=end_time,
                fields=fields,
                skip_suspended=True,
                fill_missing='Last',
                adjust=ADJUST_PREV,
                df=True
            )
            
            if data is not None and not data.empty:
                save_output(data, f"MyQuant股票数据-{symbol}", save_path)
                return data
            else:
                print(f"未获取到 {symbol} 的数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取股票数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_list(
        self,
        exchanges: Annotated[List[str], "交易所列表"] = None,
        sec_types: Annotated[List[int], "证券类型列表"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取股票列表"""
        try:
            if exchanges is None:
                exchanges = ['SHSE', 'SZSE']  # 上交所和深交所
            
            if sec_types is None:
                sec_types = [SEC_TYPE_STOCK]  # 股票类型
            
            # 获取股票列表
            instruments = get_instruments(
                exchanges=exchanges,
                sec_types=sec_types,
                df=True
            )
            
            if instruments is not None and not instruments.empty:
                save_output(instruments, "MyQuant股票列表", save_path)
                return instruments
            else:
                print("未获取到股票列表")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_financial_data(
        self,
        symbol: Annotated[str, "股票代码"],
        table: Annotated[str, "财务表类型：income, balance_sheet, cash_flow"],
        start_date: Annotated[str, "开始日期 YYYY-MM-DD"] = None,
        end_date: Annotated[str, "结束日期 YYYY-MM-DD"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取财务数据"""
        try:
            # 获取财务数据
            data = get_fundamentals(
                table=table,
                symbols=symbol,
                start_date=start_date,
                end_date=end_date,
                df=True
            )
            
            if data is not None and not data.empty:
                save_output(data, f"MyQuant财务数据-{symbol}-{table}", save_path)
                return data
            else:
                print(f"未获取到 {symbol} 的{table}财务数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取财务数据失败: {e}")
            return pd.DataFrame()
    
    def get_financial_indicators(
        self,
        symbol: Annotated[str, "股票代码"],
        start_date: Annotated[str, "开始日期 YYYY-MM-DD"] = None,
        end_date: Annotated[str, "结束日期 YYYY-MM-DD"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取财务指标数据"""
        try:
            # 获取财务指标
            data = get_fundamentals(
                table='deriv_finance_indicator',
                symbols=symbol,
                start_date=start_date,
                end_date=end_date,
                df=True
            )
            
            if data is not None and not data.empty:
                save_output(data, f"MyQuant财务指标-{symbol}", save_path)
                return data
            else:
                print(f"未获取到 {symbol} 的财务指标数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取财务指标失败: {e}")
            return pd.DataFrame()
    
    def get_market_data(
        self,
        symbols: Annotated[List[str], "股票代码列表"],
        fields: Annotated[str, "数据字段"] = "symbol,last_price,change_ratio,volume,amount",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取实时市场数据"""
        try:
            # 获取实时行情
            data = current(symbols=symbols, fields=fields, df=True)
            
            if data is not None and not data.empty:
                save_output(data, "MyQuant实时行情", save_path)
                return data
            else:
                print("未获取到实时行情数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def get_index_data(
        self,
        symbol: Annotated[str, "指数代码，如 SHSE.000001"],
        start_time: Annotated[str, "开始时间 YYYY-MM-DD"],
        end_time: Annotated[str, "结束时间 YYYY-MM-DD"],
        frequency: Annotated[str, "数据频率"] = "1d",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取指数数据"""
        try:
            # 获取指数历史数据
            data = history(
                symbol=symbol,
                frequency=frequency,
                start_time=start_time,
                end_time=end_time,
                fields='open,high,low,close,volume,amount',
                df=True
            )
            
            if data is not None and not data.empty:
                save_output(data, f"MyQuant指数数据-{symbol}", save_path)
                return data
            else:
                print(f"未获取到指数 {symbol} 的数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取指数数据失败: {e}")
            return pd.DataFrame()
    
    def get_sector_stocks(
        self,
        sector: Annotated[str, "板块代码"],
        date: Annotated[str, "日期 YYYY-MM-DD"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取板块成分股"""
        try:
            # 获取板块成分股
            data = get_history_constituents(
                index=sector,
                trade_date=date,
                df=True
            )
            
            if data is not None and not data.empty:
                save_output(data, f"MyQuant板块成分股-{sector}", save_path)
                return data
            else:
                print(f"未获取到板块 {sector} 的成分股")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取板块成分股失败: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(
        self,
        data: pd.DataFrame,
        indicators: Annotated[List[str], "技术指标列表：MA, MACD, RSI, BOLL, KDJ"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """计算技术指标"""
        try:
            result_data = data.copy()
            
            for indicator in indicators:
                if indicator.upper() == 'MA':
                    # 计算移动平均线
                    result_data['MA5'] = data['close'].rolling(window=5).mean()
                    result_data['MA10'] = data['close'].rolling(window=10).mean()
                    result_data['MA20'] = data['close'].rolling(window=20).mean()
                    
                elif indicator.upper() == 'MACD':
                    # 计算MACD
                    exp1 = data['close'].ewm(span=12).mean()
                    exp2 = data['close'].ewm(span=26).mean()
                    result_data['MACD'] = exp1 - exp2
                    result_data['MACD_signal'] = result_data['MACD'].ewm(span=9).mean()
                    result_data['MACD_hist'] = result_data['MACD'] - result_data['MACD_signal']
                    
                elif indicator.upper() == 'RSI':
                    # 计算RSI
                    delta = data['close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    result_data['RSI'] = 100 - (100 / (1 + rs))
                    
                elif indicator.upper() == 'BOLL':
                    # 计算布林带
                    result_data['BOLL_MID'] = data['close'].rolling(window=20).mean()
                    std = data['close'].rolling(window=20).std()
                    result_data['BOLL_UPPER'] = result_data['BOLL_MID'] + 2 * std
                    result_data['BOLL_LOWER'] = result_data['BOLL_MID'] - 2 * std
            
            if save_path:
                save_output(result_data, "MyQuant技术指标", save_path)
            
            return result_data
            
        except Exception as e:
            print(f"计算技术指标失败: {e}")
            return data
    
    def create_backtest_strategy(
        self,
        strategy_code: Annotated[str, "策略代码"],
        start_date: Annotated[str, "回测开始日期"],
        end_date: Annotated[str, "回测结束日期"],
        initial_capital: Annotated[float, "初始资金"] = 1000000.0,
        save_path: SavePathType = None,
    ) -> Dict[str, Any]:
        """创建回测策略（需要在策略环境中运行）"""
        try:
            # 这里只是示例框架，实际需要在策略文件中实现
            strategy_info = {
                'strategy_code': strategy_code,
                'start_date': start_date,
                'end_date': end_date,
                'initial_capital': initial_capital,
                'status': 'created',
                'message': '策略已创建，需要在掘金终端中运行'
            }
            
            if save_path:
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(strategy_info, f, ensure_ascii=False, indent=2)
            
            return strategy_info
            
        except Exception as e:
            print(f"创建策略失败: {e}")
            return {}
    
    def get_trading_calendar(
        self,
        exchange: Annotated[str, "交易所代码"] = 'SHSE',
        start_date: Annotated[str, "开始日期"] = None,
        end_date: Annotated[str, "结束日期"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取交易日历"""
        try:
            # 获取交易日历
            calendar = get_trading_dates(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date
            )
            
            if calendar:
                df = pd.DataFrame({'trading_date': calendar})
                save_output(df, f"MyQuant交易日历-{exchange}", save_path)
                return df
            else:
                print("未获取到交易日历")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取交易日历失败: {e}")
            return pd.DataFrame()
    
    def get_dividend_data(
        self,
        symbol: Annotated[str, "股票代码"],
        start_date: Annotated[str, "开始日期"] = None,
        end_date: Annotated[str, "结束日期"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取分红数据"""
        try:
            # 获取分红数据
            data = get_share_transformation(
                symbols=symbol,
                start_date=start_date,
                end_date=end_date,
                df=True
            )
            
            if data is not None and not data.empty:
                save_output(data, f"MyQuant分红数据-{symbol}", save_path)
                return data
            else:
                print(f"未获取到 {symbol} 的分红数据")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"获取分红数据失败: {e}")
            return pd.DataFrame()


# 创建实例的便捷函数
def create_myquant_utils(token: Optional[str] = None, mode: str = 'cloud') -> 'MyQuantUtils':
    """创建 MyQuantUtils 实例的便捷函数"""
    return MyQuantUtils(token=token, mode=mode)
