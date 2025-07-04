from typing import Annotated, Dict, Any
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import json
from .myquant_gm_utils import MyQuantGMUtils
from .config import get_config, DATA_DIR


def get_myquant_stock_data(
    symbol: Annotated[str, "股票代码"],
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
    look_back_days: Annotated[int, "回看天数"],
    frequency: Annotated[str, "数据频率：1d, 1h, 30m, 15m, 5m, 1m"] = "1d",
    fields: Annotated[str, "数据字段，逗号分隔"] = "open,high,low,close,volume",
) -> str:
    """获取MyQuant股票历史数据"""
    try:
        # 计算开始日期
        curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
        start_date = start_date_obj.strftime("%Y-%m-%d")
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 获取数据
        df = myquant_utils.get_stock_data(symbol, start_date, curr_date, frequency, fields)
        
        if df.empty:
            return f"未获取到 {symbol} 从 {start_date} 到 {curr_date} 的{frequency}数据"
        
        # 格式化输出
        result_str = f"## MyQuant {symbol} {frequency} 股票数据 ({start_date} 到 {curr_date}):\n\n"
        
        # 转换日期格式
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        elif 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        result_str += df.to_string(index=False)
        result_str += f"\n\n数据说明：频率为{frequency}，字段包含{fields}"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant股票数据失败: {str(e)}"


def get_myquant_financial_data(
    symbol: Annotated[str, "股票代码"],
    report_type: Annotated[str, "报表类型：income, balance, cashflow"] = "income",
    period: Annotated[str, "报告期：annual, quarterly"] = "annual",
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"] = None,
    look_back_days: Annotated[int, "回看天数"] = 365,
) -> str:
    """获取MyQuant财务数据"""
    try:
        # 计算日期范围
        if curr_date:
            curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
            start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = curr_date
        else:
            start_date = None
            end_date = None
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 获取财务数据
        df = myquant_utils.get_financial_data(symbol, report_type, period, start_date, end_date)
        
        if df.empty:
            return f"未获取到 {symbol} 的{report_type}财务数据"
        
        # 格式化输出
        report_names = {
            "income": "利润表",
            "balance": "资产负债表", 
            "cashflow": "现金流量表"
        }
        report_name = report_names.get(report_type, report_type)
        
        result_str = f"## MyQuant {symbol} {report_name} ({period}):\n\n"
        
        # 显示最近几期的数据
        for idx, row in df.head(5).iterrows():
            result_str += f"### 报告期 {row.get('period', 'N/A')}:\n"
            for col in df.columns:
                if col != 'period' and pd.notna(row[col]):
                    result_str += f"{col}: {row[col]}\n"
            result_str += "\n"
        
        result_str += f"共获取到 {len(df)} 期财务数据"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant财务数据失败: {str(e)}"


def get_myquant_technical_indicators(
    symbol: Annotated[str, "股票代码"],
    indicators: Annotated[str, "技术指标，逗号分隔：MA,MACD,RSI,BOLL,KDJ"],
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
    look_back_days: Annotated[int, "回看天数"],
    frequency: Annotated[str, "数据频率：1d, 1h, 30m, 15m, 5m, 1m"] = "1d",
) -> str:
    """获取MyQuant技术指标数据"""
    try:
        # 计算开始日期
        curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
        start_date = start_date_obj.strftime("%Y-%m-%d")
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 先获取股票数据，然后计算技术指标
        stock_data = myquant_utils.get_stock_data(symbol, start_date, curr_date, frequency, "open,high,low,close,volume")
        
        if stock_data.empty:
            return f"未获取到 {symbol} 的股票数据，无法计算技术指标"
        
        # 计算技术指标
        indicator_list = [ind.strip() for ind in indicators.split(',')]
        df = myquant_utils.calculate_technical_indicators(stock_data, indicator_list)
        
        if df.empty:
            return f"未获取到 {symbol} 从 {start_date} 到 {curr_date} 的技术指标数据"
        
        # 格式化输出
        result_str = f"## MyQuant {symbol} 技术指标 ({start_date} 到 {curr_date}):\n\n"
        result_str += f"指标: {indicators}\n"
        result_str += f"频率: {frequency}\n\n"
        
        # 转换日期格式
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        elif 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        result_str += df.to_string(index=False)
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant技术指标失败: {str(e)}"


def get_myquant_market_data(
    data_type: Annotated[str, "数据类型：index, sector, concept"] = "index",
    symbol: Annotated[str, "代码"] = None,
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"] = None,
    look_back_days: Annotated[int, "回看天数"] = 30,
) -> str:
    """获取MyQuant市场数据（指数、板块、概念等）"""
    try:
        # 计算日期范围
        if curr_date:
            curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
            start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = curr_date
        else:
            start_date = None
            end_date = None
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 根据数据类型获取相应数据
        if data_type == "index" and symbol:
            df = myquant_utils.get_index_data(symbol, start_date, end_date)
        elif data_type == "sector" and symbol:
            df = myquant_utils.get_sector_stocks(symbol, curr_date)
        else:
            # 获取股票列表作为默认市场数据
            df = myquant_utils.get_stock_list()
        
        if df.empty:
            return f"未获取到{data_type}市场数据"
        
        # 格式化输出
        data_type_names = {
            "index": "指数",
            "sector": "板块",
            "concept": "概念"
        }
        data_type_name = data_type_names.get(data_type, data_type)
        
        result_str = f"## MyQuant {data_type_name}数据"
        if symbol:
            result_str += f" ({symbol})"
        if start_date and end_date:
            result_str += f" ({start_date} 到 {end_date})"
        result_str += ":\n\n"
        
        result_str += df.to_string(index=False)
        result_str += f"\n\n共获取到 {len(df)} 条{data_type_name}数据"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant市场数据失败: {str(e)}"


def get_myquant_news_sentiment(
    symbol: Annotated[str, "股票代码"] = None,
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"] = None,
    look_back_days: Annotated[int, "回看天数"] = 7,
    source: Annotated[str, "新闻源：all, sina, eastmoney, cnstock"] = "all",
) -> str:
    """获取MyQuant新闻情感分析数据"""
    try:
        # 计算日期范围
        if curr_date:
            curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
            start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = curr_date
        else:
            start_date = None
            end_date = None
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 注意：gm库本身不提供新闻情感分析，这里返回提示信息
        return f"MyQuant (gm库) 暂不支持新闻情感分析功能，建议使用其他数据源"
        
        if df.empty:
            target = symbol if symbol else "全市场"
            return f"未获取到{target}的新闻情感数据"
        
        # 格式化输出
        target = symbol if symbol else "全市场"
        result_str = f"## MyQuant {target} 新闻情感分析"
        if start_date and end_date:
            result_str += f" ({start_date} 到 {end_date})"
        result_str += f" - 来源: {source}:\n\n"
        
        # 显示前20条新闻情感数据
        for idx, row in df.head(20).iterrows():
            title = row.get('title', '无标题')
            sentiment = row.get('sentiment', 'N/A')
            score = row.get('sentiment_score', 'N/A')
            date = row.get('publish_date', 'N/A')
            
            result_str += f"### {title} ({date})\n"
            result_str += f"情感: {sentiment}, 得分: {score}\n\n"
        
        result_str += f"共获取到 {len(df)} 条新闻情感数据"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant新闻情感数据失败: {str(e)}"


def get_myquant_factor_data(
    factor_name: Annotated[str, "因子名称"],
    symbols: Annotated[str, "股票代码列表，逗号分隔"] = None,
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"] = None,
    look_back_days: Annotated[int, "回看天数"] = 30,
) -> str:
    """获取MyQuant因子数据"""
    try:
        # 计算日期范围
        if curr_date:
            curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
            start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = curr_date
        else:
            start_date = None
            end_date = None
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 注意：gm库本身不直接提供因子数据，这里返回提示信息
        return f"MyQuant (gm库) 暂不直接支持因子数据功能，建议使用其他数据源或自行计算"
        
        if df.empty:
            return f"未获取到因子 {factor_name} 的数据"
        
        # 格式化输出
        result_str = f"## MyQuant 因子数据 - {factor_name}"
        if start_date and end_date:
            result_str += f" ({start_date} 到 {end_date})"
        result_str += ":\n\n"
        
        if symbols:
            result_str += f"股票范围: {symbols}\n\n"
        
        result_str += df.to_string(index=False)
        result_str += f"\n\n共获取到 {len(df)} 条因子数据"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant因子数据失败: {str(e)}"


def create_myquant_backtest(
    strategy_code: Annotated[str, "策略代码"],
    start_date: Annotated[str, "回测开始日期 YYYY-MM-DD"],
    end_date: Annotated[str, "回测结束日期 YYYY-MM-DD"],
    initial_capital: Annotated[float, "初始资金"] = 1000000.0,
    benchmark: Annotated[str, "基准指数"] = "000300.SH",
) -> str:
    """创建MyQuant策略回测"""
    try:
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 创建回测
        result = myquant_utils.create_backtest_strategy(strategy_code, start_date, end_date, initial_capital)
        
        if not result:
            return "创建回测失败"
        
        # 格式化输出
        result_str = f"## MyQuant 策略回测创建成功:\n\n"
        result_str += f"回测ID: {result.get('backtest_id', 'N/A')}\n"
        result_str += f"策略代码: {strategy_code}\n"
        result_str += f"回测期间: {start_date} 到 {end_date}\n"
        result_str += f"初始资金: {initial_capital:,.2f} 元\n"
        result_str += f"基准指数: {benchmark}\n"
        result_str += f"状态: {result.get('status', 'N/A')}\n"
        
        if 'message' in result:
            result_str += f"消息: {result['message']}\n"
        
        return result_str
        
    except Exception as e:
        return f"创建MyQuant回测失败: {str(e)}"


def get_myquant_backtest_result(
    backtest_id: Annotated[str, "回测ID"],
) -> str:
    """获取MyQuant回测结果"""
    try:
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 注意：gm库需要在策略环境中运行回测，这里返回提示信息
        return f"MyQuant (gm库) 回测需要在掘金终端策略环境中运行，无法直接获取回测结果"
        
        if not result:
            return f"未获取到回测ID {backtest_id} 的结果"
        
        # 格式化输出
        result_str = f"## MyQuant 回测结果 (ID: {backtest_id}):\n\n"
        
        # 显示基本信息
        if 'basic_info' in result:
            basic_info = result['basic_info']
            result_str += "### 基本信息:\n"
            for key, value in basic_info.items():
                result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        # 显示收益指标
        if 'performance' in result:
            performance = result['performance']
            result_str += "### 收益指标:\n"
            for key, value in performance.items():
                if isinstance(value, float):
                    result_str += f"{key}: {value:.4f}\n"
                else:
                    result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        # 显示风险指标
        if 'risk' in result:
            risk = result['risk']
            result_str += "### 风险指标:\n"
            for key, value in risk.items():
                if isinstance(value, float):
                    result_str += f"{key}: {value:.4f}\n"
                else:
                    result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        # 显示交易统计
        if 'trade_stats' in result:
            trade_stats = result['trade_stats']
            result_str += "### 交易统计:\n"
            for key, value in trade_stats.items():
                result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant回测结果失败: {str(e)}"


def get_myquant_portfolio_analysis(
    holdings: Annotated[str, "持仓信息，格式：股票代码1:权重1,股票代码2:权重2"],
    benchmark: Annotated[str, "基准指数"] = "000300.SH",
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"] = None,
    look_back_days: Annotated[int, "分析周期天数"] = 252,
) -> str:
    """获取MyQuant组合分析结果"""
    try:
        # 解析持仓信息
        holdings_dict = {}
        for item in holdings.split(','):
            if ':' in item:
                symbol, weight = item.strip().split(':')
                holdings_dict[symbol.strip()] = float(weight.strip())
        
        if not holdings_dict:
            return "持仓信息格式错误，请使用格式：股票代码1:权重1,股票代码2:权重2"
        
        # 计算日期范围
        if curr_date:
            curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
            start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = curr_date
        else:
            start_date = None
            end_date = None
        
        # 初始化MyQuant工具
        myquant_utils = MyQuantGMUtils()
        
        # 获取组合分析
        result = myquant_utils.get_portfolio_analysis(holdings_dict, benchmark, start_date, end_date)
        
        if not result:
            return "未获取到组合分析结果"
        
        # 格式化输出
        result_str = f"## MyQuant 组合分析结果:\n\n"
        
        # 显示持仓信息
        result_str += "### 持仓构成:\n"
        for symbol, weight in holdings_dict.items():
            result_str += f"{symbol}: {weight:.2%}\n"
        result_str += f"基准指数: {benchmark}\n"
        if start_date and end_date:
            result_str += f"分析期间: {start_date} 到 {end_date}\n"
        result_str += "\n"
        
        # 显示收益分析
        if 'return_analysis' in result:
            return_analysis = result['return_analysis']
            result_str += "### 收益分析:\n"
            for key, value in return_analysis.items():
                if isinstance(value, float):
                    result_str += f"{key}: {value:.4f}\n"
                else:
                    result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        # 显示风险分析
        if 'risk_analysis' in result:
            risk_analysis = result['risk_analysis']
            result_str += "### 风险分析:\n"
            for key, value in risk_analysis.items():
                if isinstance(value, float):
                    result_str += f"{key}: {value:.4f}\n"
                else:
                    result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        # 显示归因分析
        if 'attribution' in result:
            attribution = result['attribution']
            result_str += "### 归因分析:\n"
            for key, value in attribution.items():
                if isinstance(value, float):
                    result_str += f"{key}: {value:.4f}\n"
                else:
                    result_str += f"{key}: {value}\n"
            result_str += "\n"
        
        return result_str
        
    except Exception as e:
        return f"获取MyQuant组合分析失败: {str(e)}"
