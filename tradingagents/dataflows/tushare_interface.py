from typing import Annotated
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from .tushare_utils import TushareUtils
from .config import get_config, DATA_DIR


def get_tushare_daily_data(
    ts_code: Annotated[str, "股票代码，如000001.SZ"],
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
    look_back_days: Annotated[int, "回看天数"],
) -> str:
    """获取Tushare股票日线数据"""
    try:
        # 计算开始日期
        curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
        start_date = start_date_obj.strftime("%Y-%m-%d")
        
        # 初始化Tushare工具
        tushare_utils = TushareUtils()
        
        # 获取数据
        df = tushare_utils.get_daily_data(ts_code, start_date, curr_date)
        
        if df.empty:
            return f"未获取到 {ts_code} 从 {start_date} 到 {curr_date} 的日线数据"
        
        # 格式化输出
        result_str = f"## Tushare {ts_code} 日线数据 ({start_date} 到 {curr_date}):\n\n"
        
        # 转换为易读格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        
        # 选择主要字段
        display_df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'pct_chg']].copy()
        display_df.columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额', '涨跌幅%']
        
        result_str += display_df.to_string(index=False)
        result_str += f"\n\n数据说明：成交量单位为手，成交额单位为千元，涨跌幅为百分比"
        
        return result_str
        
    except Exception as e:
        return f"获取Tushare日线数据失败: {str(e)}"


def get_tushare_financial_data(
    ts_code: Annotated[str, "股票代码，如000001.SZ"],
    report_type: Annotated[str, "报表类型：income利润表 balance资产负债表 cashflow现金流量表"],
    period: Annotated[str, "报告期 YYYY-MM-DD"],
) -> str:
    """获取Tushare财务报表数据"""
    try:
        # 初始化Tushare工具
        tushare_utils = TushareUtils()
        
        # 根据报表类型获取数据
        if report_type == "income":
            df = tushare_utils.get_income_statement(ts_code, period)
            report_name = "利润表"
        elif report_type == "balance":
            df = tushare_utils.get_balance_sheet(ts_code, period)
            report_name = "资产负债表"
        elif report_type == "cashflow":
            df = tushare_utils.get_cashflow_statement(ts_code, period)
            report_name = "现金流量表"
        else:
            return f"不支持的报表类型: {report_type}，请使用 income、balance 或 cashflow"
        
        if df.empty:
            return f"未获取到 {ts_code} {period} 期的{report_name}数据"
        
        # 格式化输出
        result_str = f"## Tushare {ts_code} {report_name} ({period}):\n\n"
        
        # 获取最新的一条记录
        latest_record = df.iloc[0]
        
        # 显示关键财务指标
        if report_type == "income":
            key_fields = {
                'total_revenue': '营业总收入',
                'revenue': '营业收入', 
                'operate_profit': '营业利润',
                'total_profit': '利润总额',
                'n_income': '净利润',
                'basic_eps': '基本每股收益',
                'diluted_eps': '稀释每股收益'
            }
        elif report_type == "balance":
            key_fields = {
                'total_assets': '资产总计',
                'total_cur_assets': '流动资产合计',
                'total_nca': '非流动资产合计',
                'total_liab': '负债合计',
                'total_cur_liab': '流动负债合计',
                'total_hldr_eqy_inc_min_int': '所有者权益合计'
            }
        else:  # cashflow
            key_fields = {
                'n_cashflow_act': '经营活动现金流量净额',
                'n_cashflow_inv_act': '投资活动现金流量净额',
                'n_cash_flows_fnc_act': '筹资活动现金流量净额',
                'n_incr_cash_cash_equ': '现金及现金等价物净增加额'
            }
        
        for field, name in key_fields.items():
            if field in latest_record and pd.notna(latest_record[field]):
                value = latest_record[field]
                if isinstance(value, (int, float)):
                    # 转换为万元
                    value_wan = value / 10000
                    result_str += f"{name}: {value_wan:,.2f} 万元\n"
                else:
                    result_str += f"{name}: {value}\n"
        
        result_str += f"\n报告期: {latest_record.get('end_date', 'N/A')}"
        result_str += f"\n公告日期: {latest_record.get('ann_date', 'N/A')}"
        result_str += f"\n数据说明：金额单位为万元"
        
        return result_str
        
    except Exception as e:
        return f"获取Tushare财务数据失败: {str(e)}"


def get_tushare_financial_indicators(
    ts_code: Annotated[str, "股票代码，如000001.SZ"],
    period: Annotated[str, "报告期 YYYY-MM-DD"],
) -> str:
    """获取Tushare财务指标数据"""
    try:
        # 初始化Tushare工具
        tushare_utils = TushareUtils()
        
        # 获取财务指标数据
        df = tushare_utils.get_financial_indicators(ts_code, period)
        
        if df.empty:
            return f"未获取到 {ts_code} {period} 期的财务指标数据"
        
        # 格式化输出
        result_str = f"## Tushare {ts_code} 财务指标 ({period}):\n\n"
        
        # 获取最新的一条记录
        latest_record = df.iloc[0]
        
        # 显示关键财务指标
        key_indicators = {
            'eps': '每股收益',
            'roe': 'ROE净资产收益率(%)',
            'roa': 'ROA总资产收益率(%)',
            'gross_margin': '销售毛利率(%)',
            'netprofit_margin': '销售净利率(%)',
            'current_ratio': '流动比率',
            'quick_ratio': '速动比率',
            'debt_to_assets': '资产负债率(%)',
            'bps': '每股净资产',
            'ocfps': '每股经营现金流',
            'total_revenue_ps': '每股营业总收入'
        }
        
        for field, name in key_indicators.items():
            if field in latest_record and pd.notna(latest_record[field]):
                value = latest_record[field]
                if isinstance(value, (int, float)):
                    if '%' in name:
                        result_str += f"{name}: {value:.2f}%\n"
                    else:
                        result_str += f"{name}: {value:.4f}\n"
                else:
                    result_str += f"{name}: {value}\n"
        
        result_str += f"\n报告期: {latest_record.get('end_date', 'N/A')}"
        result_str += f"\n公告日期: {latest_record.get('ann_date', 'N/A')}"
        
        return result_str
        
    except Exception as e:
        return f"获取Tushare财务指标失败: {str(e)}"


def get_tushare_news(
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
    look_back_days: Annotated[int, "回看天数"],
    src: Annotated[str, "新闻源：sina新浪 10jqka同花顺 eastmoney东方财富"] = 'sina',
) -> str:
    """获取Tushare财经新闻"""
    try:
        # 计算开始日期
        curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
        start_date = start_date_obj.strftime("%Y-%m-%d")
        
        # 初始化Tushare工具
        tushare_utils = TushareUtils()
        
        # 获取新闻数据
        df = tushare_utils.get_news(src, start_date, curr_date)
        
        if df.empty:
            return f"未获取到 {src} 从 {start_date} 到 {curr_date} 的新闻数据"
        
        # 格式化输出
        result_str = f"## Tushare {src} 财经新闻 ({start_date} 到 {curr_date}):\n\n"
        
        # 按时间排序，显示最新的新闻
        df_sorted = df.sort_values('datetime', ascending=False)
        
        # 显示前20条新闻
        for idx, row in df_sorted.head(20).iterrows():
            result_str += f"### {row.get('title', '无标题')} ({row.get('datetime', 'N/A')})\n"
            content = row.get('content', '')
            if content and len(content) > 200:
                content = content[:200] + "..."
            result_str += f"{content}\n\n"
        
        result_str += f"共获取到 {len(df)} 条新闻"
        
        return result_str
        
    except Exception as e:
        return f"获取Tushare新闻失败: {str(e)}"


def get_tushare_index_data(
    ts_code: Annotated[str, "指数代码，如000001.SH"],
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
    look_back_days: Annotated[int, "回看天数"],
) -> str:
    """获取Tushare指数数据"""
    try:
        # 计算开始日期
        curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
        start_date = start_date_obj.strftime("%Y-%m-%d")
        
        # 初始化Tushare工具
        tushare_utils = TushareUtils()
        
        # 获取指数数据
        df = tushare_utils.get_index_daily(ts_code, start_date, curr_date)
        
        if df.empty:
            return f"未获取到指数 {ts_code} 从 {start_date} 到 {curr_date} 的数据"
        
        # 格式化输出
        result_str = f"## Tushare 指数 {ts_code} 数据 ({start_date} 到 {curr_date}):\n\n"
        
        # 转换为易读格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        
        # 选择主要字段
        display_df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'pct_chg']].copy()
        display_df.columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额', '涨跌幅%']
        
        result_str += display_df.to_string(index=False)
        result_str += f"\n\n数据说明：成交量单位为手，成交额单位为千元，涨跌幅为百分比"
        
        return result_str
        
    except Exception as e:
        return f"获取Tushare指数数据失败: {str(e)}"
