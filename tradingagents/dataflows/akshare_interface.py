from typing import Annotated
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from .akshare_utils import AkshareUtils
from .config import get_config, DATA_DIR


def get_akshare_stock_data(
    symbol: Annotated[str, "股票代码，如000001"],
    curr_date: Annotated[str, "当前日期 YYYY-MM-DD"],
    look_back_days: Annotated[int, "回看天数"],
    period: Annotated[str, "周期：daily, weekly, monthly"] = "daily",
    adjust: Annotated[str, "复权类型：qfq前复权 hfq后复权 空字符串不复权"] = "",
) -> str:
    """获取Akshare A股历史行情数据"""
    try:
        # 计算开始日期
        curr_date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date_obj = curr_date_obj - relativedelta(days=look_back_days)
        start_date = start_date_obj.strftime("%Y-%m-%d")
        
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取数据
        df = akshare_utils.get_stock_zh_a_hist(
            symbol=symbol,
            period=period,
            start_date=start_date,
            end_date=curr_date,
            adjust=adjust
        )
        
        if df.empty:
            return f"未获取到 {symbol} 从 {start_date} 到 {curr_date} 的{period}行情数据"
        
        # 格式化输出
        adjust_text = {"qfq": "前复权", "hfq": "后复权", "": "不复权"}.get(adjust, adjust)
        result_str = f"## Akshare {symbol} {period} {adjust_text}行情数据 ({start_date} 到 {curr_date}):\n\n"
        
        # 转换日期格式
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        # 选择主要字段并重命名
        if period == "daily":
            display_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        else:
            display_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']
        
        # 只选择存在的列
        available_columns = [col for col in display_columns if col in df.columns]
        display_df = df[available_columns].copy()
        
        result_str += display_df.to_string(index=False)
        result_str += f"\n\n数据说明：成交量单位为股，成交额单位为元"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare股票数据失败: {str(e)}"


def get_akshare_stock_info(
    symbol: Annotated[str, "股票代码，如000001"],
) -> str:
    """获取Akshare个股基本信息"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取个股信息
        df = akshare_utils.get_stock_individual_info_em(symbol)
        
        if df.empty:
            return f"未获取到 {symbol} 的个股信息"
        
        # 格式化输出
        result_str = f"## Akshare {symbol} 个股信息:\n\n"
        
        # 显示关键信息
        for idx, row in df.iterrows():
            item = row.get('item', '')
            value = row.get('value', '')
            result_str += f"{item}: {value}\n"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare个股信息失败: {str(e)}"


def get_akshare_financial_data(
    symbol: Annotated[str, "股票代码，如000001"],
    indicator: Annotated[str, "指标类型：资产负债表 利润表 现金流量表"] = "资产负债表",
    period: Annotated[str, "报告期：年报 中报 一季报 三季报"] = "年报",
) -> str:
    """获取Akshare同花顺财务数据"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取财务数据
        df = akshare_utils.get_stock_financial_abstract_ths(symbol, indicator, period)
        
        if df.empty:
            return f"未获取到 {symbol} 的{indicator}{period}数据"
        
        # 格式化输出
        result_str = f"## Akshare {symbol} {indicator} ({period}):\n\n"
        
        # 显示财务数据
        for idx, row in df.iterrows():
            for col in df.columns:
                if pd.notna(row[col]):
                    result_str += f"{col}: {row[col]}\n"
            result_str += "\n"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare财务数据失败: {str(e)}"


def get_akshare_financial_indicators(
    symbol: Annotated[str, "股票代码，如000001"],
) -> str:
    """获取Akshare财务分析指标"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取财务分析指标
        df = akshare_utils.get_stock_financial_analysis_indicator(symbol)
        
        if df.empty:
            return f"未获取到 {symbol} 的财务分析指标"
        
        # 格式化输出
        result_str = f"## Akshare {symbol} 财务分析指标:\n\n"
        
        # 显示最近几期的数据
        for idx, row in df.head(5).iterrows():
            result_str += f"### {row.get('报告期', 'N/A')}:\n"
            for col in df.columns:
                if col != '报告期' and pd.notna(row[col]):
                    result_str += f"{col}: {row[col]}\n"
            result_str += "\n"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare财务分析指标失败: {str(e)}"


def get_akshare_stock_news(
    symbol: Annotated[str, "股票代码，如000001"],
) -> str:
    """获取Akshare东方财富个股新闻"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取个股新闻
        df = akshare_utils.get_stock_news_em(symbol)
        
        if df.empty:
            return f"未获取到 {symbol} 的新闻数据"
        
        # 格式化输出
        result_str = f"## Akshare {symbol} 个股新闻:\n\n"
        
        # 显示前20条新闻
        for idx, row in df.head(20).iterrows():
            title = row.get('新闻标题', row.get('标题', '无标题'))
            date = row.get('发布时间', row.get('时间', 'N/A'))
            content = row.get('新闻内容', row.get('内容', ''))
            
            result_str += f"### {title} ({date})\n"
            if content and len(content) > 200:
                content = content[:200] + "..."
            result_str += f"{content}\n\n"
        
        result_str += f"共获取到 {len(df)} 条新闻"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare个股新闻失败: {str(e)}"


def get_akshare_stock_comment(
    symbol: Annotated[str, "股票代码，如000001"],
) -> str:
    """获取Akshare东方财富千股千评"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取千股千评
        df = akshare_utils.get_stock_comment_em(symbol)
        
        if df.empty:
            return f"未获取到 {symbol} 的千股千评数据"
        
        # 格式化输出
        result_str = f"## Akshare {symbol} 千股千评:\n\n"
        
        # 显示评价数据
        for idx, row in df.iterrows():
            for col in df.columns:
                if pd.notna(row[col]):
                    result_str += f"{col}: {row[col]}\n"
            result_str += "\n"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare千股千评失败: {str(e)}"


def get_akshare_concept_stocks(
    concept_name: Annotated[str, "概念板块名称"],
) -> str:
    """获取Akshare概念板块成分股"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 先获取概念板块列表
        concept_df = akshare_utils.get_stock_board_concept_name_em()
        
        if concept_df.empty:
            return "未获取到概念板块列表"
        
        # 查找匹配的概念板块代码
        matched_concepts = concept_df[concept_df['板块名称'].str.contains(concept_name, na=False)]
        
        if matched_concepts.empty:
            return f"未找到包含 '{concept_name}' 的概念板块"
        
        # 获取第一个匹配的概念板块成分股
        concept_code = matched_concepts.iloc[0]['板块代码']
        concept_full_name = matched_concepts.iloc[0]['板块名称']
        
        # 获取成分股
        stocks_df = akshare_utils.get_stock_board_concept_cons_em(concept_code)
        
        if stocks_df.empty:
            return f"未获取到概念板块 {concept_full_name} 的成分股"
        
        # 格式化输出
        result_str = f"## Akshare 概念板块 {concept_full_name} 成分股:\n\n"
        
        # 选择主要字段
        display_columns = ['代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收']
        available_columns = [col for col in display_columns if col in stocks_df.columns]
        
        if available_columns:
            display_df = stocks_df[available_columns].head(50)  # 显示前50只股票
            result_str += display_df.to_string(index=False)
        else:
            result_str += stocks_df.to_string(index=False)
        
        result_str += f"\n\n共包含 {len(stocks_df)} 只成分股"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare概念板块成分股失败: {str(e)}"


def get_akshare_industry_stocks(
    industry_name: Annotated[str, "行业板块名称"],
) -> str:
    """获取Akshare行业板块成分股"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 先获取行业板块列表
        industry_df = akshare_utils.get_stock_board_industry_name_em()
        
        if industry_df.empty:
            return "未获取到行业板块列表"
        
        # 查找匹配的行业板块代码
        matched_industries = industry_df[industry_df['板块名称'].str.contains(industry_name, na=False)]
        
        if matched_industries.empty:
            return f"未找到包含 '{industry_name}' 的行业板块"
        
        # 获取第一个匹配的行业板块成分股
        industry_code = matched_industries.iloc[0]['板块代码']
        industry_full_name = matched_industries.iloc[0]['板块名称']
        
        # 获取成分股
        stocks_df = akshare_utils.get_stock_board_industry_cons_em(industry_code)
        
        if stocks_df.empty:
            return f"未获取到行业板块 {industry_full_name} 的成分股"
        
        # 格式化输出
        result_str = f"## Akshare 行业板块 {industry_full_name} 成分股:\n\n"
        
        # 选择主要字段
        display_columns = ['代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收']
        available_columns = [col for col in display_columns if col in stocks_df.columns]
        
        if available_columns:
            display_df = stocks_df[available_columns].head(50)  # 显示前50只股票
            result_str += display_df.to_string(index=False)
        else:
            result_str += stocks_df.to_string(index=False)
        
        result_str += f"\n\n共包含 {len(stocks_df)} 只成分股"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare行业板块成分股失败: {str(e)}"


def get_akshare_macro_data(
    indicator: Annotated[str, "宏观指标：GDP CPI PPI PMI"] = "GDP",
) -> str:
    """获取Akshare中国宏观经济数据"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 根据指标类型获取数据
        if indicator.upper() == "GDP":
            df = akshare_utils.get_macro_china_gdp()
            indicator_name = "GDP"
        elif indicator.upper() == "CPI":
            df = akshare_utils.get_macro_china_cpi()
            indicator_name = "CPI"
        elif indicator.upper() == "PPI":
            df = akshare_utils.get_macro_china_ppi()
            indicator_name = "PPI"
        elif indicator.upper() == "PMI":
            df = akshare_utils.get_macro_china_pmi()
            indicator_name = "PMI"
        else:
            return f"不支持的宏观指标: {indicator}，请使用 GDP、CPI、PPI 或 PMI"
        
        if df.empty:
            return f"未获取到中国{indicator_name}数据"
        
        # 格式化输出
        result_str = f"## Akshare 中国{indicator_name}数据:\n\n"
        
        # 显示最近的数据
        display_df = df.head(20)
        result_str += display_df.to_string(index=False)
        
        result_str += f"\n\n共获取到 {len(df)} 条{indicator_name}数据记录"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare宏观数据失败: {str(e)}"


def get_akshare_hot_stocks() -> str:
    """获取Akshare热门股票排行"""
    try:
        # 初始化Akshare工具
        akshare_utils = AkshareUtils()
        
        # 获取热门股票排行
        df = akshare_utils.get_stock_hot_rank_em()
        
        if df.empty:
            return "未获取到热门股票排行数据"
        
        # 格式化输出
        result_str = f"## Akshare 热门股票排行:\n\n"
        
        # 显示前30只热门股票
        display_df = df.head(30)
        result_str += display_df.to_string(index=False)
        
        result_str += f"\n\n共获取到 {len(df)} 只热门股票"
        
        return result_str
        
    except Exception as e:
        return f"获取Akshare热门股票排行失败: {str(e)}"
