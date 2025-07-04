import akshare as ak
import pandas as pd
from typing import Annotated, Optional
from datetime import datetime, timedelta
import os
from .config import get_config
from .utils import save_output, SavePathType


class AkshareUtils:
    """Akshare 数据接口工具类，提供股票、期货、基金、新闻等数据获取功能"""
    
    def __init__(self):
        """初始化 Akshare 接口"""
        pass
    
    def get_stock_zh_a_hist(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        period: Annotated[str, "周期：daily, weekly, monthly"] = "daily",
        start_date: Annotated[str, "开始日期 YYYY-MM-DD"] = "19700101",
        end_date: Annotated[str, "结束日期 YYYY-MM-DD"] = "20500101",
        adjust: Annotated[str, "复权类型：qfq前复权 hfq后复权 空字符串不复权"] = "",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取A股历史行情数据"""
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust=adjust
            )
            save_output(df, f"Akshare A股历史行情-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取A股历史行情失败: {e}")
            return pd.DataFrame()
    
    def get_stock_info_a_code_name(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取A股股票代码和名称"""
        try:
            df = ak.stock_info_a_code_name()
            save_output(df, "Akshare A股代码名称", save_path)
            return df
        except Exception as e:
            print(f"获取A股代码名称失败: {e}")
            return pd.DataFrame()
    
    def get_stock_individual_info_em(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取个股信息"""
        try:
            df = ak.stock_individual_info_em(symbol=symbol)
            save_output(df, f"Akshare 个股信息-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取个股信息失败: {e}")
            return pd.DataFrame()
    
    def get_stock_financial_abstract_ths(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        indicator: Annotated[str, "指标类型：资产负债表 利润表 现金流量表"] = "资产负债表",
        period: Annotated[str, "报告期：年报 中报 一季报 三季报"] = "年报",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取同花顺财务摘要数据"""
        try:
            df = ak.stock_financial_abstract_ths(
                symbol=symbol,
                indicator=indicator,
                period=period
            )
            save_output(df, f"Akshare 财务摘要-{symbol}-{indicator}", save_path)
            return df
        except Exception as e:
            print(f"获取财务摘要失败: {e}")
            return pd.DataFrame()
    
    def get_stock_financial_analysis_indicator(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取财务分析指标"""
        try:
            df = ak.stock_financial_analysis_indicator(symbol=symbol)
            save_output(df, f"Akshare 财务分析指标-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取财务分析指标失败: {e}")
            return pd.DataFrame()
    
    def get_stock_news_em(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取东方财富个股新闻"""
        try:
            df = ak.stock_news_em(symbol=symbol)
            save_output(df, f"Akshare 个股新闻-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取个股新闻失败: {e}")
            return pd.DataFrame()
    
    def get_stock_comment_em(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取东方财富千股千评"""
        try:
            df = ak.stock_comment_em(symbol=symbol)
            save_output(df, f"Akshare 千股千评-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取千股千评失败: {e}")
            return pd.DataFrame()
    
    def get_stock_board_concept_name_em(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取概念板块名称"""
        try:
            df = ak.stock_board_concept_name_em()
            save_output(df, "Akshare 概念板块名称", save_path)
            return df
        except Exception as e:
            print(f"获取概念板块名称失败: {e}")
            return pd.DataFrame()
    
    def get_stock_board_concept_cons_em(
        self,
        symbol: Annotated[str, "概念板块代码"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取概念板块成分股"""
        try:
            df = ak.stock_board_concept_cons_em(symbol=symbol)
            save_output(df, f"Akshare 概念板块成分股-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取概念板块成分股失败: {e}")
            return pd.DataFrame()
    
    def get_stock_board_industry_name_em(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取行业板块名称"""
        try:
            df = ak.stock_board_industry_name_em()
            save_output(df, "Akshare 行业板块名称", save_path)
            return df
        except Exception as e:
            print(f"获取行业板块名称失败: {e}")
            return pd.DataFrame()
    
    def get_stock_board_industry_cons_em(
        self,
        symbol: Annotated[str, "行业板块代码"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取行业板块成分股"""
        try:
            df = ak.stock_board_industry_cons_em(symbol=symbol)
            save_output(df, f"Akshare 行业板块成分股-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取行业板块成分股失败: {e}")
            return pd.DataFrame()
    
    def get_index_zh_a_hist(
        self,
        symbol: Annotated[str, "指数代码，如000001"],
        period: Annotated[str, "周期：daily, weekly, monthly"] = "daily",
        start_date: Annotated[str, "开始日期 YYYY-MM-DD"] = "19700101",
        end_date: Annotated[str, "结束日期 YYYY-MM-DD"] = "20500101",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取A股指数历史行情"""
        try:
            df = ak.index_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', '')
            )
            save_output(df, f"Akshare A股指数历史行情-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取A股指数历史行情失败: {e}")
            return pd.DataFrame()
    
    def get_macro_china_gdp(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取中国GDP数据"""
        try:
            df = ak.macro_china_gdp()
            save_output(df, "Akshare 中国GDP", save_path)
            return df
        except Exception as e:
            print(f"获取中国GDP数据失败: {e}")
            return pd.DataFrame()
    
    def get_macro_china_cpi(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取中国CPI数据"""
        try:
            df = ak.macro_china_cpi()
            save_output(df, "Akshare 中国CPI", save_path)
            return df
        except Exception as e:
            print(f"获取中国CPI数据失败: {e}")
            return pd.DataFrame()
    
    def get_macro_china_ppi(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取中国PPI数据"""
        try:
            df = ak.macro_china_ppi()
            save_output(df, "Akshare 中国PPI", save_path)
            return df
        except Exception as e:
            print(f"获取中国PPI数据失败: {e}")
            return pd.DataFrame()
    
    def get_macro_china_pmi(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取中国PMI数据"""
        try:
            df = ak.macro_china_pmi()
            save_output(df, "Akshare 中国PMI", save_path)
            return df
        except Exception as e:
            print(f"获取中国PMI数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_hot_rank_em(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取东方财富热门股票排行"""
        try:
            df = ak.stock_hot_rank_em()
            save_output(df, "Akshare 热门股票排行", save_path)
            return df
        except Exception as e:
            print(f"获取热门股票排行失败: {e}")
            return pd.DataFrame()
    
    def get_stock_hot_rank_detail_em(
        self,
        symbol: Annotated[str, "股票代码，如000001"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取东方财富热门股票详情"""
        try:
            df = ak.stock_hot_rank_detail_em(symbol=symbol)
            save_output(df, f"Akshare 热门股票详情-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取热门股票详情失败: {e}")
            return pd.DataFrame()
    
    def get_fund_etf_category_sina(
        self,
        symbol: Annotated[str, "ETF类型：ETF基金 LOF基金 分级基金"] = "ETF基金",
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取新浪ETF基金数据"""
        try:
            df = ak.fund_etf_category_sina(symbol=symbol)
            save_output(df, f"Akshare ETF基金-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取ETF基金数据失败: {e}")
            return pd.DataFrame()
    
    def get_fund_etf_hist_sina(
        self,
        symbol: Annotated[str, "ETF代码"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取新浪ETF历史净值"""
        try:
            df = ak.fund_etf_hist_sina(symbol=symbol)
            save_output(df, f"Akshare ETF历史净值-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取ETF历史净值失败: {e}")
            return pd.DataFrame()
    
    def get_futures_main_sina(
        self,
        symbol: Annotated[str, "期货品种，如CU连续"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取新浪期货主力合约数据"""
        try:
            df = ak.futures_main_sina(symbol=symbol)
            save_output(df, f"Akshare 期货主力合约-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取期货主力合约数据失败: {e}")
            return pd.DataFrame()
    
    def get_bond_zh_us_rate(
        self,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取中美国债收益率"""
        try:
            df = ak.bond_zh_us_rate()
            save_output(df, "Akshare 中美国债收益率", save_path)
            return df
        except Exception as e:
            print(f"获取中美国债收益率失败: {e}")
            return pd.DataFrame()
    
    def get_currency_boc_safe(
        self,
        symbol: Annotated[str, "货币代码，如美元"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取中国银行外汇牌价"""
        try:
            df = ak.currency_boc_safe(symbol=symbol)
            save_output(df, f"Akshare 外汇牌价-{symbol}", save_path)
            return df
        except Exception as e:
            print(f"获取外汇牌价失败: {e}")
            return pd.DataFrame()
    
    def get_stock_margin_detail_szse(
        self,
        date: Annotated[str, "查询日期 YYYY-MM-DD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取深交所融资融券明细"""
        try:
            df = ak.stock_margin_detail_szse(date=date.replace('-', ''))
            save_output(df, f"Akshare 深交所融资融券-{date}", save_path)
            return df
        except Exception as e:
            print(f"获取深交所融资融券明细失败: {e}")
            return pd.DataFrame()
    
    def get_stock_margin_detail_sse(
        self,
        date: Annotated[str, "查询日期 YYYY-MM-DD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取上交所融资融券明细"""
        try:
            df = ak.stock_margin_detail_sse(date=date.replace('-', ''))
            save_output(df, f"Akshare 上交所融资融券-{date}", save_path)
            return df
        except Exception as e:
            print(f"获取上交所融资融券明细失败: {e}")
            return pd.DataFrame()
