import tushare as ts
import pandas as pd
from typing import Annotated, Optional
from datetime import datetime, timedelta
import os
from .config import get_config
from .utils import save_output, SavePathType


class TushareUtils:
    """Tushare 数据接口工具类，提供股票、财务、新闻等数据获取功能"""
    
    def __init__(self, token: Optional[str] = None):
        """初始化 Tushare 接口
        
        Args:
            token: Tushare API token，如果不提供则从配置中获取
        """
        if token:
            ts.set_token(token)
        else:
            config = get_config()
            if 'tushare_token' in config:
                ts.set_token(config['tushare_token'])
        
        self.pro = ts.pro_api()
    
    def get_stock_basic(
        self,
        exchange: Annotated[str, "交易所代码：SSE上交所 SZSE深交所"] = None,
        list_status: Annotated[str, "上市状态：L上市 D退市 P暂停上市"] = 'L',
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取基础信息数据，包括股票代码、名称、上市日期、退市日期等"""
        try:
            df = self.pro.stock_basic(
                exchange=exchange,
                list_status=list_status,
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )
            save_output(df, "Tushare股票基础信息", save_path)
            return df
        except Exception as e:
            print(f"获取股票基础信息失败: {e}")
            return pd.DataFrame()
    
    def get_daily_data(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        start_date: Annotated[str, "开始日期 YYYYMMDD"],
        end_date: Annotated[str, "结束日期 YYYYMMDD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取股票日线行情数据"""
        try:
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )
            # 按日期排序
            df = df.sort_values('trade_date')
            save_output(df, f"Tushare日线数据-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取日线数据失败: {e}")
            return pd.DataFrame()
    
    def get_weekly_data(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        start_date: Annotated[str, "开始日期 YYYYMMDD"],
        end_date: Annotated[str, "结束日期 YYYYMMDD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取股票周线行情数据"""
        try:
            df = self.pro.weekly(
                ts_code=ts_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )
            df = df.sort_values('trade_date')
            save_output(df, f"Tushare周线数据-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取周线数据失败: {e}")
            return pd.DataFrame()
    
    def get_monthly_data(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        start_date: Annotated[str, "开始日期 YYYYMMDD"],
        end_date: Annotated[str, "结束日期 YYYYMMDD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取股票月线行情数据"""
        try:
            df = self.pro.monthly(
                ts_code=ts_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )
            df = df.sort_values('trade_date')
            save_output(df, f"Tushare月线数据-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取月线数据失败: {e}")
            return pd.DataFrame()
    
    def get_income_statement(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        period: Annotated[str, "报告期 YYYYMMDD"],
        report_type: Annotated[str, "报告类型：1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表 6母公司报表 7母公司单季表 8 母公司调整单季表 9母公司调整表 10母公司调整前报表 11调整前合并报表"] = '1',
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取上市公司利润表数据"""
        try:
            df = self.pro.income(
                ts_code=ts_code,
                period=period.replace('-', ''),
                report_type=report_type,
                fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
            )
            save_output(df, f"Tushare利润表-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取利润表数据失败: {e}")
            return pd.DataFrame()
    
    def get_balance_sheet(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        period: Annotated[str, "报告期 YYYYMMDD"],
        report_type: Annotated[str, "报告类型"] = '1',
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取上市公司资产负债表数据"""
        try:
            df = self.pro.balancesheet(
                ts_code=ts_code,
                period=period.replace('-', ''),
                report_type=report_type,
                fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables'
            )
            save_output(df, f"Tushare资产负债表-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取资产负债表数据失败: {e}")
            return pd.DataFrame()
    
    def get_cashflow_statement(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        period: Annotated[str, "报告期 YYYYMMDD"],
        report_type: Annotated[str, "报告类型"] = '1',
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取上市公司现金流量表数据"""
        try:
            df = self.pro.cashflow(
                ts_code=ts_code,
                period=period.replace('-', ''),
                report_type=report_type,
                fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ'
            )
            save_output(df, f"Tushare现金流量表-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取现金流量表数据失败: {e}")
            return pd.DataFrame()
    
    def get_financial_indicators(
        self,
        ts_code: Annotated[str, "股票代码，如000001.SZ"],
        period: Annotated[str, "报告期 YYYYMMDD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取上市公司财务指标数据"""
        try:
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                period=period.replace('-', ''),
                fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag'
            )
            save_output(df, f"Tushare财务指标-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取财务指标数据失败: {e}")
            return pd.DataFrame()
    
    def get_news(
        self,
        src: Annotated[str, "新闻来源：sina新浪 10jqka同花顺 eastmoney东方财富 yuncaijing云财经"] = 'sina',
        start_date: Annotated[str, "开始日期 YYYY-MM-DD"] = None,
        end_date: Annotated[str, "结束日期 YYYY-MM-DD"] = None,
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取财经新闻数据"""
        try:
            df = self.pro.news(
                src=src,
                start_date=start_date.replace('-', '') if start_date else None,
                end_date=end_date.replace('-', '') if end_date else None,
                fields='datetime,content,title,channels'
            )
            save_output(df, f"Tushare财经新闻-{src}", save_path)
            return df
        except Exception as e:
            print(f"获取财经新闻失败: {e}")
            return pd.DataFrame()
    
    def get_concept_detail(
        self,
        id: Annotated[str, "概念分类ID"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取概念股分类明细数据"""
        try:
            df = self.pro.concept_detail(
                id=id,
                fields='id,concept_name,ts_code,name,in_date,out_date'
            )
            save_output(df, f"Tushare概念股明细-{id}", save_path)
            return df
        except Exception as e:
            print(f"获取概念股明细失败: {e}")
            return pd.DataFrame()
    
    def get_index_basic(
        self,
        market: Annotated[str, "交易所或服务商：SSE上交所 SZSE深交所 CICC中金所 SW申万"] = 'SSE',
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取指数基础信息"""
        try:
            df = self.pro.index_basic(
                market=market,
                fields='ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date'
            )
            save_output(df, f"Tushare指数基础信息-{market}", save_path)
            return df
        except Exception as e:
            print(f"获取指数基础信息失败: {e}")
            return pd.DataFrame()
    
    def get_index_daily(
        self,
        ts_code: Annotated[str, "指数代码"],
        start_date: Annotated[str, "开始日期 YYYY-MM-DD"],
        end_date: Annotated[str, "结束日期 YYYY-MM-DD"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """获取指数日线行情"""
        try:
            df = self.pro.index_daily(
                ts_code=ts_code,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                fields='ts_code,trade_date,close,open,high,low,pre_close,change,pct_chg,vol,amount'
            )
            df = df.sort_values('trade_date')
            save_output(df, f"Tushare指数日线-{ts_code}", save_path)
            return df
        except Exception as e:
            print(f"获取指数日线数据失败: {e}")
            return pd.DataFrame()
