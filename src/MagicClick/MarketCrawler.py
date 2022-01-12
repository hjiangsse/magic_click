#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 25 14:18:20 2021
# 本模块的功能是利用akshare和baostock提供的接口，获取各种维度的k线行情数据
#-------------------------------------------------------------------------------
import baostock as bs
import akshare as ak
import pandas as pd

from datetime import datetime
import MagicClick.CrawlerUtils as utils

#-------------------------------------------------------------------------------
#利用akshare提供的数据获取接口，获得A股票名称列表
#-------------------------------------------------------------------------------
def get_astock_list():
    a_stock_list = []
    a_sh_stocks_frame = ak.stock_info_sh_name_code()
    for code in a_sh_stocks_frame['公司代码']:
        a_stock_list.append("sh." + code)

    a_sz_stocks_frame = ak.stock_info_sz_name_code()
    for code in a_sz_stocks_frame['A股代码']:
        a_stock_list.append("sz." + code)
    
    return a_stock_list

#-------------------------------------------------------------------------------
# 得到一支股票的前复权数据
#-------------------------------------------------------------------------------
def get_day_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_day_k_data(code, data_columns, start_date, end_date, adjustflag='2')

#-------------------------------------------------------------------------------
# 得到一支股票的后复权数据
#-------------------------------------------------------------------------------
def get_day_k_data_aft_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_day_k_data(code, data_columns, start_date, end_date, adjustflag='1')

#-------------------------------------------------------------------------------
# 得到一支股票的未复权数据
#-------------------------------------------------------------------------------
def get_day_k_data_aft_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_day_k_data(code, data_columns, start_date, end_date, adjustflag='3')
