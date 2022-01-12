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
# 得到一支股票日k前复权数据
#-------------------------------------------------------------------------------
def get_day_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_day_k_data(code, columns, start_date, end_date, adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票日k后复权数据
#-------------------------------------------------------------------------------
def get_day_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_day_k_data(code, columns, start_date, end_date, adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票日k未复权数据
#-------------------------------------------------------------------------------
def get_day_k_data_no_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_day_k_data(code, columns, start_date, end_date, adjust_flag='3')

#-------------------------------------------------------------------------------
# 得到一支股票周k前复权数据
#-------------------------------------------------------------------------------
def get_week_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_week_or_month_k_data(code, columns, start_date, end_date, week_or_month='w', adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票周k后复权数据
#-------------------------------------------------------------------------------
def get_week_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_week_or_month_k_data(code, columns, start_date, end_date, week_or_month='w', adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票周k未复权数据
#-------------------------------------------------------------------------------
def get_week_k_data_no_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_week_or_month_k_data(code, columns, start_date, end_date, week_or_month='w', adjust_flag='3')

#-------------------------------------------------------------------------------
# 得到一支股票月k前复权数据
#-------------------------------------------------------------------------------
def get_month_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_week_or_month_k_data(code, columns, start_date, end_date, week_or_month='m', adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票月k后复权数据
#-------------------------------------------------------------------------------
def get_month_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_week_or_month_k_data(code, columns, start_date, end_date, week_or_month='m', adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票月k未复权数据
#-------------------------------------------------------------------------------
def get_month_k_data_no_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1990-12-19"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_week_or_month_k_data(code, columns, start_date, end_date, week_or_month='m', adjust_flag='3')

#-------------------------------------------------------------------------------
# 得到一支股票5分钟k前复权数据
#-------------------------------------------------------------------------------
def get_5_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '5', adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票5分钟k后复权数据
#-------------------------------------------------------------------------------
def get_5_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '5', adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票5分钟k未复权数据
#-------------------------------------------------------------------------------
def get_5_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '5', adjust_flag='3')

#-------------------------------------------------------------------------------
# 得到一支股票15分钟k前复权数据
#-------------------------------------------------------------------------------
def get_15_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '15', adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票15分钟k后复权数据
#-------------------------------------------------------------------------------
def get_15_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '15', adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票15分钟k未复权数据
#-------------------------------------------------------------------------------
def get_15_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '15', adjust_flag='3')

#-------------------------------------------------------------------------------
# 得到一支股票30分钟k前复权数据
#-------------------------------------------------------------------------------
def get_30_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '30', adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票30分钟k后复权数据
#-------------------------------------------------------------------------------
def get_30_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '30', adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票30分钟k未复权数据
#-------------------------------------------------------------------------------
def get_30_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '30', adjust_flag='3')

#-------------------------------------------------------------------------------
# 得到一支股票60分钟k前复权数据
#-------------------------------------------------------------------------------
def get_60_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '60', adjust_flag='2')

#-------------------------------------------------------------------------------
# 得到一支股票60分钟k后复权数据
#-------------------------------------------------------------------------------
def get_60_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '60', adjust_flag='1')

#-------------------------------------------------------------------------------
# 得到一支股票60分钟k未复权数据
#-------------------------------------------------------------------------------
def get_60_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None):
    if (start_date is None):
        start_date = "1999-07-26"
    if (end_date is None):
        end_date = datetime.today().strftime('%Y-%m-%d')
    return utils.get_minutes_k_data(code, columns, start_date, end_date, minute_freq = '60', adjust_flag='3')
