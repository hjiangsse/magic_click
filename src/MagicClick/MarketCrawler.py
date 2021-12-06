#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 25 14:18:20 2021
# 本模块的功能是利用akshare和baostock提供的接，获取各种维度的k线行情数据
#-------------------------------------------------------------------------------
import baostock as bs
import akshare as ak
import pandas as pd
from datetime import datetime
import MagicClick.Utils as utils

#利用baostock提供的接口获取A股历史K线数据
day_k_columns = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"
minutes_k_columns = "date,time,code,open,high,low,close,volume,amount,adjustflag"
week_and_month_k_columns = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"

columns_type_dict = {
    "date": "str",
    "time": "str",
    "code": "str",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "preclose": "float64",
    "volume": "int64",
    "amount": "float64",
    "adjustflag": "str",
    "turn": "float64",
    "tradestatus": "str",
    "pctChg": "float64",
    "peTTM": "float64",
    "psTTM": "float64",
    "pcfNcfTTM": "float64",
    "pbMRQ": "float64",
    "isST": "str"
}

#利用akshare提供的数据获取接口，获得A股票名称列表
def get_astock_list():
    a_stock_list = []
    a_sh_stocks_frame = ak.stock_info_sh_name_code()
    for code in a_sh_stocks_frame['公司代码']:
        a_stock_list.append("sh." + code)

    a_sz_stocks_frame = ak.stock_info_sz_name_code()
    for code in a_sz_stocks_frame['A股代码']:
        a_stock_list.append("sz." + code)
    
    return a_stock_list

#日(day)k线数据的获取(默认获取不复权的数据)
#adjust_flag = “3”表示行情数据未复权
#adjust_flag = “1”表示行情数据后复权
#adjust_flag = “2”表示行情数据前复权
def get_day_k_data(code, data_columns, start_date, end_date, adjust_flag='3'):
    lg = bs.login()
    if not lg.error_code == '0':
        print('login respond error_msg:' + lg.error_msg)
        return
    
    utils.check_date_range(start_date, end_date)
    
    if len(data_columns) == 0:
        data_columns = day_k_columns
     
    invalid_columns = set(data_columns) - set(day_k_columns)
    if len(invalid_columns) > 0:
        print("[%s] are not valid columns in [%s]" % (invalid_columns, day_k_columns))
        
    rs = bs.query_history_k_data_plus(code, data_columns, start_date, end_date, frequency="d", adjustflag=adjust_flag)
    if not rs.error_code == '0':
        print('query_history_k_data_plus respond error_msg:' + rs.error_msg)
        return     
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    result.index = [str(x) for x in result.date]
    result = result.drop(['date'], axis=1)
    
    #根据全局定义的类类型字，将frame的列转换为特定类型
    for col in result.columns:
        result[col] = result[col].astype(columns_type_dict[col])

    return result

#周线和月线行情数据的获取（默认获取不复权数据）
def get_week_or_month_k_data(code, data_columns, start_date, end_date, week_or_month='w', adjust_flag='3'):
    lg = bs.login()
    if not lg.error_code == '0':
        print('login respond error_msg:' + lg.error_msg)
        return

    utils.check_date_range(start_date, end_date)
    
    if len(data_columns) == 0:
        data_columns = week_and_month_k_columns
        
    invalid_columns = set(data_columns) - set(day_k_columns)
    if len(invalid_columns) > 0:
        print("[%s] are not valid columns in [%s]" % (invalid_columns, day_k_columns))
        
    rs = bs.query_history_k_data_plus(code, data_columns, start_date, end_date, week_or_month, adjust_flag)
    if not rs.error_code == '0':
        print('query_history_k_data_plus respond error_msg:' + rs.error_msg)
        return     
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    result.index = [str(x) for x in result.date]
    result = result.drop(['date'], axis=1)
    
    #根据全局定义的类类型字，将frame的列转换为特定类型
    for col in result.columns:
        result[col] = result[col].astype(columns_type_dict[col])

    return result

#分钟线行情数据获取（默认获取不复权数据）
#默认获取5分钟间隔的数据
def get_minutes_k_data(code, data_columns, start_date, end_date, minute_freq = '5', adjust_flag='3'):
    lg = bs.login()
    if not lg.error_code == '0':
        print('login respond error_msg:' + lg.error_msg)
        return

    utils.check_date_range(start_date, end_date)
    
    if len(data_columns) == 0:
        data_columns = minutes_k_columns
        
    invalid_columns = set(data_columns) - set(minutes_k_columns)
    if len(invalid_columns) > 0:
        print("[%s] are not valid columns in [%s]" % (invalid_columns, minutes_k_columns))
        
    rs = bs.query_history_k_data_plus(code, data_columns, start_date, end_date, minute_freq, adjust_flag)
    if not rs.error_code == '0':
        print('query_history_k_data_plus respond error_msg:' + rs.error_msg)
        return     
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    
    result = pd.DataFrame(data_list, columns=rs.fields)
    
    time_list = []
    for time in result['time']:
        time_list.append(datetime.strptime(time[0:-3], "%Y%m%d%H%M%S"))
        
    result.index = [str(x) for x in time_list]
    result = result.drop(['time'], axis=1)
    
    return result
