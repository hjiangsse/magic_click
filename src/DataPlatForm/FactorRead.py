#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:41 2021
#-------------------------------------------------------------------------------
from clickhouse_driver import Client
from multiprocessing import Pool

import time
import os
import numpy as np
import pandas as pd

from DataPlatForm.configs import *
from DataPlatForm.Utils import *

#------------------------------------------------------------------------------
# 根据股票列表和日期范围生成dataframe
#------------------------------------------------------------------------------
def __get_sub_dataframe(factor, symbol_list, start_date, end_date):
    client = Client(host=clickhouse_addr)

    new_slist = [('S' + x + ',') for x in symbol_list]

    symbols_str = ""
    for s in new_slist:
        symbols_str += s
    symbols_str = symbols_str[:-1]

    select_query = "select timestamp," + symbols_str + " from " + factor_db + "." + factor
    select_query +=  " where timestamp <= " + "\'" + end_date + "\'"
    select_query += " and timestamp >= " + "\'" + start_date + "\'"
    
    res = client.execute(select_query)

    #transform selected result into small dataframe 
    date_list = [x[0] for x in res]
    data = [x[1:] for x in res]
    dataframe = pd.DataFrame(data, index=date_list, columns=symbol_list)
    
    return dataframe.transpose()

#------------------------------------------------------------------------------
# 并发生成dataframe
#------------------------------------------------------------------------------
def read_factor(factor, start_date, end_date, symbols):
    #用户输入的列表为空，默认返回所有的股票数据
    client = Client(host=clickhouse_addr)

    #查看因子表是否存在
    if not is_table_exist(client, factor_db, factor):
        raise ValueError("%s table not exist in database(%s)" % (factor, factor_db))

    #检查日期有效性性
    check_date_range(start_date, end_date)
    if len(symbols) == 0:
        symbols = get_factor_columns(factor)

    #进程池中得进程数量
    pool_size = os.cpu_count() // parallel_level
    #并发单位（单个进程处理得列数）
    parallel_size = len(symbols) // pool_size + 1
        
    with Pool(processes=pool_size) as p:
        block_num = len(symbols) // parallel_size
        remain_num = len(symbols) % parallel_size

        #处理“整块”数据
        res_list = []
        for i in range(block_num):
            res = p.apply_async(__get_sub_dataframe, (factor, symbols[i * parallel_size: (i+1) * parallel_size], start_date, end_date))
            res_list.append(res)

        #处理“剩余”数据
        if remain_num > 0:
            res = p.apply_async(__get_sub_dataframe, (factor, symbols[block_num * parallel_size:], start_date, end_date))
            res_list.append(res)

        #合成最终的dataframe
        frame_list = []
        for res in res_list:
            frame_list.append(res.get())

        if len(frame_list) > 0:
            res = pd.concat(frame_list, axis=0)
            cols = list(res.columns.values)
            cols.sort()
            return res[cols]
        else:
            return pd.DataFrame()

#------------------------------------------------------------------------------
# 返回因子数据表的日期索引列
#------------------------------------------------------------------------------
def get_factor_index(factor):
    client = Client(host=clickhouse_addr)
    select_query = "select timestamp from " + factor_db + "." + factor
    res = client.execute(select_query)
    return [x[0] for x in res]

#------------------------------------------------------------------------------
# 返回因子表中的所有列名（股票列表）
#------------------------------------------------------------------------------
def get_factor_columns(factor):
    client = Client(host=clickhouse_addr)
    desc_query = "desc " + factor_db + "." + factor
    res = client.execute(desc_query)
    return [x[0][1:] for x in res[1:]]
