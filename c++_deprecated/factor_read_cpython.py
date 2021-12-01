import os
import subprocess
import struct
import time
import fcntl
import re
import uuid
import pandas as pd
import numpy as np
import libck

from collections import OrderedDict
from multiprocessing import Pool

clickhouse_addr = "127.0.0.1"
factor_db = "factor_database"

parallel_block_size = 200     #并发单位xx
parallel_level = 2           #并发级别（2表示有Poolsize为logic cpu数的一半）

#------------------------------------------------------------------------------
# 因子数据表 "列数据" 信息获取
#------------------------------------------------------------------------------
def get_table_columns_info(database, table):
    desc_res = libck.get_all_symbols_c(database, table)
    field_to_size_dict = OrderedDict()
    for item in desc_res:
        # if item[1].startswith("FixedString"):
        #start = item.find('(')
        #end = item.find(')')
        #field_to_size_dict[item[0]] = int(item[start+1:end])
        # if item[1].startswith("Float"):
        # field_to_size_dict[item] = int(int(item[5:]) / factor_value_size)
        field_to_size_dict[item] = 1

    #每个字段的信息
    return field_to_size_dict

#------------------------------------------------------------------------------
# 检验日期字符串是否满足YYYY-MM-DD的形式
#------------------------------------------------------------------------------
def check_date_string(date_str):
    pattern = '^\d{4}-\d{2}-\d{2}$'
    return re.match(pattern, date_str)

#------------------------------------------------------------------------------
# 因子数据查询接口
# 参数说明:
#   factor: 因子名
#   start_date: 查询开始日期
#   end_date: 查询结束日期
#   symbols: 用户想查询的股票列表
# 返回值：
#   pandas.DataFrame
#   indexes: 股票列表
#   columns: 日期列表
#   data: 因子数值
#------------------------------------------------------------------------------
def factor_selector(factor, start_date, end_date, symbols):
    start = time.time()

    #查看因子表是否存在
    test_exsits = libck.query_tables_c(factor_db, factor)
    if len(test_exsits) == 0:
        raise ValueError("%s table not exist in database(%s)" % (factor, factor_db))

    #检查日期有效性性
    if not check_date_string(start_date):
        raise ValueError("start_date[%s] syntex error! yyyy-mm-dd!" % start_date)

    if not check_date_string(end_date):
        raise ValueError("end_date[%s] syntex error! yyyy-mm-dd!" % end_date)
    
    if start_date > end_date:
        raise ValueError("start_date[%s] > end_date[%s], error!" % (start_date, end_date))

    #获取因子表的列信息字典
    column_infos_dict = get_table_columns_info(factor_db, factor)
    valid_symbols = list(column_infos_dict.keys())
    input_symbols = [('S' + x) for x in symbols]

    #如果输入symbols为空，那么默认返回所有股票信息
    input_symbols_len = len(input_symbols)
    if input_symbols_len == 0:
        input_symbols = valid_symbols
        input_symbols_len = len(valid_symbols)

    #检验用户检索的股票是否在表中存在(如有不存在的股票，报错)
    diff_symbols = set(input_symbols) - set(valid_symbols)
    if len(diff_symbols) > 0:
        raise ValueError("%s not valid columns in table" % diff_symbols)

    #用户输入的股票列表中存在重复的股票，报错退出
    if len(set(input_symbols)) < len(input_symbols):
        raise ValueError("%s have duplicate symbols" % input_symbols)

    print('start query', time.time()-start)
    if len(input_symbols) <= 100:
        res = single_query_factor(factor, start_date, end_date, input_symbols)
    else:
        res = multi_query_factor(factor, start_date, end_date, input_symbols)
    
    #date column resorted
    res.sort_index(inplace=True, axis=1)
    return res

def multi_query_factor(factor, start_date, end_date, input_symbols):
    with Pool(processes = (os.cpu_count() // parallel_level)) as p:
        res_dict = {}
        symbol_count = len(input_symbols)
        block_num = symbol_count // parallel_block_size  #以整块任务的行数
        remain_num = symbol_count % parallel_block_size  #最后一个剩余任务的行数

        #并发处理“整块”任务
        for i in range(block_num):
            res = p.apply_async(single_query_factor, (factor, start_date, end_date, input_symbols[parallel_block_size * i:parallel_block_size * (i+1)], False))
            res_dict[i] = res
        print('block_num:', block_num, 'processes:', (os.cpu_count() // parallel_level), 'time:', time.time()-start)
        #处理最后一个“零散”任务(如果其存在的话)
        if remain_num > 0:
            res = p.apply_async(single_query_factor, (factor, start_date, end_date, input_symbols[parallel_block_size * block_num :], True))
            res_dict[block_num] = res
        print('query end:', time.time()-start)
        frame_list = []
        for i in range(block_num):
            frame_list.append(res_dict[i].get())
        print('frame_list.append:', time.time()-start)
        if remain_num > 0:
            frame_list.append(res_dict[block_num].get())
        print('frame_list.append end:', time.time()-start)
        #合并各并发进程的处理结果
        res = pd.concat(frame_list, axis=0)
        print('pd.concat:', len(frame_list), ',time:', time.time()-start)
        return res

def single_query_factor(factor, start_date, end_date, input_symbols, show_log = True):
    tableQuery = libck.TableQuery()
    querySymbols = libck.symbol_vector()
    for sym in input_symbols:
    	querySymbols.append(sym)
    if show_log:
    	print('single_query_factor:', time.time()-start)
    tableQuery.SelectQuery(querySymbols, factor_db, factor, start_date, end_date)
    if show_log:
    	print('c++ query:', time.time()-start)
    #查询满足用户检索范围的有效行数
    row_num = tableQuery.GetRowCount()

    if row_num == 0:
        return pd.DataFrame()
    else:
        selected_dates = []
        selected_datas = []
        symbols_len = len(input_symbols)
        for i in range(row_num):
            timestamp_str = tableQuery.GetStringValue(i,0)
            selected_dates.append(timestamp_str)
        if show_log:
            print('timestamp query:', time.time()-start)

        for j in range(symbols_len):
            selected_datas.append(tableQuery.GetFloatValuesByCol(j+1))
        if show_log:
            print('data query:', time.time()-start)

        symbols = [x[1:] for x in input_symbols]
        res = pd.DataFrame(selected_datas, index=symbols, columns=selected_dates)
        if show_log:
            print('init dataframe:', time.time()-start)

        return res

if __name__ == "__main__":
    valid_columns = get_table_columns_info("factor_database", "factor_big_table12")
    #valid_columns = get_table_columns_info("factor_database", "test_table_create2")
    symbols = [x[1:] for x in list(valid_columns.keys())]

    start = time.time()
    res = factor_selector("factor_big_table12", "2005-01-01", "2021-01-16", symbols)
    #res = factor_selector("test_table_create2", "2020-01-01", "2020-01-16", symbols)
    print(time.time() - start)
    print(res)
