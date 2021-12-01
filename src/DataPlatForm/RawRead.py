#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:40:09 2021
#-------------------------------------------------------------------------------
from DataPlatForm.Utils import *
from DataPlatForm.configs import *
from clickhouse_driver import Client

import pandas as pd
import os
from multiprocessing import Pool

#------------------------------------------------------------------------------
# 查询单个表格中的数据，返回一个dataframe
#------------------------------------------------------------------------------
def __read_single_table(database, table, fields):
    client = Client(host=clickhouse_addr)

    all_valid_fileds = get_2d_table_columns_list(client, database, table)
    #用户输入的属性列表为空，那么直接获取所有属性
    if len(fields) == 0:
        columns = all_valid_fileds
    else:
        diff_fields = set(fields) - set(all_valid_fields)
        if len(diff_fields) > 0:
            raise ValueError("[%s] not valid fields in table. check it out" % list(diff_fields))

    #去重
    fields = list(set(fields))
        
    #构建查询SQL语句
    select_query = "select indexer,"
    for field in fields:
        select_query += field + ","
    select_query = select_query[0:-1]
    select_query += " from " + database + "." + table
    
    res = client.execute(select_query)
    if len(res) > 0:
        indexes = [x[0] for x in res]
        data = [x[1:] for x in res]
        return pd.DataFrame(data, index=indexes, columns=fields)
    else:
        return pd.DataFrame()

#------------------------------------------------------------------------------
# 二维数据读取接口
# name: 数据名
# start_date: 开始日期
# end_date: 结束日期
# columns: 读取表中哪几列
#------------------------------------------------------------------------------
def read_frame(name, start_date, end_date, fileds=[]):
    client = Client(host=clickhouse_addr)
    #校验日期
    check_date_range(start_date, end_date)
    date_lst = gen_date_list(start_date, end_date)

    valid_dates = []
    for date in date_lst:
        table_name = name + "_" + trans_date_string(date)
        if is_table_exist(client, raw_2d_db, table_name):
            valid_dates.append(date)
        else:
            print("[%s] not have data in this day[%s]." % (name, date))
            
    frame_dict = {}
    with Pool(processes=(os.cpu_count() // parallel_level)) as p:
        res_dict = {}
        for date in valid_dates:
            table = name + "_" + trans_date_string(date)
            res = p.apply_async(__read_single_table, (raw_2d_db, table, fields))
            res_dict[date] = res

        for date, res in res_dict.items():
            frame_dict[date] = res.get()
    return frame_dict
        
#------------------------------------------------------------------------------
# 读取列表内容
# name: 列表名
#------------------------------------------------------------------------------
def read_list(name):
    client = Client(host=clickhouse_addr)
    
    if not is_table_exist(client, raw_list_db, name):
        raise ValueError("%s not in %s database!" % (name, raw_list_db))

    select_query = "select * from " + raw_list_db + "." + name
    res = client.execute(select_query)

    return [x[0] for x in res]
