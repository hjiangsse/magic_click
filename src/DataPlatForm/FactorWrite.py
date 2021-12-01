#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:38:51 2021
#-------------------------------------------------------------------------------
from clickhouse_driver import Client

import numpy as np
import pandas as pd
import time

from DataPlatForm.configs import *
from DataPlatForm.Utils import *
from DataPlatForm.FactorRead import get_factor_columns

#------------------------------------------------------------------------------
# 根据用户dataframe中的股票列表，生成clickhouse表中的股票列名
# (test_passed)
#------------------------------------------------------------------------------
def __gen_table_symbols_list(symbols):
    res = ''
    if len(symbols) == 0:
        return res

    for i in range(len(symbols) - 1):
        res += 'S' + symbols[i] + ' Float64,'
    res += 'S' + symbols[len(symbols) - 1] + ' Float64'

    return res

#------------------------------------------------------------------------------
# 创建因子数据表
# (test_passed)
#------------------------------------------------------------------------------
def __create_factor_table(client, database_name, table_name, symbols):
    client.execute("create database if not exists %s" % (database_name))
    table_create_query = "create table if not exists " + database_name + "." + table_name + "("
    table_create_query += " timestamp FixedString(" + str(factor_timestamp_size) + "),"
    table_create_query += __gen_table_symbols_list(symbols) + ")"
    table_create_query += " ENGINE = MergeTree() PRIMARY KEY(timestamp) order by timestamp"
    client.execute(table_create_query)

#------------------------------------------------------------------------------
# 创建插入语句中列名序列
#------------------------------------------------------------------------------
def __gen_columns_str(symbols):
    res = "(timestamp,"
    for symbol in symbols:
        res += "S" + symbol + ","
    res = res[0:-1]
    res += ")"
    return res

#------------------------------------------------------------------------------
# 写入因子数据到clickhouse数据库中
# 使用请注意：正常的使用场景是，先创建好一张大表，然后再向其中每次更新少量数据
#           如果用户反过来使用，每次更新大量的数据，且存在大量的新列，将导致写入时间大大延长
#------------------------------------------------------------------------------
def write_factor(factor, dataframe):    
    client = Client(host=clickhouse_addr)
    user_symbols = list(dataframe.index.values)

    #数据属于一个全新的因子，那么先在因子数据库中创建一张表
    #随后直接将传入的数据写入到表中
    #这种情况下，不存在 数据 和 表 列数不一致的情况
    if not is_table_exist(client, factor_db, factor):
        __create_factor_table(client, factor_db, factor, user_symbols)
        #生成列名串
        columns_str = __gen_columns_str(user_symbols)
        #生成数据串
        data_str = para_gen_data_insert_column_str(dataframe)
        #插入语句
        insert_query = "insert into " + factor_db + "." + factor + " "
        insert_query += columns_str + " values " + data_str
        
        client.execute(insert_query)
        return

    #比较 用户传入数据中的股票列表 和 表中现有的股票列表
    # 传入的股票列表 有 不在表中存在的股票, 那么要扩展表列
    table_symbols = get_factor_columns(factor)
    diff_symbols = list(set(user_symbols) - set(table_symbols))
    diff_symbols.sort()

    #这个地方有待要注意，增加大量的新列会消耗很多时间
    for symbol in diff_symbols:
        add_column_query = "alter table " + factor_db + "." + factor
        add_column_query += " add column " + "S" + symbol + " Float64"
        client.execute(add_column_query)

    #将用户传入的数据写入表中（不改变原来的数据，只是增加新的数据）
    user_dates = list(dataframe.columns)
    user_dates.sort()

    #因子表中最后一个有效日期
    last_table_date = get_last_row_timestamp(client, factor_db, factor)

    #插入数据的最后日期 在 因子表中最后一个有效日期 之前，直接退出
    if user_dates[-1] <= last_table_date:
        return

    for date in user_dates:
        if date <= last_table_date:
            continue
        else:
            columns_str = __gen_columns_str(user_symbols)

            data_str = "(" + "\'" + date + "\'"+ ","
            for v in dataframe[date]:
                data_str += str(v) + ","
            data_str = data_str[0:-1]
            data_str += ")"

            insert_query = "insert into " + factor_db + "." + factor + " "
            insert_query += columns_str + " values " + data_str

            client.execute(insert_query)

#------------------------------------------------------------------------------
# 删除因子表中指定日期之间的数据
#------------------------------------------------------------------------------
def delete_factor(factor, start_date, end_date):
    client = Client(host=clickhouse_addr)
    delete_query = "alter table " + factor_db + "." + factor + " delete where " 
    delete_query += "substring(timestamp, 1, 10) >= " + "\'" + start_date + "\'" + " and "
    delete_query += "substring(timestamp, 1, 10) <= " + "\'" + end_date + "\'"
    client.execute(delete_query)

    #删除语句提交之后，确保删除成功才退出，因为Clickhouse服务器是异步执行删除命令的
    select_query = "select count(*) from " + factor_db + "." + factor + " where "
    select_query += "substring(timestamp, 1, 10) >= " + "\'" + start_date + "\'" + " and "
    select_query += "substring(timestamp, 1, 10) <= " + "\'" + end_date + "\'"

    while True:
        res = client.execute(select_query)        
        if res[0][0] == 0:
            break
        else:
            #if the delete is not finished, just wait for 0.2 second
            time.sleep(0.2)
