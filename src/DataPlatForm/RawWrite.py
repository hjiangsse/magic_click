#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 18 13:39:14 2021
#-------------------------------------------------------------------------------
from logging import raiseExceptions
from typing import Collection

from clickhouse_driver.result import QueryInfo
from DataPlatForm.configs import *
from DataPlatForm.Utils import *

from datetime import datetime, timedelta
from clickhouse_driver import Client

import numpy as np
import pandas as pd

import time
import os

#------------------------------------------------------------------------------
# 二维数据写入接口
# name: 数据名
# frame: 用户写入的二维数据
# date: 用户要写入数据的时间（格式: "2005-01-01"）
# 说明: 当name_yyyy_mm_dd表格已经存在时，就不写入本次数据;
#      如果用户想要更新某个已经存在的日期数据
#      可以先将已经存在的数据删除，然后再写入
#------------------------------------------------------------------------------
def write_frame(name, frame, date):
    client = Client(host=clickhouse_addr)
    
    if not check_date_string(date):
        raise ValueError("%s not valid(e.g 2005-01-04)" % date)

    #如果二维数据库不存在，创建之
    client.execute("create database if not exists %s" % (raw_2d_db))
    
    table_name = name + "_" + trans_date_string(date)
    #表已经存在，直接退出
    if is_table_exist(client, raw_2d_db, table_name):
        print("%s table alreay exists in database[%s], you can delete it and then wirte!" % (table_name, raw_2d_db))
        return

    #创建一个新的数据表
    create_table_query = "create table if not exists " + raw_2d_db + "." + table_name + " ("
    create_table_query += gen_table_fields_infos_from_dataframe(frame) + ") "
    create_table_query += "ENGINE = MergeTree() order by " + "indexer"
    client.execute(create_table_query)

    #将frame中的数据插入到新创建的表中
    columns_str = gen_columns_str_from_dataframe(frame)
    data_str = para_gen_data_insert_str(frame)
    insert_query = "insert into " + raw_2d_db + "." + table_name + " " + columns_str + " " + "values " + data_str
    client.execute(insert_query)

#------------------------------------------------------------------------------
# 二维数据删除接口
# name: 数据名
# date: 用户要删除数据的时间（格式: "2005-01-01"）
#------------------------------------------------------------------------------
def delete_frame(name, date):
    client = Client(host=clickhouse_addr)

    if not check_date_string(date):
        raise ValueError("%s not valid(e.g 2005-01-04)" % date)

    table_name = name + "_" + trans_date_string(date)

    if not is_table_exist(client, raw_2d_db, table_name):
        raise ValueError("%s table not in database[%s], can not be deleted!" % (table_name, raw_2d_db))

    delete_query = "drop table " + raw_2d_db + "." + table_name
    client.execute(delete_query)

#------------------------------------------------------------------------------
# 列表数据写入接口
# name: 列表名称, raw中所有列表数据存放在一个统一的数据库中，一个name是一张表
# data: list
#------------------------------------------------------------------------------
def write_list(name, data):
    client = Client(host=clickhouse_addr)

    #如果list数据库还不存在，创建list数据库
    create_database_query = "CREATE DATABASE IF NOT EXISTS " + raw_list_db
    client.execute(create_database_query)

    #user insert empty data, exist
    if len(data) == 0:
        return

    #如果用户传入的list是一个异质列表（其中有不同类型的元素），报错
    if not is_homogeneous_list(data):
        raise ValueError("Not homogeneous list write(Different data types in list), check your list")
    
    #如果name数据表还不存在，创建之
    column_name = 'value'
    if isinstance(data[0], str):
        column_type = 'String'
    elif isinstance(data[0], int):
        column_type = 'Int64'
    elif isinstance(data[0], float):
        column_type = 'Float64'
    else:
        raise ValueError("data type not String, Int Or Float!")
    
    create_table_query = "CREATE TABLE IF NOT EXISTS " + raw_list_db + "." + name
    create_table_query += " ("
    create_table_query += column_name + " " + column_type
    create_table_query += ") "
    create_table_query += " ENGINE = MergeTree() order by " + column_name
    client.execute(create_table_query)

    #将数据插入到数据表中
    insert_query = "insert into " + raw_list_db + "." + name + " "
    insert_query += "(" + column_name+ ") values "
    data_str = ""
    for x in data:
        data_str += "("
        if type(x) == str:
            data_str += "\'" + x + "\'"
        else:
            data_str += str(x)
        data_str += ")" + ","
    data_str = data_str[0:-1]
    insert_query += data_str
    
    client.execute(insert_query)

#------------------------------------------------------------------------------
# 列表数据删除接口
# name: 列表名称, raw中所有列表数据存放在一个统一的数据库中，一个name是一张表
#------------------------------------------------------------------------------
def delete_list(name):
    client = Client(host=clickhouse_addr)
    
    if not is_table_exist(raw_list_db, name):
        raise ValueError("%s not exist in database[%s]" % (name, raw_list_db))

    delete_query = "drop table " + raw_list_db + "." + name
    client.execute(delete_query)
    
