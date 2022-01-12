#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 25 14:18:20 2021
#-------------------------------------------------------------------------------
from MagicClick.configs import *
import re
import os

#------------------------------------------------------------------------------
# 检验日期字符串是否满足YYYY-MM-DD的形式
#------------------------------------------------------------------------------
def check_date_string(date_str):
    pattern = '^\d{4}-\d{2}-\d{2}$'
    return re.match(pattern, date_str)

def trans_date_string(date_str):
    pattern = '^(\d{4})-(\d{2})-(\d{2})$'
    res = re.search(pattern, date_str)
    return res.group(1) + "_" + res.group(2) + "_" + res.group(3) 

#------------------------------------------------------------------------------
# 检验日期以及日期范围是否有效
#------------------------------------------------------------------------------
def check_date_range(start_date, end_date):
    #检查日期有效性性
    if not check_date_string(start_date):
        raise ValueError("start_date[%s] syntex error! yyyy-mm-dd!" % start_date)

    if not check_date_string(end_date):
        raise ValueError("end_date[%s] syntex error! yyyy-mm-dd!" % end_date)

    if start_date > end_date:
        raise ValueError("start_date[%s] > end_date[%s], error!" % (start_date, end_date))
