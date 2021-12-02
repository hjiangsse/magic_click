#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 25 14:18:20 2021
# 向clickhouse中写入数据
#-------------------------------------------------------------------------------
from clickhouse_driver import Client
from FastClick.configs import *

def test():
    print("This is a test function")

def write_frame(database, table, frame):
    #如果数据库不存在，创建之
    #如果表不存在,创建之，并将frame数据写入
    #如果表已经存在，只进行差异数据更新
    client = Client(host=click_address)
    create_db_query = create_db_template % (database, cluster_name)
    client.execute(create_db_query)
