#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 25 14:18:20 2021
# ��clickhouse��д������
#-------------------------------------------------------------------------------
from clickhouse_driver import Client
from FastClick.configs import *

def test():
    print("This is a test function")

def write_frame(database, table, frame):
    #������ݿⲻ���ڣ�����֮
    #���������,����֮������frame����д��
    #������Ѿ����ڣ�ֻ���в������ݸ���
    client = Client(host=click_address)
    create_db_query = create_db_template % (database, cluster_name)
    client.execute(create_db_query)
