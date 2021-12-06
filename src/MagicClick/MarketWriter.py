#-------------------------------------------------------------------------------
# Author: hjiang
# Email: heng.jiang@jingle.ai
# Time: Thu Nov 25 14:18:20 2021
# ��clickhouse��д������
#-------------------------------------------------------------------------------
from clickhouse_driver import Client
from MagicClick.configs import *

cluster_name = "example_cluster"
create_db_template = "create database if not exists %s on cluster %s"

def write_frame(database, table, frame):
    #������ݿⲻ���ڣ�����֮
    client = Client(host=click_address)
    create_db_query = create_db_template % (database, cluster_name)
    client.execute(create_db_query)
    #���������,����֮������frame����д��
    table_fields_infos = gen_table_fields_infos_from_dataframe(frame)
    print(table_fields_infos)
    #������Ѿ����ڣ�ֻ���в������ݸ���
