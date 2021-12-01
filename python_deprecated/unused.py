#------------------------------------------------------------------------------
# 得到时间戳长度
#------------------------------------------------------------------------------
def __get_fixedstring_len(str):
    start_index = str.find('(')
    end_index = str.find(')')
    return int(str[start_index+1:end_index])

#------------------------------------------------------------------------------
# 检验用户写入的数据是否满足对应数据集合的要求
#------------------------------------------------------------------------------
def __check_data(name, dataframe, dimension=2):
    if dimension == 2:
        if not name in raw_db_infos:
            raise ValueError("%s not a valid database" % name)
    if dimension == 1:
        if not name in raw_one_dimension_infos:
            raise ValueError("%s not a valid table" % name)

    #如果用户输入空的dataframe报错
    if dataframe is None:
        raise ValueError("dataframe can not be None")

    #检查用户输入的dataframe的index(时间)格式是否满足要求
    indexes = list(dataframe.index.values)
    if len(indexes) == 0:
        raise ValueError("dataframe can not be Empty")

    if dimension == 2:
        valid_index_len = __get_fixedstring_len(raw_db_infos[name][0][1])
    if dimension == 1:
        valid_index_len = __get_fixedstring_len(raw_one_dimension_infos[name][0][1])

    if not len(indexes[0]) == valid_index_len:
        raise ValueError("dataframe timestamp index length error[want: %d, give: %d]" % (valid_index_len, len(indexes[0])))

    #检查用户输入的dataframe的columns是否满足要求
    if dimension == 2:
        valid_columns = [x[0] for x in raw_db_infos[name][1:]]
    if dimension == 1:
        valid_columns = [x[0] for x in raw_one_dimension_infos[name][1:]]
        
    input_columns = list(dataframe.columns.values)

    if not input_columns == valid_columns:
        raise ValueError("dataframe columns error[want: %s, give: %s]" % (str(valid_columns), str(input_columns)))
    
    return True

#------------------------------------------------------------------------------
# 二维数据写入接口
# database: "jk", "yk"等数据库名称
# symbol: 股票名称，作为表名
# data: 写入的二维数据
#------------------------------------------------------------------------------
def write_two_dimension_frame(name, symbol, data):
    client = Client(host=clickhouse_addr)
    table = "S" + symbol

    #校验成功之后，才执行相应的写入操作
    if __check_data(name, data, dimension = 2):
        #如果数据库不存在，创建之
        client.execute("create database if not exists %s" % (name))

        #如果以股票命名的表不存在，创建之
        if not is_table_exist(name, table):
            create_table(client, name, table, dimension = 2)
        
        #将data中的数据写入到表中(新的时间戳才插入，已经存在的时间戳不变)
        #得到表中已存在数据的最新时间戳
        last_row_stamp = get_last_row_timestamp(client, name, table)
        data_stamps = list(data.index.values)
        data_stamps.sort()
        
        #如果表中存在的时间戳 和 插入数据中得时间戳 发生重叠
        #那么只插入原来表中不存在的时间戳
        insert_stamps_filtered = filter(lambda x: x > last_row_stamp, data_stamps)
        insert_stamps_list = list(insert_stamps_filtered)
        if len(insert_stamps_list) == 0:
            #没有新的数据要插入，直接退出
            return

        #过滤出要插入的数据
        insert_data = data.filter(items = insert_stamps_list, axis=0)

        #INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
        #生成属性串
        column_list = [x[0] for x in raw_db_infos[name]]
        column_str = gen_tuple_str(column_list, quotation_add=False)
        column_str = "(" + column_str + ")"

        #生成数据串
        data_str = para_gen_data_insert_str(insert_data)
        #拼接出查询请求
        insert_query = "insert into " + name + "." + table + " " + column_str + " " + "values " + data_str
        client.execute(insert_query)

#------------------------------------------------------------------------------
# 二维数据删除接口, 删除一个二维数据库中所有表的 从 start_date 到 end_date之间的数据
# database: "jk", "yk"等数据库名称
# start_date: 开始日期
# end_date: 截止日期
#------------------------------------------------------------------------------
def delete_two_dimension_frame(database, start_date, end_date):
    client = Client(host=clickhouse_addr)
    tables = get_all_tables_in_database(database)

    for table in tables:
        delete_query = "alter table " + database + "." + table + " delete where " 
        delete_query += "substring(timestamp, 1, 10) >= " + "\'" + start_date + "\'" + " and "
        delete_query += "substring(timestamp, 1, 10) <= " + "\'" + end_date + "\'"
        client.execute(delete_query)

#------------------------------------------------------------------------------
# 一维数据写入接口
# name: 一维数据名称,raw中所有一维数据存放在一个统一的数据库中，一个name是一张表
# data: dataframe
#------------------------------------------------------------------------------
def write_one_dimension_frame(name, data):
    client = Client(host=clickhouse_addr)

    if __check_data(name, data, dimension = 1):
        #如果数据库不存在，创建之
        client.execute("create database if not exists %s" % (raw_one_dimension_db))
        
        #表不存在，创建之
        if not is_table_exist(raw_one_dimension_db, name):
            create_table(client, raw_one_dimension_db, name, dimension = 1)
        
        #将data中的数据写入到表中(新的时间戳才插入，已经存在的时间戳不变)
        #得到表中已存在数据的最新时间戳
        last_row_stamp = get_last_row_timestamp(client, raw_one_dimension_db, name)
        data_stamps = list(data.index.values)
        data_stamps.sort()
        
        #已经存在的数据时间戳不能 和 插入最晚数据的时间戳发生重叠
        insert_stamps_filtered = filter(lambda x: x > last_row_stamp, data_stamps)
        insert_stamps_list = list(insert_stamps_filtered)
        if len(insert_stamps_list) == 0:
            #没有新的数据要插入，直接退出
            return

        #过滤出要插入的数据
        insert_data = data.filter(items = insert_stamps_list, axis=0)

        #INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
        #生成属性串
        column_list = [x[0] for x in raw_one_dimension_infos[name]]
        column_str = gen_tuple_str(column_list, quotation_add=False)
        column_str = "(" + column_str + ")"

        #生成数据串
        data_str = para_gen_data_insert_str(insert_data)
        insert_query = "insert into " + raw_one_dimension_db + "." + name + " " + column_str + " " + "values " + data_str
        client.execute(insert_query)

#------------------------------------------------------------------------------
# 一维数据删除接口, raw中所有一维数据存放在一个统一的数据库中，一个name是一张表
# name: 一维数据名
# start_date: 开始日期
# end_date: 截止日期
#------------------------------------------------------------------------------
def delete_one_dimension_frame(name, start_date, end_date):
    client = Client(host=clickhouse_addr)

    #如果一维数据表不存在, 报错
    if not is_table_exist(raw_one_dimension_db, name):
        raise ValueError("%s table not in %s database." % (name, raw_one_dimension_db))
    
    delete_query = "alter table " + raw_one_dimension_db + "." + name + " delete where " 
    delete_query += "substring(timestamp, 1, 10) >= " + "\'" + start_date + "\'" + " and "
    delete_query += "substring(timestamp, 1, 10) <= " + "\'" + end_date + "\'"
    client.execute(delete_query)

#------------------------------------------------------------------------------
# 读取raw数据一维数据库中某张表中的内容，
# name: 数据表名
# start_date: 开始日期
# end_date: 结束日期
#------------------------------------------------------------------------------
def read_one_dimension_frame(name, start_date, end_date):
    #查看name是否存在
    valid_tables = list(raw_one_dimension_infos.keys())
    if not name in valid_tables:
        raise ValueError("\'%s\' is not in valid table names %s" % (name, str(valid_tables)))

    #校验日期以及日期范围的有效性
    check_date_range(start_date, end_date)

    #查看数据库中表是否存在
    table_lst = get_all_tables_in_database(raw_one_dimension_db)
    if not name in table_lst:
        raise ValueError("\'%s\' not created in database names %s" % (name))

    #获取列名
    columns = [x[0] for x in raw_one_dimension_infos[name][1:]]
    frame = __read_single_table_frame(raw_one_dimension_db, name, start_date, end_date, columns)
    return frame

#------------------------------------------------------------------------------
# 在特定的数据库中创建二维数据表
#------------------------------------------------------------------------------
def create_table(client, database, symbol, dimension = 2):
    table_create_query = "create table if not exists " + database + "." + symbol + " ("
    if dimension == 2:
        for x in raw_db_infos[database]:
            table_create_query += x[0] + " " + x[1] + ","
    if dimension == 1:
        for x in raw_one_dimension_infos[symbol]:
            table_create_query += x[0] + " " + x[1] + ","
            
    table_create_query = table_create_query[0:-1] + ")"
    table_create_query += " ENGINE = MergeTree() PRIMARY KEY(timestamp) order by timestamp"
    client.execute(table_create_query)

#------------------------------------------------------------------------------
# 二维数据读取接口
# key：股票
# value: dateframe
#------------------------------------------------------------------------------
def read_2d_frame(name, start_date, end_date, columns):
    #查看name是否存在
    valid_dbs = list(raw_db_infos.keys())
    if not name in valid_dbs:
        raise ValueError("\'%s\' is not in valid names %s" % (name, str(valid_dbs)))

    #校验日期以及日期范围的有效性
    check_date_range(start_date, end_date)

    #得到数据库中所有table列表
    table_lst = get_all_tables_in_database(name)
    
    #并发获取每个数据表的dataframe, 最终合成一个dict返回给用户
    if len(columns) == 0:
        columns = [x[0] for x in raw_db_infos[name][1:]]

    res_dict = {}
    with Pool(processes=(os.cpu_count() // parallel_level)) as p:
        #任务分配
        tmp_lst = []
        for table in table_lst:
            res = p.apply_async(__read_single_table_frame, (name, table, start_date, end_date, columns))
            tmp_lst.append((table, res))
        
        for tmp in tmp_lst:
            frame = tmp[1].get()
            symbol = tmp[0][1:]
            res_dict[symbol] = frame
    return res_dict
