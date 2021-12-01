#include <iostream>
#include <sstream>
#include <thread>
#include <type_traits>
#include <chrono>
#include <clickhouse/client.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>

using namespace std;
using namespace clickhouse;

#define GROUP_SIZE 100 //任务组大小（最小的一个并发单位）

using group_vector = vector<vector<string>>;
using group_iter = vector<vector<string>>::iterator;

//thread functions
void select_timestamp_then_write_memory(const string& addr, const string& query, char **pshared, uint64_t stamp_byte_size, uint64_t row_byte_size);
void select_group_then_write_memory(const string& addr, const string &query, char *shared, uint64_t group_len, uint64_t factor_val_size, uint64_t row_byte_size);

//utils
void check_memory(char *pmm, int symbol_num, int row_num, int row_byte_size);
vector<string> parse_symbol_list(const string& str);
vector<string> get_all_symbols(string addr, string db, string table);
uint64_t get_row_num(string addr, string db, string table, string start_date, string end_date);
group_vector get_symbol_groups(vector<string>& symbols, int group_num);
string gen_select_query(vector<string>& fields, string db, string table, string start_date, string end_date);

/*
*  二进制程序参数说明
*  argv[0]: 程序名
*  argv[1]: mmap 文件名(python传入, 后同)
*  argv[2]: clickhouse server address
*  argv[3]: symbol list(comma seprated) e.g. "S000001,S000002,S000003"
*  argv[4]: database name
*  argv[5]: table name
*  argv[6]: start date
*  argv[7]: end date
*  argv[8]: timestamp byte length
*  argv[9]: factor value byte length
*/
int main(int argc, char **argv) {
    //得到用户传入的symbol列表，并将其分组
    vector<string> symbols = parse_symbol_list(argv[3]);
    group_vector groups = get_symbol_groups(symbols, GROUP_SIZE);

    int fd = -1;
    string clickhouse_addr = argv[2];
    string database_name = argv[4];
    string table_name = argv[5];
    string start_date = argv[6];
    string end_date = argv[7];
    uint64_t timestamp_len = stoi(argv[8]);
    uint64_t value_len = stoi(argv[9]);

    uint64_t select_row_count = get_row_num(clickhouse_addr, database_name, table_name, start_date, end_date);
    uint64_t row_byte_size = timestamp_len + symbols.size() * value_len;
    
    if((fd = open(argv[1], O_RDWR, 0)) == -1) {
        printf("unable to open file %s.\n", argv[1]);
        return -1;
    }

    uint64_t mmap_size = select_row_count * row_byte_size;
    char *pmmap = (char *)mmap(NULL, mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (pmmap == NULL) {
        printf("share memory map failed!\n");
        return -1;
    }
    char *pmmap_bak = pmmap;
   
    vector<thread> m_threads;

    //创建只读取timestamp的线程
    char **ppshared = &pmmap;
    string timestamp_query = string("select timestamp from ") + database_name + "." + table_name + 
        " where timestamp >= " + "'" + start_date + "'" + " and timestamp <= " + "'" + end_date + "'";
    m_threads.push_back(std::thread(select_timestamp_then_write_memory, clickhouse_addr, timestamp_query, ppshared, timestamp_len, row_byte_size));

    //创建读取因子字段的线程
    char *group_start =  pmmap_bak + timestamp_len;
    for(group_iter it = groups.begin(); it != groups.end(); it++) {
        string select_query = gen_select_query(*it, database_name, table_name, start_date, end_date); //生成这个股票小组的查询请求
        int64_t group_len = (*it).size();                                                             //本小组含有的股票个数

        m_threads.push_back(std::thread(select_group_then_write_memory, clickhouse_addr, select_query, group_start, group_len, value_len, row_byte_size));
        group_start = group_start + group_len * value_len;                                            //移动到下一个小组的起始位置
    }

    for(vector<thread>::iterator it = m_threads.begin(); it != m_threads.end(); it++) {
        (*it).join();
    }

    close(fd);
    return 0;
}

void print_symbols(vector<string>& symbols) {
    for(vector<string>::iterator it = symbols.begin(); it != symbols.end(); it++) {
        cout << *it << endl;
    }
}

/*
*  查询timestamp列，并将结果写入内存之中
*/
void select_timestamp_then_write_memory(const string &addr, const string &query, char **pshared, uint64_t stamp_byte_size, uint64_t row_byte_size) {
    Client client(ClientOptions().SetHost(addr));
    //std::lock_guard<std::mutex> guard(mtx);
    client.Select(query, [pshared, stamp_byte_size, row_byte_size] (const Block& block) {
        char *pmm = *pshared;
        for(size_t i = 0; i < block.GetRowCount(); i++) {
            memcpy(pmm, std::string(block[0]->As<ColumnFixedString>()->At(i)).c_str(), stamp_byte_size);
            pmm += (row_byte_size);
        }
        *pshared = pmm;
    });
}

/*
*  查询symbol_group列，并将结果写入内存之中
*/ 
void select_group_then_write_memory(const string &addr, const string &query, char *shared, uint64_t group_len, uint64_t factor_val_size, uint64_t row_byte_size) {
    Client client(ClientOptions().SetHost(addr));
    //std::lock_guard<std::mutex> guard(mtx);

    char **pshared = &shared;
    client.Select(query, [pshared, group_len, factor_val_size, row_byte_size] (const Block& block) {
        char *pmm = *pshared;
        for(size_t i = 0; i < block.GetRowCount(); i++) {
            for(int j = 0; j < group_len; j++) {
                memcpy(pmm, (char*)&(block[j]->As<ColumnFloat64>()->At(i)), factor_val_size);
                pmm += factor_val_size;
            }
            pmm += (row_byte_size - group_len * factor_val_size);
        }
        *pshared = pmm;
    });
}

/*
* 得到因子表中所有的symbol列表
*/
vector<string> get_all_symbols(string addr, string db, string table) {
    Client client(ClientOptions().SetHost(addr));
    vector<string> m_columns;
    client.Select(string("desc ") + db + "." + table, [&m_columns] (const Block& block)
    {
        for (size_t i = 1; i < block.GetRowCount(); ++i) {
            m_columns.push_back(string(block[0]->As<ColumnString>()->At(i)));
        }
    });
    return m_columns;
}

/*
* 获取select出来的有效行数
*/
uint64_t get_row_num(string addr, string db, string table, string start_date, string end_date) {
    Client client(ClientOptions().SetHost(addr));
    string query = "select count(*) from " + db + "." + table + 
        " where timestamp >= " + "'" + start_date + "'" + " and timestamp <= " + "'" + end_date + "'";
    uint64_t res = 0;
    client.Select(query, [&res] (const Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            res = block[0]->As<ColumnUInt64>()->At(i);
        }
    });
    return res;
}

/*
* 将股票列表等分成小组
*/
group_vector get_symbol_groups(vector<string>& symbols, int group_size) {
    group_vector m_res;
    int symbol_size = symbols.size();               //股票个数

    vector<string>::iterator m_begin = symbols.begin();
    vector<string>::iterator m_end = symbols.end();

    //如果用户输入的股票个数 < 组大小, 组数为1, 组中元素是全体股票
    if(symbol_size < group_size) {
        vector<string> m_subvec(m_begin, m_end);
        m_res.push_back(m_subvec);
    } else {
        int group_num   = symbol_size / group_size;      //“完整”的组个数
        int remain_size = symbol_size % group_size;      //剩余组中的股票个数
    
        for(int i = 0; i < group_num; i++) {
            vector<string>::iterator end = m_begin + group_size;
            vector<string> m_subvec(m_begin, end);
            m_res.push_back(m_subvec);
            m_begin = end;
        }

        if(remain_size > 0) {
            vector<string> m_subvec(m_begin, m_end);
            m_res.push_back(m_subvec);
        }
    }
    
    return m_res;
}

/*
* 为每个股票小组生成查询串
*/
string gen_select_query(vector<string>& fields, string db, string table, string start_date, string end_date) {
    string res = "select ";
    for(vector<string>::iterator it = fields.begin(); it != fields.end(); it++) {
        res += *it + ",";
    }
    res = res.substr(0, res.size()-1);
    res += " from " + db + "." + table + 
        " where timestamp >= " + "'" + start_date + "'" + " and timestamp <= " + "'" + end_date + "'";
    return res;
}

/*
* 将逗号分隔的字符串分解成字符串vector
* example: "S000001, S000002, S000003" -> vector<string>("S000001", "S000002", "S000003")
*/
vector<string> parse_symbol_list(const string& str) {
    vector<string> m_vec;
    std::stringstream ss(str);

    while(ss.good()) {
        string substr;
        getline(ss, substr, ',');
        m_vec.push_back(substr);
    }

    return m_vec;
}
