#include <iostream>
#include <thread>
#include <type_traits>
#include <chrono>
#include <clickhouse/client.h>

#include <Python.h>
#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

using namespace std;
using namespace clickhouse;
using namespace boost::python;

#define CLICKHOUSE_ADDR "127.0.0.1"
#define DATABASE_NAME "factor_database"
#define TABLE_NAME "factor_big_table12"
#define START "2005-01-01"
#define END "2020-01-01"
#define GROUP_NUM 10
#define TIMESTAMP_LEN 10
#define FACTORVAL_LEN 8

using group_vector = vector<vector<string>>;
using symbol_vector = vector<string>;

/*
* 得到因子表中所有的symbol列表
*/
symbol_vector get_all_symbols(string db, string table)
{
    Client client(ClientOptions().SetHost(CLICKHOUSE_ADDR));
    symbol_vector m_columns;
    client.Select(string("desc ") + db + "." + table, [&m_columns](const Block &block)
                  {
                      for (size_t i = 1; i < block.GetRowCount(); ++i)
                      {
                          m_columns.push_back(string(block[0]->As<ColumnString>()->At(i)));
                      }
                  });
    return m_columns;
}

vector<string> query_tables(string db, string table)
{
    Client client(ClientOptions().SetHost(CLICKHOUSE_ADDR));
    vector<string> result;
    client.Select("show tables from " + db + " like '%" + table + "%'", [&result](const Block &block)
                  {
                      for (size_t i = 0; i < block.GetRowCount(); ++i)
                      {
                          result.push_back(string(block[0]->As<ColumnString>()->At(i)));
                      }
                  });
    return result;
}

/*
* 获取select出来的有效行数
*/
uint64_t get_row_num(string db, string table, string start_date, string end_date)
{
    cout << "get_row_num()" << endl;
    Client client(ClientOptions().SetHost(CLICKHOUSE_ADDR));
    string query = "select count(*) from " + db + "." + table +
                   " where timestamp >= " + "'" + start_date + "'" + " and timestamp <= " + "'" + end_date + "'";
    uint64_t res = 0;
    cout << query << endl;
    client.Select(query, [&res](const Block &block)
                  {
                      for (size_t i = 0; i < block.GetRowCount(); ++i)
                      {
                          res = block[0]->As<ColumnUInt64>()->At(i);
                      }
                  });
    return res;
}

/*
* 为股票小组生成查询串
*/
string gen_select_query(vector<string> &fields, string db, string table, string start_date, string end_date)
{
    string res = "select timestamp,";
    for (vector<string>::iterator it = fields.begin(); it != fields.end(); it++)
    {
        res += *it + ",";
    }
    res = res.substr(0, res.size() - 1);
    res += " from " + db + "." + table +
           " where timestamp >= " + "'" + start_date + "'" + " and timestamp <= " + "'" + end_date + "'";
    return res;
}

class TableQuery
{
public:
    void SelectQuery(symbol_vector &fields, string db, string table, string start_date, string end_date)
    {
        string query = gen_select_query(fields, db, table, start_date, end_date);
        //    cout << "query:" << query << endl;
        row_count = 0;
        Client client(ClientOptions().SetHost(CLICKHOUSE_ADDR));
        client.Select(query, [this](const Block &block)
                      {
                         // cout << "result:" << block.GetRowCount() << "," << block.GetColumnCount() << endl;
                          if (block.GetRowCount() > 0)
                          {
                              blocks.push_back(block);
                          }
                      });
    }

    int GetRowCount()
    {
        for (auto &&blk : blocks)
        {
            row_count += blk.GetRowCount();
        }
        //cout << "TableQuery.GetRowCount()==" << row_count << endl;
        return row_count;
    }

    string GetStringValue(int row, int col)
    {
        int rc = 0;
        for (auto &&blk : blocks)
        {
            if (row < rc + blk.GetRowCount())
            {
                return string(blk[col]->As<ColumnFixedString>()->At(row - rc));
            }
            rc += blk.GetRowCount();
        }
        return "";
    }

    double GetFloatValue(int row, int col)
    {
        int rc = 0;
        for (auto &&blk : blocks)
        {
            if (row < rc + blk.GetRowCount())
            {
                return blk[col]->As<ColumnFloat64>()->At(row - rc);
            }
            rc += blk.GetRowCount();
        }
        return 0;
    }

    vector<double> GetFloatValuesByCol(int col)
    {
        vector<double> values;
        for (auto &&blk : blocks)
        {
            for (int i = 0; i < blk.GetRowCount(); i++)
                values.push_back(blk[col]->As<ColumnFloat64>()->At(i));
        }
        return values;
    }

private:
    vector<Block> blocks;
    int row_count;
};

BOOST_PYTHON_MODULE(libck)
{ // This will enable user-defined docstrings and python signatures,
    // while disabling the C++ signatures
    scope().attr("__version__") = "1.0.0";
    scope().attr("__doc__") = "a demo module to use boost_python.";
    docstring_options local_docstring_options(true, false, false);
    def("get_row_num_c", &get_row_num, "获取select出来的有效行数.\n");
    def("get_all_symbols_c", &get_all_symbols, "得到因子表中所有的symbol列表.\n");
    def("query_tables_c", &query_tables, "查看表是否存在.\n");

    class_<TableQuery>("TableQuery")
        .def("SelectQuery", &TableQuery::SelectQuery)
        .def("GetRowCount", &TableQuery::GetRowCount)
        .def("GetStringValue", &TableQuery::GetStringValue)
        .def("GetFloatValue", &TableQuery::GetFloatValue)
        .def("GetFloatValuesByCol", &TableQuery::GetFloatValuesByCol);

    class_<symbol_vector>("symbol_vector")
        .def(vector_indexing_suite<symbol_vector>());
    class_<vector<double>>("double_vector")
        .def(vector_indexing_suite<vector<double>>());
}
