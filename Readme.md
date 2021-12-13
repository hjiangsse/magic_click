# MagicClick
MagicClick is a python library which is designed to finish the following tasks:
* Crawl market data from open souce internet data hub;  
* Write the crawled data into a clickhouse cluster data center;  
* Read data from the data center as pandas.DataFrame;  

## 1. Market data crawling:
### 1.1 day k data get:
> get_day_k_data(code, data_columns, start_date, end_date, adjust_flag='3')  
> code: "SZ.000001"  
> data_columns: "" stands for all columns
> start_date: "YYYY-MM-DD"  
> end_date: "YYYY-MM-DD"  
> adjust_flag:   
```
  '3': "行情未复权"
  '2': "行情前复权"
  '1': "行情后复权"
```

## 5. 项目时间节点：

工作时间段|完成任务列表
----------|---------
2021-11-01-2021-11-14|Clickhouse基础知识探索，并发编程知识探索，建立知识储备
2021-11-15-2021-11-19|因子数据插入，读取和删除代码编写，用例测试
2021-11-20-2021-11-24|raw数据插入，读取和删除代码编写，用例测试
2021-11-25-2021-11-28|所有代码集成测试，代码优化，raw数据删除异步延迟状况优化，第一版本交付业务部门使用
