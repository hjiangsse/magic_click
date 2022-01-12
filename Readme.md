*MagicClick* 主要实现了以下功能：

* 抓取互联网上免费的金融数据;  
* 提供写入数据到clickhouse数据库的接口;  
* 提供从clickhouse数据库读取数据的接口;  

# 1. 安装
``` bash
pip install --force-reinstall ./dist/magic-click-0.0.3.tar.gz -i https://pypi.tuna.tsinghua.edu.cn/simple
```

# 2. 使用样例(tests中有更详细的案例):
## 2.1 K线数据抓取:
``` python
import MagicClick.KCrawler as crawler
import time

if __name__ == "__main__":
    #日间K线数据获取回测
    df_sh_600000 = crawler.get_day_k_data_pre_adjust("sh.600000", [])
    print(df_sh_600000)
    
    df_sh_600000 = crawler.get_day_k_data_post_adjust("sh.600000", [])
    print(df_sh_600000)
    
    df_sh_600000 = crawler.get_day_k_data_no_adjust("sh.600000", [])
    print(df_sh_600000)
```

# 3. 接口分类和汇总:
## 3.1 K线数据抓取:
* KCrawler.get_astock_list()                                                                  #利用akshare提供的数据获取接口，获得A股票名称列表
* KCrawler.get_day_k_data_pre_adjust(code, columns, start_date=None, end_date=None)           #得到一支股票日k前复权数据
* KCrawler.get_day_k_data_post_adjust(code, columns, start_date=None, end_date=None)          #得到一支股票日k后复权数据
* KCrawler.get_day_k_data_no_adjust(code, columns, start_date=None, end_date=None)            #得到一支股票日k未复权数据
* KCrawler.get_week_k_data_pre_adjust(code, columns, start_date=None, end_date=None)          #得到一支股票周k前复权数据
* KCrawler.get_week_k_data_post_adjust(code, columns, start_date=None, end_date=None)         #得到一支股票周k后复权数据
* KCrawler.get_week_k_data_no_adjust(code, columns, start_date=None, end_date=None)           #得到一支股票周k未复权数据
* KCrawler.get_month_k_data_pre_adjust(code, columns, start_date=None, end_date=None)         #得到一支股票月k前复权数据
* KCrawler.get_month_k_data_post_adjust(code, columns, start_date=None, end_date=None)        #得到一支股票月k后复权数据
* KCrawler.get_month_k_data_no_adjust(code, columns, start_date=None, end_date=None)          #得到一支股票月k未复权数据
* KCrawler.get_5_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None)     #得到一支股票5分钟k前复权数据
* KCrawler.get_5_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None)    #得到一支股票5分钟k后复权数据
* KCrawler.get_5_minutes_k_data_no_adjust(code, columns, start_date=None, end_date=None)      #得到一支股票5分钟k未复权数据
* KCrawler.get_15_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None)    #得到一支股票15分钟k前复权数
* KCrawler.get_15_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None)   #得到一支股票15分钟k后复权数据
* KCrawler.get_15_minutes_k_data_no_adjust(code, columns, start_date=None, end_date=None)     #得到一支股票15分钟k未复权数据
* KCrawler.get_30_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None)    #得到一支股票30分钟k前复权数据
* KCrawler.get_30_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None)   #得到一支股票30分钟k后复权数据
* KCrawler.get_30_minutes_k_data_no_adjust(code, columns, start_date=None, end_date=None)     #得到一支股票30分钟k未复权数据
* KCrawler.get_60_minutes_k_data_pre_adjust(code, columns, start_date=None, end_date=None)    #得到一支股票60分钟k前复权
* KCrawler.get_60_minutes_k_data_post_adjust(code, columns, start_date=None, end_date=None)   #得到一支股票60分钟k后复权数据
* KCrawler.get_60_minutes_k_data_no_adjust(code, columns, start_date=None, end_date=None)     #得到一支股票60分钟k未复权数据
