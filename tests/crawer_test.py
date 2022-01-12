import MagicClick.MarketCrawler as crawler
import time

if __name__ == "__main__":
    # 1. 日间K线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_day_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_day_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_day_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)

    # 2. 周间k线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_week_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_week_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_week_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)

    # 3. 月间k线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_month_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_month_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_month_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)

    # 4. 5分钟k线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_5_minutes_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_5_minutes_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_5_minutes_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)

    # 5. 15分钟k线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_15_minutes_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_15_minutes_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_15_minutes_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)

    # 6. 30分钟k线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_30_minutes_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_30_minutes_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_30_minutes_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)

    # 6. 30分钟k线数据获取回测
    start = time.time()
    df_sh_600000 = crawler.get_60_minutes_k_data_pre_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_60_minutes_k_data_post_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
    
    start = time.time()
    df_sh_600000 = crawler.get_60_minutes_k_data_no_adjust("sh.600000", [])
    print("time cost: ", time.time() - start)
    print(df_sh_600000)
