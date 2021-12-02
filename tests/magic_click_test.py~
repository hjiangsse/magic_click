import FastClick.MarketCrawler as crawler
import FastClick.Utils as utils

if __name__ == "__main__":
    #astocks = crawler.get_astock_list()
    #print(astocks)
    frame = crawler.get_day_k_data("sh.600000", [], "2000-01-01", "2000-01-10")
    print(frame)

    fields_info = utils.gen_table_fields_infos_from_dataframe(frame)
    print(fields_info)