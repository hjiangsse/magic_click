# DataPlatForm
DataPlatForm is a thin client which can read(write) clickhouse data in a blazing fast speed! It transforms column strored data
to pandas.DataFrame seamlessly, make data analyst's life easier!

## 1. 软件包的构建和安装:
### 1.1 构建:
安装python包构建工具:
``` bash
python -m pip install --upgrade build
```

在pyproject.toml同目录下，运行下面命令:
``` bash
python -m build
```
构建成功后，在dist目录下会生成两个包文件；用户可以选择其中任意一个进行安装。

### 1.2 安装:
将安装包拷贝到目标机器，运行下面命令安装：
``` bash
pip install --force-reinstall rdsp-client-ck-0.0.3.tar.gz -i https://pypi.tuna.tsinghua.edu.cn/simple
```
安装过程中，会自动获取关联包。

## 2.使用样例:
### 2.1 因子数据的写入、读取和删除:
``` python
import DataPlatForm.FactorRead as fread
import DataPlatForm.FactorWrite as fwrite
import DataPlatForm.Utils as utils

import time

def gen_symbol_list(num):
    symbols = []
    for i in range(num):
        symbols.append("%06d" % (i + 1))
    return symbols

if __name__ == "__main__":
    symbols = gen_symbol_list(1200)
    factor = "factor_jingle_test"
	
	#生成上述产品2005-01-01以及之后的因子数据
    df = utils.gen_factor_dataframe(symbols, "2005-01-01", 1201)
    print(df)

    fwrite.write_factor(factor, df)

	#读取上述写入结果
    res = fread.read_factor(factor, "2005-01-01", "2020-01-01", [])
    print(res)

	#删除因子数据中"2020-01-01" 到 "2020-01-05"的数据
    fwrite.delete_factor(factor, "2005-01-01", "2020-01-01")
    res = fread.read_factor(factor, "2020-01-01", "2020-01-30", [])
    print(res)
```
写入和读取出的因子frame样例如下:  
```
          2006-01-02  2006-01-03  2006-01-04  2006-01-05  2006-01-06  
000001    0.697378    0.327902    0.151692    0.059930    0.633121  
000002    0.314261    0.949350    0.357480    0.382293    0.702390  
000003    0.684868    0.452919    0.548944    0.045304    0.497855  
000004    0.741715    0.336021    0.850488    0.100983    0.982476  
000005    0.819869    0.198847    0.668904    0.163227    0.087648  
...            ...         ...         ...         ...         ...  
003996    0.635063    0.085754    0.320411    0.117137    0.634944  
003997    0.714408    0.205897    0.073714    0.705919    0.398208  
003998    0.140696    0.126257    0.498904    0.581137    0.437986  
003999    0.500233    0.772402    0.198684    0.406736    0.222578  
004000    0.991414    0.461154    0.509910    0.420753    0.304597  
```
index是股票列表，columns是日期列表
### 2.2 raw二维数据写入、读取和删除:
``` python
import DataPlatForm.RawRead as rread
import DataPlatForm.RawWrite as rwrite
import DataPlatForm.Utils as utils

if __name__ == "__main__":
    #生成二维数据
    frame = utils.gen_random_frame(['home', 'value', 'america', 'china', 'japan'], "2005-01-01 09:00:00", 3600)
    print(frame)

    #将数据写入到数据库的表中，表名为name(下面写入两张表)
    rwrite.write_frame("name", frame, "2021-11-25")

    #读取刚写入的数据
    res = rread.read_frame("name", "2005-01-01", "2022-01-01", [])
    print(res)
	
	#删除刚写入的数据
	rread.delete_frame("name", "2021-11-25")
```

### 2.3 raw列表数据写入、读取和删除:
``` python
import DataPlatForm.RawRead as rread
import DataPlatForm.RawWrite as rwrite
import DataPlatForm.Utils as utils
import DataPlatForm.configs as confs

if __name__ == "__main__":
    #生成列表数据
    date_lst = ["2005-01-01 09:00:00", "2005-01-01 09:00:01", "2005-01-01 09:00:02"]

    #写入列表数据库中
    rwrite.write_list("list1", date_lst)

    #从列表数据库中读取
    res = rread.read_list("list1")
    print(res)
```

## 3. APIs
### 3.1 因子数据访问和写入接口
#### 因子数据读取
FactorRead.read_factor(factor, start_date, end_date, symbols)  
> factor: 因子名  
> start_date: 开始日期  
> end_date: 结束日期  
> symbols: 股票列表（如果用户输入为空，默认获取所有股票数据）  

#### 因子数据写入
FactorWrite.write_factor(factor, dataframe)
> factor: 因子名  
> dataframe: index为股票列表，columns为日期列表 的因子数据  
> 如果因子不存在，创建一张新的因子数据表，并将数据写入表中  
> 如果因子已经存在，表中原有的数据不会变动，只会写入dataframe中新的数据（新日期的数据，新股票的数据）  

#### 因子数据删除
FactorWrite.delete_factor(factor, start_date, end_date)
> factor: 因子名  
> start_date: 开始日期  
> end_date: 结束日期  

#### 因子日期索引获取
FactorRead.get_factor_index(factor)
> 获取因子数据表的全部日期索引  

#### 因子股票列表获取
FactorRead.get_factor_columns(factor)
> 获取因子数据表的的全部股票列表  

### 3.2 raw数据访问和写入接口
#### raw二维数据的读取
RawRead.read_frame(name, start_date, end_date, fields=[])
> name: 二维数据库名 e.g "yk", "jq_post"  
> start_date: 开始日期  
> end_date: 结束日期  
> columns: 用户输入的属性列表   
> 返回值: dict, key是数据库中所有表名，value是name数据库中所有表的查询结果  

#### raw列表数据读取
RawRead.read_list(name)
> name: 列表名  
> 返回值: list  

#### raw二维数据的写入
RawWirte.write_frame(name, frame, date)
> name: 名称  
> frame: 数据  
> data: pandas.DataFrame数据  

#### raw列表数据写入
RawWirte.write_list(name, data)
> name: 列表名  
> data: python list  

#### raw二维数据删除接口
RawWirte.delete_frame(name, date)
> name: 数据名
> date: 日期

### 列表数据删除接口
RawWirte.delete_list(name)
> name: 列表名

## 4. 性能测试
### 4.1 因子数据读取测试
4000 * 4000 的float64因子矩阵，读取时间在1s左右  
（221机器上测试，带宽10Gbps/s）
### 4.2 二维数据写入测试
以jq_post数据表为例(表结构如下)：
```
   "jq_post": [('timestamp', 'FixedString(19)'),
                ('avg', 'Float64'),
                ('close', 'Float64'),
                ('high', 'Float64'),
                ('low', 'Float64'),
                ('open', 'Float64'),
                ('money', 'Float64'),
                ('volume', 'Float64'),
                ('volume_ratio', 'Float64')]
```
插入100w行数据耗时：7.114301681518555  
（221机器上测试，带宽10Gbps/s）

## 5. 项目时间节点：

工作时间段|完成任务列表
----------|---------
2021-11-01-2021-11-14|Clickhouse基础知识探索，并发编程知识探索，建立知识储备
2021-11-15-2021-11-19|因子数据插入，读取和删除代码编写，用例测试
2021-11-20-2021-11-24|raw数据插入，读取和删除代码编写，用例测试
2021-11-25-2021-11-28|所有代码集成测试，代码优化，raw数据删除异步延迟状况优化，第一版本交付业务部门使用
