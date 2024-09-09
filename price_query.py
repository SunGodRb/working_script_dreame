# %%
# %%
#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File    :   calculate_price
@Time    :   2024/08/28 16:12:52
@Author  :   Dehua.Liu
@Version :   1.0
@Desc    :  
'''

# %%
#导入关键库
import pandas as pd
# import pymysql
# from IPython.display import display
import sys
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Border, Side,Font
import openpyxl
import numpy as np
import time
import psycopg2
from psycopg2 import OperationalError
pd.set_option('display.max_rows',50) 
from datetime import date
today = date.today()

# %%
# 告示文本(外)
def outer_text(text):
    # time.sleep(2)
    print("\n >>>{}".format(text))
# 告示文本(内)
def inner_text(text):
    # time.sleep(1)
    print("    ...{}".format(text))

# %%
# 连接pg数据库
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        outer_text("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        outer_text(f"The error '{e}' occurred")
    return connection

# 关闭pg数据库
def close_connection(connection):
    if connection:
        connection.close()
        outer_text("The connection is closed")
# 查询语句
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        outer_text(f"The error '{e}' occurred")
# # 使用示例，连接
# connection = create_connection("dwh", "hw_dws_sc_readonly", r"l8/;6\4o}8rEE", "172.26.186.79", "8000")
# # 使用示例，查询
# select_query = "SELECT * FROM public.dm_sap_srm_price"
# users = execute_read_query(connection, select_query)
# # 使用示例，关闭
# close_connection(connection)

# %%
all_material_price_query = """
-- 将所有物料及其信息取出
with total_material as(
SELECT 
itemcode 物料编码
,itemname 物料名称
,suppliercode 供应商编码
,suppliername 供应商名称
,productionline 产品线
,purorganizationcode 采购组织编码
,purorganizationname 采购组织名称
,validdatefrom 有效期开始日期
,validdateto 有效期截止日期
,pricelibrarystatus 价格库状态
,(case 
WHEN pricecategory = 'PB00' then '正式价'
WHEN pricecategory = 'ZPB0' then '试产价'
WHEN pricecategory = 'CPB0' then '暂估价'
else '' 
end ) as 价格类型
,currencycode 币种代码
,taxincludedflag 含税标志
,exchangeratedate 汇率日期
,exchangeratetype 汇率类型
,pertaxincludedprice ::numeric 含税单价
,pernetprice ::numeric 净单价
,pcbamaterial_costs pcba材料费
,pcbaprocessing_fees  pcba加工费
FROM "ods_sc_srm_item_latest_price"
where 1=1 
and (
to_date(validdateto) >= TRUNC(SYSDATE)
or pricelibrarystatus = 'VALID'
))

select * from total_material
where 1=1
and (产品线 = '扫地机' or 产品线 is null)
"""

# %%
low_material_price_query = """ 
-- 将所有物料及其信息取出
with total_material as(
SELECT 
itemcode 物料编码
,itemname 物料名称
,suppliercode 供应商编码
,suppliername 供应商名称
,productionline 产品线
,purorganizationcode 采购组织编码
,purorganizationname 采购组织名称
,validdatefrom 有效期开始日期
,validdateto 有效期截止日期
,pricelibrarystatus 价格库状态
,(case 
WHEN pricecategory = 'PB00' then '正式价'
WHEN pricecategory = 'ZPB0' then '试产价'
WHEN pricecategory = 'CPB0' then '暂估价'
else '' 
end ) as 价格类型
,currencycode 币种代码
,taxincludedflag 含税标志
,exchangeratedate 汇率日期
,exchangeratetype 汇率类型
,pertaxincludedprice ::numeric 含税单价
,pernetprice ::numeric 净单价
,pcbamaterial_costs pcba材料费
,pcbaprocessing_fees  pcba加工费
FROM "ods_sc_srm_item_latest_price"
where 1=1 
and purorganizationcode in ('1100','1101')
and suppliername not like '%追觅%'
and suppliername not like '%追创%'
and suppliername not like '%敏华%'
and suppliername not like '%工厂%'
and suppliername not like '%扫地机%'
and suppliername not like '%洗地机%'
and suppliername not like '%翻新%'
and (
to_date(validdateto) >= TRUNC(SYSDATE)
or pricelibrarystatus = 'VALID'
)),

-- 对存在料工费的材料进行filter，保留料工费俱在的行记录
cost_fee_material as (select distinct 物料编码 from total_material 
where 1=1 
and pcba材料费 is not null and pcba加工费 is not null
and pcba材料费 !='0' and pcba加工费 !='0'
-- and pcba材料费 !=0 and pcba加工费 !=0
),

-- 分别查询不含料工费的原材料和包含料工费的材料，确保有料工费的物料被取到的都是料工费，然后再连接起来
process_material as (
select * from total_material where 物料编码 not in (select 物料编码 from cost_fee_material)
union all
select * from total_material where 1=1 
and 物料编码 in (select 物料编码 from cost_fee_material) 
and pcba材料费 is not null and pcba加工费 is not null
and pcba材料费 !='0' and pcba加工费 !='0'
-- and pcba材料费 !=0 and pcba加工费 !=0
),

-- 对物料进行排序
rank_material AS
(select
*
,row_number() over (PARTITION BY 物料编码 ORDER BY 含税单价 ) 价格排序
from process_material
order by 物料编码)

select * from rank_material
where 1=1 
and 价格排序 = 1
and 价格类型 = '正式价'
and (产品线 = '扫地机' or 产品线 is null)
"""

# %%
conn_pgsql = create_connection("dwh", "hw_dws_sc_readonly", r"l8/;6\4o}8rEE", "172.26.186.79", "8000")
outer_text('查询价格库中所有有效价格')
all_material_price_dic = execute_read_query(conn_pgsql, all_material_price_query)
all_material_price = pd.DataFrame(all_material_price_dic,columns=[
'物料编码','物料名称','供应商编码','供应商名称','产品线','采购组织编码','采购组织名称','有效期开始日期','有效期截至日期','价格库状态','价格类型',
'币种代码','含税标志','汇率日期','汇率类型','含税单价','净单价','pcba加工费','pcba材料费'])
outer_text('查询价格库中最低价格')
low_material_price_dic = execute_read_query(conn_pgsql, low_material_price_query)
low_material_price = pd.DataFrame(low_material_price_dic,columns=[
'物料编码','物料名称','供应商编码','供应商名称','产品线','采购组织编码','采购组织名称','有效期开始日期','有效期截至日期','价格库状态','价格类型',
'币种代码','含税标志','汇率日期','汇率类型','含税单价','净单价','pcba加工费','pcba材料费','价格排序'])
close_connection(conn_pgsql)

# %%
low_material_price.drop(columns = ['价格排序'],inplace = True)

# %%
# 使用ExcelWriter将两个DataFrame保存到不同的sheet中
outer_text('将全量物料价格以及最低物料价格分sheet保存')
with pd.ExcelWriter('{}价格查询.xlsx'.format(today), engine='openpyxl') as writer:
    low_material_price.to_excel(writer, sheet_name='物料最低价格查询', index=False)
    all_material_price.to_excel(writer, sheet_name='全量物料价格查询', index=False)
    


