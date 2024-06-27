# sap连接 
from pyrfc import Connection 
sapconn = Connection(ashost='www.saps4hana.cn', sysnr='20', client='110', user='s110-119', passwd='tfakai0418')

import pymysql 
pymysql.install_as_MySQLdb()
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from sqlalchemy import Column,Boolean,SmallInteger, Integer, BigInteger
from sqlalchemy import Float,DECIMAL,CHAR, String, Enum, Date, Time, DateTime,func,text
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

def insert_on_conflict_update(table, conn, keys, data_iter):
    # update columns "b" and "c" on primary key conflict
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
    )
    
    key_update = {} 
    for key in keys:
        key_update[key] = getattr(stmt.inserted, key)

    stmt = stmt.on_duplicate_key_update(**key_update)
    result = conn.execute(stmt)
    return result.rowcount

# 格式为 'mysql+pymysql://账号名:密码@ip:port/数据库名'
SQLALCHEMY_DATABASE_URI:str = "mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8"
# 生成一个SQLAlchemy引擎
engine = create_engine(SQLALCHEMY_DATABASE_URI,pool_pre_ping=True)

Base = declarative_base() 


table_name = 'ZFUVBAP'

result = sapconn.call('ZFU_GET_TAB_DATA', 
                      IV_TABLE= table_name, 
                      IV_GET_DATA='X',
                      IV_GET_STRUCTURE='X',
                      IV_GET_COUNT='',
                      IV_KEY1_FIELD='',
                      IV_KEY1_FROM='',
                      IV_KEY1_TO='',
                      IV_WHERE='' 
                    )
tab_data = result['ET_TAB']
tab_info = result['ET_TAB_INFO']

tab_dic = []
tab_line = {}

for field in tab_data:
    if field['FNAME'] in tab_line:
        tab_dic.append(tab_line)
        tab_line = {}
        tab_line[field['FNAME']] = field['DATA']
    else:
        tab_line[field['FNAME']] = field['DATA']
        
df = pd.DataFrame(tab_dic)
df.to_sql(table_name, engine, if_exists='append', index=False,method=insert_on_conflict_update) # 把新数据写入 temp 临时表
print(df)