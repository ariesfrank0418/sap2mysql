
# sap连接 
from pyrfc import Connection 
sapconn = Connection(ashost='www.saps4hana.cn', sysnr='20', client='110', user='s110-119', passwd='tfakai0418')

import uuid
import datetime
import sqlalchemy
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from sqlalchemy import Column,Boolean,SmallInteger, Integer, BigInteger
from sqlalchemy import Float,DECIMAL,CHAR, String, Enum, Date, Time, DateTime,func,text
 
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

tabClassPara = {}

 

# 动态定义一个类 
tabClassPara['__tablename__'] = table_name
for field in tab_info:
    if field['INTTYPE'] == 'C' or field['INTTYPE'] == 'N': 
        field_type = String(int(field['LENG']))
    elif field['INTTYPE'] == 'D':
        field_type = Date
    elif field['INTTYPE'] == 'T':
        field_type = Time
    elif field['INTTYPE'] == 'F':
        field_type = Float
    elif field['INTTYPE'] == 'P':
        field_type = DECIMAL(int(field['LENG']),int(field['DECIMALS']))
    elif field['INTTYPE'] == 'X':
        if field['DATATYPE'] == 'INT1':
            field_type = Boolean
        elif field['DATATYPE'] == 'INT2':
            field_type = SmallInteger
        elif field['DATATYPE'] == 'INT4':
            field_type = Integer
        elif field['DATATYPE'] == 'INT8':
            field_type = BigInteger
    else:
        field_type = String(int(field['LENG']))
    if field['KEYFLAG'] == 'X':
        is_key = True
    else:
        is_key = False 
    col_info = Column(field_type, primary_key=is_key,autoincrement='') 
    tabClassPara[field['FIELDNAME']] = col_info


MyClass = type('MyClass', (Base,), tabClassPara)
my_instance = MyClass()

print(tab_info)
# for wa in result['ET_TAB']:
   