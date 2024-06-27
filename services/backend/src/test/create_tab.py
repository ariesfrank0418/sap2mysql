


import pymysql 
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import Column,Boolean,SmallInteger, Integer, BigInteger
from sqlalchemy import Float,DECIMAL,CHAR, String, Enum, Date, Time, DateTime,func,text

# sap连接 
from pyrfc import Connection 
sapconn = Connection(ashost='www.saps4hana.cn', sysnr='20', client='110', user='s110-119', passwd='tfakai0418')


# 与mysql连接####################################
# 格式为 'mysql+pymysql://账号名:密码@ip:port/数据库名'
SQLALCHEMY_DATABASE_URI:str = "mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8"
# 生成一个SQLAlchemy引擎
engine = create_engine(SQLALCHEMY_DATABASE_URI,pool_pre_ping=True)
# 生成sessionlocal类，这个类的每一个实例都是一个数据库的会话
# 注意命名为SessionLocal，与sqlalchemy的session分隔开
""" SessionLocal  = sessionmaker(autocommit=False,autoflush=False,bind=engine)
session = SessionLocal ()
metadata = MetaData() """

table_name = 'ACDOCA'

result = sapconn.call('ZFU_GET_TAB_DATA', 
                      IV_TABLE= table_name, 
                      IV_GET_DATA='',
                      IV_GET_STRUCTURE='X',
                      IV_GET_COUNT='',
                      IV_KEY1_FIELD='',
                      IV_KEY1_FROM='',
                      IV_KEY1_TO='',
                      IV_WHERE='' 
                    )


# print(result['ET_TAB_INFO'])

has_table = engine.dialect.has_table(engine.connect(), table_name) 
if not has_table:
    metadata = MetaData()
    # 定义表格
    schemaItem_list = []

    for field in result['ET_TAB_INFO']:
        field_name = field['FIELDNAME']
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
        schemaItem_list.append(Column(field_name, field_type, primary_key=is_key,autoincrement='')) 
    # end for

    schemaItem_tuple = tuple(schemaItem_list)
    myTable = Table(table_name, metadata,*schemaItem_tuple)
    # 创建表格
    metadata.create_all(engine)
else:
    print('Table already exists')