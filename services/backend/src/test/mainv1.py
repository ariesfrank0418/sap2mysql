from fastapi import FastAPI
from fastapi import Form,Body 
from fastapi import Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware   
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse

import uvicorn
# 类型
from typing import Union 
from pydantic import BaseModel 
from datetime import datetime
import uuid
import time

import pandas as pd
# 数据库

# 连接mysql数据库需要导入pymysql模块
import pymysql 
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from sqlalchemy import Column,Boolean,SmallInteger, Integer, BigInteger
from sqlalchemy import Float,DECIMAL,CHAR, String, Enum, Date, Time, DateTime,func,text


# 常量 #########################################
fake_users_db = {
    "johndoe": {
        "username": "johndoe",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "fakehashedsecret",
        "disabled": False,
    },
    "frank": {
        "username": "frank",
        "full_name": "frank",
        "email": "frank@example.com",
        "hashed_password": "fakehashed123456",
        "disabled": False,
    },
}

Base = declarative_base()  # 生成ORM基类
class sys_task_log(Base):
    __tablename__  = "sys_task_log"  # 表名
    task_id        = Column(CHAR(36), primary_key=True) 
    scheduled_time = Column(DateTime)
    interval       = Column(Integer)
    task_type      = Column(Enum('预定任务', '即时任务','周期任务'))
    execution_time = Column(Integer)
    status         = Column(Enum('未开始', '进行中', '已完成', '错误','已关闭'))
    msg            = Column(String(200)) 

class sys_tab_log(Base):
    __tablename__  = "sys_tab_log"  # 表名
    task_id        = Column(CHAR(36), primary_key=True) 
    tabupdate_id   = Column(CHAR(36), primary_key=True) 
    tab_name       = Column(CHAR(30), primary_key=True)
    date_field     = Column(CHAR(30))
    date_from      = Column(CHAR(8))
    date_to        = Column(CHAR(8))
    add_where      = Column(String(200))
    start_time     = Column(DateTime)
    execution_time = Column(Integer)
    status         = Column(Enum('未开始', '进行中', '已完成', '错误'))
    count          = Column(Integer)
    msg            = Column(String(200))


class Cond(BaseModel):
    tablename: str = None
    fieldname: str = None
    vfrom: str = None 
    vto: str = None 
    where: str = None 
    upto: int = 0 
    count: int = 0


class Task(BaseModel):
    task_Name:str = ''
    tab_list:Cond = []


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
SessionLocal  = sessionmaker(autocommit=False,autoflush=False,bind=engine)
session = SessionLocal ()

#################################################


# 创建fastapi对象
app = FastAPI() 

if __name__ == '__main__':
    uvicorn.run(app,host="0.0.0.0",port=5000)

@app.get("/")
async def index(): 
    return {"message": "Hello World"}
# 权限 ###########################################

def fake_hash_password(password: str):
    return "fakehashed" + password

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    disabled: Union[bool, None] = None

class UserInDB(User):
    hashed_password: str

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

def fake_decode_token(token):
    # This doesn't provide any security at all
    # Check the next version
    user = get_user(fake_users_db, token)
    return user


async def get_current_user(token: str = Depends(oauth2_scheme)):
    user = fake_decode_token(token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    user = UserInDB(**user_dict)
    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == user.hashed_password:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    return {"access_token": user.username, "token_type": "bearer"}

@app.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user


# 数据库操作 #############################################

# 获取所有任务列表
@app.get("/get_all_task/")
async def  get_all_task():#(current_user: User = Depends(get_current_active_user)):   
    ret = session.query(sys_task_log).all() 
    return ret

@app.get("/get_task_by_date/{i_date}")
async def  get_task_by_date(i_date:str):#,current_user: User = Depends(get_current_active_user)):  
    ret = session.query(sys_task_log).filter(func.date(sys_task_log.scheduled_time) == i_date).all()
    return ret

@app.get("/get_task_by_id/{i_uuid}")
async def  get_task_by_id(i_uuid:str):#,current_user: User = Depends(get_current_active_user)):  
    ret = session.query(sys_task_log).filter(func.date(sys_task_log.task_id) == i_uuid).all()
    return ret

@app.get("/get_tablog_by_task_id/{i_uuid}")
async def  get_tablog_by_task_id(i_uuid:str):#,current_user: User = Depends(get_current_active_user)):  
    ret = session.query(sys_tab_log).filter(sys_tab_log.task_id == i_uuid).all()
    return ret

@app.get("/get_one_tablog/{i_uuid}/{tab_name}")
async def  get_one_tablog(i_uuid:str,tab_name:str): 

    ret = session.query(sys_tab_log).filter(sys_tab_log.tabupdate_id == i_uuid,
                                            sys_tab_log.tab_name == tab_name
                                            ).all()
    return ret

@app.get("/get_tablog_by_tabname/{i_tabname}")
async def  get_tablog_by_tabname(i_tabname:str):#,current_user: User = Depends(get_current_active_user)):  
    ret = session.query(sys_tab_log).filter(sys_tab_log.tabname == i_tabname).all()
    return ret

@app.get("/get_tab_structure/{tablename}")
async def get_tab_structure(tablename:str):  
    result = sapconn.call('ZFU_GET_TAB_DATA', IV_TABLE=tablename , IV_GET_STRUCTURE='X' ) 
    return  result['ET_TAB_INFO'] 
 

@app.get("/get_tab_count/{tablename}")
async def get_tab_count(tablename:str):  
    result = sapconn.call('ZFU_GET_TAB_DATA', IV_TABLE=tablename , 
                                           IV_GET_COUNT='X'
                                           )
    return  result['EV_RECORD'] 

# 更新

# 新增即时任务
@app.post("/create_immediate_task/")
async def create_immediate_task(cond_list: list[Cond] = Body(...)):#,current_user: User = Depends(get_current_active_user)):  
    #新增一条数据
    lv_uuid = uuid.uuid1()
    obj1 = sys_task_log(
        task_id        = lv_uuid, 
        scheduled_time = datetime.now(),
        interval       = 0,
        task_type      = '即时任务',
        execution_time = 0,
        status         = '进行中',
        msg            = ''
    ) 
    session.add(obj1) 

    for cond in cond_list:
        obj2 = sys_tab_log(
            task_id        = lv_uuid,
            tabupdate_id   = lv_uuid,
            tab_name       = cond.tablename,
            date_field     = cond.fieldname,
            date_from      = cond.vfrom,
            date_to        = cond.vto,
            add_where      = cond.where,
            start_time     = datetime.now(),
            execution_time = 0,
            status         = '进行中',
            count          = 0,
            msg            = ''
            )  
        session.add(obj2) 

    session.commit() 

    return lv_uuid


@app.post("/sap_multiple_tab_data_to_mysql/")
async def sap_multiple_tab_data_to_mysql(cond_list: list[Cond] = Body(...)):#,current_user: User = Depends(get_current_active_user)):  
    return_cond = []
    for cond in cond_list:
        cond.count = sap_single_tab_data_to_mysql(cond)
        return_cond.append(cond)
    return return_cond

@app.get("/run_immediate_task/{task_id}")
async def run_immediate_task(task_id:str): 
     
    tab_list = await get_tablog_by_task_id(task_id)
     
    for tab in tab_list:
        await sap_single_tab_data_to_mysql_by_id(tab.tabupdate_id,tab.tab_name) 

    return tab_list

@app.get("/sap_single_tab_data_to_mysql_by_id/{tabupdate_id}/{tab_name}/")
async def sap_single_tab_data_to_mysql_by_id(tabupdate_id: str,tab_name:str): 
    start_time = time.time()
    tab_log = await get_one_tablog(tabupdate_id,tab_name) 
    count = await sap_single_tab_data_to_mysql( Cond( 
            tablename = tab_log[0].tab_name ,
            fieldname = tab_log[0].date_field ,
            vfrom = tab_log[0].date_from ,
            vto = tab_log[0].date_to ,
            where = tab_log[0].add_where  
        ) )
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    session.query(sys_tab_log).filter(
        sys_tab_log.tabupdate_id == tabupdate_id,
        sys_tab_log.tab_name == tab_name 
        ).update({
        'count': count,
        'status':'已完成',
        'execution_time': execution_time
    })
    session.commit()
    
    return count
 
@app.post("/sap_single_tab_data_to_mysql/")
async def sap_single_tab_data_to_mysql(cond: Cond): #,current_user: User = Depends(get_current_active_user)):  

    result = sapconn.call('ZFU_GET_TAB_DATA', IV_TABLE=cond.tablename , 
                                           IV_GET_COUNT='X',
                                           IV_KEY1_FIELD=cond.fieldname,
                                           IV_KEY1_FROM=cond.vfrom,
                                           IV_KEY1_TO=cond.vto,
                                           IV_WHERE=cond.where
                                           )
    lv_total  = 0
    lv_total = result['EV_RECORD']

    if lv_total == 0:
        return '0'
    else:
       lv_count_max = 0
       lv_offset = 0
       #lv_upto = cond.upto
       lv_upto = 100
       lv_current = 0 

    while lv_current < lv_total:  
        result = sapconn.call('ZFU_GET_TAB_DATA', IV_TABLE=cond.tablename , 
                                               IV_GET_DATA='X',
                                               IV_KEY1_FIELD=cond.fieldname,
                                               IV_KEY1_FROM=cond.vfrom,
                                               IV_KEY1_TO=cond.vto,
                                               IV_WHERE=cond.where,
                                               IV_OFFSET=lv_offset,
                                               IV_UPTO=lv_upto
                                               )
        lv_current = lv_current + lv_upto
        lv_offset = lv_offset + lv_upto
        if result['EV_JSON'] == '':
            break
        df = pd.read_json(result['EV_JSON'],encoding="utf-8", orient='records' ,convert_axes=False,dtype=False,convert_dates=True) 
        df.to_sql(f'{cond.tablename.lower()}_temp', engine, if_exists='append', index=False) # 把新数据写入 temp 临时表

    with engine.connect() as connection:
        args1 = f"REPLACE INTO {cond.tablename.lower()} SELECT * FROM {cond.tablename.lower()}_temp "
        result = connection.execute(text(args1))
        args2 = f"DROP Table If Exists {cond.tablename.lower()}_temp" # 把临时表删除
        result = connection.execute(text(args2))

    return lv_total


# 创建删除表

@app.get("/delete_table/{table_name}")
async def delete_table(table_name: str):   
    sql = text(f'DROP TABLE IF EXISTS {table_name};')
    with engine.connect() as connection:
        result = connection.execute(sql)

    return 'Deleted successfully'

@app.get("/create_table_by_sap/{table_name}")
async def create_table_by_sap(table_name: str):  

    has_table = engine.dialect.has_table(engine.connect(), table_name) 
    if not has_table:
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
        return 'Created successfully'
    else:
        return 'Table already exists'
    
