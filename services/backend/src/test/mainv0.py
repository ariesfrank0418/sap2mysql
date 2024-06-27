from fastapi import FastAPI, Form,Body 
from fastapi import Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware   
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

from typing import Union 
from typing import Optional, Dict, Any
 
import uvicorn
from pyrfc import Connection
import pandas as pd
#import mysql.connector 
 
import pymysql 
#import collections

from datetime import datetime
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert  
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, CHAR, String, Enum, Date, DateTime,func
from sqlalchemy.orm import sessionmaker


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
    task_type      = Column(Enum('预定任务', '即时任务'))
    ex_start       = Column(DateTime)
    status         = Column(Enum('未开始', '进行中', '已完成', '错误'))
    msg            = Column(String(200)) 

class sys_tab_log(Base):
    __tablename__  = "sys_tab_log"  # 表名
    task_id        = Column(CHAR(36), primary_key=True) 
    tab_name       = Column(CHAR(30), primary_key=True)
    date_field     = Column(CHAR(30))
    date_from      = Column(CHAR(8))
    date_to        = Column(CHAR(8))
    add_where      = Column(String(200))
    ex_start       = Column(DateTime)
    ex_end         = Column(DateTime)
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
 

app = FastAPI() 

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:6001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 

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
 
@app.get("/")
async def index(): 
    return {"message": "Hello World"}
 
@app.get("/get_tab_structure/{tablename}")
async def get_tab_structure(tablename:str): 
    conn = Connection(ashost='www.saps4hana.cn', sysnr='20', client='110', user='s110-119', passwd='tfakai0418')
    result = conn.call('ZFU_GET_TAB_DATA', IV_TABLE=tablename , 
                                           IV_GET_STRUCTURE='X'
                                           )
    
    return  result['ET_TAB_INFO'] 

@app.get("/get_tab_count/{tablename}")
async def get_tab_count(tablename:str): 
    conn = Connection(ashost='www.saps4hana.cn', sysnr='20', client='110', user='s110-119', passwd='tfakai0418')
    result = conn.call('ZFU_GET_TAB_DATA', IV_TABLE=tablename , 
                                           IV_GET_COUNT='X'
                                           )
    return  result['EV_RECORD'] 

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}



@app.get("/get_all_task/")
async def  get_all_task(current_user: User = Depends(get_current_active_user)):  
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8") 
    Base.metadata.create_all(engine)   # 创建表结构
    session_class = sessionmaker(bind=engine)  # 创建与数据库的会话session class ,这里返回给session的是个class,不是实例
    session = session_class()   # 生成session实例
    ret = session.query(sys_task_log).all() 
    return ret


@app.get("/get_task_by_date/{i_date}")
async def  get_task_by_date(i_date:str):  
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8") 
    Base.metadata.create_all(engine)   # 创建表结构
    session_class = sessionmaker(bind=engine)  # 创建与数据库的会话session class ,这里返回给session的是个class,不是实例
    session = session_class()   # 生成session实例
    ret = session.query(sys_task_log).filter(func.date(sys_task_log.scheduled_time) == i_date).all()
    return ret

@app.get("/get_tablog_by_id/{i_uuid}")
async def  get_tablog_by_id(i_uuid:str):  
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8") 
    Base.metadata.create_all(engine)   # 创建表结构
    session_class = sessionmaker(bind=engine)  # 创建与数据库的会话session class ,这里返回给session的是个class,不是实例
    session = session_class()   # 生成session实例
    ret = session.query(sys_tab_log).filter(sys_tab_log.task_id == i_uuid).all()
    return ret

@app.get("/get_tablog_by_tabname/{i_tabname}")
async def  get_tablog_by_id(i_tabname:str):  
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8") 
    Base.metadata.create_all(engine)   # 创建表结构
    session_class = sessionmaker(bind=engine)  # 创建与数据库的会话session class ,这里返回给session的是个class,不是实例
    session = session_class()   # 生成session实例
    ret = session.query(sys_tab_log).filter(sys_tab_log.tabname == i_tabname).all()
    return ret

@app.post("/create_immediate_task/")
async def  create_immediate_task(cond_list: list[Cond] = Body(...)): 
    
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8") 
    Base.metadata.create_all(engine)   # 创建表结构
    session_class = sessionmaker(bind=engine)  # 创建与数据库的会话session class ,这里返回给session的是个class,不是实例
    session = session_class()   # 生成session实例
    #新增一条数据
    lv_uuid = uuid.uuid1()
    obj1 = sys_task_log(
        task_id        = lv_uuid, 
        scheduled_time = datetime.now(),
        task_type      = '即时任务',
        start_time     = datetime.now(),
        status         = '进行中',
        msg            = ''
    ) 
    session.add(obj1) 
    
    for cond in cond_list:
        obj2 = sys_tab_log(
            task_id        = lv_uuid,
            tab_name       = cond.tablename,
            date_field     = cond.fieldname,
            date_from      = cond.vfrom,
            date_to        = cond.vto,
            add_where      = cond.where,
            ex_start       = datetime.now(),
            ex_end         = datetime.now(),
            status         = '进行中',
            count          = 0,
            msg            = ''
            )  
        session.add(obj2) 
    session.commit() 

    return lv_uuid

@app.post("/sap_multiple_tab_data_to_mysql/")
async def sap_multiple_tab_data_to_mysql(cond_list: list[Cond] = Body(...)):
    return_cond = []
    for cond in cond_list:
        cond.count = sap_single_tab_data_to_mysql(cond)
        return_cond.append(cond)
    return return_cond

@app.post("/sap_single_tab_data_to_mysql/")
async def sap_single_tab_data_to_mysql(cond: Cond):
    pymysql.install_as_MySQLdb()
    engine = create_engine("mysql+mysqldb://root:123456@localhost:3306/sap2mysql?charset=utf8")

    conn = Connection(ashost='www.saps4hana.cn', sysnr='20', client='110', user='s110-119', passwd='tfakai0418')

    result = conn.call('ZFU_GET_TAB_DATA', IV_TABLE=cond.tablename , 
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
       lv_upto = cond.upto
       lv_current = 0 

    while lv_current < lv_total:  
        result = conn.call('ZFU_GET_TAB_DATA', IV_TABLE=cond.tablename , 
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

