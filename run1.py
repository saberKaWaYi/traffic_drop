import os
import logging
from logging.handlers import RotatingFileHandler

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

def get_rotating_handler(filename,max_bytes=1024*1024*1024,backup_count=5):
    handler=RotatingFileHandler(
        os.path.join(log_dir,filename),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    formatter=logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s'
    )
    handler.setFormatter(formatter)
    return handler

connect_log_handler1=get_rotating_handler("mysql.log")
logging_connect_mysql=logging.getLogger("connect")
logging_connect_mysql.setLevel(logging.INFO)
logging_connect_mysql.addHandler(connect_log_handler1)

connect_log_handler2=get_rotating_handler("mongo.log")
logging_connect_mongo=logging.getLogger("connect")
logging_connect_mongo.setLevel(logging.INFO)
logging_connect_mongo.addHandler(connect_log_handler2)

import atexit
from pymysql import connect
from pymysql.cursors import DictCursor
import time
import pandas as pd

class Connect_Mysql:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        self.flag=False
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=connect(host=self.config["mysql"]["HOST"],port=self.config["mysql"]["PORT"],user=self.config["mysql"]["USERNAME"],password=self.config["mysql"]["PASSWORD"],charset="utf8",cursorclass=DictCursor)
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mysql.error("mysql登录失败。")
        raise Exception("mysql登录失败。")
    
    def close(self):
        if self.flag:
            return
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.close()
                self.flag=True
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mysql.error("mysql关闭失败。")
        raise Exception("mysql关闭失败。")
    
    def get_table_data(self,table_name,query):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                with self.client.cursor() as cursor:
                    cursor.execute(query)
                    columns=[desc[0] for desc in cursor.description]
                    data=cursor.fetchall()
                    data=pd.DataFrame(data,columns=columns).astype(str)
                    return data
            except Exception as e:
                logging_connect_mysql.error(f"{e}第{i}次。")
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mysql.error(f"{table_name}数据获取失败。")
        raise Exception(f"{table_name}数据获取失败。")

from pymongo import MongoClient

class Connect_Mongodb:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        self.db=self.get_database()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=MongoClient(host=self.config["mongodb"]["HOST"],port=self.config["mongodb"]["PORT"])
                client.cds_cmdb.authenticate(self.config["mongodb"]["USERNAME"],self.config["mongodb"]["PASSWORD"])
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mongo.error("mongodb登录失败。")
        raise Exception("mongodb登录失败。")
    
    def get_database(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                return self.client.get_database("cds_cmdb")
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mongo.error("cds_cmdb获取失败。")
        raise Exception("cds_cmdb获取失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.close()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mongo.error("mongodb关闭失败。")
        raise Exception("mongodb关闭失败。")

    def get_collection(self,name,condition1,condition2):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data=pd.DataFrame(self.db.get_collection(name).find(condition1,condition2)).astype(str)
                return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect_mongo.error(f"{name}数据获取失败。")
        raise Exception(f"{name}数据获取失败。")
    
def get_interface_list1(config):
    temp=[
        {"$match": {"status": 1}},
        {"$lookup": {
            "from": "cds_ci_att_value_interface",
            "localField": "cds_ci_att_value_interface_id",
            "foreignField": "_id",
            "as": "interfaceData"
        }},
        {"$unwind": "$interfaceData"},
        {"$match": {"interfaceData.status": 1}},
        {"$project": {
            "_id": 0,
            "hostname": "$interfaceData.hostname",
            "name": "$interfaceData.name"
        }}
    ]
    db_mongo=Connect_Mongodb(config)
    data=pd.DataFrame(list(db_mongo.db.cds_att_isp_interface_relation.aggregate(temp))).astype(str).values.tolist()
    lt=[]
    for i in data:
        lt.append((i[0],i[1]))
    return lt

def get_interface_list2(config):
    temp=[
        {"$match": {"status": 1}},
        {"$lookup": {
            "from": "cds_ci_att_value_interface",
            "localField": "a_cds_ci_att_value_interface_id",
            "foreignField": "_id",
            "as": "interfaceData"
        }},
        {"$unwind": "$interfaceData"},
        {"$match": {"interfaceData.status": 1}},
        {"$project": {
            "_id": 0,
            "hostname": "$interfaceData.hostname",
            "name": "$interfaceData.name"
        }}
    ]
    db_mongo=Connect_Mongodb(config)
    data=pd.DataFrame(list(db_mongo.db.cds_att_value_gpn.aggregate(temp))).astype(str).values.tolist()
    lt=[]
    for i in data:
        lt.append((i[0],i[1]))
    return lt

if __name__=="__main__":
    config={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    conn=Connect_Mysql(config)
    sql='''
    SELECT hostname,interface_name as name
    FROM `wukong_api`.`cmdb_interfacealterdetail`
    WHERE is_valid=1;
    '''
    lt=conn.get_table_data("",sql).values.tolist()
    config={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    lt.extend(get_interface_list1(config))
    lt.extend(get_interface_list2(config))
    with open("interfaces.txt","w",encoding="utf-8") as f:
        for item in lt:
            line=f"{item[0]},{item[1]}\n"
            f.write(line)