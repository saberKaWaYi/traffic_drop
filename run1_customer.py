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

connect_log_handler=get_rotating_handler("mysql.log")
logging_connect=logging.getLogger("connect")
logging_connect.setLevel(logging.INFO)
logging_connect.addHandler(connect_log_handler)

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
        logging_connect.error("mysql登录失败。")
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
        logging_connect.error("mysql关闭失败。")
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
                logging_connect.error(f"{e}第{i}次。")
                time.sleep(self.config["connection"]["TIME"])
        logging_connect.error(f"{table_name}数据获取失败。")
        raise Exception(f"{table_name}数据获取失败。")

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
    data=conn.get_table_data("",sql).values.tolist()
    with open("interfaces.txt","w",encoding="utf-8") as f:
        for item in data:
            line=f"{item[0]},{item[1]}\n"
            f.write(line)