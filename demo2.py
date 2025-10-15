import os

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

import logging
from logging.handlers import RotatingFileHandler

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

connect_log_handler=get_rotating_handler("clickhouse.log")
logging_connect=logging.getLogger("connect")
logging_connect.setLevel(logging.INFO)
logging_connect.addHandler(connect_log_handler)

from clickhouse_driver import Client
import time
import atexit
import pandas as pd

class Connect_Clickhouse:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=Client(host=self.config["clickhouse"]["HOST"],port=self.config["clickhouse"]["PORT"],user=self.config["clickhouse"]["USERNAME"],password=self.config["clickhouse"]["PASSWORD"])
                return client
            except Exception as e:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect.error("clickhouse登录失败。")
        raise Exception("clickhouse登录失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.disconnect()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect.error("clickhouse关闭失败。")
        raise Exception("clickhouse关闭失败。")
    
    def query(self,query):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data,columns=self.client.execute(query,with_column_types=True)
                columns=[col[0] for col in columns]
                data=pd.DataFrame(data,columns=columns).astype(str)
                return data
            except Exception as e:
                time.sleep(self.config["connection"]["TIME"])
                logging_connect.error(e)
        logging_connect.error(f"{query}数据获取失败。")
        raise Exception(f"{query}数据获取失败。")
    
if __name__=="__main__":
    config={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "clickhouse":{
            "HOST":"10.216.140.102",
            "PORT":9000,
            "USERNAME":"default",
            "PASSWORD":""
        }
    }
    conn=Connect_Clickhouse(config)
    sql=f'''
    select ts, value_traffic_in, value_traffic_out 
    from ods_snmp.ods_snmp_info_all 
    where toDateTime(ts) >= now() - interval 6 day 
        and toDateTime(ts) <= now()
        and tags_host_name = 'CNCAN-SD-MX204-GW-01' 
        and tags_ifName = 'ae21.2269' 
    order by ts asc
    '''
    data=conn.query(sql)
    print(data)