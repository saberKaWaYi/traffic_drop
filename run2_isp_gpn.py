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

init_log_handler=get_rotating_handler("init.log")
logging_init=logging.getLogger("init")
logging_init.setLevel(logging.INFO)
logging_init.addHandler(init_log_handler)

kafka_log_handler=get_rotating_handler("kafka.log")
logging_kafka=logging.getLogger("kafka")
logging_kafka.setLevel(logging.INFO)
logging_kafka.addHandler(kafka_log_handler)

process_log_handler=get_rotating_handler("process.log")
logging_process=logging.getLogger("process")
logging_process.setLevel(logging.INFO)
logging_process.addHandler(process_log_handler)

alerts_log_handler=get_rotating_handler("alerts.log")
logging_alerts=logging.getLogger("alerts")
logging_alerts.setLevel(logging.INFO)
logging_alerts.addHandler(alerts_log_handler)

exit_log_handler=get_rotating_handler("exit.log")
logging_exit=logging.getLogger("exit")
logging_exit.setLevel(logging.INFO)
logging_exit.addHandler(exit_log_handler)

tasks_log_handler=get_rotating_handler("tasks.log")
logging_tasks=logging.getLogger("tasks")
logging_tasks.setLevel(logging.INFO)
logging_tasks.addHandler(tasks_log_handler)

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

import threading
from collections import deque
import signal
from method import method
from kafka import KafkaConsumer,TopicPartition,OffsetAndMetadata
import json
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests
from datetime import datetime

class Run:

    def __init__(self,config,interfaces,flag1=True,flag2=False):
        self.config=config
        self.interfaces=interfaces
        # 数据未装满时，是否选择使用未来数据填充；选True则会正常加载接口，选False以后均不检测该口。
        self.flag1=flag1
        # 选True:数据过小依然进行监控；选False:数据过小不会进行监控
        self.flag2=flag2
        self.running=True
        signal.signal(signal.SIGINT,self.exit)
        signal.signal(signal.SIGTERM,self.exit)
        self.already_interface={};self.lock1=threading.Lock()
        self.tasks=deque();self.lock2=threading.Lock()
        self.alerts=deque();self.lock3=threading.Lock()
        self.batch=[];self.count=0

    def run(self):
        t1=threading.Thread(target=self.logic_run1)
        t2=threading.Thread(target=self.logic_run2)
        t3=threading.Thread(target=self.logic_run3)
        t4=threading.Thread(target=self.logic_run4)
        t5=threading.Thread(target=self.logic_run5)
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()

    def check(self,m,s):
        if m.flag:
            logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}数据装载成功。")
        else:
            logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}历史数据不足。")
            if self.flag1:
                logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}将会使用未来数据填充。")
                logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}数据装载成功。")
            else:
                logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}装载数据失败。")
                return False
        if m.count/7200>=m.low_limit:
            logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}准备进行监控。")
        else:
            if self.flag2:
                logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}准备进行监控。")
            else:
                logging_init.info(f"{s[0]:<50},{s[1]:<25},{s[2]:<5}放弃进行监控。")
                return False
        return True

    def logic_run1(self):
        # 每个接口提前进行加载数据来初始化
        conn=Connect_Clickhouse(config)
        count=0
        for interface in self.interfaces:
            if not self.running:
                logging_exit.info("强制退出接口的初始化。")
                break
            sql=f'''
            select ts, value_traffic_in, value_traffic_out 
            from ods_snmp.ods_snmp_info_all 
            where toDateTime(ts) >= now() - interval 6 day 
                and toDateTime(ts) <= now()
                and tags_host_name = '{interface[0]}' 
                and tags_ifName = '{interface[1]}' 
            order by ts asc
            '''
            try:
                datas=conn.query(sql).values.tolist()
                m1=method();m2=method()
                for data in datas:
                    m1.push_back_data((data[0],data[1]))
                    m2.push_back_data((data[0],data[2]))
                flag1=self.check(m1,(interface[0],interface[1],"in"))
                flag2=self.check(m2,(interface[0],interface[1],"out"))
                if flag1 or flag2:
                    logging_init.info(f"{interface[0]:<50},{interface[1]:<25}加入监控列表。")
                else:
                    logging_init.info(f"{interface[0]:<50},{interface[1]:<25}放弃进行监控。")
                    continue
                with self.lock1:
                    self.already_interface[interface]={"in":m1,"out":m2}
                count+=1
            except Exception as e:
                logging_init.error(f"{interface[0]:<50},{interface[1]:<25}发生错误{e}。")
        else:
            logging_init.info("所有接口已初始化。")
            logging_init.info(f"一共监听{count}个接口。")

    def logic_run2(self):
        consumer=KafkaConsumer(
            bootstrap_servers=[
                '172.30.22.141:9092','172.30.22.142:9092',
                '172.30.22.143:9092','172.30.22.144:9092',
                '172.30.22.145:9092','172.30.22.146:9092',
                '172.30.22.147:9092','172.30.22.148:9092'
            ],
            group_id='LCL',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='none',
            enable_auto_commit=False,
            consumer_timeout_ms=1000
        )
        partitions=consumer.partitions_for_topic('snmp_interface_agent')
        topic_partitions=[TopicPartition('snmp_interface_agent',p) for p in partitions]
        consumer.assign(topic_partitions)
        end_offsets=consumer.end_offsets(topic_partitions)
        reset_offsets={p:OffsetAndMetadata(end_offsets[p],None,leader_epoch=0) for p in topic_partitions}
        consumer.commit(offsets=reset_offsets)
        while True:
            if not self.running:
                logging_exit.info("强制退出接口的生产者。")
                break
            for message in consumer:
                try:
                    with self.lock1:
                        if (message.value["tags"]["host_name"],message.value["tags"]["ifName"]) in self.already_interface:
                            self.batch.append(message)
                    if len(self.batch)>=1000:
                        with self.lock2:
                            self.tasks.append((2,"test"))
                            for i in self.batch:
                                interface=(i.value["tags"]["host_name"],i.value["tags"]["ifName"])
                                self.tasks.append((3,interface[0],interface[1],i.value["value"]["traffic_in"],i.value["value"]["traffic_out"],i.value["timestamp"]))
                        self.batch=[]
                except Exception as e:
                    logging_kafka.error(f"{message}发生错误{e}。")
        consumer.close()
        logging_exit.info("关闭生产者的组件。")

    def transform(self,lt):
        temp=[]
        for i in lt:
            j=i*8
            if j/1000<1:
                temp.append(f"{round(j,4)}B");continue
            j/=1000
            if j/1000<1:
                temp.append(f"{round(j,4)}KB");continue
            j/=1000
            if j/1000<1:
                temp.append(f"{round(j,4)}MB");continue
            j/=1000
            temp.append(f"{round(j,4)}GB")
        return temp

    def process(self,hostname,interface,traffic_in,traffic_out,timestamp):
        key=(hostname,interface)
        if timestamp-self.already_interface[key]["in"].time>60*24*3 or timestamp-self.already_interface[key]["out"].time>60*24*3:                                                                                
            self.already_interface[key]["in"]=method()
            self.already_interface[key]["out"]=method()
            logging_process.info(f"{hostname}|{interface}流量数据缺失严重，接口已经重新初始化了。")
        self.already_interface[key]["in"].push_back_data((timestamp,traffic_in))
        self.already_interface[key]["out"].push_back_data((timestamp,traffic_out))
        if self.already_interface[key]["in"].alert:                                                                                                                  
            with self.lock3:
                self.alerts.append((3,hostname,interface,str(datetime.fromtimestamp(timestamp)),"in",self.transform(list(self.already_interface[key]["in"].data_list_5_days[-8:]))))
        if self.already_interface[key]["out"].alert:
            with self.lock3:
                self.alerts.append((3,hostname,interface,str(datetime.fromtimestamp(timestamp)),"out",self.transform(list(self.already_interface[key]["out"].data_list_5_days[-8:]))))

    def logic_run3(self):
        with ThreadPoolExecutor(max_workers=100) as executor:
            while True:
                if not self.running:
                    logging_exit.info("强制退出接口的消费者。")
                    break
                sleep=False;pool=[];jh=set()
                with self.lock2:
                    if len(self.tasks)>10000:
                        with self.lock3:
                            self.alerts.append((1,"任务堆积，需要重启。"))
                            break
                    elif len(self.tasks)==0:
                        sleep=True
                    else:
                        for _ in range(100):
                            if not self.tasks:
                                break
                            if self.tasks[0][0]==2:
                                self.tasks.popleft()
                                with self.lock3:
                                    self.alerts.append((2,"服务通路活性验证。"))
                                self.count+=1
                            elif self.tasks[0][0]==3:
                                if (self.tasks[0][1],self.tasks[0][2]) in jh:
                                    break
                                task=self.tasks.popleft()
                                pool.append(executor.submit(self.process,task[1],task[2],task[3],task[4],task[5]))
                                jh.add((task[1],task[2]))
                if sleep:
                    time.sleep(0.1)
                    continue
                for task in as_completed(pool):
                    task.result()
                    self.count+=1

    def post(self,alert):
        headers={"Content-Type":"application/json"}
        webhook_data={
            "msgtype":"text",
            "text":{}
        }
        if alert[0]==1 or alert[0]==2:
            webhook_data["text"]["content"]=f"{alert[1]}。"
        elif alert[0]==3:
            webhook_data["text"]["content"]=f"{alert[1]}|{alert[2]}|{alert[4]}|{alert[3]}流量突降。最近的点依次为{','.join(alert[-1])}。"
        try:
            response=requests.post(
                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d410a5e1-6e24-4902-94fc-27d1d0e024d5",
                headers=headers,
                json=webhook_data,
                timeout=10
            )
            result=response.json()
            logging_alerts.info(result)
        except Exception as e:
            logging_alerts.error(e)

    def logic_run4(self):
        while True:
            if not self.running:
                logging_exit.info("强制退出报警线程。")
                break
            alert=None
            with self.lock3:
                if self.alerts:
                    alert=self.alerts.popleft()
            if alert:
                self.post(alert)
                if alert[0]==1:
                    logging_alerts.error(f"{alert[1]}。")
                    self.running=False
                elif alert[0]==3:
                    logging_alerts.info(f"{alert[1]}|{alert[2]}|{alert[4]}|{alert[3]}流量突降。最近的点依次为{','.join(alert[-1])}。")
            time.sleep(1)

    def logic_run5(self):
        while True:
            if not self.running:
                logging_exit.info("强制退出监听线程。")
                break
            with self.lock2:
                logging_tasks.info(f"剩余任务数量{len(self.batch):05d},当前任务数量{len(self.tasks):05d},完成任务数量{self.count:025d}。")
            time.sleep(60)
            
    def exit(self,signum,frame):
        logging_exit.info("强制退出。")
        self.running=False

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
    lt=[]
    with open("interfaces.txt","r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            parts=line.split(",")
            lt.append((parts[0].strip(),parts[1].strip()))
    m=Run(config,lt,False,False)
    m.run()

# 可增加接口灵活度
# 可以减少kafka的数据