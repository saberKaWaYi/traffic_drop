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

from method import method

import os
import numpy as np
import matplotlib.pyplot as plt

def fc(dataset,config,name,show=True,save_=False):
    m=method()
    datas=[]
    processed_datas=[]
    processed_datas_diff1=[]
    alerts=[]
    for i in dataset:
        if m.time==None:
            m.push_back_data(i)
            datas.append(int(float(i[1])))
            processed_datas.append(m.median[-1])
            processed_datas_diff1.append(m.variation_diff_median[-1])
            alerts.append(0)
            continue
        add_nums=(int(i[0])-m.time)//60-1
        m.push_back_data(i)
        for j in range(-add_nums-1,-1):
            try:
                datas.append(m.data_list_5_days[j])
            except:
                datas.append(m.median[j])
            processed_datas.append(m.median[j])
            processed_datas_diff1.append(m.variation_diff_median[j])
            alerts.append(0)
        datas.append(m.data_list_5_days[-1])
        processed_datas.append(m.median[-1])
        processed_datas_diff1.append(m.variation_diff_median[-1])
        alerts.append(m.alert)
    plt.figure(figsize=(16,9))
    plt.rcParams["font.sans-serif"]=["SimHei"]
    plt.rcParams["axes.unicode_minus"]=False
    time_points=np.arange(1,len(datas)+1)
    plt.subplot(2,1,1)
    plt.plot(time_points,datas,color='blue',label="原始数据")
    plt.plot(time_points,processed_datas,color="red",label="平滑数据")
    true_indices=[i for i,val in enumerate(alerts) if val]
    true_time=[time_points[i] for i in true_indices]
    true_values=[processed_datas[i] for i in true_indices]
    plt.scatter(true_time,true_values,color='green',zorder=5,label="异常的点")
    plt.axvline(x=7200,color='red',linestyle='--',)
    plt.axhline(y=m.low_limit,color='red',linestyle='--')
    plt.title(f"{config["hostname"]}_{config["interface"]}_{config["begin_time"]}_{config["end_time"]}_{name}")
    plt.grid(True,alpha=0.5);plt.legend()
    plt.subplot(2,1,2)
    plt.plot(time_points,processed_datas_diff1,color='purple',label='变异差分')
    plt.title(f"{config["hostname"]}_{config["interface"]}_{config["begin_time"]}_{config["end_time"]}_{name}")
    plt.grid(True,alpha=0.5);plt.legend()
    plt.tight_layout()
    if not save_:
        if not show:
            pass
        else:
            plt.show()
    else:
        name=f"{config["hostname"]}_{config["interface"]}_{config["begin_time"]}_{config["end_time"]}_{name}_main.jpg".replace(" ","_").replace(":","_")
        plt.savefig(name)
    plt.close()
    return sum(alerts)

class Test:

    def __init__(self,show=False,save=True):
        config={
            "connection":{
                "TIMES":1000,
                "TIME":0.1
            },
            "clickhouse":{
                "HOST":"localhost",
                "PORT":5000,
                "USERNAME":"default",
                "PASSWORD":""
            }
        }
        self.conn=Connect_Clickhouse(config)
        self.show=show
        self.save=save
        self.count=0

    def process(self,data):
        lt=[]
        for i in data:
            if len(lt)==0:
                lt.append(i)
            else:
                if i[0]<=lt[-1][0]:
                    continue
                else:
                    lt.append(i)
        return lt

    def get(self,config):
        sql=f'''
        select ts,value_traffic_in,value_traffic_out from ods_snmp.ods_snmp_info_all where toDateTime(ts) >= '{config["begin_time"]}' and toDateTime(ts) <= '{config["end_time"]}' and tags_host_name = '{config["hostname"]}' and tags_ifName = '{config["interface"]}' order by ts asc
        '''
        dataset=self.conn.query(sql)
        dataset1=self.process(dataset[["ts","value_traffic_in"]].values.tolist())
        dataset2=self.process(dataset[["ts","value_traffic_out"]].values.tolist())
        x=fc(dataset1,config,"in",self.show,self.save)
        y=fc(dataset2,config,"out",self.show,self.save)
        self.count+=(x+y)

if __name__=="__main__":
    # lt=[('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2021'), ('SGSIN-GS-J10003-GW-01', 'ae38.3629'), ('CNCAN-SD-MX204-GW-01', 'ae21.2454'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3032'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2785'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2479'), ('VNSGN-FPT-MX204-GW-01', 'ae66.2044'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3266'), ('JPTKY-COLT-MX204-GW-01', 'ae8.2998'), ('JPTKY-COLT-MX204-GW-01', 'ae8.2412'), ('CNHKG-MP-MX204-GW-01', 'ae17.3768'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3271'), ('CNCAN-SD-MX204-GW-01', 'ae21.2223'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2110'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3416'), ('KRICN-KINX-MX204-GW-01', 'ae24.2096'), ('VNSGN-FPT-MX204-GW-01', 'ae66.2092'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2876'), ('USDAL-DB1-J10003-GW-04', 'ae6.3156'), ('SGSIN-GS-J10003-GW-01', 'ae49.2220'), ('CNCAN-SD-MX204-GW-01', 'ae21.3219'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.3162'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.3500'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2193'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2553'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2305'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2423'), ('CNCAN-SD-MX204-GW-01', 'ae21.3617'), ('SGSIN-GS-J10003-GW-01', 'ae49.2048'), ('CNCAN-SD-MX204-GW-01', 'ae21.3425'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3183'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2019'), ('KRICN-KINX-MX204-GW-01', 'ae24.3345'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2083'), ('DEFRA-DRT-J10003-GW-01', 'ae32.2123'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2049'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3657'), ('CNCAN-SD-MX204-GW-01', 'ae21.2592'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2103'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3749'), ('DEFRA-DRT-J10003-GW-01', 'ae12.3651'), ('USIAD-IAD39-J10003-GW-01', 'ae57.2022'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2442'), ('CNHKG-MP-MX204-GW-01', 'ae17.2933'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2590'), ('CNCAN-SD-MX204-GW-01', 'ae21.3048'), ('CNCAN-SD-MX204-GW-01', 'ae21.2354'), ('SGSIN-GS-J10003-GW-01', 'ae49.2009'), ('MYJHB-JB1-MX204-GW-01', 'ae30.2507'), ('SGSIN-GS-J10003-GW-01', 'ae49.2020'), ('DEFRA-DRT-J10003-GW-01', 'ae32.2072'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2174'), ('CNHKG-MP-MX204-GW-01', 'ae17.2905'), ('DEFRA-DRT-J10003-GW-01', 'ae12.3831'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3391'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2402'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.3034'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2280'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3661'), ('CNCAN-SD-MX204-GW-01', 'ae21.2781'), ('CNHKG-MP-MX204-GW-01', 'ae17.2487'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2087'), ('KRICN-KINX-MX204-GW-01', 'ae24.3816'), ('SGSIN-GS-J10003-GW-01', 'ae49.2076'), ('JPTKY-COLT-MX204-GW-01', 'ae8.2640'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2052'), ('SGSIN-GS-J10003-GW-01', 'ae49.2121'), ('CNCAN-SD-MX204-GW-01', 'ae21.2139'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2842'), ('SGSIN-GS-J10003-GW-01', 'ae38.2109'), ('CNCAN-SD-MX204-GW-01', 'ae21.2663'), ('KRICN-KINX-MX204-GW-01', 'ae24.3198'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2404'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3450'), ('CNCAN-SD-MX204-GW-01', 'ae21.3739'), ('SGSIN-GS-J10003-GW-01', 'ae11.2540'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3701'), ('CNCAN-SD-MX204-GW-01', 'ae21.3796'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2016'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2260'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2447'), ('KRICN-KINX-MX204-GW-01', 'ae24.3526'), ('SGSIN-GS-J10003-GW-01', 'ae38.3607'), ('CNCAN-SD-MX204-GW-01', 'ae21.2593'), ('KRICN-KINX-MX204-GW-01', 'ae24.3792'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2636'), ('SGSIN-GS-J10003-GW-01', 'ae11.2219'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3930'), ('USDAL-DAL00-J10003-GW-01', 'ae50.1865'), ('SGSIN-GS-J10003-GW-01', 'ae49.2206'), ('CNSHA-ZR-MX204-GW-01', 'ae40.2045'), ('KRICN-KINX-MX204-GW-01', 'ae24.2153'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2214'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2962'), ('SGSIN-GS-J10003-GW-01', 'ae49.2152'), ('SGSIN-GS-J10003-GW-01', 'ae49.2029'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2711'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2283'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2081'), ('CNHKG-MP-MX204-GW-01', 'ae17.3402'), ('JPTKY-COLT-MX204-GW-01', 'ae8.3152'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3035'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2420'), ('CNCAN-SD-MX204-GW-01', 'ae21.3742'), ('SGSIN-GS-J10003-GW-01', 'ae49.2071'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2427'), ('CNCAN-SD-MX204-GW-01', 'ae21.3057'), ('JPTKY-COLT-MX204-GW-01', 'ae8.3429'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.3175'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3729'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3047'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2732'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2409'), ('JPTKY-COLT-MX204-GW-01', 'ae8.3768'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.3413'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2291'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2816'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2295'), ('JPTKY-COLT-MX204-GW-01', 'ae8.2953'), ('USDAL-DAL00-J10003-GW-01', 'ae50.2005'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2515'), ('CNSHA-BX-MX240-GW-01', 'ae40.2045'), ('JPTKY-COLT-MX204-GW-01', 'ae8.3254'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3052'), ('MYJHB-JB1-MX204-GW-01', 'ae30.2492'), ('SGSIN-GS-J10003-GW-01', 'ae49.2178'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3849'), ('SGSIN-GS-J10003-GW-01', 'ae49.2241'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2462'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.3615'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2765'), ('DEFRA-DRT-J10003-GW-01', 'ae32.2274'), ('JPTKY-COLT-MX204-GW-01', 'ae8.3331'), ('DEFRA-DRT-J10003-GW-01', 'ae12.3800'), ('CNCAN-SD-MX204-GW-01', 'ae21.2612'), ('CNPEK-ZJY-MX204-GW-01', 'ae9.2542'), ('CNTSA-LY-ASR9K-GW-01', 'Bundle-Ether190.2747'), ('CNCAN-SD-MX204-GW-01', 'ae21.2269')]
    # config={
    #     "hostname":"",
    #     "interface":"",
    #     "begin_time":"2025-07-01 00:00:00",
    #     "end_time":"2025-08-01 00:00:00"
    # }
    # lt=[('USIAD-IAD39-J10003-GW-01', 'ae57')]
    # config={
    #     "hostname":"",
    #     "interface":"",
    #     "begin_time":"2025-02-01 00:00:00",
    #     "end_time":"2025-03-01 00:00:00"
    # }
    lt=[('JPTKY-COLT-MX204-GW-01', 'ae8')]
    config={
        "hostname":"",
        "interface":"",
        "begin_time":"2025-09-15 00:00:00",
        "end_time":"2025-10-10 00:00:00"
    }
    m=Test()
    for i in lt:
        try:
            config['hostname']=i[0]
            config['interface']=i[1]
            m.get(config)
        except Exception as e:
            print(config)
            print(e)
    print(m.count)