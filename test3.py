import os

log_dir='logs'
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

post_log_handler=get_rotating_handler('post.log')
logging_post=logging.getLogger('post')
logging_post.setLevel(logging.INFO)
logging_post.addHandler(post_log_handler)

from kafka import KafkaConsumer,TopicPartition,OffsetAndMetadata
import json

consumer=KafkaConsumer(
    bootstrap_servers=[
        '172.30.22.141:9092','172.30.22.142:9092',
        '172.30.22.143:9092','172.30.22.144:9092',
        '172.30.22.145:9092','172.30.22.146:9092',
        '172.30.22.147:9092','172.30.22.148:9092'
    ],
    group_id='alerts',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='none',
    enable_auto_commit=False,
    consumer_timeout_ms=5000
)
partitions=consumer.partitions_for_topic('traffic_drop_monitor')
topic_partitions=[TopicPartition('traffic_drop_monitor',p) for p in partitions]
consumer.assign(topic_partitions)
end_offsets=consumer.end_offsets(topic_partitions)
reset_offsets={p:OffsetAndMetadata(end_offsets[p],None,leader_epoch=0) for p in topic_partitions}
consumer.commit(offsets=reset_offsets)

import requests

def post1(alert):
    headers={'Content-Type':'application/json'}
    webhook_data={
        'msgtype':'text',
        'text':{}
    }
    webhook_data['text']['content']=f"alert_type:{alert['alert_type']}\nalert_level:{alert['alert_level']}\ncreate_time:{alert['create_time']}\n"
    if alert['alert_type']!='traffic_drop':
        webhook_data['text']['content']+=alert['alert_msg']
    else:
        webhook_data['text']['content']+=f"hostname:{alert['alert_msg']['hostname']}\n"
        webhook_data['text']['content']+=f"interface:{alert['alert_msg']['interface']}\n"
        webhook_data['text']['content']+=f"traffic_direction:{alert['alert_msg']['traffic_direction']}\n"
        webhook_data['text']['content']+=f"alert_time:{alert['alert_msg']['alert_time']}\n"
        webhook_data['text']['content']+=f"recent_traffic:{','.join(alert['alert_msg']['recent_traffic'])}"
    try:
        response=requests.post(
            "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d410a5e1-6e24-4902-94fc-27d1d0e024d5",
            headers=headers,
            json=webhook_data,
            timeout=10
        )
        result=response.json()
        logging_post.info(result)
    except Exception as e:
        logging_post.error(e)

def post2(alert):
    url='http://10.216.141.30:40005'
    json_data={
        'hostname':alert['alert_msg']['hostname'],
        'subObject':alert['alert_msg']['interface'],
        'type':'network_traffic',
        'group':'network_snmp_dynamic',
        'securityLevel':'Emergency',
        'message':f"{alert['alert_msg']['hostname']}_{alert['alert_msg']['interface']}[网络流量方向({alert['alert_msg']['traffic_direction']})],当前流量值为{alert['alert_msg']['recent_traffic'][-1]},出现流量突降异常.\nCustomer_最近的流量值依次为:{','.join(alert['alert_msg']['recent_traffic'])}。",
        'logTimestamp':alert['alert_msg']['alert_time'],
        'ip':'',
        'tag1':''
    }
    try:
        response=requests.post(url,json=json_data,headers={'content-type':'application/json'})
        result=response.json()
        logging_post.info(result)
    except Exception as e:
        logging_post.error(e)

while True:
    for msg in consumer:
        msg_value=msg.value
        post1(msg_value)
        if msg_value['alert_type']=='traffic_drop':
            post2(msg_value)