from kafka import KafkaConsumer,TopicPartition,OffsetAndMetadata
import json

def consumer_demo(jh):
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
        consumer_timeout_ms=1000,
        enable_auto_commit=False
    )
    partitions=consumer.partitions_for_topic('snmp_interface_agent')
    topic_partitions=[TopicPartition('snmp_interface_agent',p) for p in partitions]
    consumer.assign(topic_partitions)
    end_offsets=consumer.end_offsets(topic_partitions)
    reset_offsets={p:OffsetAndMetadata(end_offsets[p],None,leader_epoch=0) for p in topic_partitions}
    consumer.commit(offsets=reset_offsets)
    while True:
        for message in consumer:
            temp=(message.value["tags"]["host_name"],message.value["tags"]["ifName"])
            if temp in jh:
                print(message.partition,message.offset)
                print(temp[0],temp[1],message.value["value"]["traffic_in"],message.value["value"]["traffic_out"],message.value["timestamp"])

if __name__=='__main__':
    jh=set()
    jh.add(("CNCAN-SD-MX204-GW-01","ae21.2269"))
    consumer_demo(jh)