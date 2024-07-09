# from confluent_kafka import DeserializingConsumer
# from confluent_kafka.schema_registry.avro import AvroDeserializer
# from confluent_kafka.schema_registry import SchemaRegistryClient


# class Consumer:
#     def __init__(self):
#         self.schema_reg_url = "http://localhost:8081"
#         self.topic_name = "record-cassandra-leaves-avro"
#         self.consumer_group_id = 'my-avro-consumer-group'
#         self.s_client = SchemaRegistryClient({'url' : self.schema_reg_url})
#     def fetch_schema_string(self):
#         s = self.s_client.get_schema(1)
#         return s.schema_str
#     def create_consumer(self):
#         value_deserializer = AvroDeserializer(schema_registry_client=self.s_client, schema_str=self.fetch_schema_string())
#         consumer = DeserializingConsumer({
#             'bootstrap.servers': 'localhost:9092',
#             'value.deserializer' : value_deserializer,
#             'group.id': self.consumer_group_id,
#             'auto.offset.reset': 'earliest',  # Start consuming from the earliest message in the topic
#             'enable.auto.commit': False 
#         })
#         return consumer
    
#     def consume(self): 
#         consumer = self.create_consumer()
#         consumer.subscribe([self.topic_name])
#         while True:
            
#             msg = consumer.poll(timeout=15.0)
#             if msg is None:
#                 print("no messages to consume")
#                 exit(0)
#             if msg.error is not None:
#                 print("error reading")
#                 print(msg.error)
#             else:
#                 print(msg)


# if __name__ == '__main__': 
#     consumer = Consumer()
#     consumer.consume()






from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import requests
class AvroConsumer:
    def __init__(self):
        self.handler_url = "http://localhost:5000/api/leaves/data"
        self.schema_reg_url = "http://localhost:8081"
        self.topic_name = "record-cassandra-leaves-avro"
        self.consumer_group_id = 'my-avro-consumer-group'
        self.s_client = SchemaRegistryClient({'url': self.schema_reg_url})
        self.value_deserializer = AvroDeserializer(schema_registry_client=self.s_client)

    def send_to_handler(self,data):
        headers = { 'Content-Type' : 'application/json'}
        res = requests.post(url=self.handler_url, headers=headers, data=data) 
        if res.status_code == 200:
            print(res.text)
        else:
            print(res)    

    def consume(self):
        conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': self.consumer_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }

        consumer = Consumer(conf)
        consumer.subscribe([self.topic_name])

        while True:
            msg = consumer.poll(timeout=3.0)
            print(msg)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                value = self.value_deserializer(msg.value(), msg.headers())
                #val = value.copy()
                print("here")
                #print(json.dumps(value, indent=4))
                self.send_to_handler(json.dumps(value))

        consumer.close()


if __name__ == '__main__':
    consumer = AvroConsumer()
    consumer.consume()
