import os
import json
from confluent_kafka import avro
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from time import sleep


class Producer:
    def __init__(self) -> None:
        self.records = []
        self.topic_name = "record-cassandra-leaves-avro"
        self.schema_registry_url = "http://localhost:8081"

        self.s_client = SchemaRegistryClient({'url': self.schema_registry_url})
    def get_sample_records(self, filepath : str):
        with open(filepath) as f:
            json_data = f.read()
        self.records = json.loads(json_data)

    def print_records(self):
        for record in self.records: 
            print(json.dumps(record, indent=4))

    def delivery_report(self, err, msg):
        if err is not None:
            print("error producing : {}".format(err))
        else:
            print("produced message to topic : {}, partitions: [{}]".format(msg.topic(), msg.partition()))

    def fetch_schema_string(self):
        s = self.s_client.get_schema(1)
        return s.schema_str
    def produce_to_kafka(self):
        #value_schema = avro.load('./../schema/leaves-record-schema-avsc')
        schema = self.fetch_schema_string()
        value_serializer = AvroSerializer(schema_registry_client=self.s_client, schema_str=schema)
        producer = SerializingProducer({
            'bootstrap.servers' : 'localhost:9092',
            'value.serializer' : value_serializer,
            'delivery.report.only.error': False
        })
        for record in self.records:
            producer.produce(topic=self.topic_name, value=record, on_delivery=self.delivery_report)
        producer.flush()
if __name__ == '__main__':
    abs_path = os.path.abspath(os.path.dirname(__file__))
    sample_path = os.path.join(abs_path, 'sample-avro-record.json')
    producer = Producer()
    producer.get_sample_records(sample_path)
    #producer.fetch_schema_string()
    producer.produce_to_kafka()
    sleep(10)
    