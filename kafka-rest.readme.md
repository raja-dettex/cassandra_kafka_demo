curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json"      http://localhost:8082/consumers/my_avro_consumer_group/instances/my_consumer_instance/records -j 

curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
     --data '{"topics":["record-cassandra-leaves-avro"]}' \
     http://localhost:8082/consumers/my_avro_consumer_group/instances/my_consumer_instance/subscription


curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
     --data '{"name": "my_consumer_instance", "format": "avro", "auto.offset.reset": "earliest"}' \
     http://localhost:8082/consumers/my_avro_consumer_group
