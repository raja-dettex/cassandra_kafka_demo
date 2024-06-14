import os
import json
import sys
import requests 
schema_reg_url = sys.argv[1]
#topic = sys.argv[2]

print(schema_reg_url)
#print(topic)

abs_path = os.path.abspath(os.path.dirname(__file__))
schema_path = os.path.join(abs_path, 'leaves-record-schema.avsc')

with open(schema_path, 'r') as f:
    schema = f.read()

#print(schema)
    
sch = { 
    "name" : "hello",
    "fields": [
        {"value1" : {"name" : "val1", "type" : "string" }}
    ]
}

payload = "{ \"schema\": \"" \
          + schema.replace("\"", "\\\"").replace("\t", "").replace("\n", "") \
          + "\" }"


headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

url = "http://" + schema_reg_url + "/subjects/record-cassandra-leaves-avro/versions"
print(url)
res = requests.post(url, headers=headers, data=payload)

if res.status_code == 200:
    schema_id = res.json()["id"]
    print("schema has been created with schema id : " + str(id))
else:
    print(res.text) 