from cassandra.cluster import Cluster;
from cassandra.auth import PlainTextAuthProvider
import json

cloud_config = { 
    'secure_connect_bundle' : 'secure-connect-demo-kafka.zip'
}
secrets = { }
with open('demo_kafka.json') as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets['clientId'], secrets['secret'])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()


row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")