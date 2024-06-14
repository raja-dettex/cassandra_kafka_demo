from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os

absolute_path = os.path.abspath(os.path.dirname(__file__))

#cluster config
cloud_config = { 
    'secure_connect_bundle' : os.path.join(absolute_path, '../astra.credentials/secure-connect-demo-kafka.zip')
} 

#cred path
cred_path = os.path.join(absolute_path, "../astra.credentials/user_cred.json")


cred = {}
with open(cred_path, 'r') as f:
    cred = json.load(f)

# cluster auth 
auth = PlainTextAuthProvider(cred['clientId'], cred['secret'])

# initialize a cluster
cluster = Cluster(cloud=cloud_config, auth_provider=auth)
session = cluster.connect()
session.set_keyspace(cred['keyspace'])
row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")



#migrate table schema
f = open(os.path.join(absolute_path, './leaves.astra.cql'))
create_table_semantics = str(f.read())
create_table_semantics = create_table_semantics.replace('keyspace_name', cred['keyspace'], 1)
create_table_semantics = create_table_semantics.replace('table_name', cred['table'], 1)
print(create_table_semantics)
session.execute(create_table_semantics)

with open(os.path.join(absolute_path, '../assets/data.json'), 'r') as fp:
    response_json = json.load(fp)
num_docs = 1000
rows = 10
docs = response_json['response']['docs'][0:rows]
real_docs = len(docs)


print(str(real_docs)+'/'+str(num_docs))


for i in range(len(docs)):
    tmp_doc = docs[i]
    #Missing Fields Checks
    try:
        title_check = tmp_doc['title']
    except KeyError:
        continue

    try:
        lang_check = tmp_doc['language']
    except KeyError:
        tmp_doc['language'] = 'en'

    try:
        picture_check = tmp_doc['preview_picture']
    except KeyError:
        tmp_doc['preview_picture'] = 'https://dummyimage.com/170/000/ffffff&text='+(tmp_doc['title'].replace(' ','%20'))

    try:
        content_check = tmp_doc['content']
    except KeyError:
        tmp_doc['content'] = ''

    try:
        content_text_check = tmp_doc['content_text']
    except KeyError:
        tmp_doc['content_text'] = tmp_doc['content'].encode('utf-8')

    try:
        mimetype_check = tmp_doc['mimetype']
    except KeyError:
        tmp_doc['mimetype'] = ''

    try:
        http_status_check = tmp_doc['http_status']
    except KeyError:
        tmp_doc['http_status'] = ''
    
    try:
        tags_check = tmp_doc['tags']
    except KeyError:
        tmp_doc['tags'] = []
    
    try:
        slugs_check = tmp_doc['slugs']
    except KeyError:
        tmp_doc['slugs'] = []
		
    try:
        all_check = tmp_doc['all']
    except KeyError:
        tmp_doc['all'] = []

    tmp_doc['id'] = str(tmp_doc['id'])
    tmp_doc['is_public'] = str(tmp_doc['is_public'])
    tmp_doc['user_id'] = str(tmp_doc['user_id'])
    tmp_doc['http_status'] = str(tmp_doc['http_status'])
    tmp_doc['tags'] = tmp_doc['tags']
    tmp_doc['slugs'] = tmp_doc['slugs']
    tmp_doc['all'] = tmp_doc['all']
    tmp_doc['links'] = tmp_doc['_links']
    #print(tmp_doc['links'])
    
    try:
        del tmp_doc['_links']
    except KeyError:
        print("No _links key to delete")
    
    try:
        del tmp_doc['published_by']
    except KeyError:
        #print("No published_by key to delete")
        pass
       
    try:
        del tmp_doc['published_at']
    except KeyError:
        #print("No published_at key to delete")
        pass
    
    try:
        del tmp_doc['uid']
    except KeyError:
        #print("No uid key to delete")
        pass
    
    try:
       tmp_doc['content_text'] = tmp_doc['content_text'].decode()
    except (UnicodeDecodeError, AttributeError):
        pass 
    
    if i%(real_docs/10.0)==0:
        print(str(i/(real_docs/100.0))+" % complete")
    

    json_doc = str(json.dumps(tmp_doc))
    #print(json_doc)
    insert_query = session.execute(
        "INSERT INTO "+cred['keyspace']+'.'+cred['table']+" JSON %s" % "'"+json_doc.replace("'","''")+"'"
        )