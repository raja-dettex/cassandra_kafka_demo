from cassandra.cluster import Cluster;
from cassandra.auth import PlainTextAuthProvider
import json
import os
from flask import Flask, jsonify, request, abort
import logging
from bs4 import BeautifulSoup
from datetime import datetime
import readtime
import requests
import hashlib
import re
abs_path = os.path.abspath(os.path.dirname(__file__))

#cred = {}
with open(os.path.join(abs_path, '../astra.credentials/user_cred.json')) as f:
    cred = json.load(f)


cloud_config = { 
    'secure_connect_bundle' : os.path.join(abs_path, '../astra.credentials/secure-connect-demo-kafka.zip')
}

logging.basicConfig(level=logging.ERROR)


def connect_to_cassandra(auth_provider, cloud_config):
   cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
   session = cluster.connect()
   return session

auth_provider = PlainTextAuthProvider(cred['clientId'], cred['secret'])
session = connect_to_cassandra(auth_provider=auth_provider, cloud_config=cloud_config)


#Helper method for filling in other fields for a new row, given the url. Returns a python Dict/ json object containing the url as well as all other fields.
def processURL(url):
    #Scrape the page using requests
    page = requests.get(url)
    #Pull the mimetype and http status code
    mimetype = page.headers['content-type']
    http_status = str(page.status_code)
    #Transforms url into unique id
    id = hashlib.md5(url.encode()).hexdigest()
    #Set default document fields, tags and slugs defualt to empty
    is_archived = 1
    is_starred = 0
    user_name = 'raja'
    user_email = 'raja@example.com'
    user_id = str(1)
    is_public = str(False)
    created_at = str(datetime.now())[:-3]
    updated_at = str(datetime.now())[:-3]
    links = ["api/entries/"+str(id)]
    tags = str([])
    slugs = tags

    #Shorten given url to obtain domain name
    domain_name = re.search('https?:\/\/[^#?\/]+',url).group(0)

    #Load scraped page into beautiful soup parser
    bs = BeautifulSoup(page.content, 'html.parser')
    #Use beautiful soup to pull the images from the page, pick the first as preview image, if no images create one with the title as text
    images = bs.find_all('img', {'src':re.compile('.jpg')})
    title = str(bs.title.string)
    if images == []:
        preview_picture = 'https://dummyimage.com/170/000/ffffff&text='+(title.replace(' ','%20'))
    else:
        preview_picture = images[0]['src']
    #Pull whatever is in the language tag, if its empty set langugage to english
    language = bs.lang
    if language == None:
        language = 'en'
    #Pull the entire html content of the page, as well as text only content
    content = str(bs)
    content_text = bs.text
    #Use readingtime module to estimate reading time
    reading_time = str(readtime.of_html(str(bs)).minutes)
    #Collect all data into a dictionary
    result = {'is_archived':is_archived, 'is_starred':is_starred, 'user_name':user_name,'user_email':user_email, 'user_id':user_id, 'tags':tags, 'slugs':slugs,
    'is_public':is_public, 'id':id, 'title':title, 'url':url, 'content_text':content_text, 'created_at':created_at, 'updated_at':updated_at, 'mimetype':mimetype,
    'language':language, 'reading_time':reading_time, 'domain_name':domain_name, 'preview_picture':preview_picture, 'http_status':http_status, 'links':links, 
    'content':content, 'id':id}
    #Take all of the values in that dict, put them in a list and save that to all, then add all into the dictionary
    all = list(result.values())
    all = [str(i) for i in all]
    result['all'] = all
    #print(result.keys())
    #print([type(i) for i in result.values()])
    #print(id)
    return result


def preapare_data(data):
    formatted_data = {k:v for (k, v) in data.items() if k != '_links' or k != 'http_status'}
    formatted_data.__delitem__('_links')
    formatted_data.__delitem__('http_status')
    formatted_data.__delitem__('published_by')
    formatted_data.__delitem__('published_at')
    formatted_data['links'] = data['_links']
    formatted_data['http_status'] = str(data['http_status'])
    return str(json.dumps(formatted_data))

# flask app
app = Flask(__name__)

@app.route('/api/leaves/numbers&num=<number>', methods=['GET'])
def get_numbers(number: int):
    return number

@app.route('/api/leaves', methods=['GET'])
def get_all():
    cmd = f"select * from {cred['keyspace']}.{cred['table']}"
    rows = session.execute(cmd)
    result = []
    for row in rows:
        result.append(row)
    return jsonify(result)

@app.route('/api/leaves&row=<num_rows>', methods=['GET'])
def get_by_limit(num_rows : int):

    logging.info("hello here")
    logging.info(num_rows)
    cmd = f"select * from {cred['keyspace']}.{cred['table']} limit {num_rows}"
    rows = session.execute(cmd)
    result = []
    for row in rows:
        result.append(row)
    return jsonify(result)

@app.route('/api/leaves',methods=['POST'])
def pushRow():
    req_data = request.get_json()
    processed_data =processURL(req_data['url'])
    doc = str(json.dumps(processed_data))
    id = processed_data['id']
    #print(doc
    session.execute("INSERT INTO "+cred['keyspace']+'.'+cred['table']+" JSON %s" % "'"+doc.replace("'","''")+"'")
    rows = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[id])
    #print(type(rows))
    result = []
    for row in rows:
        #print(type(str(row)))
        result.append(json.loads(row.json))
    return jsonify(result[0]), 201

@app.route('/api/leaves/data', methods=['POST'])
def pushData():
    json_data = request.get_json()
    processed_data = preapare_data(json_data)
    statement = "INSERT INTO "+cred['keyspace'] + '.' + cred['table'] + " JSON %s" % "'" + processed_data.replace("'", "''") + "'"
    res = session.execute(statement)
    return jsonify({"respons" : res})
#Get a single entry from the cassandra table based on the id provided by the user
#Returns json of the resulting row, or a 404 page if there is document corresponding to the row
@app.route('/api/leaves/<id>',methods=['GET'])
def getById(id):
    rows = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    result = ''
    for row in rows:
        #print(type(str(row)))
        result = json.loads(row.json)
    if(result == ''):
        abort(404)
    else:
        return jsonify(result)

#Deletes the row with the given id from the cassandra table
#Returns a 404 page
@app.route('/api/leaves/<id>',methods=['DELETE'])
def delById(id):
    exists = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    num_results = len(exists.all())
    print(num_results)
    if num_results == 0:
        return jsonify("This item does not exist"), 404
    rows = session.execute("DELETE FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    print(rows)
    result = ''
    for row in rows:
        #print(type(str(row)))
        result = json.loads(row.json)
    return jsonify("Item Deleted"), 201

#Update the passed fields for the cassandra row with the given id, can update any field al long as it is passed in the request body with they proper key
#Returns json of the new row, retrieved from the cassandra table after the update
@app.route('/api/leaves/<id>',methods=['PATCH'])
def patchById(id):
    response = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    row = json.loads(response.one().json)
    req_data = request.get_json()
    for i in req_data.items():
        row[str(i[0])] = i[1]
    row['slugs'] = row['tags']
    del row['all']
    row['all'] = [str(i) for i in list(row.values())]
    session.execute("DELETE FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    rows = session.execute("INSERT INTO "+cred['keyspace']+'.'+cred['table']+" JSON %s" % "'"+str(json.dumps(row)).replace("'","''")+"'")
    rows = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    result = []
    for row in rows:
        result.append(json.loads(row.json))
    return jsonify(result[0]), 201

#Gets the tags of one entry from the cassandra table given the id of the entry
#Returns a list of tags. If there is no associated entry, returns a 404 page
@app.route('/api/leaves/<id>/tags',methods=['GET'])
def getTagsById(id):
    rows = session.execute("SELECT JSON tags FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    result = ''
    for row in rows:
        #print(type(str(row)))
        result = json.loads(row.json)
    if(result == ''):
        abort(404)
    else:
        return jsonify(result['tags'])

#Updates the tags of one entry from the cassandra table, given the id of the entry and a list of tags
#If the original has empty tags it puts the given tags in, if some tags already exist they are appended to the end of the list
#Also updates slugs to be the same as the tags
#Returns the entire entry retrieved from the cassandra table after the update in order to pass tests
@app.route('/api/leaves/<id>/tags',methods=['POST'])
def putTagsById(id):
    req_data = request.get_json()
    tags = req_data['tags']
    tags = [i.replace(' ','.') for i in tags]
    print(tags)
    if session.execute("SELECT JSON tags FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)]).one() == None:
        rows = session.execute("UPDATE "+cred['keyspace']+'.'+cred['table']+" SET tags = %s WHERE id=%s",[tags,str(id)])
    else:
        rows = session.execute("UPDATE "+cred['keyspace']+'.'+cred['table']+" SET tags = tags + %s WHERE id=%s",[tags,str(id)])

    if session.execute("SELECT JSON slugs FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)]).one() == None:
        rows = session.execute("UPDATE "+cred['keyspace']+'.'+cred['table']+" SET slugs = %s WHERE id=%s",[tags,str(id)])
    else:
        rows = session.execute("UPDATE "+cred['keyspace']+'.'+cred['table']+" SET slugs = slugs + %s WHERE id=%s",[tags,str(id)])
    result = ''
    for row in rows:
        #print(type(str(row)))
        result = json.loads(row.json)
    rows = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    result = []
    for row in rows:
        result.append(json.loads(row.json))
    return jsonify(result[0]), 201

#Deletes the tags of a single entry from the cassandra table, given the entries id
#Returns the entire entry after the deletion in order to pass tests
@app.route('/api/leaves/<id>/tags',methods=['DELETE'])
def delTagsById(id):
    rows = session.execute("DELETE tags FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    rows = session.execute("DELETE slugs FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    result = ''
    for row in rows:
        #print(type(str(row)))
        result = json.loads(row.json)
    rows = session.execute("SELECT JSON * FROM "+cred['keyspace']+'.'+cred['table']+" WHERE id=%s",[str(id)])
    result = []
    for row in rows:
        result.append(json.loads(row.json))
    return jsonify(result[0]), 200
    


if __name__ == '__main__' : 
    
    session = connect_to_cassandra(auth_provider, cloud_config)

    row = session.execute("select release_version from system.local").one()
    if row:
        print('connected to cassandra')
        app.run(host='0.0.0.0', port=5000, debug=False)
    else:
        print("An error occurred.")
        exit(0)
    
        
