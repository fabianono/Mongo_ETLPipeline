from requests import get
import json
from kafka import KafkaProducer
# from pymongo import MongoClient

def formatdata(indata):
    out = {}
    input = indata['results'][0]
    out['gender'] = input['gender']
    name = input['name']
    out['name'] = {}
    out['name']['title'] = name['title']
    out['name']['firstname'] = name['first']
    out['name']['lastname'] = name['last']
    out['dob'] = input['dob']['date']
    address = input['location']
    out['address'] = {}
    out['address']['main'] = str(address['street']['number']) + " " + address['street']['name']
    out['address']['postalcode'] = address['postcode']
    out['address']['city'] = address['city']
    out['address']['state'] = address['state']
    out['address']['country'] = address['country']
    out['contact'] = {}
    out['contact']['email'] = input['email']
    out['contact']['phone'] = input['phone']

    return out

randomuser_api = get('https://randomuser.me/api/')
data = randomuser_api.json()
print(json.dumps(formatdata(data),indent=4))

producer = KafkaProducer(bootstrap_servers=['broker:29092'])
producer.send("users", json.dumps(formatdata(data),indent=4).encode('utf-8'))

# def mongo_connect(collection):
#     mongodb_uri = "mongodb://root:root@127.0.0.1:27017/my_database?authSource=admin"
#     client = MongoClient(mongodb_uri)
#     db = client['my_database']
#     collection_var = db['my_collection']

#     collection_var.insert_one(collection)

#     print("Mongodb connected")

# mongo_connect(formatdata(data))