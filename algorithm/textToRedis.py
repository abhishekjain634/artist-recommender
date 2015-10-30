__author__ = 'kunal'

import redis
import sys
import pandas as pd

r = redis.StrictRedis(host='localhost', port=6379, db=0)

try:
    response = r.client_list()
except redis.ConnectionError as f:
    print ("Couldn't connect to Redis", f)
    sys.exit(1)

def load(text_file, r):
    data_frame = pd.read_csv('../data/'+ text_file, sep='\t')
    for row in data_frame.iterrows():
        print row
        #r.setVariable(i[0], i[2])

def getVariable(variable_name):
    server = redis.Redis(connection_pool=r)
    response = server.get(variable_name)
    return response

def setVariable(variable_name, variable_value):
    server = redis.Redis(connection_pool=r)
    server.set(variable_name, variable_value)

load('10MBdata', r)
print r.keys()
