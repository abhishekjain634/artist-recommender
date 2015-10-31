__author__ = 'kunal'

import redis
import sys

r = redis.Redis(host='localhost', port=6379, db=0)
a = redis.Redis(host='localhost', port=6379, db=1)

try:
    response = r.client_list()
except redis.ConnectionError as f:
    print ("Couldn't connect to Redis", f)
    sys.exit(1)


def load_ratings(text_file, r):
    arr = []
    file = open('../data/' + text_file)
    for line in file:
        arr.append(map(str, line.split('\t')))
    for index in range(len(arr)):
        setVariable(arr[index][0] + '_' + arr[index][1], arr[index][2].strip(), r)


def load_artists(text_file, a):
    arr = []
    file = open('../data/' + text_file)
    for line in file:
        arr.append(map(str, line.split('\t')))
    for index in range(len(arr)):
        setVariable(arr[index][0], arr[index][1].strip(), a)


def getVariable(variable_name, server):
    response = server.get(variable_name)
    return response


def setVariable(variable_name, variable_value, server):
    server.set(variable_name, variable_value)

load_ratings('10MBdata', r)
load_artists('artists.txt', a)
#print r.values(), r.keys()
