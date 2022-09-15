import json
import os


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def get_config(filepath, key):
    filename = os.path.join(filepath, 'config.json')
    with open(filename, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val
