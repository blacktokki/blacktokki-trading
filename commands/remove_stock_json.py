import os
from utils import load_stocklist_json

def __init__(*args):
    data_all = load_stocklist_json()
    for i, d in enumerate(data_all):
        full_code = d['full_code']
        path = os.path.join('data', 'stock', f'{full_code}.json')
        if '스팩' in d['codeName']:
            print(d['codeName'])
            # if os.path.exists(path):
            #     os.remove(path)