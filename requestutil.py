import os
import requests
import json
import pandas as pd
from datetime import datetime
from jsonio import load_json, save_json

headers = {'User-Agent': 'Chrome/78.0.3904.87 Safari/537.36',}


def request_company_list():
    r = requests.post('http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd', data={'bld': 'dbms/comm/finder/finder_stkisu',})
    return json.loads(r.text)

def request_company(full_code, start_date=datetime(1990,1,1), end_date=datetime(2100,1,1)):
    data = {
        'bld': 'dbms/MDC/STAT/issue/MDCSTAT23902',
        'isuCd': full_code,
        'isuCd2': '',
        'strtDd': start_date.strftime("%Y%m%d"),
        'endDd': end_date.strftime("%Y%m%d"),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    r = requests.post(url, data, headers=headers)
    try:
        return json.loads(r.text)
    except Exception:
        print(r._content.decode('utf-8'))

# def read_company(data):
#     df = pd.json_normalize(data['output'])
#     col_map = {'TRD_DD':'Date', 'ISU_CD':'Code', 'ISU_NM':'Name', 'MKT_NM':'Market', 
#                 'SECUGRP_NM':'SecuGroup', 'TDD_CLSPRC':'Close', 'FLUC_TP_CD':'UpDown', 
#                 'CMPPRVDD_PRC':'Change', 'FLUC_RT':'ChangeRate', 
#                 'TDD_OPNPRC':'Open', 'TDD_HGPRC':'High', 'TDD_LWPRC':'Lower', 
#                 'ACC_TRDVOL':'Volume', 'ACC_TRDVAL':'Amount', 'MKTCAP':'MarCap'}

#     df = df.rename(columns=col_map)
#     df['Date'] = pd.to_datetime(df['Date'])
#     int_cols = ['Close', 'UpDown', 'Change', 'Open', 'High', 'Lower', 'Volume', 'Amount', 'MarCap', 'ChangeRate']
#     for col in int_cols: 
#         df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
#     return df

