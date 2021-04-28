import FinanceDataReader as fdr
from datetime import datetime
import pandas as pd
import time
import requests
import json

headers = {'User-Agent': 'Chrome/78.0.3904.87 Safari/537.36',}

def read_company(company):
    data = {
        'bld': 'dbms/MDC/STAT/issue/MDCSTAT23902',
        'isuCd': d['full_code'],
        'isuCd2': '',
        'strtDd': datetime(1990,1,1).strftime("%Y%m%d"),
        'endDd': datetime(2100,1,1).strftime("%Y%m%d"),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }
    
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    r = requests.post(url, data, headers=headers)
    j = json.loads(r.text)
    df = pd.json_normalize(j['output'])
    col_map = {'TRD_DD':'Date', 'ISU_CD':'Code', 'ISU_NM':'Name', 'MKT_NM':'Market', 
                'SECUGRP_NM':'SecuGroup', 'TDD_CLSPRC':'Close', 'FLUC_TP_CD':'UpDown', 
                'CMPPRVDD_PRC':'Change', 'FLUC_RT':'ChangeRate', 
                'TDD_OPNPRC':'Open', 'TDD_HGPRC':'High', 'TDD_LWPRC':'Lower', 
                'ACC_TRDVOL':'Volume', 'ACC_TRDVAL':'Amount', 'MKTCAP':'MarCap'}

    df = df.rename(columns=col_map)
    df['Date'] = pd.to_datetime(df['Date'])
    int_cols = ['Close', 'UpDown', 'Change', 'Open', 'High', 'Lower', 'Volume', 'Amount', 'MarCap', 'ChangeRate']
    for col in int_cols: 
        df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
    return df



data = {
    'mktsel': 'ALL',
    'searchText': '',
    'bld': 'dbms/comm/finder/finder_listdelisu',
}
url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
r = requests.post(url, data, headers=headers)
j = json.loads(r.text)
for d in j['block1']:
    if d['marketCode'] == 'STK':
        continue
    print(d)
    df = read_company(d)
    for i in range(len(df['Date'])):
        print(df['Date'][i])    
