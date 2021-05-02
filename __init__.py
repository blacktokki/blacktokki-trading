import FinanceDataReader as fdr
from datetime import datetime, timedelta
import pandas as pd
import time
import requests
import json
import os
from jsonio import save_json, load_json

headers = {'User-Agent': 'Chrome/78.0.3904.87 Safari/537.36',}
INDEX_STOCK = ['ARIRANG', 'HANARO', 'KBSTAR', 'KINDEX', 'KODEX', 'TIGER', 'KOSEF', 'SMART', 'TREX']

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
    return json.loads(r.text)


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


def load_stocklist_json():
    path = os.path.join('data', 'list.json')
    if os.path.exists(path):
        j = load_json(path)
    else:
        r = requests.post('http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd', data={'bld': 'dbms/comm/finder/finder_stkisu',})
        j = json.loads(r.text)
        save_json(j, path)
    # filtering stock!
    return [
        d for d in j['block1'] if (
            d['marketCode'] in ['KSQ', 'STK'] and d['full_code'][2] == '7' and d['full_code'][8:11] == '000' and '스팩' not in d['codeName']
        )
    ]

def load_stock_json(full_code, old_data_limit=365):
    path = os.path.join('data', 'stock', f'{full_code}.json')
    try:
        j2 = load_json(path)
    except Exception:
        j2 = request_company(full_code)
        save_json(j2, path)
        j2['_status'] = 0
        time.sleep(0.25)
    else:
        output_len = len(j2['output'])
        last_date = datetime.strptime(j2['output'][0]['TRD_DD'], '%Y/%m/%d').date() if output_len else None
        if  output_len == 0:
            j2 = request_company(full_code)
            save_json(j2, path)
            j2['_status'] = 2
            time.sleep(0.25)
        elif last_date > datetime.now().date() - timedelta(days=old_data_limit):
            j3 = request_company(full_code, start_date=last_date)
            j2['output'] = j3['output'] + j2['output'][1:]
            j2['CURRENT_DATETIME'] = j3['CURRENT_DATETIME']
            save_json(j2, path)
            j2['_status'] = 3
            time.sleep(0.25)
        else:
            j2['_status'] = 1
    return j2

def is_index_stock(codename):
    for index_stock in INDEX_STOCK:
        if codename.startswith(index_stock):
            return True
    return False

def load_report_json(exclude_index=True):
    path = os.path.join('data', 'report.json')
    if os.path.exists(path):
        data_table = load_json(path)
    else:
        data_all = [d for d in load_stocklist_json() if not exclude_index or not is_index_stock(d['codeName'])]
        data_list_head = []
        data_list = []
        data_table = []
        data_all_len_pow = len(data_all) * len(data_all)
        for i, d in enumerate(data_all):
            print(f'{i*i * 100.0 / data_all_len_pow}%')
            full_code = d['full_code']
            _j2 = load_stock_json(full_code, old_data_limit=0)
            j2 = []
            for _j2_data in _j2['output']:
                j2_data = {}
                j2_data['TRD_DD'] = datetime.strptime(_j2_data['TRD_DD'], '%Y/%m/%d').date()
                j2_data['FLUC_RT'] = float(_j2_data['FLUC_RT'])
                j2_data['FLUC_RT_POW'] = j2_data['FLUC_RT'] * j2_data['FLUC_RT']
                j2.append(j2_data)

            for k in range(len(data_list_head)):
                j3_head = data_list_head[k]
                j3_body = data_list[k]
                j3_body_i = 0
                j3_body_len = len(j3_body)
                j3_sum = 0.0
                j3_pow_sum = 0.0
                j2_sum = 0.0
                j2_pow_sum = 0.0
                cov_sum = 0.0
                cov_cnt = 0
                for j2_data in j2:
                    j3_data = j3_body[j3_body_i]
                    while j2_data['TRD_DD'] < j3_data['TRD_DD'] and j3_body_i < j3_body_len-1:
                        j3_body_i += 1
                        j3_data = j3_body[j3_body_i]
                    if j2_data['TRD_DD'] == j3_data['TRD_DD']:
                        j2_sum += j2_data['FLUC_RT']
                        j2_pow_sum += j2_data['FLUC_RT_POW']
                        j3_sum += j3_data['FLUC_RT']
                        j3_pow_sum += j3_data['FLUC_RT_POW']
                        cov_sum += j3_data['FLUC_RT'] * j2_data['FLUC_RT']
                        cov_cnt += 1
                if cov_cnt and j2_sum != 0 and j3_sum != 0:
                    j2_avg = j2_sum/cov_cnt
                    j3_avg = j3_sum/cov_cnt
                    cov = (cov_sum/cov_cnt) - j3_avg * j2_avg
                    data_table.append([f'{full_code}_{j3_head}', cov / (j2_pow_sum/cov_cnt - j2_avg * j2_avg)])
                    data_table.append([f'{j3_head}_{full_code}', cov / (j3_pow_sum/cov_cnt - j3_avg * j3_avg)])
            data_list_head.append(full_code)
            data_list.append(j2)
        save_json(data_table, path)
    return data_table


def load_normal(exclude_index=False):
    data_all = load_stocklist_json()
    status = [0, 0, 0, 0]
    print('len: ', len(data_all))
    for i, d in enumerate(data_all):
        if exclude_index and is_index_stock(d['codeName']):
            continue
        full_code = d['full_code']
        j2 = load_stock_json(full_code, old_data_limit=0)
        status[j2['_status']] += 1
        if j2['_status'] == 0:
            print(i, d)
            print(j2['output'][0] if len(j2['output']) else None)
        else:
            print(i, d['codeName'])
            # print(j2['_status'], j2['output'][0]['TRD_DD']  if len(j2['output']) else None, datetime.strptime(j2['CURRENT_DATETIME'], '%Y.%m.%d %p %I:%M:%S').date())
        #df = read_company(j2)
        #for i in range(len(df['Date'])):
        #    print(df['Date'][i])
    print(status)


def remove_stock_json():
    data_all = load_stocklist_json()
    for i, d in enumerate(data_all):
        full_code = d['full_code']
        path = os.path.join('data', 'stock', f'{full_code}.json')
        if '스팩' in d['codeName']:
            print(d['codeName'])
            # if os.path.exists(path):
            #     os.remove(path)

if __name__ == "__main__":
    load_report_json(exclude_index=True)