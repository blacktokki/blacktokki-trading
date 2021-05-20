import FinanceDataReader as fdr
from datetime import datetime, timedelta
import pandas as pd
import time
import requests
import json
import os
import sys
import math
import subprocess
from jsonio import save_json, load_json

headers = {'User-Agent': 'Chrome/78.0.3904.87 Safari/537.36',}
INDEX_STOCK = ['ARIRANG', 'HANARO', 'KBSTAR', 'KINDEX', 'KODEX', 'TIGER', 'KOSEF', 'SMART', 'TREX']
FILE_SPLIT = 10

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

def load_stock_json(full_code, start_day=365, end_day=0):
    start_date = datetime.now().date() - timedelta(days=start_day)
    end_date = datetime.now().date() - timedelta(days=end_day)
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
        print(start_date, last_date, end_date)
        if  output_len == 0:
            j2 = request_company(full_code)
            save_json(j2, path)
            j2['_status'] = 2
            time.sleep(0.25)
        elif start_date < last_date and last_date < end_date:
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


def trdval_filter(data, trdval_days, min_trdval):
    trd_val_sum = 0
    trd_val_cnt = 0
    for d in data['output']:
        trd_val_sum += int(d['ACC_TRDVAL'].replace(',',''))
        trd_val_cnt += 1
        if trd_val_cnt == trdval_days:
            break
    return trd_val_sum > min_trdval

def report_json_data(output, start_date, end_date):
    j2 = []
    for _j2_data in output:
        trd_dd = datetime.strptime(_j2_data['TRD_DD'], '%Y/%m/%d').date()
        if start_date <= trd_dd and trd_dd <= end_date:
            j2_data = {}
            j2_data['TRD_DD'] = trd_dd
            j2_data['FLUC_RT'] = float(_j2_data['FLUC_RT'])
            j2_data['FLUC_RT_POW'] = j2_data['FLUC_RT'] * j2_data['FLUC_RT']
            j2.append(j2_data)
    return j2


def zscore(output, start_date, end_date):
    close_sum = 0
    close_pow_sum = 0
    close_cnt = 0
    close_first = None
    for _j2_data in output:
        trd_dd = datetime.strptime(_j2_data['TRD_DD'], '%Y/%m/%d').date()
        if start_date <= trd_dd and trd_dd <= end_date:
           close = int(_j2_data['TDD_CLSPRC'].replace(',', ''))
           close_sum += close
           close_pow_sum += close * close
           close_cnt += 1
           close_first = close_first if close_first else close
    if close_first:
        close_avg = close_sum/close_cnt
        close_var = close_pow_sum/close_cnt - close_avg * close_avg
        return (close_first - close_avg)/math.sqrt(close_var) if close_var else None
    return None


def cov_and_var(j2_body, j3_body):
    j3_body_i = 0
    j3_body_len = len(j3_body)
    j3_sum = 0.0
    j3_pow_sum = 0.0
    j2_sum = 0.0
    j2_pow_sum = 0.0
    cov_sum = 0.0
    cov_cnt = 0
    _tm = []
    for j2_data in j2_body:
        j3_data = j3_body[j3_body_i] if j3_body_len > 0 else {'TRD_DD':datetime(1990,1,1).date()}
        while j2_data['TRD_DD'] < j3_data['TRD_DD'] and j3_body_i < j3_body_len-1:
            j3_body_i += 1
            j3_data = j3_body[j3_body_i]
        if j2_data['TRD_DD'] == j3_data['TRD_DD']:
            j2_sum += j2_data['FLUC_RT']
            j2_pow_sum += j2_data['FLUC_RT_POW']
            j3_sum += j3_data['FLUC_RT']
            j3_pow_sum += j3_data['FLUC_RT_POW']
            cov_sum += j3_data['FLUC_RT'] * j2_data['FLUC_RT']
            _tm.append(j3_data['FLUC_RT'])
            cov_cnt += 1
    if cov_cnt and j2_sum != 0 and j3_sum != 0:
        j2_avg = j2_sum/cov_cnt
        j3_avg = j3_sum/cov_cnt
        cov = (cov_sum/cov_cnt) - j3_avg * j2_avg
        j2_var = j2_pow_sum/cov_cnt - j2_avg * j2_avg
        j3_var = j3_pow_sum/cov_cnt - j3_avg * j3_avg
        return cov, j2_var, j3_var
    return None, None, None


def report_merge(j2_body, j3_body, efficient):
    j4_body = []
    j3_body_i = 0
    j3_body_len = len(j3_body)
    for j2_data in j2_body:
        j3_data = j3_body[j3_body_i] if j3_body_len > 0 else {'TRD_DD':datetime(1990,1,1).date()}
        while j2_data['TRD_DD'] < j3_data['TRD_DD'] and j3_body_i < j3_body_len-1:
            j3_body_i += 1
            j3_data = j3_body[j3_body_i]
        if j2_data['TRD_DD'] == j3_data['TRD_DD']:
            j4_data = {'TRD_DD': j2_data['TRD_DD'], 'FLUC_RT': j2_data['FLUC_RT']* efficient + j3_data['FLUC_RT']* (1-efficient)}
            j4_data['FLUC_RT_POW'] = j4_data['FLUC_RT'] * j4_data['FLUC_RT']
            j4_body.append(j4_data)
    return j4_body


def save_report_json_part(start_date=datetime(1990,1,1), end_date=datetime(2100,1,1), exclude_index=True, reset=False, trdval_days=20, min_trdval=40000000000):
    data_all = [d for d in load_stocklist_json() if (not exclude_index or not is_index_stock(d['codeName']))]
    data_split = [[] for i in range(FILE_SPLIT)]
    data_all_len_pow = len(data_all) * len(data_all)
    for i, d in enumerate(data_all):
        data_split[int(i*i * FILE_SPLIT/ data_all_len_pow)].append(d)
    
    data_list_head = []
    data_list = []
    raw_i = 0
    prev_per = 0
    root_path = os.path.join('data', 'report', f'{start_date}_{end_date}')
    if not os.path.exists(root_path):
        os.mkdir(root_path)
    for k in range(FILE_SPLIT):
        path = os.path.join(root_path,  f'{FILE_SPLIT}_{k}.json')
        data_table_old = load_json(path) if os.path.exists(path) else {} 
        data_table = {} 
        for i, d in enumerate(data_split[k]):
            per = raw_i*raw_i * 100.0 / data_all_len_pow
            if per>prev_per:
                prev_per += 1
                print(f'{per}%')
            full_code = d['full_code']
            _j2 = load_stock_json(full_code, start_day=0)
            if trdval_filter(_j2, trdval_days, min_trdval) is False:
                raw_i += 1
                continue
            j2 = report_json_data(_j2['output'], start_date, end_date)
            for k in range(len(data_list_head)):
                j3_head = data_list_head[k]
                key1 = f'{full_code}_{j3_head}'
                key2 = f'{j3_head}_{full_code}'
                if key1 in data_table_old and key2 in data_table_old:
                    data_table[key1] = data_table_old.pop(key1)
                    data_table[key2] = data_table_old.pop(key2)
                elif reset is False:
                    cov, j2_var, j3_var = cov_and_var(j2, data_list[k])
                    if cov is not None and j2_var !=0 and j3_var!=0:
                        data_table[key1] = cov / j2_var
                        data_table[key2] = cov / j3_var
                else:
                    data_table[key1] = 0
                    data_table[key2] = 0
            data_list_head.append(full_code)
            data_list.append(j2)
            raw_i += 1
        save_json(data_table, path)


def load_report_json_part(start_date=datetime(1990,1,1), end_date=datetime(2100,1,1), min_correl=-1 ,max_correl=1, max_lisk=0.66):
    root_path = os.path.join('data', 'report', f'{start_date}_{end_date}')
    data_all = {}
    result = {}
    for k in range(FILE_SPLIT):
        path = os.path.join(root_path,  f'{FILE_SPLIT}_{k}.json')
        data_table_old = load_json(path)
        for k in list(data_table_old.keys()):
            value = data_table_old.pop(k, None)
            if value is not None:
                reverse_key = '_'.join(reversed(k.split('_')))
                value2 = data_table_old.pop(reverse_key)
                data_all[k] = value
                data_all[reverse_key] = value2
                correl = math.copysign(1, value)*math.sqrt(value * value2)
                editbeta1 = value/ math.copysign(correl, 1)
                editbeta2 = value2/math.copysign(correl, 1)
                lisk = max(editbeta1/(editbeta1 + editbeta2), editbeta2/(editbeta1 + editbeta2))
                if min_correl <= correl and correl < max_correl and lisk < max_lisk:
                    result[k] = [editbeta1, editbeta2, correl]
    return result


def remove_stock_json():
    data_all = load_stocklist_json()
    for i, d in enumerate(data_all):
        full_code = d['full_code']
        path = os.path.join('data', 'stock', f'{full_code}.json')
        if '스팩' in d['codeName']:
            print(d['codeName'])
            # if os.path.exists(path):
            #     os.remove(path)


def load_normal(*args):
    start_day = int(args[0]) if len(args)>0 else 0
    end_day = int(args[1]) if len(args)>1 else 0
    exclude_index = int(args[2]) if len(args) >2 else 0
    data_all = load_stocklist_json()
    cnt = 0
    status = [0, 0, 0, 0]
    print('len: ', len(data_all))
    for i, d in enumerate(data_all):
        if exclude_index and is_index_stock(d['codeName']):
            continue
        full_code = d['full_code']
        j2 = load_stock_json(full_code, start_day=start_day, end_day=end_day)
        status[j2['_status']] += 1
        if j2['_status'] == 0:
            print(i, d)
            print(j2['output'][0] if len(j2['output']) else None)
        else:
            print(i, d['codeName'])
            # print(j2['_status'], j2['output'][0]['TRD_DD']  if len(j2['output']) else None, datetime.strptime(j2['CURRENT_DATETIME'], '%Y.%m.%d %p %I:%M:%S').date())
        if trdval_filter(j2, 20, 40000000000):
            cnt+=1
        #df = read_company(j2)
        #for i in range(len(df['Date'])):
        #    print(df['Date'][i])
    print(status, cnt)


def load_zscore(*args):
    repeat = int(args[0]) if len(args) >0 else 0
    if len(args) >1:
        if  type(args[1]) == str:
            date1 = datetime.strptime(args[1], '%Y-%m-%d').date()
        else:
            date1 = args[1]
    # date1 = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args) >1 else None
    offset = int(args[2]) if len(args) >2 else None
    exclude_index = 1
    results = []
    need_create = {}
    for i in range(repeat):
        delta = i * REPORT_OFFSET2
        start_date = date1 - timedelta(offset*(delta+1))
        end_date = date1 - timedelta(offset*delta)
        # print(date1, start_date, end_date)
        path = os.path.join('data', 'zscore', f'{start_date}_{end_date}.json')
        try:
            results.append(load_json(path))
        except Exception as e:
            results.append({})
            need_create[i] = (start_date, end_date)
    if len(need_create):
        data_all = load_stocklist_json()
        print('len: ', len(data_all))
        for i, d in enumerate(data_all):
            if exclude_index and is_index_stock(d['codeName']):
                continue
            full_code = d['full_code']
            output = load_stock_json(full_code, start_day=0)['output']
            for ii, dates in need_create.items():
                z_score = zscore(output, dates[0], dates[1])
                if z_score:
                    print(i, full_code, dates, z_score)
                    results[ii][full_code] = z_score
        for ii, dates in need_create.items():
            path = os.path.join('data', 'zscore', f'{dates[0]}_{dates[1]}.json')
            save_json(results[ii], path)
        
    return results

REPORT_OFFSET2 = 0.5
MAX_PROCESS = 4

def save_report_json(*args):
    repeat = int(args[0]) if len(args) >0 else 0
    date1 = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args) >1 else None
    offset = int(args[2]) if len(args) >2 else None
    delta = int(args[3]) *REPORT_OFFSET2 if len(args) >3 else None
    cmds = []
    ps = []
    if delta is None:
        for i in range(repeat):
            cmds.append(f'{os.getcwd()}/../venv3/Scripts/python {__file__} "save_report_json" {args[0]} {args[1]} {args[2]} {i}')
        while ps + cmds:
            ps = [p for p in ps if p.poll() is None]
            cmd_cnt = 0
            while len(ps) < MAX_PROCESS and cmd_cnt < len(cmds):
                ps.append(subprocess.Popen(cmds[cmd_cnt], shell=True))
                cmd_cnt += 1
            cmds = cmds[cmd_cnt:]
            time.sleep(4)
    elif date1 and offset:
        print('delta: ', delta)
        save_report_json_part(start_date= date1 - timedelta(offset*(delta+1)), end_date=date1 - timedelta(offset*delta), exclude_index=True)


def load_report_zscore_filter(zscore_dict, keys, max_zscore):
    for key in keys.split('_'):
        z_score = zscore_dict.get(key)
        if z_score is None or z_score > max_zscore:
            return False
    return True


def load_report_json(*args):
    repeat = int(args[0]) if len(args) >0 else 0
    date1 = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args) >1 else None
    offset = int(args[2]) if len(args) >2 else None
    min_correl = float(args[3]) if len(args) >3 else -1
    max_correl = float(args[4]) if len(args) >4 else 1
    max_lisk = float(args[5]) if len(args) >5 else 0.66
    max_zscore = float(args[6]) if len(args) > 6 else 0
    results = []
    sets = None
    zscore_all = load_zscore(*[1, date1, offset])[0]

    for i in range(repeat):
        delta = i * REPORT_OFFSET2
        start_date = date1 - timedelta(offset*(delta +1))
        end_date = date1 - timedelta(offset*delta)
        result = load_report_json_part(start_date= start_date, end_date=end_date, min_correl=min_correl, max_correl=max_correl, max_lisk=max_lisk)
        if i == 0:
            for k in list(result.keys()):
                if load_report_zscore_filter(zscore_all, k, max_zscore) is False:
                    result.pop(k)
        results.append(result)
        sets = set(results[i].keys()) if i == 0  else (sets & set(results[i].keys()))
    for key in sets:
        print(key[3:9], key[16:22])
    for key in sets:
        print(key, [zscore_all.get(k2) for k2 in key.split('_')], [result[key] for result in results])


def load_past_report_json(*args):  # not work
    keys = args[0] if len(args) > 0 else None
    repeat = int(args[1]) if len(args) >1 else 0
    date1 = datetime.strptime(args[2], '%Y-%m-%d').date() if len(args) >2 else None
    offset = int(args[3]) if len(args) >3 else None
    exclude_index = 1
    
    key_all = keys.split('_')
    delta = (repeat-1) * REPORT_OFFSET2
    start_date = date1 - timedelta(offset*(delta +1))
    end_date = date1 - timedelta(offset*delta)
    j_all = [report_json_data(load_stock_json(key, start_day=0)['output'], start_date, end_date) for key in key_all]
    j2 = j_all[0]
    efficient = 1
    for i in range(1, len(j_all)):
        j3 = j_all[i]
        cov, var_j2, var_j3 = cov_and_var(j2, j3)
        efficient = (var_j3 - cov)/ (var_j2 -2*cov +var_j3)
        print(math.sqrt(var_j2), math.sqrt(var_j3), efficient, 1-efficient)
        j2 = report_merge(j2, j3, efficient)
    '''
    print('')
    data_all = load_stocklist_json()
    for i, d in enumerate(data_all):
        if exclude_index and is_index_stock(d['codeName']):
            continue
        full_code = d['full_code']
        j3 = report_json_data(load_stock_json(full_code, start_day=0)['output'], start_date, end_date)
        cov, var_j2, var_j3 = cov_and_var(j2, j3)
        if cov:
            efficient = (var_j3 - cov)/ (var_j2 -2*cov +var_j3)
            print(full_code, math.sqrt(var_j2), math.sqrt(var_j3), efficient, 1-efficient)
    '''

if __name__ == "__main__":
    locals().get(sys.argv[1])(*(sys.argv[2:]))
    # __init__.py load_zscore 5 2021-05-19 28
    # __init__.py save_report_json 5 2021-05-19 28
    # __init__.py load_report_json 5 2021-05-19 28 -1 -0.5 0.66 -1
    # __init__.py load_past_report_json KR7055490007_KR7214870008 5 2021-05-19 28
