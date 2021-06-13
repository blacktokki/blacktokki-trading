import time
import os
import math
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from .jsonio import load_json, save_json
from .requestutil import request_company, request_company_list

INDEX_STOCK = ['ARIRANG', 'HANARO', 'KBSTAR', 'KINDEX', 'KODEX', 'TIGER', 'KOSEF', 'SMART', 'TREX']
FILE_SPLIT = 10
TRDVAL_DAYS = 20
MIN_TRDVAL = 2000000000
REPORT_OFFSET2 = 0.5

def load_stocklist_json():
    path = os.path.join('data', 'list.json')
    if os.path.exists(path):
        j = load_json(path)
    else:
        j = request_company_list()
        save_json(j, path)
    # filtering stock!
    return [
        d for d in j['block1'] if (
            d['marketCode'] in ['KSQ', 'STK'] and d['full_code'][2] == '7' and d['full_code'][8:11] == '000' and '스팩' not in d['codeName']
        )
    ]


def load_stock_json(full_code, start_date=datetime.now().date() - timedelta(days=365), end_date=datetime.now().date(), log_datetime=False):
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
        if log_datetime:
            print(start_date, last_date, end_date)
        if  output_len == 0:
            j2 = request_company(full_code)
            save_json(j2, path)
            j2['_status'] = 2
            time.sleep(0.25)
        elif start_date < last_date and last_date < end_date:
            j3 = request_company(full_code, start_date=last_date, end_date=end_date)
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