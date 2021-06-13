import os
import math
from datetime import datetime, timedelta
from utils import REPORT_OFFSET2, FILE_SPLIT
from utils.jsonio import load_json
from .load_zscore import __init__ as load_zscore


def init_part(start_date=datetime(1990,1,1), end_date=datetime(2100,1,1), min_correl=-1 ,max_correl=1, max_lisk=0.8):
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


def zscore_filter(zscore_dict, keys, max_zscore):
    for key in keys.split('_'):
        z_score = zscore_dict.get(key)
        if z_score is None or z_score > max_zscore:
            return False
    return True

def __init__(*args):
    repeat = int(args[0]) if len(args) >0 else 0
    date1 = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args) >1 else None
    offset = int(args[2]) if len(args) >2 else None
    min_correl = float(args[3]) if len(args) >3 else -1
    max_correl = float(args[4]) if len(args) >4 else 1
    max_lisk = float(args[5]) if len(args) >5 else 0.8
    max_zscore = float(args[6]) if len(args) > 6 else 0
    results = []
    sets = None
    zscore_all = load_zscore(*[1, date1, offset])[0]

    for i in range(repeat):
        delta = i * REPORT_OFFSET2
        start_date = date1 - timedelta(offset*(delta +1))
        end_date = date1 - timedelta(offset*delta)
        result = init_part(start_date= start_date, end_date=end_date, min_correl=min_correl, max_correl=max_correl, max_lisk=max_lisk)
        if i == 0:
            for k in list(result.keys()):
                if zscore_filter(zscore_all, k, max_zscore) is False:
                    result.pop(k)
        results.append(result)
        sets = set(results[i].keys()) if i == 0  else (sets & set(results[i].keys()))
    for key in sets:
        print([k2[3:9] for k2 in key.split('_')], [result[key] for result in results])
    print('_'.join(sets))