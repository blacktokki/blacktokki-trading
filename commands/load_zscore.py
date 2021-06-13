import os
from datetime import datetime, timedelta
from utils import load_json, load_stock_json, load_stocklist_json, zscore, REPORT_OFFSET2
from utils.jsonio import save_json

def __init__(*args):
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
            output = load_stock_json(full_code, start_date=datetime.now().date())['output']
            for ii, dates in need_create.items():
                z_score = zscore(output, dates[0], dates[1])
                if z_score:
                    print(i, full_code, dates[0], dates[1], z_score)
                    results[ii][full_code] = z_score
        for ii, dates in need_create.items():
            path = os.path.join('data', 'zscore', f'{dates[0]}_{dates[1]}.json')
            save_json(results[ii], path)
    return results