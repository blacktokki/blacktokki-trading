from datetime import datetime
from utils import MIN_TRDVAL, TRDVAL_DAYS, load_stocklist_json, is_index_stock, load_stock_json, trdval_filter

def __init__(*args):
    start_date = datetime.strptime(args[0], '%Y-%m-%d').date() if len(args)>0 else datetime.now().date()
    end_date = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args)>1 else datetime.now().date()
    trdval_days = int(args[2]) if len(args)>2 else TRDVAL_DAYS
    min_trdval = int(args[3]) if len(args)>3 else MIN_TRDVAL
    exclude_index = int(args[2]) if len(args) >2 else 0
    data_all = load_stocklist_json()
    cnt = 0
    status = [0, 0, 0, 0]
    print('len: ', len(data_all))
    for i, d in enumerate(data_all):
        if exclude_index and is_index_stock(d['codeName']):
            continue
        full_code = d['full_code']
        j2 = load_stock_json(full_code, start_date=start_date, end_date=end_date, log_datetime=True)
        status[j2['_status']] += 1
        if j2['_status'] == 0:
            print(i, d)
            print(j2['output'][0] if len(j2['output']) else None)
        else:
            print(i, d['codeName'])
            # print(j2['_status'], j2['output'][0]['TRD_DD']  if len(j2['output']) else None, datetime.strptime(j2['CURRENT_DATETIME'], '%Y.%m.%d %p %I:%M:%S').date())
        if trdval_filter(j2, trdval_days, min_trdval):
            cnt+=1
    print(status, cnt)
