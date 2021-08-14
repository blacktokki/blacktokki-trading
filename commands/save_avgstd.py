import os
import subprocess
import time
from datetime import datetime, timedelta
from utils import load_stocklist_json, TRDVAL_DAYS, MIN_TRDVAL, is_index_stock, trdval_filter, load_stock_json, avgstd
from utils.jsonio import load_json, save_json

MAX_PROCESS = 8

def init_part(start_date=datetime(1990,1,1), end_date=datetime(2100,1,1), full_code='', days_list=[], trdval_days=TRDVAL_DAYS, min_trdval=MIN_TRDVAL):
    j2 = load_stock_json(full_code, start_date=datetime.now().date())
    if trdval_filter(j2, trdval_days, min_trdval) is False:
        return
    root_path = os.path.join('data', 'avgstd')
    if not os.path.exists(root_path):
        os.mkdir(root_path)
    path = os.path.join(root_path,  f'{full_code}.json')
    data_table = load_json(path) if os.path.exists(path) else {} 
    _end_date = start_date
    while _end_date <= end_date:
        for days in days_list:
            key = f'{_end_date}_{days}'
            if not key in data_table:
                _start_date = _end_date - timedelta(days=days - 1)
                data_table[key] = avgstd(j2['output'], _start_date, _end_date)
        _end_date += timedelta(days=1)     
    save_json(data_table, path)

def __init__(*args):
    days_list = [int(i) for i in args[0].split(',')] if len(args) >0 else 0
    date1 = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args) >1 else None
    offset = int(args[2]) if len(args) >2 else None
    code = args[3] if len(args) >3 else None
    exclude_index = True
    cmds = []
    ps = []
    if code is None:
        data_all = load_stocklist_json()
        print('len: ', len(data_all))
        for i, d in enumerate(data_all):
            if exclude_index and is_index_stock(d['codeName']):
                continue
            full_code = d['full_code']
            cmds.append(f'{os.getcwd()}/../venv3/Scripts/python __init__.py "save_avgstd" {args[0]} {args[1]} {args[2]} {full_code}')
        while ps + cmds:
            ps = [p for p in ps if p.poll() is None]
            cmd_cnt = 0
            print('remain: ', len(cmds))
            while len(ps) < MAX_PROCESS and cmd_cnt < len(cmds):
                ps.append(subprocess.Popen(cmds[cmd_cnt], shell=True))
                cmd_cnt += 1
            cmds = cmds[cmd_cnt:]
            time.sleep(1)
    elif date1 and offset:
        print('code: ', code)
        init_part(start_date= date1 - timedelta(offset), end_date=date1, full_code=code, days_list=days_list)
