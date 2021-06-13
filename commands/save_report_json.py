import os
import subprocess
import time
from datetime import datetime, timedelta
from utils import REPORT_OFFSET2, load_stocklist_json, TRDVAL_DAYS, MIN_TRDVAL, is_index_stock, FILE_SPLIT, trdval_filter, load_stock_json, report_json_data, cov_and_var
from utils.jsonio import load_json, save_json

MAX_PROCESS = 4

def init_part(start_date=datetime(1990,1,1), end_date=datetime(2100,1,1), exclude_index=True, reset=False, trdval_days=TRDVAL_DAYS, min_trdval=MIN_TRDVAL):
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
            _j2 = load_stock_json(full_code, start_date=datetime.now().date())
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

def __init__(*args):
    repeat = int(args[0]) if len(args) >0 else 0
    date1 = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args) >1 else None
    offset = int(args[2]) if len(args) >2 else None
    delta = int(args[3]) *REPORT_OFFSET2 if len(args) >3 else None
    cmds = []
    ps = []
    if delta is None:
        for i in range(repeat):
            cmds.append(f'{os.getcwd()}/../venv3/Scripts/python __init__.py "save_report_json" {args[0]} {args[1]} {args[2]} {i}')
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
        init_part(start_date= date1 - timedelta(offset*(delta+1)), end_date=date1 - timedelta(offset*delta), exclude_index=True)