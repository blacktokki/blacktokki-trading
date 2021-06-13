import math
from datetime import datetime, timedelta
from utils import report_json_data, cov_and_var, REPORT_OFFSET2, load_stock_json, report_merge
# from .load_zscore import __init__ as load_zscore

def __init__(*args):
    keys = args[0] if len(args) > 0 else None
    repeat = int(args[1]) if len(args) >1 else 0
    date1 = datetime.strptime(args[2], '%Y-%m-%d').date() if len(args) >2 else None
    offset = int(args[3]) if len(args) >3 else None
    max_lisk = float(args[4]) if len(args) >4 else 0.8
    exclude_index = 1
    
    key_all = keys.split('_')
    delta = (repeat-1) * REPORT_OFFSET2
    start_date = date1 - timedelta(offset*(delta +1))
    end_date = date1 - timedelta(offset*delta)
    j_all = [report_json_data(load_stock_json(key, start_date=datetime.now().date())['output'], start_date, end_date) for key in key_all]
    # zscore_all = load_zscore(*[1, date1, offset])[0]
    # long_zscore_all = load_zscore(*[1, date1, offset * 3])[0]
    j_dict = {}
    eff_dict = {}
    i = 0
    while i+1 < len(j_all):
        j2 = j_all[i]
        j3 = j_all[i+1]
        key2 = key_all[i]
        key3 = key_all[i+1]
        cov, var_j2, var_j3 = cov_and_var(j2, j3)
        efficient = (var_j3 - cov)/ (var_j2 -2*cov +var_j3)
        for kk, _var in [(key2, var_j2), (key3, var_j3)]:
            '''
            short_z = zscore_all.get(kk)
            long_z = long_zscore_all.get(kk)
            short_p = 1 - laplace_cdf(short_z)
            long_p = 1 - laplace_cdf(long_z)
            short_kelly = short_p * 2 -1
            long_kelly = long_p *2 -1
            kelly = short_kelly*(1-long_p) + long_kelly*long_p
            '''
            print(kk[3:9],'std:', math.sqrt(_var))
        print(key2[3:9], key3[3:9], efficient, 1-efficient)
        j_dict[f"{key2}_{key3}"] = report_merge(j2, j3, efficient)
        eff_dict[f"{key2}_{key3}"] = [efficient, 1-efficient]
        i+=2
    for i, kv in enumerate(j_dict.items()):
        key2, j2 = kv
        for key3, j3 in list(j_dict.items())[:i]:
            cov, var_j2, var_j3 = cov_and_var(j2, j3)
            efficient = (var_j3 - cov)/ (var_j2 -2*cov +var_j3)
            key_partial = f"{key2}_{key3}".split('_')
            if (1- max_lisk)< efficient and efficient <max_lisk:
                print(efficient, {
                    key_partial[0][3:9]: (eff_dict[key2][0] * efficient),
                    key_partial[1][3:9]: (eff_dict[key2][1] * efficient),
                    key_partial[2][3:9]: (eff_dict[key3][0] * (1 - efficient)),
                    key_partial[3][3:9]: (eff_dict[key3][1] * (1 - efficient))
                })