from datetime import datetime

def __init__(*args):
    repeat = int(args[0]) if len(args) >0 else 0
    start_date = datetime.strptime(args[1], '%Y-%m-%d').date() if len(args)>1 else datetime.now().date()
    end_date = datetime.strptime(args[2], '%Y-%m-%d').date() if len(args)>2 else datetime.now().date()
    offset = int(args[3]) if len(args) >3 else 28
    print(f'__init__.py load_normal {start_date} {end_date} 1')
    print(f'__init__.py load_zscore {repeat} {end_date} {offset}')
    print(f'__init__.py load_zscore 1 {end_date} {offset*3}')
    print(f'__init__.py save_report_json {repeat} {end_date} {offset}')
    print(f'__init__.py load_report_json {repeat} {end_date} {offset} -1 -0.22 0.66 0') # -0.22 -> -0.2 ~ -0.33