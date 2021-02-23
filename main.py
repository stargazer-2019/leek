import argparse
import warnings
import datetime

from task import FlushDataTask

warnings.filterwarnings('ignore')

if __name__ == '__main__':
    yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(description="welcome.")
    parser.add_argument("--op", type=str, help="操作")
    parser.add_argument("--start", type=str, help="开始日期", default=yesterday)
    parser.add_argument("--end", type=str, default=yesterday)
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--code", type=str, default=None)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--quarter", type=int, default=None)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--num", type=int, default=50)
    parser.add_argument("--reset", type=bool, default=False)
    args = parser.parse_args()
    op = args.op
    code = args.code
    start = args.start
    end = args.end
    date = args.date
    reset = args.reset
    page = args.page
    if args.date:
        start = end = args.date
    if op == "holder_num":
        FlushDataTask.flush_holder_num_task(date)
    elif op == "adj":
        FlushDataTask.flush_adj_task(date)
    elif op == "finance":
        FlushDataTask.flush_finance_task(date)
    elif op == "holder":
        FlushDataTask.flush_holder_task(date)
    elif op == "market":
        FlushDataTask.flush_market_task(start, end)
    elif op == "secretary":
        FlushDataTask.flush_secretary_task(date, reset)
    elif op == "pbc":
        FlushDataTask.flush_pbc_task(page)
    else:
        print("args wrong.")
