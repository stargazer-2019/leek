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
    parser.add_argument("--page", type=int, default=None)
    parser.add_argument("--num", type=int, default=50)
    args = parser.parse_args()
    op = args.op
    code = args.code
    start = args.start
    end = args.end
    if args.date:
        start = end = args.date
    if op == "holder_num":
        FlushDataTask.flush_holder_num_task()
    else:
        print("args wrong.")