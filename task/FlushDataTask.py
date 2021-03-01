import datetime

from mongoengine import connect, disconnect
from pymongo import UpdateOne
from tqdm import tqdm

from backend.DataResource import HolderNumData, HolderData, AdjData, FinanceData, SecretaryData, MarketData, PbcData
from domain.Documents import HolderNumDocument, ShareDocument, HolderDocument, AdjDocument, FinanceDocument, \
    SecretaryDocument, MarketDocument, PbcDocument

yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
today = datetime.datetime.now().strftime("%Y-%m-%d")


def flush_holder_num_task(last_date: str = None):
    """
    刷新股东人数数据
    :param last_date: 最后一个报告期对应日期
    :return:
    """
    job = HolderNumData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    if last_date:
        executed_code_list = HolderNumDocument.objects(publish_date__gte=last_date).distinct("code")
        code_list = list(set(code_list).difference(executed_code_list))
    bar = tqdm(code_list)
    for code in bar:
        data = job.get_by_code(code)
        # noinspection PyProtectedMember
        bulk = [UpdateOne(
            {
                "code": row.code,
                "publish_date": row.publish_date
            },
            {
                "$set": row._asdict()
            },
            upsert=True
        )
            for row in data]
        if bulk:
            # noinspection PyProtectedMember
            HolderNumDocument._get_collection().bulk_write(bulk, ordered=False)
        bar.set_description(f"{code} flush {len(bulk)} records.")
    disconnect()


def flush_holder_task(last_date: str = None):
    """
    刷新股东信息
    :param last_date: 最后一个报告期对应日期
    :return:
    """
    job = HolderData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    if last_date:
        executed_code_list = HolderDocument.objects(publish_date__gte=last_date).distinct("code")
        code_list = list(set(code_list).difference(executed_code_list))
    bar = tqdm(code_list)
    for code in bar:
        data = job.get_by_code(code)
        # noinspection PyProtectedMember
        bulk = [UpdateOne(
            {
                "code": row.code,
                "holder": row.holder,
                "publish_date": row.publish_date
            },
            {
                "$set": row._asdict()
            },
            upsert=True
        )
            for row in data]
        if bulk:
            # noinspection PyProtectedMember
            HolderDocument._get_collection().bulk_write(bulk, ordered=False)
        bar.set_description(f"{code} flush {len(bulk)} records.")
    disconnect()


def flush_adj_task(last_date: str = None):
    """
    刷新复权数据
    :param last_date: 最后一个报告期对应日期
    :return:
    """
    job = AdjData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    if last_date:
        executed_code_list = AdjDocument.objects(trade_date__gte=last_date).distinct("code")
        code_list = list(set(code_list).difference(executed_code_list))
    bar = tqdm(code_list)
    for code in bar:
        data = job.get_by_code(code)
        # noinspection PyProtectedMember
        bulk = [UpdateOne(
            {
                "code": row.code,
                "trade_date": row.trade_date
            },
            {
                "$set": row._asdict()
            },
            upsert=True
        )
            for row in data]
        if bulk:
            # noinspection PyProtectedMember
            AdjDocument._get_collection().bulk_write(bulk, ordered=False)
        bar.set_description(f"{code} flush {len(bulk)} records.")
    disconnect()


def flush_finance_task(last_date: str = None):
    """
    刷新财务数据
    :param last_date: 最后一个报告期对应日期
    :return:
    """
    last_date = last_date if last_date else today
    dt = datetime.datetime.strptime(last_date, "%Y-%m-%d")
    year = dt.year
    last_date_tail = dt.strftime("%m-%d")
    quarter = 4
    if last_date_tail <= "03-31":
        quarter = 1
        year -= 1
    elif last_date_tail <= "06-30":
        quarter = 2
    elif last_date_tail <= "09-30":
        quarter = 3
    job = FinanceData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    if last_date:
        executed_code_list = FinanceDocument.objects(statDate__gte=last_date).distinct("code")
        code_list = list(set(code_list).difference(executed_code_list))
    bar = tqdm(code_list)
    for code in bar:
        data = job.get_by_code_and_quarter(code, year, quarter)
        if data:
            bulk = [UpdateOne(
                {
                    "code": data["code"],
                    "statDate": data["statDate"]
                },
                {
                    "$set": data
                },
                upsert=True
            )]
            # noinspection PyProtectedMember
            FinanceDocument._get_collection().bulk_write(bulk, ordered=False)
        bar.set_description(f"{code} flush {len(data)} records.")
    disconnect()


def flush_secretary_task(last_date: str = None, only_one=True):
    """
    刷新董秘数据
    :param only_one: 是否只刷新第一页
    :param last_date: 最后一个报告期对应日期
    :return:
    """
    job = SecretaryData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    if last_date:
        executed_code_list = SecretaryDocument.objects(cTime__gte=last_date).distinct("code")
        code_list = list(set(code_list).difference(executed_code_list))
    bar = tqdm(code_list)
    for code in bar:
        data = []
        data_dict = job.get_by_code(code)
        if not data_dict:
            continue
        pages = data_dict["pages"]
        if data_dict["data"]:
            data.extend(data_dict["data"])
        if not only_one:
            for i in range(2, pages):
                data_dict = job.get_by_code(code, page=i)
                if data_dict["data"]:
                    data.extend(data_dict["data"])
        # noinspection PyProtectedMember
        bulk = [UpdateOne(
            {
                "url": row.url
            },
            {
                "$set": row._asdict()
            },
            upsert=True
        )
            for row in data]
        if bulk:
            # noinspection PyProtectedMember
            SecretaryDocument._get_collection().bulk_write(bulk, ordered=False)
        bar.set_description(f"{code} flush {len(bulk)} records.")
    disconnect()


def flush_market_task(start_date: str = None, end_date: str = None):
    """
    刷新交易数据
    :param start_date:
    :param end_date:
    :return:
    """
    start_date = start_date if start_date else yesterday
    end_date = end_date if end_date else yesterday
    job = MarketData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    # executed_code_list = MarketDocument.objects(date__gte=end_date).distinct("code")
    # code_list = list(set(code_list).difference(executed_code_list))
    bar = tqdm(code_list)
    for code in bar:
        data = job.get_by_code_and_date(code, start_date, end_date)
        if data:
            bulk = [UpdateOne(
                {
                    "code": row["code"],
                    "date": row["date"]
                },
                {
                    "$set": row
                },
                upsert=True
            ) for row in data]
            # noinspection PyProtectedMember
            MarketDocument._get_collection().bulk_write(bulk, ordered=False)
        bar.set_description(f"{code} flush {len(data)} records.")
    disconnect()


def flush_pbc_task(page: int = 1):
    """
    刷新央行公告
    :param page:
    :return:
    """
    job = PbcData()
    connect('jiucai', host='master:17585')
    data = job.get_by_page(page)
    if data:
        # noinspection PyProtectedMember
        bulk = [UpdateOne(
            {
                "url": row.url,
            },
            {
                "$set": row._asdict()
            },
            upsert=True
        ) for row in data]
        # noinspection PyProtectedMember
        PbcDocument._get_collection().bulk_write(bulk, ordered=False)


if __name__ == '__main__':
    flush_pbc_task()
