from mongoengine import connect
from pymongo import UpdateOne
from tqdm import tqdm

from backend.DataResource import HolderNumData
from domain.Documents import HolderNumDocument, ShareDocument


def flush_holder_num_task(last_date: str = None):
    job = HolderNumData()
    connect('jiucai', host='master:17585')
    code_list = [obj.code for obj in ShareDocument.objects().all()]
    if last_date:
        executed_code_list = [obj.code for obj in HolderNumDocument.objects(publish_date=last_date).all()]
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


if __name__ == '__main__':
    flush_holder_num_task("2020-09-30")
