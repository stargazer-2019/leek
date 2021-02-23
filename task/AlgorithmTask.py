from typing import List

from mongoengine import connect, disconnect

from domain.Documents import MarketDocument


def seek():
    connect('jiucai', host='master:17585')
    data: List[MarketDocument] = MarketDocument.objects(code="600330", date__gte="2019-01-01", date__lte="2019-02-01")
    disconnect()
    print(len(data))
    print(data[0].to_json())


def alg():
    pass


if __name__ == '__main__':
    seek()
