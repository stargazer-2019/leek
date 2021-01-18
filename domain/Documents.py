import datetime

from mongoengine import Document, StringField, FloatField, IntField, connect, disconnect_all, DateTimeField


class HolderDocument(Document):
    code = StringField(max_length=10, required=True)
    publish_date = StringField(max_length=20, required=True)
    holder = StringField(max_length=50, required=True)
    ratio = FloatField(min_value=0, max_value=100, required=True)
    trend = IntField(min_value=-1, max_value=1)
    create_time = StringField(required=True, default=datetime.datetime.now().strftime("%Y-%m-%d %X"))


class ShareDocument(Document):
    code = StringField(max_length=10, unique=True, required=True)
    name = StringField(max_length=20, required=True)
    create_time = StringField(required=True, default=datetime.datetime.now().strftime("%Y-%m-%d %X"))


class AdjDocument(Document):
    code = StringField(max_length=10, required=True)
    trade_date = StringField(max_length=20, required=True)
    adj_score = FloatField(required=True)


class MarketDocument(Document):
    code = StringField(max_length=10, required=True)
    date = StringField(max_length=20, required=True)
    open = FloatField(required=True)
    close = FloatField(required=True)
    high = FloatField(required=True)
    low = FloatField(required=True)
    peTTM = FloatField(required=True)
    pbMRQ = FloatField(required=True)
    psTTM = FloatField(required=True)
    pcfNcfTTM = FloatField(required=True)
    turn = FloatField(required=True)


class FinanceDocument(Document):
    code = StringField(max_length=20, required=True)
    pubDate = StringField(max_length=20, required=False)
    statDate = StringField(max_length=20, required=True)
    roeAvg = StringField(max_length=20, required=False)
    npMargin = StringField(max_length=20, required=False)
    gpMargin = StringField(max_length=20, required=False)
    netProfit = StringField(max_length=20, required=False)
    epsTTM = StringField(max_length=20, required=False)
    MBRevenue = StringField(max_length=20, required=False)
    totalShare = StringField(max_length=20, required=False)
    liqaShare = StringField(max_length=20, required=False)
    NRTurnRatio = StringField(max_length=20, required=False)
    NRTurnDays = StringField(max_length=20, required=False)
    INVTurnRatio = StringField(max_length=20, required=False)
    INVTurnDays = StringField(max_length=20, required=False)
    CATurnRatio = StringField(max_length=20, required=False)
    AssetTurnRatio = StringField(max_length=20, required=False)
    YOYEquity = StringField(max_length=20, required=False)
    YOYAsset = StringField(max_length=20, required=False)
    YOYNI = StringField(max_length=20, required=False)
    YOYEPSBasic = StringField(max_length=20, required=False)
    YOYPNI = StringField(max_length=20, required=False)
    currentRatio = StringField(max_length=20, required=False)
    quickRatio = StringField(max_length=20, required=False)
    cashRatio = StringField(max_length=20, required=False)
    YOYLiability = StringField(max_length=20, required=False)
    liabilityToAsset = StringField(max_length=20, required=False)
    assetToEquity = StringField(max_length=20, required=False)
    CAToAsset = StringField(max_length=20, required=False)
    NCAToAsset = StringField(max_length=20, required=False)
    tangibleAssetToAsset = StringField(max_length=20, required=False)
    ebitToInterest = StringField(max_length=20, required=False)
    CFOToOR = StringField(max_length=20, required=False)
    CFOToNP = StringField(max_length=20, required=False)
    CFOToGr = StringField(max_length=20, required=False)
    dupontROE = StringField(max_length=20, required=False)
    dupontAssetStoEquity = StringField(max_length=20, required=False)
    dupontAssetTurn = StringField(max_length=20, required=False)
    dupontPnitoni = StringField(max_length=20, required=False)
    dupontNitogr = StringField(max_length=20, required=False)
    dupontTaxBurden = StringField(max_length=20, required=False)
    dupontIntburden = StringField(max_length=20, required=False)
    dupontEbittogr = StringField(max_length=20, required=False)


class SecretaryDocument(Document):
    estockQuestion = StringField(max_length=10000, required=False)
    estockAnswer = StringField(max_length=10000, required=False)
    url = StringField(max_length=200, required=False)
    stockCode = StringField(max_length=20, required=True)
    stockMarket = StringField(max_length=20, required=False)
    cTime = StringField(max_length=20, required=False)
    qTime = StringField(max_length=20, required=False)
    questioner = StringField(max_length=50, required=False)
    answerer = StringField(max_length=20, required=False)
    stockName = StringField(max_length=20, required=False)
    crawlFromURL = StringField(max_length=200, required=True)
    source = StringField(max_length=20, required=False)


class PbcDocument(Document):
    title = StringField(max_length=100, required=True)
    date = StringField(max_length=20, required=True)
    days = StringField(required=True)
    amount = StringField(required=True)
    rate = StringField(required=True)
    url = StringField(max_length=300, required=True)
    content = StringField(max_length=10000, required=True)


class HolderNumDocument(Document):
    code = StringField(max_length=20, required=True)
    publish_date = StringField(max_length=20, required=False)
    num = StringField(max_length=20, required=True)
    value = FloatField(required=False)


if __name__ == '__main__':
    connect('jiucai', host='master:17585')
    HolderDocument.objects(code="600000").delete()
    disconnect_all()
