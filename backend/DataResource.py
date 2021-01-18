import datetime
import json
import re
import traceback
from collections import namedtuple

import requests

import baostock as bs
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.phantomjs.webdriver import WebDriver
from selenium.webdriver.phantomjs import webdriver


class AdjData(object):
    entity = namedtuple("Adj", ["trade_date", "adj_score", "code"])
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }

    def get_by_code(self, code: str) -> list:
        note_patten = re.compile("\/\*(.*)+\*\/")
        data = []
        new_code = ("sh" if code.startswith("6") else "sz") + code
        url = f"http://finance.sina.com.cn/realstock/company/{new_code}/qianfuquan.js"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            r.encoding = "gbk"
            text = note_patten.sub("", r.text).strip()
            text = re.sub("\[{total:\d+,data:", "", text)
            text = re.sub("}]", "", text)
            if text != "{}":
                text = re.sub("{", "{\"", text)
                text = re.sub(",", ",\"", text)
                text = re.sub(":", "\":", text)
                data = [
                    self.entity(trade_date=datetime.datetime.strftime(
                        datetime.datetime.strptime(k.replace("_", ""), "%Y%m%d"), "%Y-%m-%d"),
                        adj_score=float(v),
                        code=code)
                    for k, v in json.loads(text).items()]
        except (requests.Timeout, json.decoder.JSONDecodeError, TypeError):
            traceback.print_exc()
        return data


class FinanceData(object):

    def __init__(self):
        bs.login()
        self.bs = bs

    def get_by_code_and_quarter(self, code: str, year: int, quarter: int) -> dict:
        data = {}
        new_code = ("sh." if code.startswith("6") else "sz.") + code
        for rs in [
            self.bs.query_profit_data(code=new_code, year=year, quarter=quarter),
            self.bs.query_operation_data(code=new_code, year=year, quarter=quarter),
            self.bs.query_growth_data(code=new_code, year=year, quarter=quarter),
            self.bs.query_balance_data(code=new_code, year=year, quarter=quarter),
            self.bs.query_cash_flow_data(code=new_code, year=year, quarter=quarter),
            self.bs.query_dupont_data(code=new_code, year=year, quarter=quarter),
        ]:
            if rs:
                df = rs.get_data()
                if df is not None and len(df) > 0:
                    data.update(**df.to_dict(orient="record")[0])
        if data:
            data["code"] = code
        return data


class HolderData(object):
    entity = namedtuple("Holder", ["code", "publish_date", "holder", "ratio", "trend"])
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }

    def get_by_code(self, code: str) -> list:
        url = f"http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/{code}.phtml"
        result = []
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            r.encoding = "gbk"
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.select("table[id=Table1]")
            if table:
                table = table[0]
                tr = table.select("tr")
                publish_date = ""
                start_insert_holder = False
                # holder ratio
                holders = []
                for tr1 in tr:
                    td = [td1.text.strip() for td1 in tr1.select("td")]
                    if not td:
                        continue
                    if td[0] == "截至日期":
                        start_insert_holder = False
                        if publish_date and holders:
                            result.extend([(publish_date, holder, ratio) for holder, ratio in holders])
                        holders.clear()
                        publish_date = td[1]
                    elif td[0] == "编号":
                        start_insert_holder = True
                    else:
                        if start_insert_holder and len(td) >= 4:
                            holders.append((td[1], td[3]))
                if publish_date and holders:
                    result.extend([(publish_date, holder, ratio) for holder, ratio in holders])
        except (requests.Timeout, requests.exceptions.ReadTimeout):
            traceback.print_exc()
        result = [
            self.entity(code=code,
                        publish_date=datetime.datetime.strftime(
                            datetime.datetime.strptime(trade_date.replace("-", ""), "%Y%m%d"), "%Y-%m-%d"),
                        holder=holder,
                        ratio=float(ratio.rstrip("↓↑")),
                        trend=1 if str(ratio).endswith("↑") else (-1 if str(ratio).endswith("↓") else 0))
            for trade_date, holder, ratio in result]
        return result


class MarketData(object):

    def __init__(self):
        bs.login()
        self.bs = bs

    def get_by_code_and_date(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        new_code = ("sh." if code.startswith("6") else "sz.") + code
        rs = self.bs.query_history_k_data_plus(
            new_code,
            "date,code,open,close,high,low,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn",
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="3")
        if rs:
            df: pd.DataFrame = rs.get_data()
            if df is not None and len(df) > 0:
                df["code"] = df.code.apply(lambda x: x.lstrip("zsh."))
            return df
        else:
            return pd.DataFrame([])


class SecretaryData(object):
    entity = namedtuple("Secretary", ["time", "total", "pages", "codeName", "code", "data"])
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }

    def get_by_code(self, code: str = None, page: int = 1, num: int = 50) -> dict:
        code = "sh" + code if code.startswith("600") else "sz" + code
        url = f"https://interface.sina.cn/finance/column/stock/dongmiqa.d.json?stock={code}&page={page}&num={num}"
        r = requests.get(url, headers=self.headers)
        return r.json()


class HolderNumData(object):
    entity = namedtuple("HolderNum", ["code", "publish_date", "num", "value"])
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE7.0; WindowsNT5.1; Maxthon2.0)"
    }
    browser: WebDriver = None
    n = 0
    retry_num = 3

    def __init__(self):
        self.start()

    def start(self):
        self.browser = webdriver.WebDriver()

    def stop(self):
        if self.browser is not None:
            self.browser.close()
            self.browser.quit()
        self.n = 0

    def __del__(self):
        self.stop()

    def get_by_code(self, code: str) -> list:
        data = []
        for i in range(self.retry_num):
            try:
                data = self._get_by_code(code)
                break
            except WebDriverException:
                traceback.print_exc()
        return data

    @staticmethod
    def _parse_num(num: str) -> float:
        _num = num.replace(",", "")
        _v = -1
        if _num.endswith("万"):
            _num = _num.replace("万", "")
            try:
                _v = float(_num) * 10000
            except ValueError:
                pass
        else:
            try:
                _v = float(_num)
            except ValueError:
                pass
        return _v

    def _get_by_code(self, code: str) -> list:
        if self.n >= 2:
            self.stop()
            self.start()
        self.n += 1
        new_code = "sh" + code if code.startswith("600") else "sz" + code
        url = f"http://f10.eastmoney.com/f10_v2/ShareholderResearch.aspx?code={new_code}"
        self.browser.get(url)
        key_list = []
        value_list = []
        table = self.browser.find_element_by_id("Table0")
        if table:
            tr_list = table.find_elements_by_tag_name("tr")
            for tr in tr_list:
                if tr.text.startswith("20"):
                    key_list = tr.text.split()
                elif tr.text.startswith("股东人数"):
                    value_list = tr.text.split()[1:]
        result = []
        if len(key_list) == len(value_list):
            result = [self.entity(code=code,
                                  publish_date=k,
                                  num=v,
                                  value=self._parse_num(v))
                      for k, v in zip(key_list, value_list)]
        return result


if __name__ == '__main__':
    print(HolderNumData().get_by_code("600690"))
