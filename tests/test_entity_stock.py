# -*- coding: utf-8 -*-
import io
import os

import pandas as pd
import requests
from sqlalchemy import Column, DateTime
from sqlalchemy.ext.declarative import declarative_base

from tests.consts import DEFAULT_SH_HEADER, DEFAULT_SZ_HEADER
from zvdata.api import init_entities, get_entities, get_data
from zvdata.domain import EntityMixin, register_schema, init_context, register_api, generate_api
from zvdata.recorder import Recorder
from zvdata.utils.time_utils import to_pd_timestamp

# init the context at first
DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasample'))
init_context(data_path=DATA_PATH, domain_module='tests.test_entity_stock')

# define the db
MetaBase = declarative_base()

api_tmp_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))


# define the schema
@register_api(provider='sina', api_dir=api_tmp_path)
@register_schema(providers=['eastmoney', 'sina'], db_name='meta', schema_base=MetaBase, entity_type='stock')
class Stock(MetaBase, EntityMixin):
    __tablename__ = 'stocks'
    # 上市日期
    list_date = Column(DateTime)


# write the recorder
class ChinaStockListSpider(Recorder):
    data_schema = Stock

    def __init__(self, batch_size=10, force_update=False, sleeping_time=10, provider='sina') -> None:
        self.provider = provider
        super().__init__(batch_size, force_update, sleeping_time)

    def run(self):
        url = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1'

        resp = requests.get(url, headers=DEFAULT_SH_HEADER)
        self.download_stock_list(response=resp, exchange='sh')

        url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.20932135244582617'

        resp = requests.get(url, headers=DEFAULT_SZ_HEADER)
        self.download_stock_list(response=resp, exchange='sz')

    def download_stock_list(self, response, exchange):
        df = None
        if exchange == 'sh':
            df = pd.read_csv(io.BytesIO(response.content), sep='\s+', encoding='GB2312', dtype=str,
                             parse_dates=['上市日期'])
            if df is not None:
                df = df.loc[:, ['公司代码', '公司简称', '上市日期']]

        elif exchange == 'sz':
            df = pd.read_excel(io.BytesIO(response.content), sheet_name='A股列表', dtype=str, parse_dates=['A股上市日期'])
            if df is not None:
                df = df.loc[:, ['A股代码', 'A股简称', 'A股上市日期']]

        if df is not None:
            df.columns = ['code', 'name', 'list_date']

            df = df.dropna(subset=['code'])

            # handle the dirty data
            # 600996,贵广网络,2016-12-26,2016-12-26,sh,stock,stock_sh_600996,,次新股,贵州,,
            df.loc[df['code'] == '600996', 'list_date'] = '2016-12-26'
            print(df[df['list_date'] == '-'])
            df['list_date'] = df['list_date'].apply(lambda x: to_pd_timestamp(x))
            df['exchange'] = exchange
            df['entity_type'] = 'stock'
            df['id'] = df[['entity_type', 'exchange', 'code']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
            df['entity_id'] = df['id']
            df['timestamp'] = df['list_date']
            df = df.dropna(axis=0, how='any')
            df = df.drop_duplicates(subset=('id'), keep='last')
            init_entities(df, provider=self.provider)


# ChinaStockListSpider().run()


def test_get_entities():
    df = get_entities(entity_type='stock', provider='sina')
    assert '000001' in df.index
    assert '12345' not in df.index


def test_get_data():
    df = get_data(data_schema=Stock, entity_ids=['stock_sz_000338', 'stock_sz_000778'], provider='sina')
    assert len(df) == 2

    df = get_data(data_schema=Stock, codes=['000338', '000778'], provider='sina')
    assert len(df) == 2

    df = get_data(data_schema=Stock, start_timestamp='2019-01-01', provider='sina')
    print(f'2019 list count:{len(df.index)}')

    df = get_data(data_schema=Stock, end_timestamp='2018-12-31', provider='sina')
    print(f'from start to 2019 list count:{len(df.index)}')

    df = get_data(data_schema=Stock, end_timestamp='2018-12-31', limit=10, provider='sina')
    assert len(df) == 10


def test_generate_api():
    generate_api(api_tmp_path, api_tmp_path)
    exec('from tests.api import get_stocks')
    get_stocks1 = eval('get_stocks')
    df = get_stocks1(limit=10)
    assert len(df) == 10
