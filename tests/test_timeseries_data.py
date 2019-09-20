# -*- coding: utf-8 -*-
import io

import pandas as pd
import requests

from tests.domain import *
from zvdata import IntervalLevel
from zvdata.recorder import FixedCycleDataRecorder
from zvdata.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601, \
    TIME_FORMAT_DAY1, now_time_str
# init the context at first
from zvdata.utils.utils import read_csv


def generate_kdata_id(entity_id, timestamp, level):
    if level == IntervalLevel.LEVEL_1DAY:
        return "{}_{}".format(entity_id, to_time_str(timestamp, fmt=TIME_FORMAT_DAY))
    else:
        return "{}_{}".format(entity_id, to_time_str(timestamp, fmt=TIME_FORMAT_ISO8601))


class ChinaStockDayKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'sina'
    entity_schema = Stock

    provider = 'netease'
    data_schema = Stock1dKdata
    url = 'http://quotes.money.163.com/service/chddata.html?code={}{}&start={}&end={}&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER'

    def __init__(self, entity_type='stock', exchanges=['sh', 'sz'], entity_ids=None, codes=None, batch_size=10,
                 force_update=True, sleeping_time=10, default_size=2000, real_time=False, fix_duplicate_way='ignore',
                 start_timestamp=None, end_timestamp=None, level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False,
                 close_hour=0, close_minute=0, one_day_trading_minutes=24 * 60) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, level,
                         kdata_use_begin_time, close_hour, close_minute, one_day_trading_minutes)

    def get_data_map(self):
        return {}

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def generate_request_param(self, security_item, start, end, size, timestamp):
        return {
            'security_item': security_item,
            'start': to_time_str(start, fmt=TIME_FORMAT_DAY1),
            'end': now_time_str(fmt=TIME_FORMAT_DAY1),
            'level': self.level.value
        }

    def record(self, entity, start, end, size, timestamps):

        start = to_time_str(start, fmt=TIME_FORMAT_DAY1)
        end = now_time_str(fmt=TIME_FORMAT_DAY1)

        if entity.exchange == 'sh':
            exchange_flag = 0
        else:
            exchange_flag = 1

        url = self.url.format(exchange_flag, entity.code, start, end)
        response = requests.get(url=url)

        df = read_csv(io.BytesIO(response.content), encoding='GB2312', na_values='None')

        if df is None:
            return []

        df['name'] = entity.name
        # 指数数据
        if entity.entity_type == 'index':
            df = df.loc[:,
                 ['日期', 'name', '最低价', '开盘价', '收盘价', '最高价', '成交量', '成交金额', '涨跌幅']]
            df.columns = ['timestamp', 'name', 'low', 'open', 'close', 'high', 'volume', 'turnover', 'change_pct']
        # 股票数据
        else:
            df = df.loc[:,
                 ['日期', 'name', '最低价', '开盘价', '收盘价', '最高价', '成交量', '成交金额', '涨跌幅', '换手率']]
            df.columns = ['timestamp', 'name', 'low', 'open', 'close', 'high', 'volume', 'turnover', 'change_pct',
                          'turnover_rate']
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['provider'] = 'netease'
        df['level'] = self.level.value

        return df.to_dict(orient='records')


def test_recorder():
    try:
        recorder = ChinaStockDayKdataRecorder(codes=['000338'])
        # recorder.run()
    except:
        assert False
