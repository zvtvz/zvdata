# -*- coding: utf-8 -*-

import os

from sqlalchemy import Column, DateTime, String, Float
from sqlalchemy.ext.declarative import declarative_base

from zvdata import Mixin
from zvdata.contract import EntityMixin, register_schema, register_api, register_entity, \
    domain_name_to_table_name, init_data_env

DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasample'))
LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
init_data_env(data_path=DATA_PATH, domain_module='tests.domain')

# define the db
MetaBase = declarative_base()

api_tmp_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))


# define the schema
@register_api(provider='sina')
@register_entity(entity_type='stock')
class Stock(MetaBase, EntityMixin):
    __tablename__ = domain_name_to_table_name('Stock')
    # 上市日期
    list_date = Column(DateTime)


register_schema(providers=['eastmoney', 'sina'], db_name='meta', schema_base=MetaBase)

# define the db
MetaBase = declarative_base()

api_tmp_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))


class StockKdataCommon(Mixin):
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))
    # level = Column(Enum(TradingLevel, values_callable=enum_value))
    level = Column(String(length=32))

    open = Column(Float)
    hfq_open = Column(Float)
    qfq_open = Column(Float)
    close = Column(Float)
    hfq_close = Column(Float)
    qfq_close = Column(Float)
    high = Column(Float)
    hfq_high = Column(Float)
    qfq_high = Column(Float)
    low = Column(Float)
    hfq_low = Column(Float)
    qfq_low = Column(Float)
    volume = Column(Float)
    turnover = Column(Float)
    change_pct = Column(Float)
    turnover_rate = Column(Float)
    factor = Column(Float)


Stock1DKdataBase = declarative_base()


class Stock1dKdata(Stock1DKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1d_kdata'


register_schema(providers=['netease'], db_name='stock_1d_kdata', schema_base=Stock1DKdataBase)
