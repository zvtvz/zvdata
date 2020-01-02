# -*- coding: utf-8 -*-
import enum
import inspect
import math
from typing import List, Union

import pandas as pd
from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import Session


class IntervalLevel(enum.Enum):
    LEVEL_TICK = 'tick'
    LEVEL_1MIN = '1m'
    LEVEL_5MIN = '5m'
    LEVEL_15MIN = '15m'
    LEVEL_30MIN = '30m'
    LEVEL_1HOUR = '1h'
    LEVEL_4HOUR = '4h'
    LEVEL_1DAY = '1d'
    LEVEL_1WEEK = '1wk'
    LEVEL_1MON = '1mon'

    def to_pd_freq(self):
        if self == IntervalLevel.LEVEL_1MIN:
            return '1min'
        if self == IntervalLevel.LEVEL_5MIN:
            return '5min'
        if self == IntervalLevel.LEVEL_15MIN:
            return '15min'
        if self == IntervalLevel.LEVEL_30MIN:
            return '30min'
        if self == IntervalLevel.LEVEL_1HOUR:
            return '1H'
        if self == IntervalLevel.LEVEL_4HOUR:
            return '4H'
        if self >= IntervalLevel.LEVEL_1DAY:
            return '1D'

    def count_from_timestamp(self, pd_timestamp, one_day_trading_minutes):
        current_time = pd.Timestamp.now()
        time_delta = current_time - pd_timestamp

        one_day_trading_seconds = one_day_trading_minutes * 60

        if self == IntervalLevel.LEVEL_1DAY:
            return None, time_delta.days

        if self == IntervalLevel.LEVEL_1WEEK:
            return None, int(math.ceil(time_delta.days / 7)) + 1

        if self == IntervalLevel.LEVEL_1MON:
            return None, int(math.ceil(time_delta.days / 30)) + 1

        if time_delta.days > 0:
            seconds = (time_delta.days + 1) * one_day_trading_seconds
            return None, int(math.ceil(seconds / self.to_second())) + 1
        else:
            seconds = time_delta.total_seconds()
            return self.to_second() - seconds, min(int(math.ceil(seconds / self.to_second())) + 1,
                                                   one_day_trading_seconds / self.to_second())

    def floor_timestamp(self, pd_timestamp):
        if self == IntervalLevel.LEVEL_1MIN:
            return pd_timestamp.floor('1min')
        if self == IntervalLevel.LEVEL_5MIN:
            return pd_timestamp.floor('5min')
        if self == IntervalLevel.LEVEL_15MIN:
            return pd_timestamp.floor('15min')
        if self == IntervalLevel.LEVEL_30MIN:
            return pd_timestamp.floor('30min')
        if self == IntervalLevel.LEVEL_1HOUR:
            return pd_timestamp.floor('1h')
        if self == IntervalLevel.LEVEL_4HOUR:
            return pd_timestamp.floor('4h')
        if self == IntervalLevel.LEVEL_1DAY:
            return pd_timestamp.floor('1d')

    def to_minute(self):
        return int(self.to_second() / 60)

    def to_second(self):
        return int(self.to_ms() / 1000)

    def to_ms(self):
        # we treat tick intervals is 5s, you could change it
        if self == IntervalLevel.LEVEL_TICK:
            return 5 * 1000
        if self == IntervalLevel.LEVEL_1MIN:
            return 60 * 1000
        if self == IntervalLevel.LEVEL_5MIN:
            return 5 * 60 * 1000
        if self == IntervalLevel.LEVEL_15MIN:
            return 15 * 60 * 1000
        if self == IntervalLevel.LEVEL_30MIN:
            return 30 * 60 * 1000
        if self == IntervalLevel.LEVEL_1HOUR:
            return 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_4HOUR:
            return 4 * 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_1DAY:
            return 24 * 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_1WEEK:
            return 7 * 24 * 60 * 60 * 1000
        if self == IntervalLevel.LEVEL_1MON:
            return 31 * 7 * 24 * 60 * 60 * 1000

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.to_ms() >= other.to_ms()
        return NotImplemented

    def __gt__(self, other):

        if self.__class__ is other.__class__:
            return self.to_ms() > other.to_ms()
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.to_ms() <= other.to_ms()
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.to_ms() < other.to_ms()
        return NotImplemented


class Mixin(object):
    id = Column(String, primary_key=True)
    # entity id for this model
    entity_id = Column(String)

    # the meaning could be different for different case,most of time it means 'happen time'
    timestamp = Column(DateTime)

    @classmethod
    def help(cls):
        print(inspect.getsource(cls))

    @classmethod
    def important_cols(cls):
        return []

    @classmethod
    def time_field(cls):
        return 'timestamp'

    @classmethod
    def register_recorder_cls(cls, recorder_cls):
        # make sure recorder_classes is created for every class
        if not hasattr(cls, 'recorders'):
            cls.recorders = []
        if recorder_cls not in cls.recorders:
            cls.recorders.append(recorder_cls)

    @classmethod
    def register_provider(cls, provider):
        if not hasattr(cls, 'providers'):
            cls.providers = []
        if provider not in cls.providers:
            cls.providers.append(provider)

    @classmethod
    def query_data(cls,
                   provider_index: int = 0,
                   ids: List[str] = None,
                   entity_ids: List[str] = None,
                   entity_id: str = None,
                   codes: List[str] = None,
                   code: str = None,
                   level: Union[IntervalLevel, str] = None,
                   provider: str = None,
                   columns: List = None,
                   return_type: str = 'df',
                   start_timestamp: Union[pd.Timestamp, str] = None,
                   end_timestamp: Union[pd.Timestamp, str] = None,
                   filters: List = None,
                   session: Session = None,
                   order=None,
                   limit: int = None,
                   index: Union[str, list] = None,
                   time_field: str = 'timestamp'):
        from .api import get_data
        if not provider:
            provider = cls.providers[provider_index]
        return get_data(data_schema=cls, ids=ids, entity_ids=entity_ids, entity_id=entity_id, codes=codes,
                        code=code, level=level, provider=provider, columns=columns, return_type=return_type,
                        start_timestamp=start_timestamp, end_timestamp=end_timestamp, filters=filters, session=session,
                        order=order, limit=limit, index=index, time_field=time_field)

    @classmethod
    def record_data(cls,
                    recorder_index: int = 0,
                    exchanges=None,
                    entity_ids=None,
                    codes=None,
                    batch_size=10,
                    force_update=False,
                    sleeping_time=5,
                    default_size=2000,
                    real_time=False,
                    fix_duplicate_way='add',
                    start_timestamp=None,
                    end_timestamp=None,
                    close_hour=0,
                    close_minute=0):
        if hasattr(cls, 'recorders') and cls.recorders:
            recorder_class = cls.recorders[recorder_index]
            print(f'{cls.__name__} registered recorders:{cls.recorders}')

            # FixedCycleDataRecorder
            from zvdata.recorder import FixedCycleDataRecorder
            if issubclass(recorder_class, FixedCycleDataRecorder):
                # contract:
                # 1)use FixedCycleDataRecorder to record the data with IntervalLevel
                # 2)the table of schema with IntervalLevel format is {entity}_{level}_{event}
                table: str = cls.__tablename__
                level = IntervalLevel(table.split('_')[1])
                r = recorder_class(exchanges=exchanges, entity_ids=entity_ids, codes=codes, batch_size=batch_size,
                                   force_update=force_update, sleeping_time=sleeping_time, default_size=default_size,
                                   real_time=real_time, fix_duplicate_way=fix_duplicate_way,
                                   start_timestamp=start_timestamp, end_timestamp=end_timestamp, close_hour=close_hour,
                                   close_minute=close_minute, level=level)
                r.run()
                return

            # TimeSeriesDataRecorder
            from zvdata.recorder import TimeSeriesDataRecorder
            if issubclass(recorder_class, TimeSeriesDataRecorder):
                r = recorder_class(exchanges=exchanges, entity_ids=entity_ids, codes=codes, batch_size=batch_size,
                                   force_update=force_update, sleeping_time=sleeping_time, default_size=default_size,
                                   real_time=real_time, fix_duplicate_way=fix_duplicate_way,
                                   start_timestamp=start_timestamp, end_timestamp=end_timestamp, close_hour=close_hour,
                                   close_minute=close_minute)
                r.run()
                return

            # Recorder
            from zvdata.recorder import Recorder
            if issubclass(recorder_class, Recorder):
                r = recorder_class(batch_size=batch_size,
                                   force_update=force_update,
                                   sleeping_time=sleeping_time)
                r.run()
                return
        else:
            print(f'no recorders for {cls.__name__}')


class NormalMixin(Mixin):
    # the record created time in db
    created_timestamp = Column(DateTime, default=pd.Timestamp.now())
    # the record updated time in db, some recorder would check it for whether need to refresh
    updated_timestamp = Column(DateTime)


class EntityMixin(Mixin):
    entity_type = Column(String(length=64))
    exchange = Column(String(length=32))
    code = Column(String(length=64))
    name = Column(String(length=128))


class NormalEntityMixin(EntityMixin):
    # the record created time in db
    created_timestamp = Column(DateTime, default=pd.Timestamp.now())
    # the record updated time in db, some recorder would check it for whether need to refresh
    updated_timestamp = Column(DateTime)
