
# -*- coding: utf-8 -*-
from typing import List, Union

import pandas as pd
from sqlalchemy.orm import Session
from zvdata.api import get_data
from zvdata.structs import IntervalLevel

from tests.domain import Stock

def get_stocks(
        entity_ids: List[str] = None,
        entity_id: str = None,
        codes: List[str] = None,
        level: Union[IntervalLevel, str] = None,
        provider: str = 'sina',
        columns: List = None,
        return_type: str = 'df',
        start_timestamp: Union[pd.Timestamp, str] = None,
        end_timestamp: Union[pd.Timestamp, str] = None,
        filters: List = None,
        session: Session = None,
        order=None,
        limit: int = None,
        index: str = 'timestamp',
        index_is_time: bool = True,
        time_field: str = 'timestamp'):
    return get_data(data_schema=Stock, entity_ids=entity_ids, entity_id=entity_id, codes=codes, level=level,
                    provider=provider,
                    columns=columns, return_type=return_type, start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp, filters=filters, session=session, order=order, limit=limit,
                    index=index, index_is_time=index_is_time, time_field=time_field)

