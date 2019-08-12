# -*- coding: utf-8 -*-
import enum
from typing import List

import numpy as np
import pandas as pd

from zvdata.utils.pd_utils import df_is_not_null, fill_with_same_index, index_df_with_category_xfield


class TableType(enum.Enum):
    single_single_single = 'single_single_single'
    single_single_multiple = 'single_single_multiple'
    single_multiple_single = 'single_multiple_single'
    single_multiple_multiple = 'single_multiple_multiple'

    multiple_single_single = 'multiple_single_single'
    multiple_single_multiple = 'multiple_single_multiple'
    multiple_multiple_single = 'multiple_multiple_single'
    multiple_multiple_multiple = 'multiple_multiple_multiple'


class NormalData(object):
    table_type_sample = None

    def __init__(self, df, annotation_df=None, category_field='entity_id', index_field='timestamp',
                 is_timeseries: bool = True) -> None:
        self.data_df = df
        self.annotation_df: pd.DataFrame = annotation_df
        self.category_field = category_field
        self.index_field = index_field
        self.is_timeseries = is_timeseries

        self.entity_ids = []
        self.df_list = []
        self.entity_map_df = {}

        self.normalize()

    def is_normalized(self):
        if df_is_not_null(self.data_df):
            names = self.data_df.index.names

            # it has been normalized
            if len(names) == 1 and names[0] == self.category_field:
                return True

            if len(names) == 2 and names[0] == self.category_field and names[1] == self.index_field:
                return True

        return False

    def normalize(self):
        """
        normalize data_df to
                                    col1    col2    col3
        entity_id    index_field

        """
        if df_is_not_null(self.data_df):
            if not self.is_normalized():
                names = self.data_df.index.names
                for level, name in enumerate(names):
                    if name in self.data_df.columns.tolist():
                        self.data_df = self.data_df.reset_index(level=level, drop=True)
                    else:
                        self.data_df = self.data_df.reset_index(level=level)

                self.data_df = index_df_with_category_xfield(self.data_df, category_field=self.category_field,
                                                             xfield=self.index_field, is_timeseries=self.is_timeseries)

            if isinstance(self.data_df.index, pd.MultiIndex):
                self.entity_ids = list(self.data_df.index.get_level_values(0).values)
            else:
                self.entity_ids = list(self.data_df.index.values)

            for entity_id, df_item in self.data_df.groupby(self.category_field):
                df = df_item.copy()
                df.reset_index(inplace=True, level=self.category_field)
                self.df_list.append(df)

            if len(self.df_list) > 1:
                self.df_list = fill_with_same_index(df_list=self.df_list)

            for df in self.df_list:
                entity_id = df[df[self.category_field].notna()][self.category_field][0]
                columns = list(df.columns)
                columns.remove(self.category_field)
                self.entity_map_df[entity_id] = df.loc[:, columns]

    def get_table_type(self):
        entity_size = len(self.entity_ids)
        row_count = int(len(self.data_df) / entity_size)
        column_size = len(self.data_df.columns)

        if entity_size == 1:
            a = 'single'
        else:
            a = 'multiple'

        if row_count == 1:
            b = 'single'
        else:
            b = 'multiple'

        if column_size == 1:
            c = 'single'
        else:
            c = 'multiple'

        return f'{a}_{b}_{c}'

    @staticmethod
    def sample(table_type: TableType = TableType.multiple_multiple_single):

        if NormalData.table_type_sample is None:
            NormalData.table_type_sample = {
                TableType.single_single_single: NormalData._sample(entity_ids=['jack'], row_size=1,
                                                                   columns=['score']),
                TableType.single_single_multiple: NormalData._sample(entity_ids=['jack'], row_size=1),
                TableType.single_multiple_single: NormalData._sample(entity_ids=['jack'], columns=['score']),
                TableType.single_multiple_multiple: NormalData._sample(entity_ids=['jack']),

                TableType.multiple_single_single: NormalData._sample(row_size=1, columns=['math']),
                TableType.multiple_single_multiple: NormalData._sample(row_size=1),
                TableType.multiple_multiple_single: NormalData._sample(columns=['math']),
                TableType.multiple_multiple_multiple: NormalData._sample()
            }

        return NormalData.table_type_sample.get(table_type)

    @staticmethod
    def _sample(entity_ids: List[str] = ['jack', 'helen', 'kris'],
                x_field: str = 'timestamp',
                row_size: int = 10,
                is_timeseries: bool = True,
                columns: List[str] = ['math', 'physics', 'programing']):
        dfs = pd.DataFrame()
        for entity in entity_ids:
            if x_field is not None and is_timeseries:
                df = pd.DataFrame(np.random.randint(low=0, high=100, size=(row_size, len(columns))), columns=columns)
                df[x_field] = pd.date_range(end='1/1/2018', periods=row_size)
            else:
                df = pd.DataFrame(np.random.randint(low=0, high=100, size=(row_size, len(columns))), columns=columns)

            df['entity_id'] = entity
            dfs = dfs.append(df)

        dfs = index_df_with_category_xfield(df=dfs, xfield=x_field, is_timeseries=is_timeseries)

        return dfs
