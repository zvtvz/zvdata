# -*- coding: utf-8 -*-
import enum
from typing import List

import numpy as np
import pandas as pd
import plotly
import plotly.graph_objs as go

from zvdata.api import decode_entity_id
from zvdata.utils.pd_utils import df_is_not_null, fill_with_same_index, index_df_with_entity_xfield
from zvdata.utils.time_utils import now_time_str, TIME_FORMAT_ISO8601


def get_ui_path(name):
    if name is None:
        name = '{}.html'.format(now_time_str(fmt=TIME_FORMAT_ISO8601))
    return '{}.html'.format(name)


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

        dfs = index_df_with_entity_xfield(df=dfs, xfield=x_field, is_timeseries=is_timeseries)

        return dfs


class Drawer(object):
    def __init__(self,
                 df: pd.DataFrame = None,
                 entity_field: str = 'entity_id',
                 index_field: str = 'timestamp',
                 is_timeseries: bool = True,
                 render: str = 'html',
                 file_name: str = None,
                 width: int = None,
                 height: int = None,
                 title: str = None,
                 keep_ui_state: bool = True) -> None:
        self.keep_ui_state = keep_ui_state
        self.data_df: pd.DataFrame = df
        self.entity_field = entity_field
        self.index_field = index_field
        self.is_timeseries = is_timeseries
        self.render = render
        self.file_name = file_name
        self.width = width
        self.height = height

        if title:
            self.title = title
        else:
            self.title = type(self).__name__.lower()

        self.entity_ids = []
        self.df_list = []
        self.entity_map_df = {}

        self.annotation_df: pd.DataFrame = None

        self.normalize()

    def is_normalized(self):
        names = self.data_df.index.names

        # it has been normalized
        if len(names) == 1 and names[0] == self.entity_field:
            return True

        if len(names) == 2 and names[0] == self.entity_field and names[1] == self.index_field:
            return True

        return False

    def normalize(self):
        if df_is_not_null(self.data_df):
            if not self.is_normalized():
                self.data_df.reset_index(inplace=True)
                self.data_df = index_df_with_entity_xfield(self.data_df, entity_field=self.entity_field,
                                                           xfield=self.index_field, is_timeseries=self.is_timeseries)

            if isinstance(self.data_df.index, pd.MultiIndex):
                self.entity_ids = list(self.data_df.index.get_level_values(0).values)
            else:
                self.entity_ids = list(self.data_df.index.values)

            for entity_id, df_item in self.data_df.groupby(self.entity_field):
                df = df_item.copy()
                df.reset_index(inplace=True, level=self.entity_field)
                self.df_list.append(df)

            if len(self.df_list) > 1:
                self.df_list = fill_with_same_index(df_list=self.df_list)

            for df in self.df_list:
                entity_id = df[df[self.entity_field].notna()][self.entity_field][0]
                columns = list(df.columns)
                columns.remove(self.entity_field)
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

    def set_data_df(self, df):
        self.data_df = df

    def set_annotation_df(self, df):
        """
        annotation_df should in this format:
                                           flag value  color
        self.trace_field      timestamp

        stock_sz_000338       2019-01-02   buy  100    "#ec0000"

        :param df:
        :type df:
        """
        self.annotation_df = df

    def get_annotation_df(self):
        return self.annotation_df

    def get_data_df(self):
        return self.data_df

    def get_plotly_annotations(self):
        annotations = []

        if df_is_not_null(self.get_annotation_df()):
            for trace_name, df in self.annotation_df.groupby(level=0):
                if df_is_not_null(df):
                    for (_, timestamp), item in df.iterrows():
                        if 'color' in item:
                            color = item['color']
                        else:
                            color = '#ec0000'

                        value = round(item['value'], 2)
                        annotations.append(dict(
                            x=timestamp,
                            y=value,
                            xref='x',
                            yref='y',
                            text=item['flag'],
                            showarrow=True,
                            align='center',
                            arrowhead=2,
                            arrowsize=1,
                            arrowwidth=2,
                            # arrowcolor='#030813',
                            ax=-10,
                            ay=-30,
                            bordercolor='#c7c7c7',
                            borderwidth=1,
                            bgcolor=color,
                            opacity=0.8
                        ))
        return annotations

    def get_plotly_layout(self):
        if self.keep_ui_state:
            uirevision = True
        else:
            uirevision = None

        layout = go.Layout(showlegend=True,
                           uirevision=uirevision,
                           height=self.height,
                           width=self.width,
                           title=self.title,
                           annotations=self.get_plotly_annotations(),
                           yaxis=dict(
                               autorange=True,
                               fixedrange=False
                           ))
        if self.is_timeseries:
            layout.xaxis = dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1,
                             label='1m',
                             step='month',
                             stepmode='backward'),
                        dict(count=6,
                             label='6m',
                             step='month',
                             stepmode='backward'),
                        dict(count=1,
                             label='YTD',
                             step='year',
                             stepmode='todate'),
                        dict(count=1,
                             label='1y',
                             step='year',
                             stepmode='backward'),
                        dict(step='all')
                    ])
                ),
                rangeslider=dict(
                    visible=True
                ),
                type='date'
            )
        return layout

    def draw(self, data, layout=None):
        if layout is None:
            layout = self.get_plotly_layout()

        if self.render == 'html':
            plotly.offline.plot(figure_or_data={'data': data,
                                                'layout': layout
                                                }, filename=get_ui_path(self.file_name), )
        elif self.render == 'notebook':
            plotly.offline.init_notebook_mode(connected=True)
            plotly.offline.iplot(figure_or_data={'data': data,
                                                 'layout': layout
                                                 })

        return data, layout

    def draw_line(self):
        self.draw_scatter(mode='lines')

    def draw_scatter(self, mode='markers'):
        data = []
        for entity_id, df in self.entity_map_df.items():
            for col in df.columns:
                if self.index_field not in df.index.names:
                    raise Exception('the table_type:{} not for line'.format(self.get_table_type()))

                trace_name = '{}_{}'.format(entity_id, col)
                ydata = df.loc[:, col].values.tolist()
                data.append(go.Scatter(x=df.index, y=ydata, mode=mode, name=trace_name))

        self.draw(data=data)

    def draw_bar(self, x='columns'):
        data = []
        for entity_id, df in self.entity_map_df.items():
            for col in df.columns:
                trace_name = '{}_{}'.format(entity_id, col)
                ydata = df.loc[:, col].values.tolist()
                data.append(go.Bar(x=df.index, y=ydata, name=trace_name))

        self.draw(data=data)

    def draw_pie(self):
        data = []
        df: pd.DataFrame
        for entity_id, df in self.entity_map_df.items():
            for _, row in df.iterrows():
                data.append(go.Pie(name=entity_id, labels=df.columns.tolist(), values=row.tolist()))

        self.draw(data=data)

    def draw_histogram(self):
        pass

    def draw_kline(self):
        data = []
        for entity_id, df in self.entity_map_df.items():
            entity_type, _, _ = decode_entity_id(entity_id)
            trace_name = '{}_kdata'.format(entity_id)

            if entity_type == 'stock':
                open = df.loc[:, 'qfq_open']
                close = df.loc[:, 'qfq_close']
                high = df.loc[:, 'qfq_high']
                low = df.loc[:, 'qfq_low']
            else:
                open = df.loc[:, 'open']
                close = df.loc[:, 'close']
                high = df.loc[:, 'high']
                low = df.loc[:, 'low']

            data.append(go.Candlestick(x=df.index, open=open, close=close, low=low, high=high, name=trace_name))

        self.draw(data=data)

    def draw_table(self):
        pass

    def draw_polar(self):
        data = []
        df: pd.DataFrame
        for entity_id, df in self.entity_map_df.items():
            for _, row in df.iterrows():
                trace = go.Scatterpolar(
                    r=row.to_list(),
                    theta=df.columns.tolist(),
                    fill='toself',
                    name=entity_id
                )
                data.append(trace)

        self.draw(data=data)


if __name__ == '__main__':
    for table_type in TableType:
        df = NormalData.sample(table_type=table_type)
        drawer = Drawer(df=df)
        drawer.draw_pie()
