# -*- coding: utf-8 -*-
from typing import List

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


class Drawer(object):
    def __init__(self,
                 df: pd.DataFrame = None,
                 entity_field: str = 'entity_id',
                 x_field: str = 'timestamp',
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
        self.x_field = x_field
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

        if len(names) == 2 and names[0] == self.entity_field and names[1] == self.x_field:
            return True

        return False

    def normalize(self):
        if df_is_not_null(self.data_df):
            if not self.is_normalized():
                self.data_df.reset_index(inplace=True)
                self.data_df = index_df_with_entity_xfield(self.data_df, entity_field=self.entity_field,
                                                           xfield=self.x_field, is_timeseries=self.is_timeseries)

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

    def get_composition_type(self):
        entity_size = len(self.entity_ids)
        row_count = int(len(self.data_df) / entity_size)
        column_size = len(self.data_df.columns)

        if entity_size == 1:
            a = 'single'
        else:
            a = 'multiple'

        if row_count <= 10:
            b = 'few'
        else:
            b = 'many'

        if column_size == 1:
            c = 'single'
        else:
            c = 'multiple'

        return f'{a} - {b} - {c}'

    def draw_line(self, mode='lines'):
        data = []
        for entity_id, df in self.entity_map_df.items():
            for col in df.columns:
                if self.x_field not in df.index.names:
                    raise Exception('the table shape:{} not for line'.format(self.get_composition_type()))

                trace_name = '{}_{}'.format(entity_id, col)
                ydata = df.loc[:, col].values.tolist()
                data.append(go.Scatter(x=df.index, y=ydata, mode=mode, name=trace_name))

        self.draw(data=data)

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

        return go.Layout(showlegend=True,
                         uirevision=uirevision,
                         height=self.height,
                         width=self.width,
                         title=self.title,
                         annotations=self.get_plotly_annotations(),
                         yaxis=dict(
                             autorange=True,
                             fixedrange=False
                         ),
                         xaxis=dict(
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
                         ))

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

    def draw_scatter(self):
        pass

    def draw_histogram(self):
        pass

    def draw_kline(self):
        pass

    def draw_table(self):
        pass

    def draw_bar(self):
        pass

    def draw_polar(self):
        pass

    def draw_pie(self):
        pass


class Chart(object):
    def __init__(self,
                 entity_field: str = 'entity_id',
                 time_field: str = 'timestamp',
                 figure: go._BaseTraceType = go.Scatter,
                 modes: List[str] = ['lines'],
                 value_fields: List[str] = ['close'],
                 render: str = 'html',
                 file_name: str = None,
                 width: int = None,
                 height: int = None,
                 title: str = None,
                 keep_ui_state: bool = True) -> None:
        self.figure = figure
        self.modes = modes
        self.entity_field = entity_field
        self.time_field = time_field
        self.value_fields = value_fields
        self.render = render
        self.file_name = file_name
        self.width = width
        self.height = height

        if title:
            self.title = title
        else:
            self.title = type(self).__name__.lower()

        self.keep_ui_state = keep_ui_state

        self.data_df: pd.DataFrame = None
        self.annotation_df: pd.DataFrame = None

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

    def get_plotly_data(self):
        if not df_is_not_null(self.data_df):
            return []

        df_list: List[pd.DataFrame] = []
        for _, df_item in self.data_df.groupby(self.entity_field):
            df = df_item.copy()
            df.reset_index(inplace=True, level=self.entity_field)
            df_list.append(df)

        if len(df_list) > 1:
            print(df_list)
            df_list = fill_with_same_index(df_list=df_list)

        data = []
        for df in df_list:
            series_name = df[df[self.entity_field].notna()][self.entity_field][0]

            # for timeseries data
            if len(df.index) > 1:
                xdata = [timestamp for timestamp in df.index]

            # 雷达图
            if self.figure == go.Scatterpolar:
                assert len(df) == 1
                trace_name = series_name
                cols = [col for col in list(df.columns) if col != self.entity_field]

                trace = go.Scatterpolar(
                    r=df.iloc[0, 1:].to_list(),
                    theta=cols,
                    fill='toself',
                    name=trace_name
                )
                data.append(trace)
            # k线图
            elif self.figure == go.Candlestick:
                entity_type, _, _ = decode_entity_id(series_name)
                trace_name = '{}_kdata'.format(series_name)

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

                data.append(go.Candlestick(x=xdata, open=open, close=close, low=low, high=high, name=trace_name))
            # 表格
            elif self.figure == go.Table:
                cols = [self.time_field] + list(df.columns)
                trace = go.Table(
                    header=dict(values=cols,
                                fill=dict(color='#C2D4FF'),
                                align=['left'] * 5),
                    cells=dict(values=[df.index] + [df[col] for col in df.columns],
                               fill=dict(color='#F5F8FF'),
                               align=['left'] * 5))

                data.append(trace)
            # 柱状图
            elif self.figure == go.Bar:
                trace_name = '{}_{}'.format(series_name, self.value_fields[i])

                ydata = df.loc[:, self.value_fields[i]].values.tolist()
                data.append(self.figure(x=xdata, y=ydata, name=trace_name))


            else:
                trace_name = '{}_{}'.format(series_name, self.value_fields[i])

                ydata = df.loc[:, self.value_fields[i]].values.tolist()
                data.append(self.figure(x=xdata, y=ydata, mode=self.modes[i], name=trace_name))

        return data

    def get_plotly_layout(self):
        if self.keep_ui_state:
            uirevision = True
        else:
            uirevision = None

        return go.Layout(showlegend=True,
                         uirevision=uirevision,
                         height=self.height,
                         width=self.width,
                         title=self.title,
                         annotations=self.get_plotly_annotations(),
                         yaxis=dict(
                             autorange=True,
                             fixedrange=False
                         ),
                         xaxis=dict(
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
                         ))

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


if __name__ == '__main__':
    from zvdata.reader import NormalData

    df = NormalData.sample(x_field='timestamp')
    drawer = Drawer(df=df)
    drawer.draw_line()
