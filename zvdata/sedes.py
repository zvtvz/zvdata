# -*- coding: utf-8 -*-
import inspect
import json
from enum import Enum

import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import pandas as pd
from dash.dependencies import State
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute

from zvdata.structs import IntervalLevel
from zvdata.utils.time_utils import to_time_str


class Jsonable(object):
    def __json__(self):
        result = {}

        spec = inspect.getfullargspec(self.__class__)
        args = [arg for arg in spec.args if arg != 'self']
        for arg in args:
            value = eval('self.{}'.format(arg))
            json_value = value

            if isinstance(value, pd.Timestamp):
                json_value = to_time_str(value)

            if isinstance(value.__class__, DeclarativeMeta):
                json_value = value.__class__.__name__

            if isinstance(value, InstrumentedAttribute):
                json_value = value.name

            if isinstance(value, Enum):
                json_value = value.value

            result[arg] = json_value

        return result

    for_json = __json__  # supported by simplejson


class UiComposable(object):
    @classmethod
    def to_html_inputs(cls):
        """
        construct ui input from the class constructor arguments spec

        """
        spec = inspect.getfullargspec(cls)
        args = [arg for arg in spec.args if arg != 'self']
        annotations = spec.annotations
        defaults = [cls.marshal_data_for_ui(default) for default in spec.defaults]

        divs = []
        states = []
        for i, arg in enumerate(args):
            left = html.Label(arg, style={'display': 'inline-block', 'width': '100px'})

            annotation = annotations.get(arg)
            text = defaults[i]

            right = None
            state = None

            if annotation is bool:
                right = daq.BooleanSwitch(id=arg, on=text)
                state = State(arg, 'on')
            elif 'level' == arg:
                right = dcc.Dropdown(id=arg,
                                     options=[{'label': item.value, 'value': item.value} for item in IntervalLevel],
                                     value=text)

            elif 'filters' == arg and text:
                filters = [str(filter) for filter in text]
                text = ','.join(filters)

            elif 'columns' == arg and text:
                columns = [column.name for column in text]
                text = ','.join(columns)

            elif 'timestamp' in arg:
                right = dcc.DatePickerSingle(id=arg, date=text)
                state = State(arg, 'date')

            if isinstance(text, list):
                if isinstance(text[0], str):
                    text = ','.join(text)
                else:
                    text = json.dumps(text)

            if isinstance(text, dict):
                text = json.dumps(text)

            if right is None:
                right = dcc.Input(id=arg, type='text', value=text)
            if state is None:
                state = State(arg, 'value')

            right.style = {'display': 'inline-block'}
            divs.append(html.Div([left, right], style={'margin-left': '120px'}))
            states.append(state)

        return divs, states

    @classmethod
    def ui_meta(cls):
        return {}

    @classmethod
    def marshal_data_for_ui(cls, data):
        if isinstance(data, Enum):
            return data.value

        if isinstance(data, pd.Timestamp):
            return to_time_str(data)

        return data

    @classmethod
    def unmarshal_data_for_arg(cls, data):
        return data

    @classmethod
    def from_html_inputs(cls, *inputs):
        arg_values = []

        spec = inspect.getfullargspec(cls)
        args = [arg for arg in spec.args if arg != 'self']
        annotations = spec.annotations

        for i, input in enumerate(inputs):
            result = input

            annotation = annotations.get(args[i])

            if annotation == dict or issubclass(annotation, list):
                result = json.loads(input)
            arg_values.append(result)

        return arg_values
