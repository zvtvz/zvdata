# -*- coding: utf-8 -*-
import pandas as pd

from zvdata.reader import NormalData


def test_normal_sample():
    df: pd.DataFrame = None
    df = NormalData.sample()
    print(df)

    df = NormalData.sample(x_field='timestamp')
    print(df)

    df.reset_index(inplace=True)
    a = list(df.columns)
    a.remove('entity_id')
    print(a)
