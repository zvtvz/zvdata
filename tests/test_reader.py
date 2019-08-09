# -*- coding: utf-8 -*-
from zvdata.reader import NormalData


def test_normal_sample():
    df = NormalData.sample()
    print(df)

    df = NormalData.sample(x_field='timestamp')
    print(df)
