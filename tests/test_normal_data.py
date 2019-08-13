# -*- coding: utf-8 -*-

from zvdata.chart import Drawer
from zvdata.normal_data import TableType
from zvdata.reader import NormalData


def test_normal_sample():
    for table_type in TableType:
        drawer = Drawer(data=NormalData(NormalData.sample(table_type=table_type)))
        drawer.draw_table()
