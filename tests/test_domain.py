# -*- coding: utf-8 -*-
from zvdata.contract import domain_name_to_table_name, table_name_to_domain_name


def test_domain_name_to_table_name():
    assert domain_name_to_table_name(domain_name='Abc') == 'abc'
    assert domain_name_to_table_name(domain_name='ABCD') == 'a_b_c_d'
    assert domain_name_to_table_name(domain_name='12345') == '1_2_3_4_5'
    assert domain_name_to_table_name(domain_name='AbcD') == 'abc_d'
    assert domain_name_to_table_name(domain_name='AbcDe') == 'abc_de'
    assert domain_name_to_table_name(domain_name='Abc1D') == 'abc_1_d'
    assert domain_name_to_table_name(domain_name='Abc1dKdata') == 'abc_1d_kdata'


def test_table_name_to_domain_name():
    assert table_name_to_domain_name(table_name='abc') == 'Abc'
    assert table_name_to_domain_name(table_name='a_b_c_d') == 'ABCD'
    assert table_name_to_domain_name(table_name='1_2_3_4_5') == '12345'
    assert table_name_to_domain_name(table_name='abc_d') == 'AbcD'
    assert table_name_to_domain_name(table_name='abc_de') == 'AbcDe'
    assert table_name_to_domain_name(table_name='abc_1_d') == 'Abc1D'
    assert table_name_to_domain_name(table_name='abc_1d_kdata') == 'Abc1dKdata'
