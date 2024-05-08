# -*- coding: utf-8 -*-


def check_tables_result(error: Exception):

    assert "1..12: xxxx 32.32.2022" in str(error)
