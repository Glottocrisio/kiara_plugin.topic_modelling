# -*- coding: utf-8 -*-
from kiara.models.values.value import Value


def check_date(date: Value):

    assert (
        str(date.data) == "2022-01-02 00:00:00"
    ), f"Date is {date.data}, not 2022-02-02 00:00:00"
