#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.topic_modelling` package."""

import kiara_plugin.topic_modelling
import pytest  # noqa


def test_assert():

    assert kiara_plugin.topic_modelling.get_version() is not None
