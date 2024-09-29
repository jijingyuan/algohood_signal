# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:22
@File: __init__.py.py
@Author: Jingyuan
"""
import abc


class Base:

    @abc.abstractmethod
    def generate_targets(self, _data: list) -> dict or None:
        """
        generate target for signals
        :param _data: data source
        :return: {**kwargs: stats that you need for analyzing}
        """
        return
