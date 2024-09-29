# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:22
@File: __init__.py.py
@Author: Jingyuan
"""
import abc


class Base:

    @abc.abstractmethod
    def generate_signals(self, _data: list) -> list or None:
        """
        generate signal to execution module
        :param _data: data source
        :return: [{
            'bind_id': str(uuid),
            'symbol': 'btc_usdt|binance_future',
            'action': 'open' or 'close'
            'position': 'long' or 'short'
            'execution_type': 'taker' or 'maker' or 'exec_1',
            **kwargs: other info that you need for analyze
        }, ...]
        """
        return
