# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:44
@File: targetMgr.py
@Author: Jingyuan
"""
import importlib

import pandas as pd

from .dataMgr import DataMgr
from RookieUtil.RDateUtil import timestamp_local_datetime
from ..algoUtil.loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class TargetMgr:

    def __init__(self, _sign, _method_name, _method_param):
        self.target_mgr = self.get_target_method(_sign, _method_name, _method_param)
        self.data_mgr = DataMgr()

    @staticmethod
    def get_target_method(_author, _method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algohood_{}.target_{}'.format(_author.lower(), _method_name))
        cls_method = getattr(module, _method_name)
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    async def init_data_mgr(self):
        await self.data_mgr.client.initiate_clients(_local=True)

    async def close_data_mgr(self):
        await self.data_mgr.client.close_redis_connection()

    async def handle_signals(self, _signals, _symbols, _forward_window):
        all_targets = []
        for signal in _signals:
            start_timestamp = signal['signal_timestamp']
            end_timestamp = start_timestamp + _forward_window
            trades = await self.data_mgr.get_trades_given_start_end(_symbols, start_timestamp, end_timestamp)
            if not trades:
                all_targets.append(signal)
            else:
                target = self.target_mgr.generate_targets(trades) or {}
                adj_target = {'target_{}'.format(k): v for k, v in target.items()}
                all_targets.append({**signal, **adj_target})

            logger.info('{} finished'.format(timestamp_local_datetime(start_timestamp)))

        pd.DataFrame(all_targets).to_csv('../RookieFile/targets.csv')
