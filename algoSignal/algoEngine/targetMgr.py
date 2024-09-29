# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:44
@File: targetMgr.py
@Author: Jingyuan
"""
import importlib

import pandas as pd

from .dataMgr import DataMgr
from ..algoConfig.loggerConfig import logger
from algoUtils.dateUtil import timestamp_local_datetime


class TargetMgr:

    def __init__(self, _method_name, _method_param, _data_mgr: DataMgr):
        self.target_mgr = self.get_target_method(_method_name, _method_param)
        self.data_mgr = _data_mgr

    @staticmethod
    def get_target_method(_method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.target{}'.format(_method_name))
        cls_method = getattr(module, _method_name)
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    def handle_signals(self, _signals, _symbols, _forward_window):
        all_targets = []
        for signal in _signals:
            start_timestamp = signal['signal_timestamp']
            end_timestamp = start_timestamp + _forward_window
            data = self.data_mgr.get_trades_given_start_end(_symbols, start_timestamp, end_timestamp)
            if not data:
                all_targets.append(signal)
            else:
                target = self.target_mgr.generate_targets(data) or {}
                adj_target = {'target_{}'.format(k): v for k, v in target.items()}
                all_targets.append({**signal, **adj_target})

            logger.info('{} finished'.format(timestamp_local_datetime(start_timestamp)))

        pd.DataFrame(all_targets).to_csv('../RookieFile/targets.csv')
