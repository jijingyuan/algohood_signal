# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:44
@File: targetMgr.py
@Author: Jingyuan
"""
import importlib

from algoUtils.dateUtil import timestamp_local_datetime
from .dataMgr import DataMgr
from ..algoConfig.loggerConfig import logger


class TargetMgr:

    def __init__(self, _method_name, _method_param, _data_type):
        self.target_mgr = self.get_target_method(_method_name, _method_param)
        self.data_mgr = DataMgr(_data_type)

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

    def start_task(self, _signals, _forward_window):
        all_targets = []
        self.data_mgr.init_data_mgr()
        for signal in _signals:
            symbol = signal['signal_symbol']
            start_timestamp = signal['signal_timestamp']
            end_timestamp = start_timestamp + _forward_window
            data = self.data_mgr.get_all_data_by_symbol(symbol, start_timestamp, end_timestamp)
            if not data:
                all_targets.append(signal)
            else:
                target = self.target_mgr.generate_targets(data) or {}
                adj_target = {'target_{}'.format(k): v for k, v in target.items()}
                all_targets.append({**signal, **adj_target})

            logger.info('{} finished'.format(timestamp_local_datetime(start_timestamp)))

        return all_targets
