# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:44
@File: performanceMgr.py
@Author: Jingyuan
"""
import importlib

from algoUtils.reloadUtil import reload_all
from .dataMgr import DataMgr


class PerformanceMgr:

    def __init__(self, _performance_method_name, _performance_method_param, _data_mgr):
        self.performance_mgr = self.get_performance_method(_performance_method_name, _performance_method_param)
        self.data_mgr: DataMgr = _data_mgr

    @staticmethod
    def get_performance_method(_method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.algoPerformances.{}'.format(_method_name))
        reload_all(module)
        cls_method = getattr(module, 'Algo')
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    @classmethod
    async def generate_abstract(cls, _performances, _abstract_method, _abstract_param):
        if _abstract_param is None:
            _abstract_param = {}
        module = importlib.import_module('algoStrategy.algoAbstracts.{}'.format(_abstract_method))
        reload_all(module)
        cls_method = getattr(module, 'Algo')
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_abstract_method))
        instance = cls_method(**_abstract_param)
        return instance.generate_abstract(_performances)

    async def start_task(self, _signal, _keep_empty: bool):
        self.data_mgr.clear_cache_data(_signal['symbol'], _signal['signal_timestamp'])
        if isinstance(_signal['signal_price'], str):
            _signal['signal_price'] = eval(_signal['signal_price'])

        performance = await self.performance_mgr.generate_performances(_signal, self.data_mgr) or {}
        if not performance and not _keep_empty:
            return

        adj_performance = {'performance_{}'.format(k): v for k, v in performance.items()}
        return {**_signal, **adj_performance}
