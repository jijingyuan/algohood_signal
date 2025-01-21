# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:44
@File: targetMgr.py
@Author: Jingyuan
"""
import importlib

from algoUtils.reloadUtil import reload_all
from .dataMgr import DataMgr


class TargetMgr:

    def __init__(self, _target_method_name, _target_method_param, _data_mgr):
        self.target_mgr = self.get_target_method(_target_method_name, _target_method_param)
        self.data_mgr: DataMgr = _data_mgr

    @staticmethod
    def get_target_method(_method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.algoTargets.{}'.format(_method_name))
        reload_all(module)
        cls_method = getattr(module, 'Algo')
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    @classmethod
    async def generate_abstract(cls, _targets, _abstract_method, _abstract_param):
        if _abstract_param is None:
            _abstract_param = {}
        module = importlib.import_module('algoStrategy.algoAbstracts.{}'.format(_abstract_method))
        reload_all(module)
        cls_method = getattr(module, 'Algo')
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_abstract_method))
        instance = cls_method(**_abstract_param)
        return instance.generate_abstract(_targets)

    async def start_task(self, _signal, _keep_empty: bool):
        self.data_mgr.clear_cache_data(_signal['symbol'], _signal['signal_timestamp'])
        if isinstance(_signal['signal_price'], str):
            _signal['signal_price'] = eval(_signal['signal_price'])

        target = await self.target_mgr.generate_targets(_signal, self.data_mgr) or {}
        if not target and not _keep_empty:
            return

        adj_target = {'target_{}'.format(k): v for k, v in target.items()}
        return {**_signal, **adj_target}
