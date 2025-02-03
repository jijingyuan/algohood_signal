# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:43
@File: signalMgr.py
@Author: Jingyuan
"""
import asyncio
import importlib
from tkinter import NO
import traceback
from typing import Optional
from itertools import groupby

from algoUtils.reloadUtil import reload_all
from ..algoConfig.loggerConfig import logger
from algoUtils.defUtil import SignalBase, InterceptBase


class SignalMgr:

    def __init__(
            self, _signal_method_name, _signal_method_param, _intercept_method_name, _intercept_method_param, _data_mgr
    ):
        self.signal_mgr = self.get_signal_method(_signal_method_name, _signal_method_param)
        self.intercept_mgr = self.get_intercept_method(_intercept_method_name, _intercept_method_param)
        self.data_mgr = _data_mgr
        self.cache = []
        self.signals = []
        self.features = {}
        self.check_signals = {}
        self.signal_id = 1

    @staticmethod
    def get_signal_method(_method_name, _method_param) -> SignalBase:
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.algoSignals.{}'.format(_method_name))
        reload_all(module)
        cls_method = getattr(module, 'Algo')
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    @staticmethod
    def get_intercept_method(_method_name, _method_param) -> Optional[InterceptBase]:
        if _method_name is None:
            return

        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.algoIntercepts.{}'.format(_method_name))
        reload_all(module)
        cls_method = getattr(module, 'Algo')
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    async def start_task(self, _lag, _symbols, _start_timestamp, _end_timestamp):
        tasks = [
            self.data_mgr.load_data(_symbols, _start_timestamp, _end_timestamp),
            self.handle_data(_lag)
        ]

        await asyncio.gather(*tasks)
        return self.signals

    @staticmethod
    def reshape(_ticks, _lag):
        keep = len(str(_lag).split('.')[-1]) if _lag < 1 else 0
        g = groupby(_ticks, lambda x: round(int(x[0] / _lag) * _lag, keep))
        return {k: list(v) for k, v in g}

    def handle_batch_data(self, _signal_ts, _signal_price, _data: dict):
        features = {}
        is_intercept = False
        if self.intercept_mgr is not None:
            features = self.intercept_mgr.generate_features(_data) or {}
            is_intercept = self.intercept_mgr.intercept_signal(features)

            for signal_id in list(self.check_signals.keys()):
                signal, features, is_intercept = self.check_signals[signal_id]
                target = self.intercept_mgr.generate_target(signal_id, signal, _data)
                if target is None:
                    continue
                
                self.check_signals.pop(signal_id)
                self.signals.append({**signal, **features, **target, 'intercept': is_intercept})

        signals = self.signal_mgr.generate_signals(_data) or []
        for signal in signals:
            self.check_fields(signal)
            signal.update({'signal_timestamp': _signal_ts, 'signal_price': _signal_price})
            self.check_signals[self.signal_id] = [signal, features, is_intercept]
            self.signal_id += 1

        return signals

    async def handle_data(self, _lag):
        while True:
            try:
                data = await self.data_mgr.get_data()
                if data is None:
                    return

                current_data = self.cache + data
                if _lag is None:
                    for signal_data in current_data:
                        signal_ts = signal_data[0]
                        signal_price = {signal_data[1]: signal_data[2][2]}
                        self.handle_batch_data(signal_ts, signal_price, {signal_data[1]: [signal_data[2]]})

                else:
                    last_cut = None
                    last_ticks = []
                    for cut_timestamp, ticks in self.reshape(current_data, _lag).items():
                        if last_cut is None:
                            last_cut = cut_timestamp
                            last_ticks = ticks 
                            continue

                        signal_data = {}
                        for v in last_ticks:
                            signal_data.setdefault(v[1], []).append(v[2])

                        signal_ts = round(last_cut + _lag, 6)
                        signal_price = {k: v[-1][2] for k, v in signal_data.items()}
                        self.handle_batch_data(signal_ts, signal_price, signal_data)

                        last_cut = cut_timestamp
                        last_ticks = ticks

                    self.cache = last_ticks

            except Exception:
                logger.error(traceback.format_exc())

    @staticmethod
    def check_fields(_signal):
        default_keys = ['batch_id', 'symbol', 'action', 'position']
        signal_keys = list(_signal.keys())
        for key in default_keys:
            if key not in signal_keys:
                raise Exception('{} does not exist'.format(key))
