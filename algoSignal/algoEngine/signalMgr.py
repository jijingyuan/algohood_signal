# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:43
@File: signalMgr.py
@Author: Jingyuan
"""
import asyncio
import importlib
import traceback
from itertools import groupby

from algoUtils.reloadUtil import reload_all
from ..algoConfig.loggerConfig import logger


class SignalMgr:

    def __init__(self, _signal_method_name, _signal_method_param, _data_mgr):
        self.signal_mgr = self.get_signal_method(_signal_method_name, _signal_method_param)
        self.data_mgr = _data_mgr
        self.cache = []
        self.signals = []

    @staticmethod
    def get_signal_method(_method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.algoSignals.{}'.format(_method_name))
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
                        signals = self.signal_mgr.generate_signals({signal_data[1]: [signal_data[2]]}) or []
                        for signal in signals:
                            adj_signals = self.check_fields(signal)
                            self.signals.append({
                                **adj_signals,
                                'signal_timestamp': signal_ts,
                                'signal_price': signal_price
                            })

                else:
                    last_cut = last_ticks = None
                    for cut_timestamp, ticks in self.reshape(current_data, _lag).items():
                        if last_cut is None:
                            last_cut = cut_timestamp
                            last_ticks = ticks
                            continue

                        signal_data = {}
                        for v in last_ticks:
                            signal_data.setdefault(v[1], []).append(v[2])

                        signals = self.signal_mgr.generate_signals(signal_data) or []
                        signal_ts = last_cut + _lag
                        signal_price = {k: v[-1][2] for k, v in signal_data.items()}
                        for signal in signals:
                            adj_signals = self.check_fields(signal)
                            self.signals.append({
                                **adj_signals,
                                'signal_timestamp': signal_ts,
                                'signal_price': signal_price
                            })

                        last_cut = cut_timestamp
                        last_ticks = ticks

                    self.cache = last_ticks

            except Exception:
                logger.error(traceback.format_exc())

    @staticmethod
    def check_fields(_signal):
        adj_signal = {}
        default_keys = ['batch_id', 'symbol', 'action', 'position']
        for field, v in _signal.items():
            if field in default_keys:
                adj_signal[field] = v
            else:
                adj_signal['signal_{}'.format(field)] = v

        for key in default_keys:
            if key not in adj_signal:
                raise Exception('{} does not exist'.format(key))

        return adj_signal
