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

import pandas as pd

from .dataMgr import DataMgr
from ..algoUtil.loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class SignalMgr:

    def __init__(self, _author, _method_name, _method_param):
        self.signal_mgr = self.get_signal_method(_author, _method_name, _method_param)
        self.data_mgr = DataMgr()
        self.cache = []
        self.signals = []

    @staticmethod
    def get_signal_method(_author, _method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algohood_{}.signal_{}'.format(_author.lower(), _method_name))
        cls_method = getattr(module, _method_name)
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    async def start_task(self, _lag, _symbols, _start_timestamp, _end_timestamp, _save_signals):
        tasks = [
            self.data_mgr.load_data(_symbols, _start_timestamp, _end_timestamp),
            self.handle_data(_lag)
        ]

        await asyncio.gather(*tasks)
        if self.signals:
            if _save_signals:
                pd.DataFrame(self.signals).to_csv('../RookieFile/signals.csv')
            else:
                return self.signals

    @staticmethod
    def reshape(_ticks, _lag):
        keep = len(str(_lag).split('.')[-1]) if _lag < 1 else 0
        tmp = [[v['rank_timestamp'], v] for v in _ticks]
        g = groupby(tmp, lambda x: round(int(x[0] / _lag) * _lag, keep))
        return {k: list(v) for k, v in g}

    async def handle_data(self, _lag):
        while True:
            try:
                data = await self.data_mgr.get_data()
                if data is None:
                    return

                current_data = self.cache + data
                if _lag is None:
                    for data in current_data:
                        signals = self.signal_mgr.generate_targets([data])
                        if signals:
                            adj_signals = {'signal_{}'.format(k): v for k, v in signals.items()}
                            self.signals.append({**adj_signals, 'signal_timestamp': data[-1]['rank_timestamp']})

                else:
                    last_cut = last_ticks = None
                    for cut_timestamp, ticks in self.reshape(current_data, _lag).items():
                        if last_cut is None:
                            last_cut = cut_timestamp
                            last_ticks = ticks
                            continue

                        signals = self.signal_mgr.generate_targets([v[1] for v in last_ticks])
                        if signals:
                            adj_signals = {'signal_{}'.format(k): v for k, v in signals.items()}
                            self.signals.append({**adj_signals, 'signal_timestamp': cut_timestamp})

                        last_cut = cut_timestamp
                        last_ticks = ticks
                        await asyncio.sleep(0)

                    self.cache = [v[1] for v in last_ticks]

            except Exception:
                logger.error(traceback.format_exc())
