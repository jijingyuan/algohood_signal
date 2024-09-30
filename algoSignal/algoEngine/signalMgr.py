# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:43
@File: signalMgr.py
@Author: Jingyuan
"""
import importlib
import time
import traceback
from concurrent.futures import wait
from itertools import groupby

import pandas as pd

from .dataMgr import DataMgr
from ..algoConfig.loggerConfig import logger
from ..algoConfig.threadPoolConfig import pool


class SignalMgr:

    def __init__(self, _method_name, _method_param, _data_type):
        self.signal_mgr = self.get_signal_method(_method_name, _method_param)
        self.data_mgr = DataMgr(_data_type)
        self.cache = []
        self.signals = []

    @staticmethod
    def get_signal_method(_method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoStrategy.signal{}'.format(_method_name))
        cls_method = getattr(module, _method_name)
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    def start_task(self, _lag, _symbols, _start_timestamp, _end_timestamp):
        self.data_mgr.init_data_mgr()
        tasks = [
            pool.submit(self.data_mgr.load_data, *(_symbols, _start_timestamp, _end_timestamp)),
            pool.submit(self.handle_data, *(_lag,))
        ]

        wait(tasks)
        return self.signals

    @staticmethod
    def reshape(_ticks, _lag):
        keep = len(str(_lag).split('.')[-1]) if _lag < 1 else 0
        tmp = [[v['rank_timestamp'], v] for v in _ticks]
        g = groupby(tmp, lambda x: round(int(x[0] / _lag) * _lag, keep))
        return {k: list(v) for k, v in g}

    def handle_data(self, _lag):
        while True:
            try:
                data = self.data_mgr.get_data()
                if data is None:
                    return

                current_data = self.cache + data
                if _lag is None:
                    for data in current_data:
                        signals = self.signal_mgr.generate_signals([data]) or []
                        for signal in signals:
                            self.check_fields(signal)
                            adj_signals = {'signal_{}'.format(k): v for k, v in signal.items()}
                            self.signals.append({**adj_signals, 'signal_timestamp': data[-1]['rank_timestamp']})

                else:
                    last_cut = last_ticks = None
                    for cut_timestamp, ticks in self.reshape(current_data, _lag).items():
                        if last_cut is None:
                            last_cut = cut_timestamp
                            last_ticks = ticks
                            continue

                        signals = self.signal_mgr.generate_signals([v[1] for v in last_ticks]) or []
                        for signal in signals:
                            self.check_fields(signal)
                            adj_signals = {'signal_{}'.format(k): v for k, v in signal.items()}
                            self.signals.append({**adj_signals, 'signal_timestamp': cut_timestamp})

                        last_cut = cut_timestamp
                        last_ticks = ticks
                        time.sleep(0)

                    self.cache = [v[1] for v in last_ticks]

            except Exception:
                logger.error(traceback.format_exc())

    @staticmethod
    def check_fields(_signal):
        assert 'bind_id' in _signal
        assert 'symbol' in _signal
        assert 'action' in _signal
        assert 'position' in _signal
        assert 'executing_mod' in _signal
