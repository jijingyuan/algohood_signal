# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:44
@File: broker.py
@Author: Jingyuan
"""
from algoSignal.algoEngine.signalMgr import SignalMgr
from algoSignal.algoEngine.targetMgr import TargetMgr
from algoSignal.algoEngine.dataMgr import DataMgr


class Broker:

    @classmethod
    def start_signal_task(
            cls, _signal_method_name, _signal_method_param, _data_type, _symbols, _lag, _start_timestamp,
            _end_timestamp, _forward_window=None, _target_method_name=None, _target_method_param=None
    ):
        data_mgr = DataMgr(_data_type)
        data_mgr.init_data_mgr()
        signal_mgr = SignalMgr(_signal_method_name, _signal_method_param, data_mgr)

        save_signals = True if _forward_window is None else False
        signals = signal_mgr.start_task(_lag, _symbols, _start_timestamp, _end_timestamp, save_signals)

        if not signals:
            return

        if _forward_window is None:
            return

        assert _forward_window is not None
        assert _target_method_name is not None

        target_mgr = TargetMgr(_target_method_name, _target_method_param, data_mgr)
        target_mgr.handle_signals(signals, _symbols, _forward_window)
