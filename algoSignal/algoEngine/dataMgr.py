# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:43
@File: dataMgr.py
@Author: Jingyuan
"""

import time
import traceback
from collections import deque
from concurrent.futures import wait
from queue import Queue, PriorityQueue

from algoUtils.dateUtil import timestamp_local_datetimestamp
from .clusterMgr import RedisCluster
from ..algoConfig.loggerConfig import logger
from ..algoConfig.redisConfig import host, port, is_localhost
from ..algoConfig.threadPoolConfig import pool


class DataMgr:
    NODE_LIMIT = 200

    def __init__(self, _data_type):
        self.data_type = _data_type
        self.redis_cluster = RedisCluster(host, port, 'data')
        self.ticks_q = Queue(maxsize=3)
        self.cursor_dict = {}
        self.current_batch = {}
        self.current_data = PriorityQueue()
        self.lack_symbol = None
        self.cursor_loaded = False

    def init_data_mgr(self):
        self.redis_cluster.init_cluster(_is_localhost=is_localhost)

    def get_all_data_by_symbol(self, _symbol, _start_ts, _end_ts):
        symbol_key = '{}|{}'.format(_symbol, self.data_type)
        all_data = []
        cut_timestamp = _start_ts
        while True:
            data = self.get_data_by_symbol_key(symbol_key, cut_timestamp)
            if data[-1]['rank_timestamp'] > _end_ts:
                all_data.extend([v for v in data if v['rank_timestamp'] <= _end_ts])
                break
            else:
                all_data.extend(data)
                cut_timestamp = data[-1]['rank_timestamp'] + 0.000001

        return sorted(all_data, key=lambda x: x['rank_timestamp'])

    def load_data(self, _symbols, _start_timestamp, _end_timestamp):
        logger.info('start receiving offline data')
        tasks = [pool.submit(self.offline_data)]
        if isinstance(_symbols, str):
            symbol_keys = ['{}|{}'.format(_symbols, self.data_type)]
        else:
            symbol_keys = ['{}|{}'.format(v, self.data_type) for v in _symbols]

        for symbol_key in symbol_keys:
            q = Queue(maxsize=1)
            self.cursor_dict[symbol_key] = q
            task = pool.submit(self.load_batch_data, *(q, symbol_key, _start_timestamp, _end_timestamp))
            tasks.append(task)

        self.cursor_loaded = True

        wait(tasks)

    def get_data(self):
        return self.ticks_q.get()

    @staticmethod
    def format_node_data(_cluster_rsp):
        all_data = []
        last_ts = float('inf')
        for info in _cluster_rsp.values():
            data_list = {}
            cache_ts = 0
            for v in info:
                for key, tmp in v.items():
                    pair, exchange, data_type, field = key.split('|')
                    symbol = '{}|{}'.format(pair, exchange)
                    for value in tmp[1]:
                        rank_timestamp = round(value[0] / 1000000, 6)
                        cache_ts = rank_timestamp
                        ts_info = data_list.setdefault(value[0], {
                            'symbol': symbol, 'data_type': data_type, 'rank_timestamp': rank_timestamp
                        })
                        if field == 'direction':
                            ts_info['direction'] = 'buy' if value[1] > 0 else 'sell'
                        elif field == 'timestamp':
                            ts_info[field] = round(value[1] / 1000000, 6)
                        else:
                            ts_info[field] = value[1]
            all_data.extend(data_list.values())
            last_ts = min(last_ts, cache_ts)

        return sorted([v for v in all_data if v['rank_timestamp'] <= last_ts], key=lambda x: x['rank_timestamp'])

    def get_data_by_symbol_key(self, _symbol_key, _cut_timestamp, _end_timestamp='+', _limit=NODE_LIMIT):
        pair, exchange, data_type = _symbol_key.split('|')
        labels = {'pair': pair, 'exchange': exchange, 'data_type': data_type}
        rsp = self.redis_cluster.get_batch_by_labels(0, _cut_timestamp, _end_timestamp, labels, _limit)
        if rsp is None:
            logger.error('cluster node abnormal')
            return []

        return deque(self.format_node_data(rsp))

    def load_batch_data(self, _q: Queue, _symbol_key, _start_timestamp, _end_timestamp):
        cut_timestamp = None
        while True:
            try:
                if cut_timestamp is None:
                    cut_timestamp = _start_timestamp

                t1 = time.time()
                data = self.get_data_by_symbol_key(_symbol_key, cut_timestamp, _end_timestamp)
                t2 = time.time()

                end_timestamp = data[-1]['rank_timestamp'] if data else 0
                if end_timestamp > _end_timestamp:
                    data = deque([v for v in data if v['rank_timestamp'] <= _end_timestamp])

                if not data:
                    logger.info('data over for {}'.format(_symbol_key))
                    _q.put(False)
                    break

                if len(data) < 1000:
                    start_ts = data[-1]['rank_timestamp']
                    left_data = self.get_data_by_symbol_key(_symbol_key, start_ts, _end_timestamp)
                    _q.put(data + deque(left_data))
                    logger.info('data over for {}'.format(_symbol_key))
                    _q.put(False)
                    break

                if data:
                    end_date = timestamp_local_datetimestamp(data[-1]['rank_timestamp'])
                    logger.info('prepared {} data of {} to {}, cost: {}'.format(
                        len(data), _symbol_key, end_date, t2 - t1
                    ))
                    _q.put(data)

                if end_timestamp > _end_timestamp:
                    logger.info('data over for {}'.format(_symbol_key))
                    _q.put(False)
                    break
                else:
                    cut_timestamp = end_timestamp + 0.000001

            except Exception as e:
                logger.error(traceback.format_exc())
                time.sleep(5)

    def offline_data(self):
        while not self.cursor_loaded:
            time.sleep(2)

        cache_ticks = []
        while True:
            # task one batch
            symbol_keys = list(self.cursor_dict.keys())

            if self.lack_symbol is None:
                for symbol_key in symbol_keys:
                    data_deque = self.cursor_dict[symbol_key].get()

                    if not data_deque:
                        self.cursor_dict.pop(symbol_key)
                        continue

                    self.current_batch[symbol_key] = data_deque

                    data = data_deque.popleft()
                    self.current_data.put((data['rank_timestamp'], data['timestamp'], data['symbol'], data))

            else:
                if self.lack_symbol in self.current_batch.keys():
                    data = self.current_batch[self.lack_symbol].popleft()
                    self.current_data.put((data['rank_timestamp'], data['timestamp'], data['symbol'], data))

                else:
                    data_deque = self.cursor_dict[self.lack_symbol].get()

                    if not data_deque:
                        self.cursor_dict.pop(self.lack_symbol)

                    else:
                        self.current_batch[self.lack_symbol] = data_deque
                        data = data_deque.popleft()
                        self.current_data.put((data['rank_timestamp'], data['timestamp'], data['symbol'], data))

            # 获取数据

            if not self.current_batch:
                if cache_ticks:
                    self.ticks_q.put(cache_ticks)
                self.ticks_q.put(None)
                return

            _, _, _, ret_data = self.current_data.get()
            self.lack_symbol = '{}|{}'.format(ret_data['symbol'], ret_data['data_type'])

            data_deque = self.current_batch[self.lack_symbol]
            if not data_deque:
                self.current_batch.pop(self.lack_symbol)

            cache_ticks.append(ret_data)
            if len(cache_ticks) > 10000:
                self.ticks_q.put(cache_ticks)
                cache_ticks = []
