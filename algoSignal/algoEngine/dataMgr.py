# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:43
@File: dataMgr.py
@Author: Jingyuan
"""
import numpy as np
import asyncio
import time
import traceback
from asyncio import Queue, PriorityQueue
from collections import deque

from algoUtils.asyncRedisUtil import AsyncRedisClient
from algoUtils.dateUtil import timestamp_local_datetime
from .clusterMgr import RedisCluster
from ..algoConfig.loggerConfig import logger
from ..algoConfig.redisConfig import host, port, is_localhost


class DataMgr:
    NODE_LIMIT = 20000

    def __init__(self):
        self.config_redis = AsyncRedisClient(host, port)
        self.redis_cluster = RedisCluster(self.config_redis)
        self.ticks_q = Queue(maxsize=3)
        self.cursor_dict = {}
        self.current_batch = {}
        self.current_data = PriorityQueue()
        self.lack_symbol = None
        self.cursor_loaded = False
        self.abstract_dict = {}
        self.cache = {}
        self.data_type = None

    async def init_data_mgr(self):
        await self.redis_cluster.init_cluster(_is_localhost=is_localhost)
        logger.info('data mgr initiated')

    def clear_cache(self):
        self.ticks_q = Queue(maxsize=3)
        self.cursor_dict = {}
        self.current_batch = {}
        self.current_data = PriorityQueue()
        self.lack_symbol = None
        self.abstract_dict = {}
        self.cache = {}
        self.cursor_loaded = False

    def set_data_type(self, _data_type):
        self.data_type = _data_type

    async def get_all_data_by_symbol(self, _symbol, _start_ts, _end_ts):
        cache = self.cache.setdefault(_symbol, {})
        data = cache.get(_end_ts)
        if data is not None:
            return data

        symbol_key = '{}|{}'.format(_symbol, self.data_type)
        data = await self.get_data_by_symbol_key(symbol_key, _start_ts, _end_ts)
        tmp = self.format_node_data(data, _keep_all=True)
        cache[_end_ts] = np.array(tmp)
        return cache[_end_ts]

    def clear_cache_data(self, _symbol, _start_ts):
        cache = self.cache.setdefault(_symbol, {})
        delete_ts = [v for v in cache.keys() if _start_ts > v]
        for ts in delete_ts:
            cache.pop(ts)

        if len(cache) > 5:
            sorted_ts = sorted(list(cache.keys()))
            delete_ts = sorted_ts[5:]
            for ts in delete_ts:
                cache.pop(ts)

    async def get_abstract(self, _symbol):
        await self.update_abstract(_symbol)
        return self.abstract_dict.get(_symbol)

    async def update_abstract(self, _symbol):
        if self.abstract_dict.get(_symbol) is not None:
            return

        pair, exchange = _symbol.split('|')
        labels = {'data_type': self.data_type, 'pair': pair, 'exchange': exchange}
        rsp = await self.config_redis.get_ts_batch_by_labels(1, '-', '+', labels)
        abstract = {}
        for v in rsp:
            for k, info in v.items():
                pair, exchange, _, key = k.split('|')
                for value in info[1]:
                    tmp = abstract.setdefault(value[0], [round(value[0] / 1000000, 6)])
                    if key == 'ts':
                        tmp.append(round(value[1] / 1000000, 6))
                    else:
                        tmp.append(value[1])

        self.abstract_dict[_symbol] = np.array([x for x in abstract.values()])

    async def load_data(self, _symbols, _start_timestamp, _end_timestamp):
        logger.info('start receiving offline data')
        tasks = [self.offline_data()]
        symbols = [_symbols] if isinstance(_symbols, str) else _symbols
        for symbol in symbols:
            q = Queue(maxsize=1)
            self.cursor_dict[symbol] = q
            tasks.append(self.load_batch_data(q, symbol, _start_timestamp, _end_timestamp))

        self.cursor_loaded = True

        await asyncio.gather(*tasks)

    async def get_data(self):
        return await self.ticks_q.get()

    @staticmethod
    def format_node_data(_cluster_rsp, _keep_all=False):
        all_data = []
        last_ts = float('inf')
        for tmp in _cluster_rsp.values():
            for info in zip(*[list(v.values())[0][1] for v in tmp]):
                rank_ts = round(info[0][0] / 1000000, 6)
                delay = round((info[0][0] - info[3][1]) / 1000000, 6)
                all_data.append([rank_ts, delay, info[1][1], info[0][1], int(info[2][1])])

            if all_data:
                last_ts = min(last_ts, all_data[-1][0])

        if _keep_all:
            data = sorted(all_data, key=lambda x: x[0])
        else:
            data = sorted([v for v in all_data if v[0] <= last_ts], key=lambda x: x[0])

        if data:
            start_dt = timestamp_local_datetime(data[0][0])
            end_dt = timestamp_local_datetime(data[-1][0])
            logger.info('sync {} to {}: {}'.format(start_dt, end_dt, len(data)))
            
        return data


    async def get_data_by_symbol_key(self, _symbol_key, _cut_timestamp, _end_timestamp='+', _limit=None):
        pair, exchange, data_type = _symbol_key.split('|')
        labels = {'pair': pair, 'exchange': exchange, 'data_type': data_type}
        rsp = await self.redis_cluster.get_batch_by_labels(0, _cut_timestamp, _end_timestamp, labels, _limit)
        if rsp is None:
            logger.error('cluster node abnormal')

        return rsp or []

    async def load_batch_data(self, _q: Queue, _symbol, _start_timestamp, _end_timestamp):
        cut_timestamp = None
        symbol_key = '{}|{}'.format(_symbol, self.data_type)
        while True:
            try:
                if cut_timestamp is None:
                    cut_timestamp = _start_timestamp

                node_limit = int(self.NODE_LIMIT / len(self.redis_cluster.cluster.keys()))
                raw = await self.get_data_by_symbol_key(symbol_key, cut_timestamp, _end_timestamp, node_limit)
                data = deque(self.format_node_data(raw))

                end_timestamp = data[-1][0] if data else 0
                if end_timestamp > _end_timestamp:
                    data = deque([v for v in data if v[0] <= _end_timestamp])

                if not data:
                    logger.info('data over for {}'.format(symbol_key))
                    await _q.put(False)
                    break

                if len(data) < 1000:
                    start_ts = data[-1][0] + 0.000001
                    raw = await self.get_data_by_symbol_key(symbol_key, start_ts, _end_timestamp, self.NODE_LIMIT)
                    await _q.put(data + deque(self.format_node_data(raw, _keep_all=True)))
                    logger.info('data over for {}'.format(symbol_key))
                    await _q.put(False)
                    break

                if data:
                    await _q.put(data)

                if end_timestamp > _end_timestamp:
                    logger.info('data over for {}'.format(symbol_key))
                    await _q.put(False)
                    break
                else:
                    cut_timestamp = end_timestamp + 0.000001

            except Exception as e:
                logger.error(traceback.format_exc())
                time.sleep(5)

    async def offline_data(self):
        while not self.cursor_loaded:
            time.sleep(2)

        index = 0
        cache_ticks = []
        while True:
            # task one batch
            symbols = list(self.cursor_dict.keys())

            if self.lack_symbol is None:
                for symbol in symbols:
                    data_deque = await self.cursor_dict[symbol].get()

                    if not data_deque:
                        self.cursor_dict.pop(symbol)
                        continue

                    self.current_batch[symbol] = data_deque

                    data = data_deque.popleft()
                    await self.current_data.put((data[0], data[1], symbol, data))

            else:
                if self.lack_symbol in self.current_batch.keys():
                    data = self.current_batch[self.lack_symbol].popleft()
                    await self.current_data.put((data[0], data[1], self.lack_symbol, data))

                else:
                    data_deque = await self.cursor_dict[self.lack_symbol].get()

                    if not data_deque:
                        self.cursor_dict.pop(self.lack_symbol)

                    else:
                        self.current_batch[self.lack_symbol] = data_deque
                        data = data_deque.popleft()
                        await self.current_data.put((data[0], data[1], self.lack_symbol, data))

            # 获取数据

            if not self.current_batch:
                if cache_ticks:
                    await self.ticks_q.put(cache_ticks)
                await self.ticks_q.put(None)
                return

            rank_ts, _, self.lack_symbol, ret_data = await self.current_data.get()

            data_deque = self.current_batch[self.lack_symbol]
            if not data_deque:
                self.current_batch.pop(self.lack_symbol)

            cache_ticks.append((rank_ts, self.lack_symbol, ret_data))
            index += 1
            if index >= 10000:
                await self.ticks_q.put(cache_ticks)
                cache_ticks = []
                index = 0
