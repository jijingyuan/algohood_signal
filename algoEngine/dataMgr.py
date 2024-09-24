# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:43
@File: dataMgr.py
@Author: Jingyuan
"""

import asyncio
import time
from collections import deque

from ..algoUtil.dateUtil import timestamp_local_datetimestamp
from ..algoConfig.redisConfig import host, port
from ..algoUtil.loggerUtil import generate_logger
from ..algoUtil.redisClient import RedisClient

logger = generate_logger(level='INFO')


class DataMgr:

    def __init__(self):
        self.client = RedisClient(host, port)
        self.ticks_q = asyncio.Queue(maxsize=3)
        self.cursor_dict = {}
        self.current_batch = {}
        self.current_data = asyncio.PriorityQueue()
        self.lack_symbol = None
        self.cursor_loaded = False

    @staticmethod
    def get_ticker_symbol(_symbols):
        _symbols = [_symbols] if isinstance(_symbols, str) else _symbols
        symbol_tickers = set()
        for symbol in _symbols:
            symbol_tickers.add('{}|ticker'.format(symbol))

        return list(symbol_tickers)

    async def get_trades_given_start_end(self, _symbols, _start_timestamp, _end_timestamp):
        all_data = []
        for symbol_key in self.get_ticker_symbol(_symbols):
            trades = await self.client.get_data_with_start_end_all(
                0, symbol_key, _start_timestamp, _end_timestamp
            )
            if trades:
                all_data.extend(trades)

        return sorted(all_data, key=lambda x: x['rank_timestamp'])

    async def load_data(self, _symbols, _start_timestamp, _end_timestamp):
        logger.info('start receiving offline data')
        await self.client.initiate_clients(_local=True)
        tasks = [self.offline_data()]
        for symbol_key in self.get_ticker_symbol(_symbols):
            q = asyncio.Queue(maxsize=1)
            self.cursor_dict[symbol_key] = q
            tasks.append(self.load_batch_data(q, symbol_key, _start_timestamp, _end_timestamp))

        self.cursor_loaded = True

        if tasks:
            await asyncio.gather(*tasks)

        await self.client.close_redis_connection()

    async def get_data(self):
        return await self.ticks_q.get()

    async def load_batch_data(self, _q: asyncio.Queue, _symbol_key, _start_timestamp, _end_timestamp):
        cut_timestamp = None
        while True:
            try:
                if cut_timestamp is None:
                    cut_timestamp = _start_timestamp

                t1 = time.time()
                data = await self.client.get_data_with_limit(0, _symbol_key, cut_timestamp, None, 222, 'forward')
                t2 = time.time()

                end_timestamp = data[-1]['rank_timestamp']
                if end_timestamp > _end_timestamp:
                    data = deque([v for v in data if v['rank_timestamp'] <= _end_timestamp])

                if not data:
                    logger.info('data over for {}'.format(_symbol_key))
                    await _q.put(False)
                    break

                if len(data) < 1000:
                    start_ts = data[-1]['rank_timestamp']
                    left_data = await self.client.get_data_with_start_end_all(0, _symbol_key, start_ts, _end_timestamp)
                    await _q.put(data + deque(left_data))
                    logger.info('data over for {}'.format(_symbol_key))
                    await _q.put(False)
                    break

                if data:
                    end_date = timestamp_local_datetimestamp(data[-1]['rank_timestamp'])
                    logger.info('prepared {} data of {} to {}, cost: {}'.format(
                        len(data), _symbol_key, end_date, t2 - t1
                    ))
                    await _q.put(data)

                if end_timestamp > _end_timestamp:
                    logger.info('data over for {}'.format(_symbol_key))
                    await _q.put(False)
                    break
                else:
                    cut_timestamp = end_timestamp

            except Exception as e:
                logger.error(e)
                await asyncio.sleep(5)

    async def offline_data(self):
        while not self.cursor_loaded:
            await asyncio.sleep(2)

        cache_ticks = []
        while True:
            # task one batch
            symbol_keys = list(self.cursor_dict.keys())

            if self.lack_symbol is None:
                for symbol_key in symbol_keys:
                    data_deque = await self.cursor_dict[symbol_key].get()

                    if not data_deque:
                        self.cursor_dict.pop(symbol_key)
                        continue

                    self.current_batch[symbol_key] = data_deque

                    data = data_deque.popleft()
                    await self.current_data.put((data['rank_timestamp'], data['timestamp'], data['symbol'], data))

            else:
                if self.lack_symbol in self.current_batch.keys():
                    data = self.current_batch[self.lack_symbol].popleft()
                    await self.current_data.put((data['rank_timestamp'], data['timestamp'], data['symbol'], data))

                else:
                    data_deque = await self.cursor_dict[self.lack_symbol].get()

                    if not data_deque:
                        self.cursor_dict.pop(self.lack_symbol)

                    else:
                        self.current_batch[self.lack_symbol] = data_deque
                        data = data_deque.popleft()
                        await self.current_data.put((data['rank_timestamp'], data['timestamp'], data['symbol'], data))

            # 获取数据

            if not self.current_batch:
                if cache_ticks:
                    await self.ticks_q.put(cache_ticks)
                await self.ticks_q.put(None)
                return

            _, _, _, ret_data = await self.current_data.get()
            self.lack_symbol = '{}|{}'.format(ret_data['symbol'], ret_data['data_key'])

            data_deque = self.current_batch[self.lack_symbol]
            if not data_deque:
                self.current_batch.pop(self.lack_symbol)

            cache_ticks.append(ret_data)
            if len(cache_ticks) > 4999:
                await self.ticks_q.put(cache_ticks)
                cache_ticks = []
